//! Closed Resource lifecycle orchestration for `insight apply`.
//!
//! The manifest contains the same typed authoring documents and deployment bindings accepted by
//! `/v1`. It omits only the Resource/Version IDs that the authority creates during this exact
//! lifecycle; those self references are filled from the publish response before Deployment
//! creation. No Plan, tool, URL, shell, Secret value, or execution semantics are invented here.

use crate::public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, AdministrativeGate, AgentDeploymentClosure, ArtifactRef,
    CapabilityBackendBinding, CapabilityDeploymentClosure, ClosedJsonValue, ContextBackendBinding,
    ContextDeploymentClosure, DeploymentClosure, EntityLifecycle, ExactDeploymentRef,
    ExactPolicyBinding, ExactSecretBindingRef, ExactVersionRef, FrozenSlotBinding, JsonLimits,
    McpDeploymentClosure, McpTransportBinding, ModelDeploymentClosure, OperationViewV1,
    PlanNodeKind, PolicyDeploymentClosure, PublicJobKind, PublicJobState, PublicJobTarget,
    RegistryResourceKind, ResourceDocument, ResourceDraftPayload, ResourceId, ResourceKind,
    SandboxProfileDeploymentClosure, Sha256Digest, SkillDeploymentClosure, UtcTimestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, time::Duration};

const APPLY_MANIFEST_KIND: &str = "insight.platform.apply/v1";
const MAX_APPLY_MANIFEST_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub enum ApplyError {
    InvalidManifest(String),
    Public(PublicClientError),
    InvalidResponse(String),
    OperationTerminal {
        operation_id: String,
        state: String,
        detail: String,
    },
}

impl fmt::Display for ApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest(detail) => {
                write!(formatter, "apply manifest is invalid: {detail}")
            }
            Self::Public(error) => write!(formatter, "{error}"),
            Self::InvalidResponse(detail) => {
                write!(
                    formatter,
                    "apply lifecycle received invalid authority state: {detail}"
                )
            }
            Self::OperationTerminal {
                operation_id,
                state,
                detail,
            } => write!(
                formatter,
                "validation operation {operation_id} reached terminal state {state}: {detail}"
            ),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Public(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for ApplyError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ApplyResourceNoun {
    Agents,
    Skills,
    Capabilities,
    Contexts,
    Models,
    McpServers,
    Policies,
    Sandboxes,
}

impl ApplyResourceNoun {
    const fn as_path(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::Skills => "skills",
            Self::Capabilities => "capabilities",
            Self::Contexts => "contexts",
            Self::Models => "models",
            Self::McpServers => "mcp-servers",
            Self::Policies => "policies",
            Self::Sandboxes => "sandboxes",
        }
    }

    const fn resource_kind(self) -> RegistryResourceKind {
        match self {
            Self::Agents => RegistryResourceKind::Agent,
            Self::Skills => RegistryResourceKind::Skill,
            Self::Capabilities => RegistryResourceKind::CapabilityInterface,
            Self::Contexts => RegistryResourceKind::ContextSourceInterface,
            Self::Models => RegistryResourceKind::ModelProfile,
            Self::McpServers => RegistryResourceKind::McpServer,
            Self::Policies => RegistryResourceKind::Policy,
            Self::Sandboxes => RegistryResourceKind::SandboxProfile,
        }
    }

    const fn primary_version_kind(self) -> ResourceKind {
        match self {
            Self::Agents => ResourceKind::AgentPlanRevision,
            Self::Skills => ResourceKind::SkillRevision,
            Self::Capabilities => ResourceKind::CapabilityInterfaceRevision,
            Self::Contexts => ResourceKind::ContextSourceInterfaceRevision,
            Self::Models => ResourceKind::ModelProfileRevision,
            Self::McpServers => ResourceKind::McpServerRevision,
            Self::Policies => ResourceKind::PolicyRevision,
            Self::Sandboxes => ResourceKind::SandboxProfileRevision,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyManifestV1 {
    schema_version: u16,
    kind: String,
    resource_noun: ApplyResourceNoun,
    create: ApplyCreateResourceRequestV1,
    publish: ApplyPublishResourceRequestV1,
    deployment: ApplyDeploymentRequestV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyCreateResourceRequestV1 {
    display_name: String,
    document: ResourceDocument,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ApplyPublishResourceRequestV1 {
    Single {
        revision_no: u64,
        content_digest: Sha256Digest,
        artifact_id: Option<ResourceId>,
    },
    Agent {
        revision_no: u64,
        interface_content_digest: Sha256Digest,
        plan_content_digest: Sha256Digest,
        artifact_id: Option<ResourceId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyDeploymentRequestV1 {
    environment: String,
    closure: ApplyDeploymentClosure,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "resource_kind",
    content = "bindings",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ApplyDeploymentClosure {
    Agent(ApplyAgentDeploymentBindings),
    Skill(ApplySkillDeploymentBindings),
    CapabilityInterface(ApplyCapabilityDeploymentBindings),
    ContextSourceInterface(ApplyContextDeploymentBindings),
    McpServer(ApplyMcpDeploymentBindings),
    ModelProfile(ApplyModelDeploymentBindings),
    Policy(ApplyPolicyDeploymentBindings),
    SandboxProfile(ApplySandboxDeploymentBindings),
}

impl ApplyDeploymentClosure {
    const fn resource_kind(&self) -> RegistryResourceKind {
        match self {
            Self::Agent(_) => RegistryResourceKind::Agent,
            Self::Skill(_) => RegistryResourceKind::Skill,
            Self::CapabilityInterface(_) => RegistryResourceKind::CapabilityInterface,
            Self::ContextSourceInterface(_) => RegistryResourceKind::ContextSourceInterface,
            Self::McpServer(_) => RegistryResourceKind::McpServer,
            Self::ModelProfile(_) => RegistryResourceKind::ModelProfile,
            Self::Policy(_) => RegistryResourceKind::Policy,
            Self::SandboxProfile(_) => RegistryResourceKind::SandboxProfile,
        }
    }

    fn resolve(
        self,
        published: &[PublishedResourceVersionSummaryV1],
    ) -> Result<DeploymentClosure, ApplyError> {
        let exact = |kind| published_exact_version(published, kind);
        let closure = match self {
            Self::Agent(bindings) => DeploymentClosure::Agent(AgentDeploymentClosure {
                interface: exact(ResourceKind::AgentInterfaceRevision)?,
                plan: exact(ResourceKind::AgentPlanRevision)?,
                entry_node_id: bindings.entry_node_id,
                entry_node_kind: bindings.entry_node_kind,
                slots: bindings.slots,
                policies: bindings.policies,
                execution_profile: bindings.execution_profile,
            }),
            Self::Skill(bindings) => DeploymentClosure::Skill(SkillDeploymentClosure {
                skill_revision: exact(ResourceKind::SkillRevision)?,
                requirements: bindings.requirements,
                selection_policy: bindings.selection_policy,
                qualification_evidence: bindings.qualification_evidence,
            }),
            Self::CapabilityInterface(bindings) => {
                DeploymentClosure::CapabilityInterface(CapabilityDeploymentClosure {
                    implementation: bindings.implementation,
                    interface: exact(ResourceKind::CapabilityInterfaceRevision)?,
                    backend: bindings.backend,
                    secret_bindings: bindings.secret_bindings,
                    policies: bindings.policies,
                    conformance_evidence: bindings.conformance_evidence,
                })
            }
            Self::ContextSourceInterface(bindings) => {
                DeploymentClosure::ContextSourceInterface(ContextDeploymentClosure {
                    implementation: bindings.implementation,
                    interface: exact(ResourceKind::ContextSourceInterfaceRevision)?,
                    required_worker_manifest_digest: bindings.required_worker_manifest_digest,
                    backend: bindings.backend,
                    secret_bindings: bindings.secret_bindings,
                    network_policy: bindings.network_policy,
                    tls_policy: bindings.tls_policy,
                    trust_policy: bindings.trust_policy,
                    parser_policy: bindings.parser_policy,
                    chunker_policy: bindings.chunker_policy,
                    embedding_model_deployment: bindings.embedding_model_deployment,
                    ranking_policy: bindings.ranking_policy,
                    data_policy: bindings.data_policy,
                    conformance_evidence: bindings.conformance_evidence,
                })
            }
            Self::McpServer(bindings) => DeploymentClosure::McpServer(McpDeploymentClosure {
                server_revision: exact(ResourceKind::McpServerRevision)?,
                server_identity_digest: bindings.server_identity_digest,
                transport: bindings.transport,
                protocol_policy: bindings.protocol_policy,
                trust_policy: bindings.trust_policy,
                auth_policy: bindings.auth_policy,
                secret_bindings: bindings.secret_bindings,
                conformance_evidence: bindings.conformance_evidence,
            }),
            Self::ModelProfile(bindings) => {
                DeploymentClosure::ModelProfile(ModelDeploymentClosure {
                    profile_revision: exact(ResourceKind::ModelProfileRevision)?,
                    provider_deployment: bindings.provider_deployment,
                    data_policy: bindings.data_policy,
                    safety_policy: bindings.safety_policy,
                    budget_policy: bindings.budget_policy,
                    public_projection_policy: bindings.public_projection_policy,
                    generation_defaults: bindings.generation_defaults,
                })
            }
            Self::Policy(bindings) => DeploymentClosure::Policy(PolicyDeploymentClosure {
                policy_revision: exact(ResourceKind::PolicyRevision)?,
                applicability_digest: bindings.applicability_digest,
                qualification_evidence: bindings.qualification_evidence,
            }),
            Self::SandboxProfile(bindings) => {
                DeploymentClosure::SandboxProfile(SandboxProfileDeploymentClosure {
                    profile_revision: exact(ResourceKind::SandboxProfileRevision)?,
                    runtime_revision: bindings.runtime_revision,
                    policy_bindings: bindings.policy_bindings,
                    qualification_evidence: bindings.qualification_evidence,
                })
            }
        };
        closure.validate().map_err(|error| {
            ApplyError::InvalidManifest(format!("resolved Deployment closure: {error}"))
        })?;
        Ok(closure)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyAgentDeploymentBindings {
    entry_node_id: String,
    entry_node_kind: PlanNodeKind,
    slots: Vec<FrozenSlotBinding>,
    policies: Vec<ExactPolicyBinding>,
    execution_profile: ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplySkillDeploymentBindings {
    requirements: Vec<FrozenSlotBinding>,
    selection_policy: ExactPolicyBinding,
    qualification_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyCapabilityDeploymentBindings {
    implementation: ExactVersionRef,
    backend: CapabilityBackendBinding,
    secret_bindings: Vec<ExactSecretBindingRef>,
    policies: Vec<ExactVersionRef>,
    conformance_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyContextDeploymentBindings {
    implementation: ExactVersionRef,
    required_worker_manifest_digest: Sha256Digest,
    backend: ContextBackendBinding,
    secret_bindings: Vec<ExactSecretBindingRef>,
    network_policy: Option<ExactVersionRef>,
    tls_policy: Option<ExactVersionRef>,
    trust_policy: Option<ExactVersionRef>,
    parser_policy: ExactVersionRef,
    chunker_policy: ExactVersionRef,
    embedding_model_deployment: Option<ExactDeploymentRef>,
    ranking_policy: ExactVersionRef,
    data_policy: ExactVersionRef,
    conformance_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyMcpDeploymentBindings {
    server_identity_digest: Sha256Digest,
    transport: McpTransportBinding,
    protocol_policy: ExactVersionRef,
    trust_policy: ExactVersionRef,
    auth_policy: Option<ExactVersionRef>,
    secret_bindings: Vec<ExactSecretBindingRef>,
    conformance_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyModelDeploymentBindings {
    provider_deployment: ExactDeploymentRef,
    data_policy: ExactVersionRef,
    safety_policy: ExactVersionRef,
    budget_policy: ExactVersionRef,
    public_projection_policy: ExactVersionRef,
    generation_defaults: ClosedJsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyPolicyDeploymentBindings {
    applicability_digest: Sha256Digest,
    qualification_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplySandboxDeploymentBindings {
    runtime_revision: ExactVersionRef,
    policy_bindings: Vec<ExactPolicyBinding>,
    qualification_evidence: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResourceViewV1 {
    schema_version: u32,
    resource_id: ResourceId,
    resource_kind: RegistryResourceKind,
    lifecycle_state: EntityLifecycle,
    gate_state: AdministrativeGate,
    draft_generation: u64,
    version: u64,
    draft: ResourceDraftPayload,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct CreateDeploymentRequestV1<'a> {
    resource_version_id: &'a ResourceId,
    environment: &'a str,
    closure: &'a DeploymentClosure,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApplyPublishedVersionReport {
    pub resource_version_id: String,
    pub resource_kind: String,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApplyReportV1 {
    pub schema_version: u16,
    pub kind: &'static str,
    pub manifest_digest: String,
    pub resource_id: String,
    pub validation_operation_id: String,
    pub published_versions: Vec<ApplyPublishedVersionReport>,
    pub deployment_id: String,
    pub active_deployment_id: String,
    pub final_resource_etag: String,
    pub step_trace_ids: BTreeMap<&'static str, String>,
}

pub fn apply_manifest(
    client: &PublicHttpClient,
    expected_tenant_id: &ResourceId,
    manifest_bytes: &[u8],
    operation_timeout: Duration,
) -> Result<ApplyReportV1, ApplyError> {
    let (manifest, manifest_digest) = parse_manifest(manifest_bytes)?;
    let noun = manifest.resource_noun;
    let kind = noun.resource_kind();
    let base_path = format!("/v1/{}", noun.as_path());
    let mut traces = BTreeMap::new();

    let create: PublicJsonResponse<ResourceViewV1> = client.post_json(
        &base_path,
        &manifest.create,
        StatusCode::CREATED,
        &receipt_key(&manifest_digest, "create"),
        None,
    )?;
    validate_resource_response(&create, kind, None)?;
    require_location(&create, &format!("{base_path}/{}", create.body.resource_id))?;
    traces.insert("create", create.trace_id.to_string());
    let resource_id = create.body.resource_id.clone();

    let validate_path = format!("{base_path}/{resource_id}/draft:validate");
    let validation: PublicJsonResponse<OperationViewV1> = client.post_empty(
        &validate_path,
        StatusCode::ACCEPTED,
        &receipt_key(&manifest_digest, "validate"),
        &create.etag,
    )?;
    validation.body.validate().map_err(|_| {
        ApplyError::InvalidResponse("validation Operation violates its contract".to_owned())
    })?;
    if validation.body.tenant_id != *expected_tenant_id
        || validation.body.etag != validation.etag
        || validation.body.kind != PublicJobKind::ResourceValidation
        || !matches!(
            &validation.body.target,
            PublicJobTarget::ResourceVersion {
                resource_id: target_resource_id,
                resource_version,
            } if target_resource_id == &resource_id && *resource_version == create.body.version
        )
    {
        return Err(ApplyError::InvalidResponse(
            "validation Operation identity differs from the exact Draft request".to_owned(),
        ));
    }
    require_location(
        &validation,
        &format!("/v1/operations/{}", validation.body.operation_id),
    )?;
    traces.insert("validate", validation.trace_id.to_string());
    let validation_operation_id = validation.body.operation_id.clone();
    let terminal = client.wait_operation(
        &validation_operation_id,
        expected_tenant_id,
        operation_timeout,
    )?;
    require_succeeded_operation(&terminal)?;

    let validated: PublicJsonResponse<ResourceViewV1> =
        client.get_json(&format!("{base_path}/{resource_id}"), StatusCode::OK)?;
    validate_resource_response(&validated, kind, Some(&resource_id))?;
    if validated.body.draft.validation.is_none() {
        return Err(ApplyError::InvalidResponse(
            "validation Operation succeeded without an exact Draft validation summary".to_owned(),
        ));
    }
    traces.insert("read_validated", validated.trace_id.to_string());

    let publish: PublicJsonResponse<PublishResourceDraftResponseV1> = client.post_json(
        &format!("{base_path}/{resource_id}/draft:publish"),
        &manifest.publish,
        StatusCode::OK,
        &receipt_key(&manifest_digest, "publish"),
        Some(&validated.etag),
    )?;
    validate_publish_response(&publish, noun, &resource_id, &manifest.publish)?;
    traces.insert("publish", publish.trace_id.to_string());

    let primary_version = publish
        .body
        .published_versions
        .iter()
        .find(|version| version.resource_version_id.kind() == noun.primary_version_kind())
        .ok_or_else(|| {
            ApplyError::InvalidResponse("publish response omitted the primary Version".to_owned())
        })?;
    let deployment_closure = manifest
        .deployment
        .closure
        .resolve(&publish.body.published_versions)?;
    let deployment_request = CreateDeploymentRequestV1 {
        resource_version_id: &primary_version.resource_version_id,
        environment: &manifest.deployment.environment,
        closure: &deployment_closure,
    };
    let deployment: PublicJsonResponse<DeploymentViewV1> = client.post_json(
        &format!("{base_path}/{resource_id}/deployments"),
        &deployment_request,
        StatusCode::CREATED,
        &receipt_key(&manifest_digest, "deploy"),
        Some(&publish.etag),
    )?;
    validate_deployment_response(
        &deployment,
        kind,
        &resource_id,
        &primary_version.resource_version_id,
        &manifest.deployment.environment,
        &deployment_closure,
    )?;
    require_location(
        &deployment,
        &format!(
            "{base_path}/{resource_id}/deployments/{}",
            deployment.body.deployment_id
        ),
    )?;
    traces.insert("deploy", deployment.trace_id.to_string());
    let deployment_id = deployment.body.deployment_id.clone();

    let activated: PublicJsonResponse<ResourceViewV1> = client.post_empty(
        &format!("{base_path}/{resource_id}/deployments/{deployment_id}:activate"),
        StatusCode::OK,
        &receipt_key(&manifest_digest, "activate"),
        &publish.etag,
    )?;
    validate_resource_response(&activated, kind, Some(&resource_id))?;
    if activated.body.gate_state != AdministrativeGate::Enabled {
        return Err(ApplyError::InvalidResponse(
            "activation did not leave the Resource gate enabled".to_owned(),
        ));
    }
    traces.insert("activate", activated.trace_id.to_string());

    let published_versions = publish
        .body
        .published_versions
        .iter()
        .map(|version| ApplyPublishedVersionReport {
            resource_version_id: version.resource_version_id.to_string(),
            resource_kind: version.resource_version_id.kind().to_string(),
            content_digest: version.content_digest.to_string(),
        })
        .collect();
    Ok(ApplyReportV1 {
        schema_version: 1,
        kind: "insight.platform.apply-report/v1",
        manifest_digest,
        resource_id: resource_id.to_string(),
        validation_operation_id: validation_operation_id.to_string(),
        published_versions,
        deployment_id: deployment_id.to_string(),
        active_deployment_id: deployment_id.to_string(),
        final_resource_etag: activated.etag,
        step_trace_ids: traces,
    })
}

fn parse_manifest(bytes: &[u8]) -> Result<(ApplyManifestV1, String), ApplyError> {
    if bytes.is_empty() || bytes.len() > MAX_APPLY_MANIFEST_BYTES {
        return Err(ApplyError::InvalidManifest(
            "size is outside 1..=1048576 bytes".to_owned(),
        ));
    }
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_APPLY_MANIFEST_BYTES,
            max_depth: 64,
            max_properties_per_object: 256,
            max_items_per_array: 4_096,
            max_string_bytes: 262_144,
        },
    )
    .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
    let digest =
        canonical_digest(&value).map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
    let manifest = serde_json::from_value::<ApplyManifestV1>(value)
        .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
    validate_manifest(&manifest)?;
    Ok((manifest, digest))
}

fn validate_manifest(manifest: &ApplyManifestV1) -> Result<(), ApplyError> {
    let kind = manifest.resource_noun.resource_kind();
    if manifest.schema_version != 1
        || manifest.kind != APPLY_MANIFEST_KIND
        || manifest.create.document.kind() != kind
        || manifest.deployment.closure.resource_kind() != kind
        || manifest.deployment.environment.is_empty()
        || manifest.deployment.environment.len() > 128
        || !manifest
            .deployment
            .environment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ApplyError::InvalidManifest(
            "kind, noun, document, deployment closure, or environment mismatch".to_owned(),
        ));
    }
    ResourceDraftPayload {
        display_name: manifest.create.display_name.clone(),
        document: manifest.create.document.clone(),
        validation: None,
    }
    .validate()
    .map_err(|error| ApplyError::InvalidManifest(format!("create request: {error}")))?;
    let (revision_no, artifact_id, correct_shape) = match &manifest.publish {
        ApplyPublishResourceRequestV1::Single {
            revision_no,
            artifact_id,
            ..
        } => (
            *revision_no,
            artifact_id,
            kind != RegistryResourceKind::Agent,
        ),
        ApplyPublishResourceRequestV1::Agent {
            revision_no,
            artifact_id,
            ..
        } => (
            *revision_no,
            artifact_id,
            kind == RegistryResourceKind::Agent,
        ),
    };
    if !correct_shape
        || revision_no == 0
        || i64::try_from(revision_no).is_err()
        || artifact_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Artifact)
    {
        return Err(ApplyError::InvalidManifest(
            "publish request does not match the Resource kind".to_owned(),
        ));
    }
    Ok(())
}

fn receipt_key(manifest_digest: &str, step: &str) -> String {
    format!(
        "insight-apply-v1-{}-{step}",
        manifest_digest
            .strip_prefix("sha256:")
            .unwrap_or(manifest_digest)
    )
}

fn require_location<T>(response: &PublicJsonResponse<T>, expected: &str) -> Result<(), ApplyError> {
    if response.location.as_deref() != Some(expected) {
        return Err(ApplyError::InvalidResponse(format!(
            "Location does not identify {expected}"
        )));
    }
    Ok(())
}

fn validate_resource_response(
    response: &PublicJsonResponse<ResourceViewV1>,
    expected_kind: RegistryResourceKind,
    expected_id: Option<&ResourceId>,
) -> Result<(), ApplyError> {
    let body = &response.body;
    if body.schema_version != 1
        || body.resource_id.kind() != expected_kind.id_kind()
        || body.resource_kind != expected_kind
        || expected_id.is_some_and(|id| id != &body.resource_id)
        || body.lifecycle_state != EntityLifecycle::Active
        || body.draft_generation == 0
        || body.version == 0
        || body.draft.document.kind() != expected_kind
        || body.draft.validate().is_err()
        || body.etag != response.etag
    {
        return Err(ApplyError::InvalidResponse(
            "Resource projection violates the expected lifecycle identity".to_owned(),
        ));
    }
    Ok(())
}

fn validate_publish_response(
    response: &PublicJsonResponse<PublishResourceDraftResponseV1>,
    noun: ApplyResourceNoun,
    resource_id: &ResourceId,
    request: &ApplyPublishResourceRequestV1,
) -> Result<(), ApplyError> {
    let body = &response.body;
    let expected_count = if noun == ApplyResourceNoun::Agents {
        2
    } else {
        1
    };
    let request_matches = match request {
        ApplyPublishResourceRequestV1::Single {
            revision_no,
            content_digest,
            artifact_id,
        } => body.published_versions.iter().all(|version| {
            version.revision_no == *revision_no
                && &version.content_digest == content_digest
                && &version.artifact_id == artifact_id
        }),
        ApplyPublishResourceRequestV1::Agent {
            revision_no,
            interface_content_digest,
            plan_content_digest,
            artifact_id,
        } => body.published_versions.iter().all(|version| {
            let expected_digest = match version.resource_version_id.kind() {
                ResourceKind::AgentInterfaceRevision => interface_content_digest,
                ResourceKind::AgentPlanRevision => plan_content_digest,
                _ => return false,
            };
            version.revision_no == *revision_no
                && &version.content_digest == expected_digest
                && &version.artifact_id == artifact_id
        }),
    };
    let mut kinds = std::collections::BTreeSet::new();
    if body.schema_version != 1
        || &body.resource_id != resource_id
        || body.resource_kind != noun.resource_kind()
        || body.draft_generation == 0
        || body.version == 0
        || body.etag != response.etag
        || body.published_versions.len() != expected_count
        || body.published_versions.iter().any(|version| {
            version.revision_no == 0
                || !body
                    .resource_kind
                    .allows_version_kind(version.resource_version_id.kind())
                || !kinds.insert(version.resource_version_id.kind())
                || version
                    .artifact_id
                    .as_ref()
                    .is_some_and(|id| id.kind() != ResourceKind::Artifact)
                || version.etag.is_empty()
        })
        || !kinds.contains(&noun.primary_version_kind())
        || (noun == ApplyResourceNoun::Agents
            && !kinds.contains(&ResourceKind::AgentInterfaceRevision))
        || !request_matches
    {
        return Err(ApplyError::InvalidResponse(
            "publish response violates the public Version matrix".to_owned(),
        ));
    }
    Ok(())
}

fn published_exact_version(
    versions: &[PublishedResourceVersionSummaryV1],
    kind: ResourceKind,
) -> Result<ExactVersionRef, ApplyError> {
    let version = versions
        .iter()
        .find(|version| version.resource_version_id.kind() == kind)
        .ok_or_else(|| ApplyError::InvalidResponse(format!("publish response omitted {kind}")))?;
    ExactVersionRef::new(
        version.resource_version_id.clone(),
        version.content_digest.clone(),
    )
    .map_err(|error| ApplyError::InvalidResponse(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn validate_deployment_response(
    response: &PublicJsonResponse<DeploymentViewV1>,
    kind: RegistryResourceKind,
    resource_id: &ResourceId,
    resource_version_id: &ResourceId,
    environment: &str,
    closure: &DeploymentClosure,
) -> Result<(), ApplyError> {
    let body = &response.body;
    let closure_digest = canonical_digest(
        &serde_json::to_value(closure)
            .map_err(|error| ApplyError::InvalidResponse(error.to_string()))?,
    )
    .map_err(|error| ApplyError::InvalidResponse(error.to_string()))?;
    if body.schema_version != 1
        || body.deployment_id.kind()
            != kind.deployment_kind().ok_or_else(|| {
                ApplyError::InvalidManifest("Resource kind cannot be deployed".to_owned())
            })?
        || &body.resource_id != resource_id
        || body.resource_kind != kind
        || &body.resource_version_id != resource_version_id
        || body.environment != environment
        || &body.closure != closure
        || body.closure_digest.to_string() != closure_digest
        || body.etag != response.etag
        || body.created_at.as_str().is_empty()
    {
        return Err(ApplyError::InvalidResponse(
            "Deployment projection differs from the exact request closure".to_owned(),
        ));
    }
    Ok(())
}

fn require_succeeded_operation(operation: &OperationViewV1) -> Result<(), ApplyError> {
    match operation.state {
        PublicJobState::Succeeded => Ok(()),
        PublicJobState::Failed
        | PublicJobState::Cancelled
        | PublicJobState::TimedOut
        | PublicJobState::ReconciliationRequired => Err(ApplyError::OperationTerminal {
            operation_id: operation.operation_id.to_string(),
            state: public_job_state_name(operation.state).to_owned(),
            detail: operation.error.as_ref().map_or_else(
                || "no public failure detail was provided".to_owned(),
                |error| format!("code={} message={}", error.code, error.message),
            ),
        }),
        PublicJobState::Queued | PublicJobState::Running | PublicJobState::Waiting => Err(
            ApplyError::InvalidResponse("Operation wait returned a non-terminal state".to_owned()),
        ),
    }
}

const fn public_job_state_name(state: PublicJobState) -> &'static str {
    match state {
        PublicJobState::Queued => "queued",
        PublicJobState::Running => "running",
        PublicJobState::Waiting => "waiting",
        PublicJobState::Succeeded => "succeeded",
        PublicJobState::Failed => "failed",
        PublicJobState::Cancelled => "cancelled",
        PublicJobState::TimedOut => "timed_out",
        PublicJobState::ReconciliationRequired => "reconciliation_required",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_api::resource::{deployment_etag, resource_etag, resource_version_etag};
    use insight_platform_contracts::{
        operation_etag, AuthoringPackage, DataClassification, PolicyKind, PolicyResourceSpec,
        SafeJobResult, ValidationSummary,
    };
    use std::{
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
        thread,
    };
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn policy_manifest() -> serde_json::Value {
        let authoring_artifact = ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest('1'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("policy authoring package".to_owned()),
        )
        .unwrap();
        let document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: authoring_artifact,
                manifest_digest: digest('2'),
            },
            contract_digest: digest('3'),
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: PolicyKind::Authorization,
            rules_digest: digest('4'),
            selection: None,
            scheduling: None,
            retention: None,
            model_safety: None,
            model_budget: None,
            model_public_projection: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        }));
        let qualification_evidence = ArtifactRef::new(
            id(ResourceKind::Artifact),
            digest('5'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("policy qualification".to_owned()),
        )
        .unwrap();
        serde_json::json!({
            "schema_version": 1,
            "kind": APPLY_MANIFEST_KIND,
            "resource_noun": "policies",
            "create": {
                "display_name": "local execution policy",
                "document": document
            },
            "publish": {
                "kind": "single",
                "revision_no": 1,
                "content_digest": digest('a'),
                "artifact_id": null
            },
            "deployment": {
                "environment": "local",
                "closure": {
                    "resource_kind": "policy",
                    "bindings": {
                        "applicability_digest": digest('b'),
                        "qualification_evidence": qualification_evidence
                    }
                }
            }
        })
    }

    struct ScriptedResponse {
        method: &'static str,
        path: String,
        expected_if_match: Option<String>,
        status: &'static str,
        etag: String,
        location: Option<String>,
        body: Vec<u8>,
        assert_deployment_version: Option<ResourceId>,
    }

    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "connection closed before request headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            assert!(bytes.len() <= MAX_APPLY_MANIFEST_BYTES);
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = header_value(&head, "content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "connection closed before request body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            head,
            bytes[header_end..header_end + content_length].to_vec(),
        )
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn request_trace_id(head: &str) -> String {
        let traceparent = header_value(head, "traceparent").expect("mutation traceparent");
        let mut fields = traceparent.split('-');
        assert_eq!(fields.next(), Some("00"));
        let trace_id = fields.next().unwrap();
        assert_eq!(trace_id.len(), 32);
        trace_id.to_owned()
    }

    fn write_response(stream: &mut TcpStream, response: &ScriptedResponse, trace_id: &str) {
        write!(
            stream,
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace_id}\r\netag: {}\r\n",
            response.status, response.etag
        )
        .unwrap();
        if let Some(location) = &response.location {
            write!(stream, "location: {location}\r\n").unwrap();
        }
        write!(
            stream,
            "content-length: {}\r\nconnection: close\r\n\r\n",
            response.body.len()
        )
        .unwrap();
        stream.write_all(&response.body).unwrap();
    }

    #[test]
    fn manifest_wrapper_is_closed_and_kind_bound() {
        let manifest = policy_manifest();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed = parse_manifest(&bytes);
        assert!(parsed.is_ok(), "{parsed:?}");

        let mut open = manifest;
        open.as_object_mut()
            .unwrap()
            .insert("unknown".to_owned(), serde_json::Value::Bool(true));
        assert!(parse_manifest(&serde_json::to_vec(&open).unwrap()).is_err());
    }

    #[test]
    fn apply_executes_the_public_policy_lifecycle_and_resolves_self_version() {
        let manifest_value = policy_manifest();
        let manifest_bytes = serde_json::to_vec(&manifest_value).unwrap();
        let (manifest, _) = parse_manifest(&manifest_bytes).unwrap();
        let tenant_id = id(ResourceKind::Tenant);
        let resource_id = id(ResourceKind::Policy);
        let operation_id = id(ResourceKind::Job);
        let version_id = id(ResourceKind::PolicyRevision);
        let deployment_id = id(ResourceKind::PolicyDeployment);
        let validation = ValidationSummary {
            validator_digest: digest('6'),
            validated_draft_digest: digest('7'),
            dependency_closure_digest: digest('8'),
            security_evidence_digest: digest('9'),
            warnings: Vec::new(),
        };
        let created_draft = ResourceDraftPayload {
            display_name: manifest.create.display_name.clone(),
            document: manifest.create.document.clone(),
            validation: None,
        };
        let validated_draft = ResourceDraftPayload {
            validation: Some(validation),
            ..created_draft.clone()
        };
        let create_etag = resource_etag(&resource_id, 1);
        let validated_etag = resource_etag(&resource_id, 2);
        let publish_etag = resource_etag(&resource_id, 3);
        let activated_etag = resource_etag(&resource_id, 4);
        let version_digest = digest('a');
        let version_etag = resource_version_etag(&version_id, &version_digest);
        let published = vec![PublishedResourceVersionSummaryV1 {
            resource_version_id: version_id.clone(),
            revision_no: 1,
            content_digest: version_digest,
            artifact_id: None,
            etag: version_etag,
        }];
        let closure = manifest
            .deployment
            .closure
            .clone()
            .resolve(&published)
            .unwrap();
        let closure_digest = canonical_digest(&serde_json::to_value(&closure).unwrap())
            .unwrap()
            .parse::<Sha256Digest>()
            .unwrap();
        let deployment_etag = deployment_etag(&deployment_id, &closure_digest);
        let operation_etag = operation_etag(&operation_id.to_string(), 2);
        let responses = vec![
            ScriptedResponse {
                method: "POST",
                path: "/v1/policies".to_owned(),
                expected_if_match: None,
                status: "201 Created",
                etag: create_etag.clone(),
                location: Some(format!("/v1/policies/{resource_id}")),
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 1,
                    draft: created_draft,
                    etag: create_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/draft:validate"),
                expected_if_match: Some(create_etag),
                status: "202 Accepted",
                etag: operation_etag.clone(),
                location: Some(format!("/v1/operations/{operation_id}")),
                body: serde_json::to_vec(&OperationViewV1 {
                    operation_id: operation_id.clone(),
                    tenant_id: tenant_id.clone(),
                    kind: PublicJobKind::ResourceValidation,
                    target: PublicJobTarget::ResourceVersion {
                        resource_id: resource_id.clone(),
                        resource_version: 1,
                    },
                    state: PublicJobState::Queued,
                    progress: None,
                    result: None,
                    error: None,
                    created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    updated_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    etag: operation_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "GET",
                path: format!("/v1/operations/{operation_id}"),
                expected_if_match: None,
                status: "200 OK",
                etag: operation_etag.clone(),
                location: None,
                body: serde_json::to_vec(&OperationViewV1 {
                    operation_id: operation_id.clone(),
                    tenant_id: tenant_id.clone(),
                    kind: PublicJobKind::ResourceValidation,
                    target: PublicJobTarget::ResourceVersion {
                        resource_id: resource_id.clone(),
                        resource_version: 1,
                    },
                    state: PublicJobState::Succeeded,
                    progress: None,
                    result: Some(SafeJobResult {
                        result_digest: digest('d'),
                    }),
                    error: None,
                    created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
                    updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
                    etag: operation_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "GET",
                path: format!("/v1/policies/{resource_id}"),
                expected_if_match: None,
                status: "200 OK",
                etag: validated_etag.clone(),
                location: None,
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 2,
                    draft: validated_draft.clone(),
                    etag: validated_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/draft:publish"),
                expected_if_match: Some(validated_etag),
                status: "200 OK",
                etag: publish_etag.clone(),
                location: None,
                body: serde_json::to_vec(&PublishResourceDraftResponseV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    draft_generation: 1,
                    version: 3,
                    published_versions: published.clone(),
                    etag: publish_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/deployments"),
                expected_if_match: Some(publish_etag.clone()),
                status: "201 Created",
                etag: deployment_etag.clone(),
                location: Some(format!(
                    "/v1/policies/{resource_id}/deployments/{deployment_id}"
                )),
                body: serde_json::to_vec(&DeploymentViewV1 {
                    schema_version: 1,
                    deployment_id: deployment_id.clone(),
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    resource_version_id: version_id.clone(),
                    environment: "local".to_owned(),
                    closure: closure.clone(),
                    closure_digest,
                    created_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
                    etag: deployment_etag,
                })
                .unwrap(),
                assert_deployment_version: Some(version_id.clone()),
            },
            ScriptedResponse {
                method: "POST",
                path: format!("/v1/policies/{resource_id}/deployments/{deployment_id}:activate"),
                expected_if_match: Some(publish_etag),
                status: "200 OK",
                etag: activated_etag.clone(),
                location: None,
                body: serde_json::to_vec(&ResourceViewV1 {
                    schema_version: 1,
                    resource_id: resource_id.clone(),
                    resource_kind: RegistryResourceKind::Policy,
                    lifecycle_state: EntityLifecycle::Active,
                    gate_state: AdministrativeGate::Enabled,
                    draft_generation: 1,
                    version: 4,
                    draft: validated_draft,
                    etag: activated_etag.clone(),
                })
                .unwrap(),
                assert_deployment_version: None,
            },
        ];

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert!(
                    head.starts_with(&format!("{} {} HTTP/1.1", response.method, response.path))
                );
                assert_eq!(
                    header_value(&head, "authorization"),
                    Some("Bearer test-token")
                );
                assert_eq!(
                    header_value(&head, "if-match"),
                    response.expected_if_match.as_deref()
                );
                let trace_id = if response.method == "POST" {
                    assert!(header_value(&head, "idempotency-key")
                        .is_some_and(|value| value.starts_with("insight-apply-v1-")));
                    request_trace_id(&head)
                } else {
                    "33333333333333333333333333333333".to_owned()
                };
                if let Some(expected_version) = &response.assert_deployment_version {
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(request["resource_version_id"], expected_version.to_string());
                    assert_eq!(
                        request["closure"]["bindings"]["policy_revision"]["revision_id"],
                        expected_version.to_string()
                    );
                }
                write_response(&mut stream, &response, &trace_id);
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let report =
            apply_manifest(&client, &tenant_id, &manifest_bytes, Duration::from_secs(2)).unwrap();
        assert_eq!(report.resource_id, resource_id.to_string());
        assert_eq!(report.validation_operation_id, operation_id.to_string());
        assert_eq!(report.deployment_id, deployment_id.to_string());
        assert_eq!(report.active_deployment_id, deployment_id.to_string());
        assert_eq!(report.final_resource_etag, activated_etag);
        assert_eq!(report.published_versions.len(), 1);
        assert_eq!(
            report.published_versions[0].resource_version_id,
            version_id.to_string()
        );
        server.join().unwrap();
    }
}
