//! Closed Resource lifecycle orchestration for `insight apply`.
//!
//! The manifest contains the same typed authoring documents and deployment bindings accepted by
//! `/v1`. It omits only the Resource/Version IDs that the authority creates during this exact
//! lifecycle; those self references are filled from the publish response before Deployment
//! creation. No Plan, tool, URL, shell, Secret value, or execution semantics are invented here.

use crate::{
    apply_journal::{
        self, ApplyJournalError, ApplyJournalV2, JournalDeployment, JournalIntent,
        JournalPublishResult, JournalPublishedVersion, JournalResource,
    },
    public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse},
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, AdministrativeGate, AgentDeploymentClosure, ArtifactRef,
    CapabilityBackendBinding, CapabilityDeploymentClosure, ClosedJsonValue, ContextBackendBinding,
    ContextBindingSnapshot, ContextConsistencyPolicy, ContextDeploymentClosure, DataRegion,
    DeploymentClosure, EntityLifecycle, ExactDeploymentRef, ExactPolicyBinding,
    ExactSecretBindingRef, ExactVersionRef, FrozenSlotBinding, FrozenSlotTarget, JsonLimits,
    McpDeploymentClosure, McpTransportBinding, ModelDeploymentClosure,
    ModelProviderDeploymentClosure, OperationViewV1, PlanNodeKind, PolicyDeploymentClosure,
    PublicJobKind, PublicJobState, PublicJobTarget, RegistryResourceKind, ResourceDocument,
    ResourceDraftPayload, ResourceId, ResourceKind, SandboxProfileDeploymentClosure, Sha256Digest,
    SkillDeploymentClosure, UtcTimestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, path::Path, time::Duration};

const APPLY_MANIFEST_KIND: &str = "insight.platform.apply/v1";
const MAX_APPLY_MANIFEST_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub enum ApplyError {
    InvalidManifest(String),
    Public(PublicClientError),
    Journal(ApplyJournalError),
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
            Self::Journal(error) => write!(formatter, "{error}"),
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
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for ApplyError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

impl From<ApplyJournalError> for ApplyError {
    fn from(value: ApplyJournalError) -> Self {
        Self::Journal(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ApplyResourceNoun {
    Agents,
    Skills,
    Capabilities,
    CapabilityImplementations,
    Contexts,
    ContextImplementations,
    Models,
    ModelProviders,
    McpServers,
    Policies,
    SandboxRuntimes,
    SandboxPackages,
    Sandboxes,
}

impl ApplyResourceNoun {
    const fn as_path(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::Skills => "skills",
            Self::Capabilities => "capabilities",
            Self::CapabilityImplementations => "capability-implementations",
            Self::Contexts => "contexts",
            Self::ContextImplementations => "context-implementations",
            Self::Models => "models",
            Self::ModelProviders => "model-providers",
            Self::McpServers => "mcp-servers",
            Self::Policies => "policies",
            Self::SandboxRuntimes => "sandbox-runtimes",
            Self::SandboxPackages => "sandbox-packages",
            Self::Sandboxes => "sandboxes",
        }
    }

    const fn resource_kind(self) -> RegistryResourceKind {
        match self {
            Self::Agents => RegistryResourceKind::Agent,
            Self::Skills => RegistryResourceKind::Skill,
            Self::Capabilities => RegistryResourceKind::CapabilityInterface,
            Self::CapabilityImplementations => RegistryResourceKind::CapabilityImplementation,
            Self::Contexts => RegistryResourceKind::ContextSourceInterface,
            Self::ContextImplementations => RegistryResourceKind::ContextSourceImplementation,
            Self::Models => RegistryResourceKind::ModelProfile,
            Self::ModelProviders => RegistryResourceKind::ModelProvider,
            Self::McpServers => RegistryResourceKind::McpServer,
            Self::Policies => RegistryResourceKind::Policy,
            Self::SandboxRuntimes => RegistryResourceKind::SandboxRuntime,
            Self::SandboxPackages => RegistryResourceKind::SandboxPackage,
            Self::Sandboxes => RegistryResourceKind::SandboxProfile,
        }
    }

    const fn primary_version_kind(self) -> ResourceKind {
        match self {
            Self::Agents => ResourceKind::AgentPlanRevision,
            Self::Skills => ResourceKind::SkillRevision,
            Self::Capabilities => ResourceKind::CapabilityInterfaceRevision,
            Self::CapabilityImplementations => ResourceKind::CapabilityImplementationRevision,
            Self::Contexts => ResourceKind::ContextSourceInterfaceRevision,
            Self::ContextImplementations => ResourceKind::ContextSourceImplementationRevision,
            Self::Models => ResourceKind::ModelProfileRevision,
            Self::ModelProviders => ResourceKind::ModelProviderRevision,
            Self::McpServers => ResourceKind::McpServerRevision,
            Self::Policies => ResourceKind::PolicyRevision,
            Self::SandboxRuntimes => ResourceKind::SandboxRuntimeRevision,
            Self::SandboxPackages => ResourceKind::SandboxPackageRevision,
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
    deployment: Option<ApplyDeploymentRequestV1>,
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
    ModelProvider(ApplyModelProviderDeploymentBindings),
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
            Self::ModelProvider(_) => RegistryResourceKind::ModelProvider,
            Self::ModelProfile(_) => RegistryResourceKind::ModelProfile,
            Self::Policy(_) => RegistryResourceKind::Policy,
            Self::SandboxProfile(_) => RegistryResourceKind::SandboxProfile,
        }
    }

    fn resolve(
        self,
        published: &[PublishedResourceVersionSummaryV1],
    ) -> Result<CreateDeploymentClosureV1, ApplyError> {
        let exact = |kind| published_exact_version(published, kind);
        let closure = match self {
            Self::Agent(bindings) => {
                CreateDeploymentClosureV1::Agent(AgentDeploymentClosureInputV1 {
                    interface: exact(ResourceKind::AgentInterfaceRevision)?,
                    plan: exact(ResourceKind::AgentPlanRevision)?,
                    entry_node_id: bindings.entry_node_id,
                    entry_node_kind: bindings.entry_node_kind,
                    slots: bindings.slots,
                    policies: bindings.policies,
                    execution_profile: bindings.execution_profile,
                })
            }
            Self::Skill(bindings) => CreateDeploymentClosureV1::Skill(SkillDeploymentClosure {
                skill_revision: exact(ResourceKind::SkillRevision)?,
                requirements: bindings.requirements,
                selection_policy: bindings.selection_policy,
                qualification_evidence: bindings.qualification_evidence,
            }),
            Self::CapabilityInterface(bindings) => {
                CreateDeploymentClosureV1::CapabilityInterface(CapabilityDeploymentClosure {
                    implementation: bindings.implementation,
                    interface: exact(ResourceKind::CapabilityInterfaceRevision)?,
                    backend: bindings.backend,
                    secret_bindings: bindings.secret_bindings,
                    policies: bindings.policies,
                    conformance_evidence: bindings.conformance_evidence,
                })
            }
            Self::ContextSourceInterface(bindings) => {
                CreateDeploymentClosureV1::ContextSourceInterface(ContextDeploymentClosure {
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
            Self::McpServer(bindings) => {
                CreateDeploymentClosureV1::McpServer(McpDeploymentClosure {
                    server_revision: exact(ResourceKind::McpServerRevision)?,
                    server_identity_digest: bindings.server_identity_digest,
                    transport: bindings.transport,
                    protocol_policy: bindings.protocol_policy,
                    trust_policy: bindings.trust_policy,
                    auth_policy: bindings.auth_policy,
                    secret_bindings: bindings.secret_bindings,
                    conformance_evidence: bindings.conformance_evidence,
                })
            }
            Self::ModelProvider(bindings) => {
                CreateDeploymentClosureV1::ModelProvider(ModelProviderDeploymentClosure {
                    provider_revision: exact(ResourceKind::ModelProviderRevision)?,
                    endpoint_identity_digest: bindings.endpoint_identity_digest,
                    secret_bindings: bindings.secret_bindings,
                    protocol_policy: bindings.protocol_policy,
                    network_policy: bindings.network_policy,
                    tls_policy: bindings.tls_policy,
                    trust_policy: bindings.trust_policy,
                    data_policy: bindings.data_policy,
                    region: bindings.region,
                    conformance_evidence: bindings.conformance_evidence,
                })
            }
            Self::ModelProfile(bindings) => {
                CreateDeploymentClosureV1::ModelProfile(ModelDeploymentClosure {
                    profile_revision: exact(ResourceKind::ModelProfileRevision)?,
                    provider_deployment: bindings.provider_deployment,
                    data_policy: bindings.data_policy,
                    safety_policy: bindings.safety_policy,
                    budget_policy: bindings.budget_policy,
                    public_projection_policy: bindings.public_projection_policy,
                    generation_defaults: bindings.generation_defaults,
                })
            }
            Self::Policy(bindings) => CreateDeploymentClosureV1::Policy(PolicyDeploymentClosure {
                policy_revision: exact(ResourceKind::PolicyRevision)?,
                applicability_digest: bindings.applicability_digest,
                qualification_evidence: bindings.qualification_evidence,
            }),
            Self::SandboxProfile(bindings) => {
                CreateDeploymentClosureV1::SandboxProfile(SandboxProfileDeploymentClosure {
                    schema_version: insight_platform_contracts::SANDBOX_CONTRACT_SCHEMA_VERSION,
                    profile_revision: exact(ResourceKind::SandboxProfileRevision)?,
                    runtime_revision: bindings.runtime_revision,
                    provider_binding_digest: bindings.provider_binding_digest,
                    network_mode: bindings.network_mode,
                    limits: bindings.limits,
                    provisioning_limits: bindings.provisioning_limits,
                    secret_injection_disabled: bindings.secret_injection_disabled,
                    qualification_evidence: bindings.qualification_evidence,
                })
            }
        };
        closure.validate()?;
        Ok(closure)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "resource_kind",
    content = "bindings",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CreateDeploymentClosureV1 {
    Agent(AgentDeploymentClosureInputV1),
    Skill(SkillDeploymentClosure),
    CapabilityInterface(CapabilityDeploymentClosure),
    ContextSourceInterface(ContextDeploymentClosure),
    McpServer(McpDeploymentClosure),
    ModelProvider(ModelProviderDeploymentClosure),
    ModelProfile(ModelDeploymentClosure),
    Policy(PolicyDeploymentClosure),
    SandboxProfile(SandboxProfileDeploymentClosure),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentDeploymentClosureInputV1 {
    interface: ExactVersionRef,
    plan: ExactVersionRef,
    entry_node_id: String,
    entry_node_kind: PlanNodeKind,
    slots: Vec<FrozenSlotBindingInputV1>,
    policies: Vec<ExactPolicyBinding>,
    execution_profile: ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FrozenSlotBindingInputV1 {
    slot_id: String,
    requirement_digest: Sha256Digest,
    target: FrozenSlotTargetInputV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum FrozenSlotTargetInputV1 {
    Model {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
    },
    Capability {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
        tool_alias: Option<String>,
    },
    Context {
        binding: Box<ContextBindingInputV1>,
    },
    ChildAgent {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
    },
    Skill {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ContextBindingInputV1 {
    context_deployment: ExactDeploymentRef,
    consistency: ContextConsistencyPolicy,
    allowed_projection: Vec<String>,
    authorization_policy: ExactVersionRef,
    ranking_policy: ExactVersionRef,
}

impl CreateDeploymentClosureV1 {
    fn validate(&self) -> Result<(), ApplyError> {
        let deployment_kind = self.resource_kind().deployment_kind().ok_or_else(|| {
            ApplyError::InvalidManifest("Resource kind cannot be deployed".to_owned())
        })?;
        let deployment_id = ResourceId::from_uuid_v7(deployment_kind, uuid::Uuid::now_v7())
            .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
        let context_ids = (0..self.context_binding_count())
            .map(|_| {
                ResourceId::from_uuid_v7(ResourceKind::ContextBinding, uuid::Uuid::now_v7())
                    .map_err(|error| ApplyError::InvalidManifest(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.materialized(&deployment_id, context_ids)?
            .validate()
            .map_err(|error| {
                ApplyError::InvalidManifest(format!("resolved Deployment closure: {error}"))
            })
    }

    const fn resource_kind(&self) -> RegistryResourceKind {
        match self {
            Self::Agent(_) => RegistryResourceKind::Agent,
            Self::Skill(_) => RegistryResourceKind::Skill,
            Self::CapabilityInterface(_) => RegistryResourceKind::CapabilityInterface,
            Self::ContextSourceInterface(_) => RegistryResourceKind::ContextSourceInterface,
            Self::McpServer(_) => RegistryResourceKind::McpServer,
            Self::ModelProvider(_) => RegistryResourceKind::ModelProvider,
            Self::ModelProfile(_) => RegistryResourceKind::ModelProfile,
            Self::Policy(_) => RegistryResourceKind::Policy,
            Self::SandboxProfile(_) => RegistryResourceKind::SandboxProfile,
        }
    }

    fn context_binding_count(&self) -> usize {
        match self {
            Self::Agent(agent) => agent
                .slots
                .iter()
                .filter(|slot| matches!(slot.target, FrozenSlotTargetInputV1::Context { .. }))
                .count(),
            _ => 0,
        }
    }

    fn expected_response(
        &self,
        deployment_id: &ResourceId,
        actual: &DeploymentClosure,
    ) -> Result<DeploymentClosure, ApplyError> {
        let context_binding_ids = match actual {
            DeploymentClosure::Agent(agent) => agent
                .slots
                .iter()
                .filter_map(|slot| match &slot.target {
                    FrozenSlotTarget::Context { binding } => {
                        Some(binding.context_binding_id.clone())
                    }
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        self.materialized(deployment_id, context_binding_ids)
    }

    fn materialized(
        &self,
        deployment_id: &ResourceId,
        context_binding_ids: Vec<ResourceId>,
    ) -> Result<DeploymentClosure, ApplyError> {
        let closure = match self {
            Self::Agent(agent) => {
                if context_binding_ids.len() != self.context_binding_count() {
                    return Err(ApplyError::InvalidResponse(
                        "Agent Deployment returned a different Context binding count".to_owned(),
                    ));
                }
                let mut ids = context_binding_ids.into_iter();
                let slots = agent
                    .slots
                    .iter()
                    .cloned()
                    .map(|slot| slot.materialize(deployment_id, &mut ids))
                    .collect::<Result<Vec<_>, _>>()?;
                DeploymentClosure::Agent(AgentDeploymentClosure {
                    interface: agent.interface.clone(),
                    plan: agent.plan.clone(),
                    entry_node_id: agent.entry_node_id.clone(),
                    entry_node_kind: agent.entry_node_kind,
                    slots,
                    policies: agent.policies.clone(),
                    execution_profile: agent.execution_profile.clone(),
                })
            }
            Self::Skill(closure) => DeploymentClosure::Skill(closure.clone()),
            Self::CapabilityInterface(closure) => {
                DeploymentClosure::CapabilityInterface(closure.clone())
            }
            Self::ContextSourceInterface(closure) => {
                DeploymentClosure::ContextSourceInterface(closure.clone())
            }
            Self::McpServer(closure) => DeploymentClosure::McpServer(closure.clone()),
            Self::ModelProvider(closure) => DeploymentClosure::ModelProvider(closure.clone()),
            Self::ModelProfile(closure) => DeploymentClosure::ModelProfile(closure.clone()),
            Self::Policy(closure) => DeploymentClosure::Policy(closure.clone()),
            Self::SandboxProfile(closure) => DeploymentClosure::SandboxProfile(closure.clone()),
        };
        Ok(closure)
    }
}

impl FrozenSlotBindingInputV1 {
    fn materialize(
        self,
        deployment_id: &ResourceId,
        context_binding_ids: &mut impl Iterator<Item = ResourceId>,
    ) -> Result<FrozenSlotBinding, ApplyError> {
        let target = match self.target {
            FrozenSlotTargetInputV1::Model {
                candidates,
                selection_policy,
            } => FrozenSlotTarget::Model {
                candidates,
                selection_policy,
            },
            FrozenSlotTargetInputV1::Capability {
                candidates,
                selection_policy,
                tool_alias,
            } => FrozenSlotTarget::Capability {
                candidates,
                selection_policy,
                tool_alias,
            },
            FrozenSlotTargetInputV1::Context { binding } => FrozenSlotTarget::Context {
                binding: Box::new(
                    ContextBindingSnapshot::build(
                        context_binding_ids.next().ok_or_else(|| {
                            ApplyError::InvalidResponse(
                                "Agent Deployment omitted a Context binding identity".to_owned(),
                            )
                        })?,
                        deployment_id.clone(),
                        binding.context_deployment,
                        binding.consistency,
                        binding.allowed_projection,
                        binding.authorization_policy,
                        binding.ranking_policy,
                    )
                    .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?,
                ),
            },
            FrozenSlotTargetInputV1::ChildAgent {
                candidates,
                selection_policy,
            } => FrozenSlotTarget::ChildAgent {
                candidates,
                selection_policy,
            },
            FrozenSlotTargetInputV1::Skill {
                candidates,
                selection_policy,
            } => FrozenSlotTarget::Skill {
                candidates,
                selection_policy,
            },
        };
        let binding_digest = canonical_digest(&serde_json::json!({
            "slot_id": &self.slot_id,
            "requirement_digest": &self.requirement_digest,
            "target": &target,
        }))
        .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?
        .parse()
        .map_err(|error| ApplyError::InvalidManifest(format!("slot binding digest: {error}")))?;
        Ok(FrozenSlotBinding {
            slot_id: self.slot_id,
            requirement_digest: self.requirement_digest,
            target,
            binding_digest,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplyAgentDeploymentBindings {
    entry_node_id: String,
    entry_node_kind: PlanNodeKind,
    slots: Vec<FrozenSlotBindingInputV1>,
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
struct ApplyModelProviderDeploymentBindings {
    endpoint_identity_digest: Sha256Digest,
    secret_bindings: Vec<ExactSecretBindingRef>,
    protocol_policy: ExactVersionRef,
    network_policy: ExactVersionRef,
    tls_policy: ExactVersionRef,
    trust_policy: ExactVersionRef,
    data_policy: ExactVersionRef,
    region: DataRegion,
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
    provider_binding_digest: Sha256Digest,
    network_mode: insight_platform_contracts::SandboxNetworkMode,
    limits: insight_platform_contracts::SandboxResourceLimitsV1,
    provisioning_limits: insight_platform_contracts::SandboxProvisioningLimitsV1,
    secret_injection_disabled: bool,
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
    closure: &'a CreateDeploymentClosureV1,
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
    pub deployment_id: Option<String>,
    pub active_deployment_id: Option<String>,
    pub final_resource_etag: String,
    pub step_trace_ids: BTreeMap<String, String>,
}

pub fn apply_manifest(
    client: &PublicHttpClient,
    expected_tenant_id: &ResourceId,
    manifest_bytes: &[u8],
    operation_timeout: Duration,
    journal_directory: &Path,
) -> Result<ApplyReportV1, ApplyError> {
    let (manifest, manifest_digest) = parse_manifest(manifest_bytes)?;
    let noun = manifest.resource_noun;
    let kind = noun.resource_kind();
    let base_path = format!("/v1/{}", noun.as_path());
    let journal_path = apply_journal::journal_path(journal_directory, &manifest_digest);
    let mut journal = match apply_journal::load(&journal_path)? {
        Some(journal) => journal,
        None => {
            let journal = ApplyJournalV2::new(
                manifest_digest.clone(),
                receipt_key(&manifest_digest, "create"),
            );
            apply_journal::save(&journal_path, &journal)?;
            journal
        }
    };
    journal.validate(&manifest_digest)?;
    validate_resume_journal(&journal, &manifest)?;

    if journal.resource.is_none() {
        let create: PublicJsonResponse<ResourceViewV1> = client.post_json(
            &base_path,
            &manifest.create,
            StatusCode::CREATED,
            &journal.create_intent.receipt,
            None,
        )?;
        validate_resource_response(&create, kind, None)?;
        require_location(&create, &format!("{base_path}/{}", create.body.resource_id))?;
        journal
            .step_trace_ids
            .insert("create".to_owned(), create.trace_id);
        journal.resource = Some(JournalResource {
            resource_id: create.body.resource_id,
            etag: create.etag,
            version: create.body.version,
        });
        apply_journal::save(&journal_path, &journal)?;
    }
    let resource = journal.resource.clone().ok_or_else(|| {
        ApplyError::InvalidResponse("apply journal omitted the created Resource".to_owned())
    })?;
    let resource_id = resource.resource_id;

    let validate_path = format!("{base_path}/{resource_id}/draft:validate");
    let validation_intent = JournalIntent {
        receipt: receipt_key(&manifest_digest, "validate"),
        if_match: Some(resource.etag.clone()),
    };
    persist_intent(
        &journal_path,
        &mut journal,
        JournalStep::Validation,
        validation_intent,
    )?;
    if journal.validation_operation_id.is_none() {
        let intent = require_intent(journal.validation_intent.as_ref(), "validation")?;
        let validation: PublicJsonResponse<OperationViewV1> = client.post_empty(
            &validate_path,
            StatusCode::ACCEPTED,
            &intent.receipt,
            intent.if_match.as_deref().ok_or_else(|| {
                ApplyError::InvalidResponse("validation intent omitted If-Match".to_owned())
            })?,
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
                } if target_resource_id == &resource_id && *resource_version == resource.version
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
        journal
            .step_trace_ids
            .insert("validate".to_owned(), validation.trace_id);
        journal.validation_operation_id = Some(validation.body.operation_id);
        apply_journal::save(&journal_path, &journal)?;
    }
    let validation_operation_id = journal.validation_operation_id.clone().ok_or_else(|| {
        ApplyError::InvalidResponse("apply journal omitted validation Operation".to_owned())
    })?;
    if journal.validated_resource_etag.is_none() {
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
                "validation Operation succeeded without an exact Draft validation summary"
                    .to_owned(),
            ));
        }
        journal
            .step_trace_ids
            .insert("read_validated".to_owned(), validated.trace_id);
        journal.validated_resource_etag = Some(validated.etag);
        apply_journal::save(&journal_path, &journal)?;
    }
    let validated_etag = journal.validated_resource_etag.clone().ok_or_else(|| {
        ApplyError::InvalidResponse("apply journal omitted validated Resource ETag".to_owned())
    })?;

    let publish_intent = JournalIntent {
        receipt: receipt_key(&manifest_digest, "publish"),
        if_match: Some(validated_etag),
    };
    persist_intent(
        &journal_path,
        &mut journal,
        JournalStep::Publish,
        publish_intent,
    )?;
    if journal.publish.is_none() {
        let intent = require_intent(journal.publish_intent.as_ref(), "publish")?;
        let publish: PublicJsonResponse<PublishResourceDraftResponseV1> = client.post_json(
            &format!("{base_path}/{resource_id}/draft:publish"),
            &manifest.publish,
            StatusCode::OK,
            &intent.receipt,
            intent.if_match.as_deref(),
        )?;
        validate_publish_response(&publish, noun, &resource_id, &manifest.publish)?;
        journal
            .step_trace_ids
            .insert("publish".to_owned(), publish.trace_id);
        journal.publish = Some(JournalPublishResult {
            resource_etag: publish.etag,
            versions: publish
                .body
                .published_versions
                .into_iter()
                .map(JournalPublishedVersion::from)
                .collect(),
        });
        apply_journal::save(&journal_path, &journal)?;
    }
    let publish = journal.publish.clone().ok_or_else(|| {
        ApplyError::InvalidResponse("apply journal omitted publish authority result".to_owned())
    })?;
    let published_versions = publish
        .versions
        .iter()
        .cloned()
        .map(PublishedResourceVersionSummaryV1::from)
        .collect::<Vec<_>>();
    let primary_version = published_versions
        .iter()
        .find(|version| version.resource_version_id.kind() == noun.primary_version_kind())
        .ok_or_else(|| {
            ApplyError::InvalidResponse("publish response omitted the primary Version".to_owned())
        })?;
    if kind.deployment_kind().is_none() {
        if manifest.deployment.is_some()
            || journal.deployment_intent.is_some()
            || journal.deployment.is_some()
            || journal.activation_intent.is_some()
            || journal.final_resource_etag.is_some()
        {
            return Err(ApplyError::InvalidResponse(
                "definition-only apply contains Deployment state".to_owned(),
            ));
        }
        return Ok(ApplyReportV1 {
            schema_version: 1,
            kind: "insight.platform.apply-report/v1",
            manifest_digest: manifest_digest.to_string(),
            resource_id: resource_id.to_string(),
            validation_operation_id: validation_operation_id.to_string(),
            published_versions: published_versions
                .iter()
                .map(|version| ApplyPublishedVersionReport {
                    resource_version_id: version.resource_version_id.to_string(),
                    resource_kind: version.resource_version_id.kind().to_string(),
                    content_digest: version.content_digest.to_string(),
                })
                .collect(),
            deployment_id: None,
            active_deployment_id: None,
            final_resource_etag: publish.resource_etag,
            step_trace_ids: journal
                .step_trace_ids
                .iter()
                .map(|(step, trace_id)| (step.clone(), trace_id.to_string()))
                .collect(),
        });
    }
    let deployment_spec = manifest.deployment.ok_or_else(|| {
        ApplyError::InvalidManifest("deployable Resource requires deployment".to_owned())
    })?;
    let deployment_closure = deployment_spec.closure.resolve(&published_versions)?;
    let deployment_request = CreateDeploymentRequestV1 {
        resource_version_id: &primary_version.resource_version_id,
        environment: &deployment_spec.environment,
        closure: &deployment_closure,
    };
    let deployment_intent = JournalIntent {
        receipt: receipt_key(&manifest_digest, "deploy"),
        if_match: Some(publish.resource_etag.clone()),
    };
    persist_intent(
        &journal_path,
        &mut journal,
        JournalStep::Deployment,
        deployment_intent,
    )?;
    if journal.deployment.is_none() {
        let intent = require_intent(journal.deployment_intent.as_ref(), "deployment")?;
        let deployment: PublicJsonResponse<DeploymentViewV1> = client.post_json(
            &format!("{base_path}/{resource_id}/deployments"),
            &deployment_request,
            StatusCode::CREATED,
            &intent.receipt,
            intent.if_match.as_deref(),
        )?;
        validate_deployment_response(
            &deployment,
            kind,
            &resource_id,
            &primary_version.resource_version_id,
            &deployment_spec.environment,
            &deployment_closure,
        )?;
        require_location(
            &deployment,
            &format!(
                "{base_path}/{resource_id}/deployments/{}",
                deployment.body.deployment_id
            ),
        )?;
        journal
            .step_trace_ids
            .insert("deploy".to_owned(), deployment.trace_id);
        let deployed_resource: PublicJsonResponse<ResourceViewV1> =
            client.get_json(&format!("{base_path}/{resource_id}"), StatusCode::OK)?;
        validate_resource_response(&deployed_resource, kind, Some(&resource_id))?;
        require_post_deployment_resource_version(
            &resource_id,
            &publish.resource_etag,
            deployed_resource.body.version,
        )?;
        journal
            .step_trace_ids
            .insert("read_deployed".to_owned(), deployed_resource.trace_id);
        journal.deployment = Some(JournalDeployment {
            deployment_id: deployment.body.deployment_id,
            etag: deployment.etag,
            resource_etag: deployed_resource.etag,
        });
        apply_journal::save(&journal_path, &journal)?;
    }
    let deployment = journal.deployment.as_ref().ok_or_else(|| {
        ApplyError::InvalidResponse("apply journal omitted Deployment result".to_owned())
    })?;
    let deployment_id = deployment.deployment_id.clone();
    let deployed_resource_etag = deployment.resource_etag.clone();

    let activation_intent = JournalIntent {
        receipt: receipt_key(&manifest_digest, "activate"),
        if_match: Some(deployed_resource_etag),
    };
    persist_intent(
        &journal_path,
        &mut journal,
        JournalStep::Activation,
        activation_intent,
    )?;
    if journal.final_resource_etag.is_none() {
        let intent = require_intent(journal.activation_intent.as_ref(), "activation")?;
        let activated: PublicJsonResponse<ResourceViewV1> = client.post_empty(
            &format!("{base_path}/{resource_id}/deployments/{deployment_id}:activate"),
            StatusCode::OK,
            &intent.receipt,
            intent.if_match.as_deref().ok_or_else(|| {
                ApplyError::InvalidResponse("activation intent omitted If-Match".to_owned())
            })?,
        )?;
        validate_resource_response(&activated, kind, Some(&resource_id))?;
        if activated.body.gate_state != AdministrativeGate::Enabled {
            return Err(ApplyError::InvalidResponse(
                "activation did not leave the Resource gate enabled".to_owned(),
            ));
        }
        journal
            .step_trace_ids
            .insert("activate".to_owned(), activated.trace_id);
        journal.final_resource_etag = Some(activated.etag);
        apply_journal::save(&journal_path, &journal)?;
    }

    let published_version_report = published_versions
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
        manifest_digest: manifest_digest.to_string(),
        resource_id: resource_id.to_string(),
        validation_operation_id: validation_operation_id.to_string(),
        published_versions: published_version_report,
        deployment_id: Some(deployment_id.to_string()),
        active_deployment_id: Some(deployment_id.to_string()),
        final_resource_etag: journal.final_resource_etag.clone().ok_or_else(|| {
            ApplyError::InvalidResponse("apply journal omitted final Resource ETag".to_owned())
        })?,
        step_trace_ids: journal
            .step_trace_ids
            .iter()
            .map(|(step, trace_id)| (step.clone(), trace_id.to_string()))
            .collect(),
    })
}

fn require_post_deployment_resource_version(
    resource_id: &ResourceId,
    published_etag: &str,
    deployed_version: u64,
) -> Result<(), ApplyError> {
    let prefix = format!("\"{resource_id}-");
    let published_version = published_etag
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|version| *version > 0)
        .ok_or_else(|| {
            ApplyError::InvalidResponse("publish response Resource ETag is invalid".to_owned())
        })?;
    if published_version.checked_add(1) != Some(deployed_version) {
        return Err(ApplyError::InvalidResponse(
            "post-Deployment Resource version is not the exact successor".to_owned(),
        ));
    }
    Ok(())
}

fn parse_manifest(bytes: &[u8]) -> Result<(ApplyManifestV1, Sha256Digest), ApplyError> {
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
    let digest = canonical_digest(&value)
        .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?
        .parse::<Sha256Digest>()
        .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
    let manifest = serde_json::from_value::<ApplyManifestV1>(value)
        .map_err(|error| ApplyError::InvalidManifest(error.to_string()))?;
    validate_manifest(&manifest)?;
    Ok((manifest, digest))
}

fn validate_manifest(manifest: &ApplyManifestV1) -> Result<(), ApplyError> {
    let kind = manifest.resource_noun.resource_kind();
    let deployment_matches = match (&manifest.deployment, kind.deployment_kind()) {
        (Some(deployment), Some(_)) => {
            deployment.closure.resource_kind() == kind
                && !deployment.environment.is_empty()
                && deployment.environment.len() <= 128
                && deployment
                    .environment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        }
        (None, None) => true,
        _ => false,
    };
    if manifest.schema_version != 1
        || manifest.kind != APPLY_MANIFEST_KIND
        || manifest.create.document.kind() != kind
        || !deployment_matches
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

fn receipt_key(manifest_digest: &Sha256Digest, step: &str) -> String {
    let manifest_digest = manifest_digest.to_string();
    format!(
        "insight-apply-v1-{}-{step}",
        manifest_digest
            .strip_prefix("sha256:")
            .unwrap_or(&manifest_digest)
    )
}

#[derive(Debug, Clone, Copy)]
enum JournalStep {
    Validation,
    Publish,
    Deployment,
    Activation,
}

fn persist_intent(
    path: &Path,
    journal: &mut ApplyJournalV2,
    step: JournalStep,
    expected: JournalIntent,
) -> Result<(), ApplyError> {
    let slot = match step {
        JournalStep::Validation => &mut journal.validation_intent,
        JournalStep::Publish => &mut journal.publish_intent,
        JournalStep::Deployment => &mut journal.deployment_intent,
        JournalStep::Activation => &mut journal.activation_intent,
    };
    match slot {
        Some(existing) if existing != &expected => {
            return Err(ApplyError::InvalidResponse(
                "persisted apply intent differs from the deterministic request".to_owned(),
            ));
        }
        Some(_) => return Ok(()),
        None => *slot = Some(expected),
    }
    apply_journal::save(path, journal)?;
    Ok(())
}

fn require_intent<'a>(
    intent: Option<&'a JournalIntent>,
    step: &str,
) -> Result<&'a JournalIntent, ApplyError> {
    intent.ok_or_else(|| {
        ApplyError::InvalidResponse(format!("apply journal omitted the {step} intent"))
    })
}

fn validate_resume_journal(
    journal: &ApplyJournalV2,
    manifest: &ApplyManifestV1,
) -> Result<(), ApplyError> {
    let digest = &journal.manifest_digest;
    if journal.create_intent
        != (JournalIntent {
            receipt: receipt_key(digest, "create"),
            if_match: None,
        })
    {
        return Err(ApplyError::InvalidResponse(
            "persisted create intent is not deterministic".to_owned(),
        ));
    }
    let Some(resource) = &journal.resource else {
        return Ok(());
    };
    let noun = manifest.resource_noun;
    if resource.resource_id.kind() != noun.resource_kind().id_kind() {
        return Err(ApplyError::InvalidResponse(
            "journal Resource kind differs from the manifest".to_owned(),
        ));
    }
    validate_optional_intent(
        journal.validation_intent.as_ref(),
        receipt_key(digest, "validate"),
        &resource.etag,
    )?;
    if journal
        .validation_operation_id
        .as_ref()
        .is_some_and(|id| id.kind() != ResourceKind::Job)
    {
        return Err(ApplyError::InvalidResponse(
            "journal validation Operation has the wrong ID kind".to_owned(),
        ));
    }
    if let Some(validated_etag) = &journal.validated_resource_etag {
        validate_optional_intent(
            journal.publish_intent.as_ref(),
            receipt_key(digest, "publish"),
            validated_etag,
        )?;
    }
    if let Some(publish) = &journal.publish {
        validate_journal_publish(publish, noun, &manifest.publish)?;
        validate_optional_intent(
            journal.deployment_intent.as_ref(),
            receipt_key(digest, "deploy"),
            &publish.resource_etag,
        )?;
        if let Some(deployment) = &journal.deployment {
            validate_optional_intent(
                journal.activation_intent.as_ref(),
                receipt_key(digest, "activate"),
                &deployment.resource_etag,
            )?;
        }
    }
    if journal.deployment.as_ref().is_some_and(|deployment| {
        noun.resource_kind().deployment_kind() != Some(deployment.deployment_id.kind())
    }) {
        return Err(ApplyError::InvalidResponse(
            "journal Deployment kind differs from the manifest".to_owned(),
        ));
    }
    Ok(())
}

fn validate_optional_intent(
    actual: Option<&JournalIntent>,
    expected_receipt: String,
    expected_if_match: &str,
) -> Result<(), ApplyError> {
    if actual.is_some_and(|intent| {
        intent.receipt != expected_receipt || intent.if_match.as_deref() != Some(expected_if_match)
    }) {
        return Err(ApplyError::InvalidResponse(
            "persisted apply intent differs from the manifest closure".to_owned(),
        ));
    }
    Ok(())
}

fn validate_journal_publish(
    publish: &JournalPublishResult,
    noun: ApplyResourceNoun,
    request: &ApplyPublishResourceRequestV1,
) -> Result<(), ApplyError> {
    let expected_count = if noun == ApplyResourceNoun::Agents {
        2
    } else {
        1
    };
    let mut kinds = std::collections::BTreeSet::new();
    let versions_match = publish.versions.iter().all(|version| {
        if !noun
            .resource_kind()
            .allows_version_kind(version.resource_version_id.kind())
            || !kinds.insert(version.resource_version_id.kind())
        {
            return false;
        }
        match request {
            ApplyPublishResourceRequestV1::Single {
                revision_no,
                content_digest,
                artifact_id,
            } => {
                version.revision_no == *revision_no
                    && &version.content_digest == content_digest
                    && &version.artifact_id == artifact_id
            }
            ApplyPublishResourceRequestV1::Agent {
                revision_no,
                interface_content_digest,
                plan_content_digest,
                artifact_id,
            } => {
                let expected_digest = match version.resource_version_id.kind() {
                    ResourceKind::AgentInterfaceRevision => interface_content_digest,
                    ResourceKind::AgentPlanRevision => plan_content_digest,
                    _ => return false,
                };
                version.revision_no == *revision_no
                    && &version.content_digest == expected_digest
                    && &version.artifact_id == artifact_id
            }
        }
    });
    if publish.versions.len() != expected_count
        || !versions_match
        || !kinds.contains(&noun.primary_version_kind())
        || (noun == ApplyResourceNoun::Agents
            && !kinds.contains(&ResourceKind::AgentInterfaceRevision))
    {
        return Err(ApplyError::InvalidResponse(
            "journal publish result differs from the manifest Version matrix".to_owned(),
        ));
    }
    Ok(())
}

impl From<PublishedResourceVersionSummaryV1> for JournalPublishedVersion {
    fn from(value: PublishedResourceVersionSummaryV1) -> Self {
        Self {
            resource_version_id: value.resource_version_id,
            revision_no: value.revision_no,
            content_digest: value.content_digest,
            artifact_id: value.artifact_id,
            etag: value.etag,
        }
    }
}

impl From<JournalPublishedVersion> for PublishedResourceVersionSummaryV1 {
    fn from(value: JournalPublishedVersion) -> Self {
        Self {
            resource_version_id: value.resource_version_id,
            revision_no: value.revision_no,
            content_digest: value.content_digest,
            artifact_id: value.artifact_id,
            etag: value.etag,
        }
    }
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
    closure: &CreateDeploymentClosureV1,
) -> Result<(), ApplyError> {
    let body = &response.body;
    let expected_closure = closure.expected_response(&body.deployment_id, &body.closure)?;
    let closure_digest = deployment_closure_digest_v1(&expected_closure)?;
    if body.schema_version != 1
        || body.deployment_id.kind()
            != kind.deployment_kind().ok_or_else(|| {
                ApplyError::InvalidManifest("Resource kind cannot be deployed".to_owned())
            })?
        || &body.resource_id != resource_id
        || body.resource_kind != kind
        || &body.resource_version_id != resource_version_id
        || body.environment != environment
        || body.closure != expected_closure
        || body.closure_digest != closure_digest
        || body.etag != response.etag
        || body.created_at.as_str().is_empty()
    {
        return Err(ApplyError::InvalidResponse(
            "Deployment projection differs from the exact request closure".to_owned(),
        ));
    }
    Ok(())
}

fn deployment_closure_digest_v1(closure: &DeploymentClosure) -> Result<Sha256Digest, ApplyError> {
    // Persisted Deployment bindings are a typed JSONB payload. Its digest includes the owning
    // schema_version inserted by the authority, even though the public closure DTO omits that
    // envelope field.
    let mut value = serde_json::to_value(closure)
        .map_err(|error| ApplyError::InvalidResponse(error.to_string()))?;
    value
        .as_object_mut()
        .ok_or_else(|| {
            ApplyError::InvalidResponse("Deployment closure is not an object".to_owned())
        })?
        .insert("schema_version".to_owned(), serde_json::json!(1));
    canonical_digest(&value)
        .map_err(|error| ApplyError::InvalidResponse(error.to_string()))?
        .parse()
        .map_err(|error| ApplyError::InvalidResponse(format!("Deployment digest: {error}")))
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
include!("apply_tests.rs");
