use crate::{
    canonical_digest, validate_capability_conformance_evidence,
    validate_capability_credential_requirements, validate_capability_interface_schema,
    validate_mcp_server_contract, validate_model_profile_contract,
    validate_model_provider_contract, ArtifactRef, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendKind,
    CapabilityBackendLimits, CapabilityCancellationKind, CapabilityDataFlowPolicy,
    CapabilityIdempotencyKind, CapabilityInterfaceLimits, CapabilityName,
    CapabilityProgressDurability, CapabilityProgressMode, ClosedJsonSchema, ClosedJsonValue,
    CodeTrustClass, ContextBackendBinding, ContextBackendKind, ContextBindingSnapshot,
    ContextCitationContract, ContextConsistencyMode, ContextDataPolicyContract,
    ContextDatasetGenerationSpec, ContextImplementationContract, ContextInterfaceLimits,
    ContextPaginationContract, ContextRankingContract, ContextWindowContract, DataRegion, Effect,
    InstalledModelAdapter, McpAuthPolicyDocument, McpProtocolPolicyDocument, McpServerLimits,
    McpTransportBinding, McpTransportKind, ModelArtifactDeliveryContract, ModelCatalogEvidence,
    ModelLimits, ModelModalities, ModelToolContract, ModelUsageContract, PolicyKind,
    PrincipalSnapshot, ProviderDataHandlingContract, ProviderModelIdentity, ProviderRequestLimits,
    ResourceId, ResourceKind, SandboxAbiVersion, SandboxCleanupPolicy, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxRuntimeFamily, SecretPurpose, Sha256Digest,
    StructuredOutputContract,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

pub const MAX_RESOURCE_DEPENDENCIES: usize = 512;
pub const MAX_RESOURCE_POLICIES: usize = 64;
pub const MAX_FROZEN_SLOTS: usize = 512;
pub const MAX_CODE_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryResourceKind {
    Agent,
    Skill,
    CapabilityInterface,
    CapabilityImplementation,
    ContextSourceInterface,
    ContextSourceImplementation,
    ContextDataset,
    McpServer,
    ModelProvider,
    ModelProfile,
    Policy,
    SandboxRuntime,
    SandboxPackage,
    SandboxProfile,
}

impl RegistryResourceKind {
    pub const ALL: &'static [Self] = &[
        Self::Agent,
        Self::Skill,
        Self::CapabilityInterface,
        Self::CapabilityImplementation,
        Self::ContextSourceInterface,
        Self::ContextSourceImplementation,
        Self::ContextDataset,
        Self::McpServer,
        Self::ModelProvider,
        Self::ModelProfile,
        Self::Policy,
        Self::SandboxRuntime,
        Self::SandboxPackage,
        Self::SandboxProfile,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Skill => "skill",
            Self::CapabilityInterface => "capability_interface",
            Self::CapabilityImplementation => "capability_implementation",
            Self::ContextSourceInterface => "context_source_interface",
            Self::ContextSourceImplementation => "context_source_implementation",
            Self::ContextDataset => "context_dataset",
            Self::McpServer => "mcp_server",
            Self::ModelProvider => "model_provider",
            Self::ModelProfile => "model_profile",
            Self::Policy => "policy",
            Self::SandboxRuntime => "sandbox_runtime",
            Self::SandboxPackage => "sandbox_package",
            Self::SandboxProfile => "sandbox_profile",
        }
    }

    pub const fn id_kind(self) -> ResourceKind {
        match self {
            Self::Agent => ResourceKind::Agent,
            Self::Skill => ResourceKind::Skill,
            Self::CapabilityInterface => ResourceKind::CapabilityInterface,
            Self::CapabilityImplementation => ResourceKind::CapabilityImplementation,
            Self::ContextSourceInterface => ResourceKind::ContextSourceInterface,
            Self::ContextSourceImplementation => ResourceKind::ContextSourceImplementation,
            Self::ContextDataset => ResourceKind::ContextDataset,
            Self::McpServer => ResourceKind::McpServer,
            Self::ModelProvider => ResourceKind::ModelProvider,
            Self::ModelProfile => ResourceKind::ModelProfile,
            Self::Policy => ResourceKind::Policy,
            Self::SandboxRuntime => ResourceKind::SandboxRuntime,
            Self::SandboxPackage => ResourceKind::SandboxPackage,
            Self::SandboxProfile => ResourceKind::SandboxProfile,
        }
    }

    pub const fn activation_target(self) -> ActivationTargetKind {
        match self {
            Self::Agent
            | Self::CapabilityInterface
            | Self::ContextSourceInterface
            | Self::McpServer
            | Self::ModelProvider
            | Self::ModelProfile => ActivationTargetKind::Deployment,
            _ => ActivationTargetKind::Version,
        }
    }

    pub const fn allows_version_kind(self, version_kind: ResourceKind) -> bool {
        matches!(
            (self, version_kind),
            (Self::Agent, ResourceKind::AgentInterfaceRevision)
                | (Self::Agent, ResourceKind::AgentPlanRevision)
                | (Self::Skill, ResourceKind::SkillRevision)
                | (
                    Self::CapabilityInterface,
                    ResourceKind::CapabilityInterfaceRevision
                )
                | (
                    Self::CapabilityImplementation,
                    ResourceKind::CapabilityImplementationRevision
                )
                | (
                    Self::ContextSourceInterface,
                    ResourceKind::ContextSourceInterfaceRevision
                )
                | (
                    Self::ContextSourceImplementation,
                    ResourceKind::ContextSourceImplementationRevision
                )
                | (Self::ContextDataset, ResourceKind::DatasetGeneration)
                | (Self::McpServer, ResourceKind::McpServerRevision)
                | (Self::ModelProvider, ResourceKind::ModelProviderRevision)
                | (Self::ModelProfile, ResourceKind::ModelProfileRevision)
                | (Self::Policy, ResourceKind::PolicyRevision)
                | (Self::SandboxRuntime, ResourceKind::SandboxRuntimeRevision)
                | (Self::SandboxPackage, ResourceKind::SandboxPackageRevision)
                | (Self::SandboxProfile, ResourceKind::SandboxProfileRevision)
        )
    }

    pub const fn deployment_kind(self) -> Option<ResourceKind> {
        match self {
            Self::Agent => Some(ResourceKind::AgentDeployment),
            Self::CapabilityInterface => Some(ResourceKind::CapabilityDeployment),
            Self::ContextSourceInterface => Some(ResourceKind::ContextDeployment),
            Self::McpServer => Some(ResourceKind::McpDeployment),
            Self::ModelProvider => Some(ResourceKind::ModelProviderDeployment),
            Self::ModelProfile => Some(ResourceKind::ModelDeployment),
            _ => None,
        }
    }
}

impl fmt::Display for RegistryResourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RegistryResourceKind {
    type Err = ResourceContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_str() == value)
            .ok_or_else(|| ResourceContractError::UnknownResourceKind(value.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationTargetKind {
    Version,
    Deployment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactVersionRef {
    pub revision_id: ResourceId,
    pub resource_kind: ResourceKind,
    pub semantic_digest: Sha256Digest,
}

impl ExactVersionRef {
    pub fn new(
        revision_id: ResourceId,
        semantic_digest: Sha256Digest,
    ) -> Result<Self, ResourceContractError> {
        if !revision_id.kind().is_revision() {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        Ok(Self {
            resource_kind: revision_id.kind(),
            revision_id,
            semantic_digest,
        })
    }

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if !self.revision_id.kind().is_revision() || self.revision_id.kind() != self.resource_kind {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDeploymentRef {
    pub deployment_id: ResourceId,
    pub resource_kind: ResourceKind,
    pub deployment_digest: Sha256Digest,
}

impl ExactDeploymentRef {
    pub fn new(
        deployment_id: ResourceId,
        deployment_digest: Sha256Digest,
    ) -> Result<Self, ResourceContractError> {
        if !deployment_id.kind().is_deployment() {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        Ok(Self {
            resource_kind: deployment_id.kind(),
            deployment_id,
            deployment_digest,
        })
    }

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if !self.deployment_id.kind().is_deployment()
            || self.deployment_id.kind() != self.resource_kind
        {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveTarget {
    Version { version: ExactVersionRef },
    Deployment { deployment: ExactDeploymentRef },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationFinding {
    pub code: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationSummary {
    pub validator_digest: Sha256Digest,
    pub validated_draft_digest: Sha256Digest,
    pub dependency_closure_digest: Sha256Digest,
    pub security_evidence_digest: Sha256Digest,
    pub warnings: Vec<ValidationFinding>,
}

impl ValidationSummary {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.warnings.len() > 256
            || self
                .warnings
                .iter()
                .any(|finding| !is_code(&finding.code) || finding.path.len() > 512)
        {
            return Err(ResourceContractError::UnboundedValue);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringPackage {
    pub artifact: ArtifactRef,
    pub manifest_digest: Sha256Digest,
}

impl AuthoringPackage {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        self.artifact
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

macro_rules! authoring_spec {
    ($name:ident { $($field:ident : $kind:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            pub authoring_package: AuthoringPackage,
            pub contract_digest: Sha256Digest,
            pub dependency_versions: Vec<ExactVersionRef>,
            pub policy_versions: Vec<ExactVersionRef>,
            $(pub $field: $kind),*
        }

        impl $name {
            fn validate(&self) -> Result<(), ResourceContractError> {
                self.authoring_package.validate()?;
                validate_exact_versions(&self.dependency_versions, MAX_RESOURCE_DEPENDENCIES)?;
                validate_policy_versions(&self.policy_versions)?;
                Ok(())
            }
        }
    };
}

authoring_spec!(AgentResourceSpec {
    interface_schema_digest: Sha256Digest,
    typed_plan_digest: Sha256Digest,
});
authoring_spec!(SkillResourceSpec {
    instruction_set_digest: Sha256Digest,
    requirement_set_digest: Sha256Digest,
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityProgressContract {
    pub mode: CapabilityProgressMode,
    pub schema_digest: Option<Sha256Digest>,
    pub max_events: u32,
    pub max_bytes_per_event: u32,
    pub minimum_interval_milliseconds: u64,
    pub durability: CapabilityProgressDurability,
}

impl CapabilityProgressContract {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        let valid = match self.mode {
            CapabilityProgressMode::None => {
                self.schema_digest.is_none()
                    && self.max_events == 0
                    && self.max_bytes_per_event == 0
                    && self.minimum_interval_milliseconds == 0
                    && self.durability == CapabilityProgressDurability::None
            }
            CapabilityProgressMode::Events => {
                self.schema_digest.is_some()
                    && self.max_events > 0
                    && self.max_bytes_per_event > 0
                    && self.minimum_interval_milliseconds > 0
                    && matches!(
                        self.durability,
                        CapabilityProgressDurability::LiveOnly
                            | CapabilityProgressDurability::CoarseDurable
                    )
            }
        };
        if !valid {
            return Err(ResourceContractError::InvalidCapabilityContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityBackendFeatures {
    pub deferred: bool,
    pub input_required: bool,
    pub callback: bool,
    pub poll: bool,
    pub progress: bool,
    pub cancellation: bool,
    pub max_remote_state_bytes: u32,
    pub max_poll_count: u32,
}

impl CapabilityBackendFeatures {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if (self.callback || self.poll) && !self.deferred
            || self.poll != (self.max_poll_count > 0)
            || self.deferred != (self.max_remote_state_bytes > 0)
            || self.max_remote_state_bytes > 1_048_576
            || self.max_poll_count > 10_000
        {
            return Err(ResourceContractError::InvalidCapabilityContract);
        }
        Ok(())
    }
}

authoring_spec!(CapabilityInterfaceResourceSpec {
    qualified_name: CapabilityName,
    input_schema: ClosedJsonSchema,
    output_schema: ClosedJsonSchema,
    error_schema: ClosedJsonSchema,
    artifacts: CapabilityArtifactContract,
    data_policy: CapabilityDataFlowPolicy,
    execution_limits: CapabilityInterfaceLimits,
    effect: Effect,
    idempotency: CapabilityIdempotencyKind,
    cancellation: CapabilityCancellationKind,
    progress: CapabilityProgressContract,
});
authoring_spec!(CapabilityImplementationResourceSpec {
    interface_revision: ExactVersionRef,
    backend_kind: CapabilityBackendKind,
    backend_contract: CapabilityBackendContract,
    backend_contract_digest: Sha256Digest,
    credential_requirements: Vec<SecretPurpose>,
    backend_limits: CapabilityBackendLimits,
    features: CapabilityBackendFeatures,
});
authoring_spec!(ContextInterfaceResourceSpec {
    query_schema_digest: Sha256Digest,
    filter_schema_digest: Sha256Digest,
    item_schema_digest: Sha256Digest,
    observation_schema_digest: Sha256Digest,
    allowed_consistency: Vec<ContextConsistencyMode>,
    citation: ContextCitationContract,
    pagination: ContextPaginationContract,
    ranking: ContextRankingContract,
    data_policy: ContextDataPolicyContract,
    limits: ContextInterfaceLimits,
});
authoring_spec!(ContextImplementationResourceSpec {
    interface_revision: ExactVersionRef,
    backend_kind: ContextBackendKind,
    contract: ContextImplementationContract,
});
authoring_spec!(ContextDatasetResourceSpec {
    generation: ContextDatasetGenerationSpec,
});
authoring_spec!(McpServerResourceSpec {
    transport: McpTransportKind,
    protocol_policy: ExactVersionRef,
    deployment_credential_requirements: Vec<SecretPurpose>,
    authorization_credential_purpose: Option<SecretPurpose>,
    limits: McpServerLimits,
});
authoring_spec!(ModelProviderResourceSpec {
    installed_adapter: InstalledModelAdapter,
    protocol_policy: ExactVersionRef,
    credential_requirements: Vec<SecretPurpose>,
    request_limits: ProviderRequestLimits,
});
authoring_spec!(ModelProfileResourceSpec {
    provider_revision: ExactVersionRef,
    model_identity: ProviderModelIdentity,
    modalities: ModelModalities,
    context: ContextWindowContract,
    tools: ModelToolContract,
    structured_output: StructuredOutputContract,
    parameter_schema_digest: Sha256Digest,
    artifact_delivery: ModelArtifactDeliveryContract,
    usage: ModelUsageContract,
    data_handling: ProviderDataHandlingContract,
    limits: ModelLimits,
    catalog_evidence: ModelCatalogEvidence,
});
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulingPolicyDocument {
    pub version: u16,
    pub weight: u16,
    pub burst: u16,
    pub aging_rounds: u16,
}

impl SchedulingPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.version != 1 || self.weight == 0 || self.burst == 0 || self.aging_rounds == 0 {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        let value =
            serde_json::to_value(self).map_err(|_| ResourceContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRetentionPolicy {
    pub version: u16,
    pub minimum_retention_seconds: u64,
    pub gc_grace_seconds: u64,
    pub tombstone_retention_seconds: u64,
    pub retain_provenance_sources: bool,
    pub delete_requires_approval: bool,
}

impl ArtifactRetentionPolicy {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.version != 1
            || self.minimum_retention_seconds > 3_155_760_000
            || !(1..=31_536_000).contains(&self.gc_grace_seconds)
            || !(1..=3_155_760_000).contains(&self.tombstone_retention_seconds)
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        self.validate()?;
        let value =
            serde_json::to_value(self).map_err(|_| ResourceContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyResourceSpec {
    pub authoring_package: AuthoringPackage,
    pub contract_digest: Sha256Digest,
    pub dependency_versions: Vec<ExactVersionRef>,
    pub policy_versions: Vec<ExactVersionRef>,
    pub policy_kind: PolicyKind,
    pub rules_digest: Sha256Digest,
    pub scheduling: Option<SchedulingPolicyDocument>,
    pub retention: Option<ArtifactRetentionPolicy>,
    pub mcp_protocol: Option<McpProtocolPolicyDocument>,
    pub mcp_auth: Option<Box<McpAuthPolicyDocument>>,
    pub sandbox_isolation: Option<crate::SandboxIsolationPolicyDocument>,
    pub sandbox_resource: Option<crate::SandboxResourcePolicyDocument>,
    pub sandbox_network: Option<crate::SandboxNetworkPolicyDocument>,
    pub sandbox_artifact_io: Option<crate::SandboxArtifactIoPolicyDocument>,
    pub sandbox_secret_resolution: Option<crate::SandboxSecretResolutionPolicyDocument>,
}

impl PolicyResourceSpec {
    fn validate(&self) -> Result<(), ResourceContractError> {
        self.authoring_package.validate()?;
        validate_exact_versions(&self.dependency_versions, MAX_RESOURCE_DEPENDENCIES)?;
        validate_policy_versions(&self.policy_versions)?;
        match (
            &self.policy_kind,
            &self.scheduling,
            &self.retention,
            &self.mcp_protocol,
            &self.mcp_auth,
            &self.sandbox_isolation,
            &self.sandbox_resource,
            &self.sandbox_network,
            &self.sandbox_artifact_io,
            &self.sandbox_secret_resolution,
        ) {
            (
                PolicyKind::Scheduling,
                Some(document),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ) => {
                document.validate()?;
                if document.canonical_digest()? != self.rules_digest {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            (
                PolicyKind::Retention,
                None,
                Some(document),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ) => {
                document.validate()?;
                if document.canonical_digest()? != self.rules_digest {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            (
                PolicyKind::Protocol,
                None,
                None,
                Some(document),
                None,
                None,
                None,
                None,
                None,
                None,
            ) => {
                document
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidPolicyDocument)?;
                if document
                    .canonical_digest()
                    .map_err(|_| ResourceContractError::InvalidPolicyDocument)?
                    != self.rules_digest
                {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            (
                PolicyKind::McpAuth,
                None,
                None,
                None,
                Some(document),
                None,
                None,
                None,
                None,
                None,
            ) => {
                document
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidPolicyDocument)?;
                if document
                    .canonical_digest()
                    .map_err(|_| ResourceContractError::InvalidPolicyDocument)?
                    != self.rules_digest
                {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            (
                PolicyKind::Isolation,
                None,
                None,
                None,
                None,
                Some(document),
                None,
                None,
                None,
                None,
            ) => validate_sandbox_policy(document, &self.rules_digest),
            (
                PolicyKind::Resource,
                None,
                None,
                None,
                None,
                None,
                Some(document),
                None,
                None,
                None,
            ) => validate_sandbox_policy(document, &self.rules_digest),
            (
                PolicyKind::Network,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(document),
                None,
                None,
            ) => validate_sandbox_policy(document, &self.rules_digest),
            (
                PolicyKind::ArtifactIo,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(document),
                None,
            ) => validate_sandbox_policy(document, &self.rules_digest),
            (
                PolicyKind::SecretResolution,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(document),
            ) => validate_sandbox_policy(document, &self.rules_digest),
            (
                PolicyKind::Scheduling
                | PolicyKind::Retention
                | PolicyKind::McpAuth
                | PolicyKind::Isolation
                | PolicyKind::Resource
                | PolicyKind::ArtifactIo
                | PolicyKind::SecretResolution,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
            )
            | (_, Some(_), _, _, _, _, _, _, _, _)
            | (_, _, Some(_), _, _, _, _, _, _, _)
            | (_, _, _, Some(_), _, _, _, _, _, _)
            | (_, _, _, _, Some(_), _, _, _, _, _)
            | (_, _, _, _, _, Some(_), _, _, _, _)
            | (_, _, _, _, _, _, Some(_), _, _, _)
            | (_, _, _, _, _, _, _, Some(_), _, _)
            | (_, _, _, _, _, _, _, _, Some(_), _)
            | (_, _, _, _, _, _, _, _, _, Some(_)) => {
                Err(ResourceContractError::InvalidPolicyDocument)
            }
            (_, None, None, None, None, None, None, None, None, None) => Ok(()),
        }
    }
}

trait SandboxPolicyDocument {
    fn validate_policy(&self) -> Result<(), ResourceContractError>;
    fn policy_digest(&self) -> Result<Sha256Digest, ResourceContractError>;
}

macro_rules! sandbox_policy_document {
    ($kind:ty) => {
        impl SandboxPolicyDocument for $kind {
            fn validate_policy(&self) -> Result<(), ResourceContractError> {
                self.validate()
            }

            fn policy_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
                self.canonical_digest()
            }
        }
    };
}

sandbox_policy_document!(crate::SandboxIsolationPolicyDocument);
sandbox_policy_document!(crate::SandboxResourcePolicyDocument);
sandbox_policy_document!(crate::SandboxNetworkPolicyDocument);
sandbox_policy_document!(crate::SandboxArtifactIoPolicyDocument);
sandbox_policy_document!(crate::SandboxSecretResolutionPolicyDocument);

fn validate_sandbox_policy<T: SandboxPolicyDocument>(
    document: &T,
    expected_digest: &Sha256Digest,
) -> Result<(), ResourceContractError> {
    document.validate_policy()?;
    if document.policy_digest()? != *expected_digest {
        return Err(ResourceContractError::InvalidPolicyDocument);
    }
    Ok(())
}
authoring_spec!(SandboxRuntimeResourceSpec {
    runtime_family: SandboxRuntimeFamily,
    runtime_version: String,
    image_or_module_digest: Sha256Digest,
    guest_kernel_digest: Option<Sha256Digest>,
    guest_agent_digest: Sha256Digest,
    supported_isolation: Vec<SandboxIsolationClass>,
    abi: SandboxAbiVersion,
    builtin_modules_manifest_digest: Sha256Digest,
    sbom_artifact: ArtifactRef,
    provenance_evidence: ArtifactRef,
    semantic_digest: Sha256Digest,
});
authoring_spec!(SandboxPackageResourceSpec {
    source_artifact: ArtifactRef,
    source_digest: Sha256Digest,
    runtime_revision: ExactVersionRef,
    entrypoint_kind: SandboxEntrypointKind,
    entrypoint: String,
    dependency_lock_digest: Sha256Digest,
    runtime_bundle_artifact: ArtifactRef,
    build_evidence: ArtifactRef,
    trust_class: CodeTrustClass,
    package_digest: Sha256Digest,
});
authoring_spec!(SandboxProfileResourceSpec {
    allowed_trust_classes: Vec<CodeTrustClass>,
    allowed_runtime_families: Vec<SandboxRuntimeFamily>,
    minimum_isolation: SandboxIsolationClass,
    isolation_policy: ExactVersionRef,
    resource_policy: ExactVersionRef,
    network_policy: ExactVersionRef,
    artifact_io_policy: ExactVersionRef,
    secret_policy: Option<ExactVersionRef>,
    cleanup: SandboxCleanupPolicy,
    max_job_duration_milliseconds: u64,
    semantic_digest: Sha256Digest,
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "resource_kind",
    content = "spec",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResourceDocument {
    Agent(AgentResourceSpec),
    Skill(SkillResourceSpec),
    CapabilityInterface(CapabilityInterfaceResourceSpec),
    CapabilityImplementation(CapabilityImplementationResourceSpec),
    ContextSourceInterface(ContextInterfaceResourceSpec),
    ContextSourceImplementation(ContextImplementationResourceSpec),
    ContextDataset(ContextDatasetResourceSpec),
    McpServer(McpServerResourceSpec),
    ModelProvider(ModelProviderResourceSpec),
    ModelProfile(Box<ModelProfileResourceSpec>),
    Policy(PolicyResourceSpec),
    SandboxRuntime(SandboxRuntimeResourceSpec),
    SandboxPackage(SandboxPackageResourceSpec),
    SandboxProfile(SandboxProfileResourceSpec),
}

impl ResourceDocument {
    pub const fn kind(&self) -> RegistryResourceKind {
        match self {
            Self::Agent(_) => RegistryResourceKind::Agent,
            Self::Skill(_) => RegistryResourceKind::Skill,
            Self::CapabilityInterface(_) => RegistryResourceKind::CapabilityInterface,
            Self::CapabilityImplementation(_) => RegistryResourceKind::CapabilityImplementation,
            Self::ContextSourceInterface(_) => RegistryResourceKind::ContextSourceInterface,
            Self::ContextSourceImplementation(_) => {
                RegistryResourceKind::ContextSourceImplementation
            }
            Self::ContextDataset(_) => RegistryResourceKind::ContextDataset,
            Self::McpServer(_) => RegistryResourceKind::McpServer,
            Self::ModelProvider(_) => RegistryResourceKind::ModelProvider,
            Self::ModelProfile(_) => RegistryResourceKind::ModelProfile,
            Self::Policy(_) => RegistryResourceKind::Policy,
            Self::SandboxRuntime(_) => RegistryResourceKind::SandboxRuntime,
            Self::SandboxPackage(_) => RegistryResourceKind::SandboxPackage,
            Self::SandboxProfile(_) => RegistryResourceKind::SandboxProfile,
        }
    }

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        match self {
            Self::Agent(spec) => spec.validate(),
            Self::Skill(spec) => spec.validate(),
            Self::CapabilityInterface(spec) => {
                spec.validate()?;
                CapabilityName::new(spec.qualified_name.as_str())
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                validate_capability_interface_schema(&spec.input_schema)
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                validate_capability_interface_schema(&spec.output_schema)
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                validate_capability_interface_schema(&spec.error_schema)
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                spec.artifacts
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                spec.data_policy
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                spec.execution_limits
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                if spec
                    .artifacts
                    .maximum_artifact_count()
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?
                    != spec.execution_limits.maximum_artifacts
                {
                    return Err(ResourceContractError::InvalidCapabilityContract);
                }
                spec.progress.validate()
            }
            Self::CapabilityImplementation(spec) => {
                spec.validate()?;
                spec.features.validate()?;
                require_kind(
                    &spec.interface_revision.revision_id,
                    ResourceKind::CapabilityInterfaceRevision,
                )?;
                spec.backend_limits
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                spec.backend_contract
                    .validate(&spec.features)
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                validate_capability_credential_requirements(&spec.credential_requirements)
                    .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
                if spec.backend_kind != spec.backend_contract.kind()
                    || spec.backend_contract_digest
                        != spec
                            .backend_contract
                            .canonical_digest()
                            .map_err(|_| ResourceContractError::Canonicalization)?
                {
                    return Err(ResourceContractError::InvalidCapabilityContract);
                }
                Ok(())
            }
            Self::ContextSourceInterface(spec) => {
                spec.validate()?;
                validate_unique_bounded(&spec.allowed_consistency, 4)?;
                spec.citation
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)?;
                spec.pagination
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)?;
                spec.ranking
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)?;
                spec.data_policy
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)?;
                spec.limits
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)
            }
            Self::ContextSourceImplementation(spec) => {
                spec.validate()?;
                require_kind(
                    &spec.interface_revision.revision_id,
                    ResourceKind::ContextSourceInterfaceRevision,
                )?;
                spec.contract
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)?;
                if spec.backend_kind != spec.contract.backend.kind() {
                    return Err(ResourceContractError::InvalidContextContract);
                }
                Ok(())
            }
            Self::ContextDataset(spec) => {
                spec.validate()?;
                spec.generation
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidContextContract)
            }
            Self::McpServer(spec) => {
                spec.validate()?;
                validate_mcp_server_contract(
                    spec.transport,
                    &spec.protocol_policy,
                    &spec.deployment_credential_requirements,
                    spec.authorization_credential_purpose.as_ref(),
                    &spec.limits,
                )
                .map_err(|_| ResourceContractError::MissingTransportBinding)
            }
            Self::ModelProvider(spec) => {
                spec.validate()?;
                require_kind(
                    &spec.protocol_policy.revision_id,
                    ResourceKind::PolicyRevision,
                )?;
                validate_model_provider_contract(
                    &spec.installed_adapter,
                    &spec.credential_requirements,
                    &spec.request_limits,
                )
                .map_err(|_| ResourceContractError::InvalidModelContract)
            }
            Self::ModelProfile(spec) => {
                spec.validate()?;
                validate_model_profile_contract(
                    &spec.provider_revision.revision_id,
                    &spec.model_identity,
                    &spec.modalities,
                    &spec.context,
                    &spec.tools,
                    &spec.structured_output,
                    &spec.artifact_delivery,
                    &spec.usage,
                    &spec.data_handling,
                    &spec.limits,
                    &spec.catalog_evidence,
                )
                .map_err(|_| ResourceContractError::InvalidModelContract)
            }
            Self::Policy(spec) => spec.validate(),
            Self::SandboxRuntime(spec) => {
                spec.validate()?;
                spec.sbom_artifact
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidArtifact)?;
                spec.provenance_evidence
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidArtifact)?;
                if !is_code(&spec.runtime_version)
                    || spec.supported_isolation.is_empty()
                    || spec.supported_isolation.len() > SandboxIsolationClass::ALL.len()
                    || !spec
                        .supported_isolation
                        .windows(2)
                        .all(|pair| pair[0].as_str() < pair[1].as_str())
                    || spec
                        .supported_isolation
                        .contains(&SandboxIsolationClass::MicroVm)
                        != spec.guest_kernel_digest.is_some()
                {
                    return Err(ResourceContractError::InvalidSandboxContract);
                }
                Ok(())
            }
            Self::SandboxPackage(spec) => {
                spec.validate()?;
                spec.source_artifact
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidArtifact)?;
                spec.runtime_bundle_artifact
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidArtifact)?;
                spec.build_evidence
                    .validate()
                    .map_err(|_| ResourceContractError::InvalidArtifact)?;
                require_kind(
                    &spec.runtime_revision.revision_id,
                    ResourceKind::SandboxRuntimeRevision,
                )?;
                if !is_relative_path(&spec.entrypoint)
                    || (spec.entrypoint_kind == SandboxEntrypointKind::ReviewedExecutable
                        && spec.trust_class != CodeTrustClass::ReviewedPublished)
                    || (spec.entrypoint_kind == SandboxEntrypointKind::ManagedMcpServer
                        && !matches!(
                            spec.trust_class,
                            CodeTrustClass::BuiltIn | CodeTrustClass::ReviewedPublished
                        ))
                {
                    return Err(ResourceContractError::InvalidSandboxContract);
                }
                Ok(())
            }
            Self::SandboxProfile(spec) => {
                spec.validate()?;
                let policies = [
                    &spec.isolation_policy,
                    &spec.resource_policy,
                    &spec.network_policy,
                    &spec.artifact_io_policy,
                ];
                validate_distinct_policy_roles(&policies)?;
                if let Some(secret_policy) = &spec.secret_policy {
                    let mut with_secret = policies.to_vec();
                    with_secret.push(secret_policy);
                    validate_distinct_policy_roles(&with_secret)?;
                }
                let declared_policies = spec
                    .policy_versions
                    .iter()
                    .map(exact_version_identity)
                    .collect::<BTreeSet<_>>();
                let mut role_policies = policies
                    .iter()
                    .map(|policy| exact_version_identity(policy))
                    .collect::<BTreeSet<_>>();
                role_policies.extend(spec.secret_policy.iter().map(exact_version_identity));
                if spec.allowed_trust_classes.is_empty()
                    || spec.allowed_trust_classes.len() > CodeTrustClass::ALL.len()
                    || !spec
                        .allowed_trust_classes
                        .windows(2)
                        .all(|pair| pair[0].as_str() < pair[1].as_str())
                    || spec.allowed_runtime_families.is_empty()
                    || spec.allowed_runtime_families.len() > SandboxRuntimeFamily::ALL.len()
                    || !spec
                        .allowed_runtime_families
                        .windows(2)
                        .all(|pair| pair[0].as_str() < pair[1].as_str())
                    || spec.max_job_duration_milliseconds == 0
                    || declared_policies != role_policies
                {
                    return Err(ResourceContractError::InvalidSandboxContract);
                }
                Ok(())
            }
        }
    }

    pub fn authoring_package(&self) -> &AuthoringPackage {
        match self {
            Self::Agent(spec) => &spec.authoring_package,
            Self::Skill(spec) => &spec.authoring_package,
            Self::CapabilityInterface(spec) => &spec.authoring_package,
            Self::CapabilityImplementation(spec) => &spec.authoring_package,
            Self::ContextSourceInterface(spec) => &spec.authoring_package,
            Self::ContextSourceImplementation(spec) => &spec.authoring_package,
            Self::ContextDataset(spec) => &spec.authoring_package,
            Self::McpServer(spec) => &spec.authoring_package,
            Self::ModelProvider(spec) => &spec.authoring_package,
            Self::ModelProfile(spec) => &spec.authoring_package,
            Self::Policy(spec) => &spec.authoring_package,
            Self::SandboxRuntime(spec) => &spec.authoring_package,
            Self::SandboxPackage(spec) => &spec.authoring_package,
            Self::SandboxProfile(spec) => &spec.authoring_package,
        }
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        let mut refs = Vec::new();
        macro_rules! common {
            ($spec:expr) => {{
                refs.extend($spec.dependency_versions.iter());
                refs.extend($spec.policy_versions.iter());
            }};
        }
        match self {
            Self::Agent(spec) => common!(spec),
            Self::Skill(spec) => common!(spec),
            Self::CapabilityInterface(spec) => common!(spec),
            Self::CapabilityImplementation(spec) => {
                common!(spec);
                refs.push(&spec.interface_revision);
            }
            Self::ContextSourceInterface(spec) => {
                common!(spec);
                refs.extend([
                    &spec.data_policy.entitlement_policy,
                    &spec.data_policy.cache_policy,
                ]);
            }
            Self::ContextSourceImplementation(spec) => {
                common!(spec);
                refs.push(&spec.interface_revision);
                if let crate::ContextBackendContract::McpResources { uri_policy, .. } =
                    &spec.contract.backend
                {
                    refs.push(uri_policy);
                }
            }
            Self::ContextDataset(spec) => {
                common!(spec);
                refs.extend([
                    &spec.generation.parser_profile,
                    &spec.generation.chunker_profile,
                    &spec.generation.ranking_profile,
                ]);
            }
            Self::McpServer(spec) => {
                common!(spec);
                refs.push(&spec.protocol_policy);
            }
            Self::ModelProvider(spec) => {
                common!(spec);
                refs.push(&spec.protocol_policy);
            }
            Self::ModelProfile(spec) => {
                common!(spec);
                refs.push(&spec.provider_revision);
            }
            Self::Policy(spec) => common!(spec),
            Self::SandboxRuntime(spec) => common!(spec),
            Self::SandboxPackage(spec) => {
                common!(spec);
                refs.push(&spec.runtime_revision);
            }
            Self::SandboxProfile(spec) => {
                common!(spec);
                refs.extend([
                    &spec.isolation_policy,
                    &spec.resource_policy,
                    &spec.network_policy,
                    &spec.artifact_io_policy,
                ]);
                refs.extend(spec.secret_policy.iter());
            }
        }
        refs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDraftPayload {
    pub display_name: String,
    pub document: ResourceDocument,
    pub validation: Option<ValidationSummary>,
}

impl ResourceDraftPayload {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if !is_name(&self.display_name, 255) {
            return Err(ResourceContractError::UnboundedValue);
        }
        self.document.validate()?;
        if let Some(validation) = &self.validation {
            validation.validate()?;
        }
        Ok(())
    }

    pub fn document_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        let value = serde_json::to_value(&self.document)
            .map_err(|_| ResourceContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedVersionPayload {
    pub document: ResourceDocument,
    pub validation: ValidationSummary,
}

impl PublishedVersionPayload {
    pub fn validate_for(
        &self,
        resource_kind: RegistryResourceKind,
        version_id: &ResourceId,
    ) -> Result<(), ResourceContractError> {
        self.document.validate()?;
        self.validation.validate()?;
        if self.document.kind() != resource_kind
            || !resource_kind.allows_version_kind(version_id.kind())
        {
            return Err(ResourceContractError::KindMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSlotBinding {
    pub slot_id: String,
    pub requirement_digest: Sha256Digest,
    pub target: FrozenSlotTarget,
    pub binding_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FrozenSlotTarget {
    Model {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactVersionRef,
    },
    Capability {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactVersionRef,
        tool_alias: Option<String>,
    },
    Context {
        binding: Box<ContextBindingSnapshot>,
    },
    ChildAgent {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactVersionRef,
    },
    Skill {
        candidates: Vec<ExactVersionRef>,
        selection_policy: ExactVersionRef,
    },
}

impl FrozenSlotBinding {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if !is_code(&self.slot_id) {
            return Err(ResourceContractError::InvalidCode);
        }
        match &self.target {
            FrozenSlotTarget::Model {
                candidates,
                selection_policy,
            } => {
                validate_deployments(candidates, ResourceKind::ModelDeployment)?;
                require_kind(&selection_policy.revision_id, ResourceKind::PolicyRevision)
            }
            FrozenSlotTarget::Capability {
                candidates,
                selection_policy,
                tool_alias,
            } => {
                validate_deployments(candidates, ResourceKind::CapabilityDeployment)?;
                require_kind(&selection_policy.revision_id, ResourceKind::PolicyRevision)?;
                if tool_alias.as_ref().is_some_and(|alias| !is_code(alias)) {
                    return Err(ResourceContractError::InvalidCode);
                }
                Ok(())
            }
            FrozenSlotTarget::Context { binding } => binding
                .validate()
                .map_err(|_| ResourceContractError::InvalidContextContract),
            FrozenSlotTarget::ChildAgent {
                candidates,
                selection_policy,
            } => {
                validate_deployments(candidates, ResourceKind::AgentDeployment)?;
                require_kind(&selection_policy.revision_id, ResourceKind::PolicyRevision)
            }
            FrozenSlotTarget::Skill {
                candidates,
                selection_policy,
            } => {
                validate_exact_versions(candidates, MAX_FROZEN_SLOTS)?;
                if candidates
                    .iter()
                    .any(|candidate| candidate.resource_kind != ResourceKind::SkillRevision)
                {
                    return Err(ResourceContractError::WrongResourceIdKind);
                }
                require_kind(&selection_policy.revision_id, ResourceKind::PolicyRevision)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "resource_kind",
    content = "bindings",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DeploymentClosure {
    Agent(AgentDeploymentClosure),
    CapabilityInterface(CapabilityDeploymentClosure),
    ContextSourceInterface(ContextDeploymentClosure),
    McpServer(McpDeploymentClosure),
    ModelProvider(ModelProviderDeploymentClosure),
    ModelProfile(ModelDeploymentClosure),
}

impl DeploymentClosure {
    pub const fn resource_kind(&self) -> RegistryResourceKind {
        match self {
            Self::Agent(_) => RegistryResourceKind::Agent,
            Self::CapabilityInterface(_) => RegistryResourceKind::CapabilityInterface,
            Self::ContextSourceInterface(_) => RegistryResourceKind::ContextSourceInterface,
            Self::McpServer(_) => RegistryResourceKind::McpServer,
            Self::ModelProvider(_) => RegistryResourceKind::ModelProvider,
            Self::ModelProfile(_) => RegistryResourceKind::ModelProfile,
        }
    }

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        match self {
            Self::Agent(closure) => closure.validate(),
            Self::CapabilityInterface(closure) => closure.validate(),
            Self::ContextSourceInterface(closure) => closure.validate(),
            Self::McpServer(closure) => closure.validate(),
            Self::ModelProvider(closure) => closure.validate(),
            Self::ModelProfile(closure) => closure.validate(),
        }
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        let mut refs = Vec::new();
        match self {
            Self::Agent(closure) => {
                refs.extend([
                    &closure.interface,
                    &closure.plan,
                    &closure.execution_profile,
                ]);
                refs.extend(closure.policies.iter());
                for slot in &closure.slots {
                    match &slot.target {
                        FrozenSlotTarget::Model {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::Capability {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::ChildAgent {
                            selection_policy, ..
                        } => {
                            refs.push(selection_policy);
                        }
                        FrozenSlotTarget::Skill {
                            candidates,
                            selection_policy,
                        } => {
                            refs.extend(candidates.iter());
                            refs.push(selection_policy);
                        }
                        FrozenSlotTarget::Context { binding } => {
                            refs.extend([&binding.authorization_policy, &binding.ranking_policy])
                        }
                    }
                }
            }
            Self::CapabilityInterface(closure) => {
                refs.extend([&closure.implementation, &closure.interface]);
                refs.extend(closure.backend.exact_version_refs());
                refs.extend(closure.policies.iter());
            }
            Self::ContextSourceInterface(closure) => {
                refs.extend([&closure.implementation, &closure.interface]);
                refs.extend([
                    &closure.parser_policy,
                    &closure.ranking_policy,
                    &closure.data_policy,
                ]);
                refs.extend(closure.network_policy.iter());
            }
            Self::McpServer(closure) => {
                refs.push(&closure.server_revision);
                refs.extend([&closure.protocol_policy, &closure.trust_policy]);
                refs.extend(closure.auth_policy.iter());
                refs.extend(closure.transport.exact_version_refs());
            }
            Self::ModelProvider(closure) => {
                refs.push(&closure.provider_revision);
                refs.extend([
                    &closure.protocol_policy,
                    &closure.network_policy,
                    &closure.tls_policy,
                    &closure.trust_policy,
                    &closure.data_policy,
                ]);
            }
            Self::ModelProfile(closure) => {
                refs.push(&closure.profile_revision);
                refs.extend([
                    &closure.data_policy,
                    &closure.budget_policy,
                    &closure.public_projection_policy,
                ]);
            }
        }
        refs
    }

    pub fn exact_deployment_refs(&self) -> Vec<&ExactDeploymentRef> {
        let mut refs = Vec::new();
        match self {
            Self::Agent(closure) => {
                for slot in &closure.slots {
                    match &slot.target {
                        FrozenSlotTarget::Model { candidates, .. }
                        | FrozenSlotTarget::Capability { candidates, .. }
                        | FrozenSlotTarget::ChildAgent { candidates, .. } => {
                            refs.extend(candidates.iter());
                        }
                        FrozenSlotTarget::Context { binding } => {
                            refs.push(&binding.context_deployment);
                        }
                        FrozenSlotTarget::Skill { .. } => {}
                    }
                }
            }
            Self::CapabilityInterface(closure) => {
                refs.extend(closure.backend.exact_deployment_refs())
            }
            Self::ModelProfile(closure) => refs.push(&closure.provider_deployment),
            Self::ContextSourceInterface(closure) => {
                if let ContextBackendBinding::McpResources { mcp_deployment, .. } = &closure.backend
                {
                    refs.push(mcp_deployment);
                }
            }
            Self::McpServer(_) | Self::ModelProvider(_) => {}
        }
        refs
    }

    pub fn exact_dataset_generation_refs(&self) -> Vec<&crate::ExactDatasetGenerationRef> {
        match self {
            Self::Agent(closure) => closure
                .slots
                .iter()
                .filter_map(|slot| match &slot.target {
                    FrozenSlotTarget::Context { binding } => match &binding.consistency {
                        crate::ContextConsistencyPolicy::PinnedGeneration { generation } => {
                            Some(generation)
                        }
                        crate::ContextConsistencyPolicy::PinAtRunAdmission { .. }
                        | crate::ContextConsistencyPolicy::LatestAtQueryStart { .. }
                        | crate::ContextConsistencyPolicy::ExternalObservation => None,
                    },
                    FrozenSlotTarget::Model { .. }
                    | FrozenSlotTarget::Capability { .. }
                    | FrozenSlotTarget::ChildAgent { .. }
                    | FrozenSlotTarget::Skill { .. } => None,
                })
                .collect(),
            Self::CapabilityInterface(_)
            | Self::ContextSourceInterface(_)
            | Self::McpServer(_)
            | Self::ModelProvider(_)
            | Self::ModelProfile(_) => Vec::new(),
        }
    }

    pub fn secret_bindings(&self) -> &[crate::ExactSecretBindingRef] {
        match self {
            Self::CapabilityInterface(closure) => &closure.secret_bindings,
            Self::ContextSourceInterface(closure) => &closure.secret_bindings,
            Self::McpServer(closure) => &closure.secret_bindings,
            Self::ModelProvider(closure) => &closure.secret_bindings,
            Self::Agent(_) | Self::ModelProfile(_) => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDeploymentClosure {
    pub interface: ExactVersionRef,
    pub plan: ExactVersionRef,
    pub slots: Vec<FrozenSlotBinding>,
    pub policies: Vec<ExactVersionRef>,
    pub execution_profile: ExactVersionRef,
}

impl AgentDeploymentClosure {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.interface.revision_id,
            ResourceKind::AgentInterfaceRevision,
        )?;
        require_kind(&self.plan.revision_id, ResourceKind::AgentPlanRevision)?;
        validate_policy_versions(&self.policies)?;
        require_kind(
            &self.execution_profile.revision_id,
            ResourceKind::PolicyRevision,
        )?;
        if self.slots.len() > MAX_FROZEN_SLOTS {
            return Err(ResourceContractError::UnboundedValue);
        }
        let mut slot_ids = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            if !slot_ids.insert(&slot.slot_id) {
                return Err(ResourceContractError::DuplicateValue);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDeploymentClosure {
    pub implementation: ExactVersionRef,
    pub interface: ExactVersionRef,
    pub backend: CapabilityBackendBinding,
    pub secret_bindings: Vec<crate::ExactSecretBindingRef>,
    pub policies: Vec<ExactVersionRef>,
    pub conformance_evidence: ArtifactRef,
}

impl CapabilityDeploymentClosure {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.implementation.revision_id,
            ResourceKind::CapabilityImplementationRevision,
        )?;
        require_kind(
            &self.interface.revision_id,
            ResourceKind::CapabilityInterfaceRevision,
        )?;
        self.backend
            .validate()
            .map_err(|_| ResourceContractError::InvalidCapabilityContract)?;
        validate_secret_bindings(&self.secret_bindings)?;
        validate_policy_versions(&self.policies)?;
        validate_capability_conformance_evidence(&self.conformance_evidence)
            .map_err(|_| ResourceContractError::InvalidCapabilityContract)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDeploymentClosure {
    pub implementation: ExactVersionRef,
    pub interface: ExactVersionRef,
    pub backend: ContextBackendBinding,
    pub secret_bindings: Vec<crate::ExactSecretBindingRef>,
    pub network_policy: Option<ExactVersionRef>,
    pub parser_policy: ExactVersionRef,
    pub ranking_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub conformance_evidence: ArtifactRef,
}

impl ContextDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.implementation.revision_id,
            ResourceKind::ContextSourceImplementationRevision,
        )?;
        require_kind(
            &self.interface.revision_id,
            ResourceKind::ContextSourceInterfaceRevision,
        )?;
        self.backend
            .validate()
            .map_err(|_| ResourceContractError::InvalidContextContract)?;
        validate_secret_bindings(&self.secret_bindings)?;
        let mut policies = vec![&self.parser_policy, &self.ranking_policy, &self.data_policy];
        policies.extend(self.network_policy.iter());
        validate_distinct_policy_roles(&policies)?;
        self.conformance_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDeploymentClosure {
    pub server_revision: ExactVersionRef,
    pub server_identity_digest: Sha256Digest,
    pub transport: McpTransportBinding,
    pub protocol_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub auth_policy: Option<ExactVersionRef>,
    pub secret_bindings: Vec<crate::ExactSecretBindingRef>,
    pub conformance_evidence: ArtifactRef,
}

impl McpDeploymentClosure {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.server_revision.revision_id,
            ResourceKind::McpServerRevision,
        )?;
        self.transport
            .validate()
            .map_err(|_| ResourceContractError::MissingTransportBinding)?;
        let mut policies = vec![&self.protocol_policy, &self.trust_policy];
        policies.extend(self.auth_policy.iter());
        for reference in self.transport.exact_version_refs() {
            if reference.resource_kind == ResourceKind::PolicyRevision {
                policies.push(reference);
            }
        }
        validate_distinct_policy_roles(&policies)?;
        validate_secret_bindings(&self.secret_bindings)?;
        self.conformance_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderDeploymentClosure {
    pub provider_revision: ExactVersionRef,
    pub endpoint_identity_digest: Sha256Digest,
    pub secret_bindings: Vec<crate::ExactSecretBindingRef>,
    pub protocol_policy: ExactVersionRef,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub region: DataRegion,
    pub conformance_evidence: ArtifactRef,
}

impl ModelProviderDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.provider_revision.revision_id,
            ResourceKind::ModelProviderRevision,
        )?;
        validate_secret_bindings(&self.secret_bindings)?;
        validate_distinct_policy_roles(&[
            &self.protocol_policy,
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.data_policy,
        ])?;
        self.conformance_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDeploymentClosure {
    pub profile_revision: ExactVersionRef,
    pub provider_deployment: ExactDeploymentRef,
    pub data_policy: ExactVersionRef,
    pub budget_policy: ExactVersionRef,
    pub public_projection_policy: ExactVersionRef,
    pub generation_defaults: ClosedJsonValue,
}

impl ModelDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.profile_revision.revision_id,
            ResourceKind::ModelProfileRevision,
        )?;
        if self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        validate_distinct_policy_roles(&[
            &self.data_policy,
            &self.budget_policy,
            &self.public_projection_policy,
        ])?;
        self.generation_defaults
            .validate()
            .map_err(|_| ResourceContractError::InvalidModelContract)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBindingsSnapshot {
    pub schema_version: u32,
    pub agent: ExactDeploymentRef,
    pub agent_interface: ExactVersionRef,
    pub plan: ExactVersionRef,
    pub principal: PrincipalSnapshot,
    pub slots: Vec<FrozenSlotBinding>,
    pub context_dataset_views: Vec<crate::RunContextDatasetView>,
    pub policies: Vec<ExactVersionRef>,
    pub execution_profile: ExactVersionRef,
    pub canonical_digest: Sha256Digest,
}

impl RunBindingsSnapshot {
    pub fn build(
        agent: ExactDeploymentRef,
        principal: PrincipalSnapshot,
        closure: &AgentDeploymentClosure,
    ) -> Result<Self, ResourceContractError> {
        Self::build_with_context_dataset_views(agent, principal, closure, Vec::new())
    }

    pub fn build_with_context_dataset_views(
        agent: ExactDeploymentRef,
        principal: PrincipalSnapshot,
        closure: &AgentDeploymentClosure,
        mut context_dataset_views: Vec<crate::RunContextDatasetView>,
    ) -> Result<Self, ResourceContractError> {
        if agent.resource_kind != ResourceKind::AgentDeployment {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        principal
            .validate()
            .map_err(|_| ResourceContractError::InvalidPrincipalSnapshot)?;
        closure.validate()?;
        let mut slots = closure.slots.clone();
        slots.sort_by(|left, right| left.slot_id.cmp(&right.slot_id));
        let mut policies = closure.policies.clone();
        policies.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
        context_dataset_views
            .sort_by(|left, right| left.context_binding_id.cmp(&right.context_binding_id));
        let unsigned = UnsignedRunBindingsSnapshot {
            schema_version: 1,
            agent: &agent,
            agent_interface: &closure.interface,
            plan: &closure.plan,
            principal: &principal,
            slots: &slots,
            context_dataset_views: &context_dataset_views,
            policies: &policies,
            execution_profile: &closure.execution_profile,
        };
        let value =
            serde_json::to_value(&unsigned).map_err(|_| ResourceContractError::Canonicalization)?;
        let digest = canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)?;
        let snapshot = Self {
            schema_version: 1,
            agent,
            agent_interface: closure.interface.clone(),
            plan: closure.plan.clone(),
            principal,
            slots,
            context_dataset_views,
            policies,
            execution_profile: closure.execution_profile.clone(),
            canonical_digest: digest,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        self.agent.validate()?;
        self.agent_interface.validate()?;
        self.plan.validate()?;
        self.execution_profile.validate()?;
        if self.schema_version != 1
            || self.agent.resource_kind != ResourceKind::AgentDeployment
            || self.agent_interface.resource_kind != ResourceKind::AgentInterfaceRevision
            || self.plan.resource_kind != ResourceKind::AgentPlanRevision
            || self.execution_profile.resource_kind != ResourceKind::PolicyRevision
        {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        self.principal
            .validate()
            .map_err(|_| ResourceContractError::InvalidPrincipalSnapshot)?;
        validate_slot_bindings(&self.slots)?;
        validate_run_context_dataset_views(&self.slots, &self.context_dataset_views)?;
        validate_policy_versions(&self.policies)?;
        if !self
            .slots
            .windows(2)
            .all(|pair| pair[0].slot_id < pair[1].slot_id)
            || !self
                .context_dataset_views
                .windows(2)
                .all(|pair| pair[0].context_binding_id < pair[1].context_binding_id)
            || !self
                .policies
                .windows(2)
                .all(|pair| pair[0].revision_id < pair[1].revision_id)
        {
            return Err(ResourceContractError::DuplicateValue);
        }
        let unsigned = UnsignedRunBindingsSnapshot {
            schema_version: self.schema_version,
            agent: &self.agent,
            agent_interface: &self.agent_interface,
            plan: &self.plan,
            principal: &self.principal,
            slots: &self.slots,
            context_dataset_views: &self.context_dataset_views,
            policies: &self.policies,
            execution_profile: &self.execution_profile,
        };
        let value =
            serde_json::to_value(&unsigned).map_err(|_| ResourceContractError::Canonicalization)?;
        let digest: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)?;
        if digest != self.canonical_digest {
            return Err(ResourceContractError::Canonicalization);
        }
        Ok(())
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        let mut references = vec![&self.agent_interface, &self.plan, &self.execution_profile];
        references.extend(self.policies.iter());
        for slot in &self.slots {
            match &slot.target {
                FrozenSlotTarget::Model {
                    selection_policy, ..
                }
                | FrozenSlotTarget::Capability {
                    selection_policy, ..
                }
                | FrozenSlotTarget::ChildAgent {
                    selection_policy, ..
                } => references.push(selection_policy),
                FrozenSlotTarget::Skill {
                    candidates,
                    selection_policy,
                } => {
                    references.extend(candidates.iter());
                    references.push(selection_policy);
                }
                FrozenSlotTarget::Context { binding } => {
                    references.extend([&binding.authorization_policy, &binding.ranking_policy])
                }
            }
        }
        references
    }

    pub fn exact_deployment_refs(&self) -> Vec<&ExactDeploymentRef> {
        let mut references = vec![&self.agent];
        for slot in &self.slots {
            match &slot.target {
                FrozenSlotTarget::Model { candidates, .. }
                | FrozenSlotTarget::Capability { candidates, .. }
                | FrozenSlotTarget::ChildAgent { candidates, .. } => {
                    references.extend(candidates.iter());
                }
                FrozenSlotTarget::Context { binding } => {
                    references.push(&binding.context_deployment);
                }
                FrozenSlotTarget::Skill { .. } => {}
            }
        }
        references
    }

    pub fn exact_dataset_generation_refs(&self) -> Vec<&crate::ExactDatasetGenerationRef> {
        let mut references = self
            .slots
            .iter()
            .filter_map(|slot| match &slot.target {
                FrozenSlotTarget::Context { binding } => match &binding.consistency {
                    crate::ContextConsistencyPolicy::PinnedGeneration { generation } => {
                        Some(generation)
                    }
                    crate::ContextConsistencyPolicy::PinAtRunAdmission { .. }
                    | crate::ContextConsistencyPolicy::LatestAtQueryStart { .. }
                    | crate::ContextConsistencyPolicy::ExternalObservation => None,
                },
                FrozenSlotTarget::Model { .. }
                | FrozenSlotTarget::Capability { .. }
                | FrozenSlotTarget::ChildAgent { .. }
                | FrozenSlotTarget::Skill { .. } => None,
            })
            .collect::<Vec<_>>();
        references.extend(
            self.context_dataset_views
                .iter()
                .map(|view| &view.generation),
        );
        references.sort_by(|left, right| left.generation_id.cmp(&right.generation_id));
        references.dedup_by(|left, right| left.generation_id == right.generation_id);
        references
    }
}

#[derive(Serialize)]
struct UnsignedRunBindingsSnapshot<'a> {
    schema_version: u32,
    agent: &'a ExactDeploymentRef,
    agent_interface: &'a ExactVersionRef,
    plan: &'a ExactVersionRef,
    principal: &'a PrincipalSnapshot,
    slots: &'a [FrozenSlotBinding],
    context_dataset_views: &'a [crate::RunContextDatasetView],
    policies: &'a [ExactVersionRef],
    execution_profile: &'a ExactVersionRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceContractError {
    UnknownResourceKind(String),
    WrongResourceIdKind,
    KindMismatch,
    InvalidArtifact,
    InvalidCode,
    InvalidEntrypoint,
    InvalidCapabilityContract,
    InvalidContextContract,
    InvalidModelContract,
    InvalidSandboxContract,
    InvalidPolicyDocument,
    InvalidPrincipalSnapshot,
    InvalidSecretBinding,
    MissingTransportBinding,
    DuplicateValue,
    UnboundedValue,
    Canonicalization,
}

impl fmt::Display for ResourceContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownResourceKind(kind) => write!(formatter, "unknown resource kind {kind:?}"),
            Self::WrongResourceIdKind => formatter.write_str("resource ID has the wrong kind"),
            Self::KindMismatch => {
                formatter.write_str("resource, document, version, or deployment kind mismatch")
            }
            Self::InvalidArtifact => formatter.write_str("authoring artifact is invalid"),
            Self::InvalidCode => formatter.write_str("bounded code is invalid"),
            Self::InvalidEntrypoint => {
                formatter.write_str("sandbox entrypoint is not a safe relative path")
            }
            Self::InvalidCapabilityContract => {
                formatter.write_str("Capability contract has an invalid closed shape")
            }
            Self::InvalidContextContract => {
                formatter.write_str("Context contract has an invalid closed shape")
            }
            Self::InvalidModelContract => {
                formatter.write_str("Model contract has an invalid closed shape")
            }
            Self::InvalidSandboxContract => {
                formatter.write_str("Sandbox contract has an invalid closed shape")
            }
            Self::InvalidPolicyDocument => {
                formatter.write_str("policy document does not match its closed policy kind")
            }
            Self::InvalidPrincipalSnapshot => {
                formatter.write_str("embedded principal snapshot is invalid")
            }
            Self::InvalidSecretBinding => {
                formatter.write_str("exact secret binding reference is invalid")
            }
            Self::MissingTransportBinding => {
                formatter.write_str("MCP transport binding is incomplete")
            }
            Self::DuplicateValue => formatter.write_str("canonical set contains a duplicate"),
            Self::UnboundedValue => formatter.write_str("resource value exceeds its hard bound"),
            Self::Canonicalization => {
                formatter.write_str("resource snapshot canonicalization failed")
            }
        }
    }
}

impl Error for ResourceContractError {}

fn validate_exact_versions(
    values: &[ExactVersionRef],
    maximum: usize,
) -> Result<(), ResourceContractError> {
    if values.len() > maximum {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut identities = BTreeSet::new();
    for value in values {
        value.validate()?;
        if !identities.insert(value.revision_id.to_string()) {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    Ok(())
}

fn validate_policy_versions(values: &[ExactVersionRef]) -> Result<(), ResourceContractError> {
    validate_exact_versions(values, MAX_RESOURCE_POLICIES)?;
    if values
        .iter()
        .any(|value| value.resource_kind != ResourceKind::PolicyRevision)
    {
        return Err(ResourceContractError::WrongResourceIdKind);
    }
    Ok(())
}

fn exact_version_identity(value: &ExactVersionRef) -> (String, String) {
    (
        value.revision_id.to_string(),
        value.semantic_digest.to_string(),
    )
}

fn validate_distinct_policy_roles(
    values: &[&ExactVersionRef],
) -> Result<(), ResourceContractError> {
    if values.is_empty() || values.len() > MAX_RESOURCE_POLICIES {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut identities = BTreeSet::new();
    for value in values {
        value.validate()?;
        if value.resource_kind != ResourceKind::PolicyRevision {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        if !identities.insert(value.revision_id.to_string()) {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    Ok(())
}

fn validate_slot_bindings(values: &[FrozenSlotBinding]) -> Result<(), ResourceContractError> {
    if values.len() > MAX_FROZEN_SLOTS {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut slot_ids = BTreeSet::new();
    for value in values {
        value.validate()?;
        if !slot_ids.insert(value.slot_id.as_str()) {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    Ok(())
}

fn validate_run_context_dataset_views(
    slots: &[FrozenSlotBinding],
    views: &[crate::RunContextDatasetView],
) -> Result<(), ResourceContractError> {
    if views.len() > MAX_FROZEN_SLOTS
        || !views
            .windows(2)
            .all(|pair| pair[0].context_binding_id < pair[1].context_binding_id)
    {
        return Err(ResourceContractError::DuplicateValue);
    }
    for view in views {
        let binding = slots.iter().find_map(|slot| match &slot.target {
            FrozenSlotTarget::Context { binding }
                if binding.context_binding_id == view.context_binding_id =>
            {
                Some(binding.as_ref())
            }
            FrozenSlotTarget::Model { .. }
            | FrozenSlotTarget::Capability { .. }
            | FrozenSlotTarget::Context { .. }
            | FrozenSlotTarget::ChildAgent { .. }
            | FrozenSlotTarget::Skill { .. } => None,
        });
        view.validate_for(binding.ok_or(ResourceContractError::InvalidContextContract)?)
            .map_err(|_| ResourceContractError::InvalidContextContract)?;
    }
    for binding in slots.iter().filter_map(|slot| match &slot.target {
        FrozenSlotTarget::Context { binding } => Some(binding.as_ref()),
        FrozenSlotTarget::Model { .. }
        | FrozenSlotTarget::Capability { .. }
        | FrozenSlotTarget::ChildAgent { .. }
        | FrozenSlotTarget::Skill { .. } => None,
    }) {
        if matches!(
            binding.consistency,
            crate::ContextConsistencyPolicy::PinAtRunAdmission { .. }
        ) && !views
            .iter()
            .any(|view| view.context_binding_id == binding.context_binding_id)
        {
            return Err(ResourceContractError::InvalidContextContract);
        }
    }
    Ok(())
}

fn validate_deployments(
    values: &[ExactDeploymentRef],
    expected: ResourceKind,
) -> Result<(), ResourceContractError> {
    if values.is_empty() || values.len() > MAX_FROZEN_SLOTS {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut identities = BTreeSet::new();
    for value in values {
        value.validate()?;
        if value.resource_kind != expected {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        if !identities.insert(value.deployment_id.to_string()) {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    Ok(())
}

fn validate_secret_bindings(
    values: &[crate::ExactSecretBindingRef],
) -> Result<(), ResourceContractError> {
    if values.len() > MAX_RESOURCE_POLICIES {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut identities = BTreeSet::new();
    let mut purposes = BTreeSet::new();
    for value in values {
        value
            .validate()
            .map_err(|_| ResourceContractError::InvalidSecretBinding)?;
        if !identities.insert(value.secret_binding_id.to_string())
            || !purposes.insert(value.purpose.clone())
        {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    if !values.windows(2).all(|pair| {
        (&pair[0].purpose, &pair[0].secret_binding_id)
            < (&pair[1].purpose, &pair[1].secret_binding_id)
    }) {
        return Err(ResourceContractError::DuplicateValue);
    }
    Ok(())
}

fn validate_unique_bounded<T: Ord>(
    values: &[T],
    maximum: usize,
) -> Result<(), ResourceContractError> {
    if values.is_empty() || values.len() > maximum {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut unique = BTreeSet::new();
    for value in values {
        if !unique.insert(value) {
            return Err(ResourceContractError::DuplicateValue);
        }
    }
    Ok(())
}

fn require_kind(value: &ResourceId, expected: ResourceKind) -> Result<(), ResourceContractError> {
    if value.kind() != expected {
        return Err(ResourceContractError::WrongResourceIdKind);
    }
    Ok(())
}

fn is_name(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum * 4
        && value.chars().count() <= maximum
        && !value.chars().any(char::is_control)
}

fn is_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CODE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_relative_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !value.starts_with('/')
        && !value.starts_with('\\')
        && !value.contains('\0')
        && !value.split(['/', '\\']).any(|segment| segment == "..")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CapabilityArtifactContract, CapabilityArtifactDirection, CapabilityArtifactPort,
        CapabilityDataFlowPolicy, ClosedJsonSchema, DataClassification, DataRegion,
        ExactSecretBindingRef, Permission, PermissionSet, PrincipalKind, SecretResolutionPolicy,
    };
    use serde_json::json;

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn secret_binding(suffix: &str, purpose: &str) -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id(&format!("sbd_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
            1,
            id(&format!("spr_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
            purpose.parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest('a'),
            },
        )
        .unwrap()
    }

    #[test]
    fn exact_secret_bindings_are_sorted_and_purpose_unique() {
        let api = secret_binding("7b85", "provider.api_key");
        let oauth = secret_binding("7b86", "provider.oauth");
        assert!(validate_secret_bindings(&[api.clone(), oauth.clone()]).is_ok());
        assert_eq!(
            validate_secret_bindings(&[oauth, api.clone()]),
            Err(ResourceContractError::DuplicateValue)
        );
        let duplicate_purpose = secret_binding("7b87", "provider.api_key");
        assert_eq!(
            validate_secret_bindings(&[api, duplicate_purpose]),
            Err(ResourceContractError::DuplicateValue)
        );
    }

    #[test]
    fn registry_kind_matrix_is_closed() {
        assert_eq!(RegistryResourceKind::ALL.len(), 14);
        for kind in RegistryResourceKind::ALL {
            assert_eq!(kind.id_kind().descriptor().name, kind.as_str());
        }
        assert_eq!(
            RegistryResourceKind::Agent.activation_target(),
            ActivationTargetKind::Deployment
        );
        assert_eq!(
            RegistryResourceKind::Skill.activation_target(),
            ActivationTargetKind::Version
        );
        assert!(
            RegistryResourceKind::Agent.allows_version_kind(ResourceKind::AgentInterfaceRevision)
        );
        assert!(!RegistryResourceKind::Agent.allows_version_kind(ResourceKind::SkillRevision));
    }

    #[test]
    fn run_bindings_digest_is_stable() {
        let closure = AgentDeploymentClosure {
            interface: ExactVersionRef::new(
                id("aif_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
                digest('b'),
            )
            .unwrap(),
            plan: ExactVersionRef::new(
                id("arev_0198f1c3-8f49-7c3e-b1f3-773c28367b92"),
                digest('c'),
            )
            .unwrap(),
            slots: vec![],
            policies: vec![],
            execution_profile: ExactVersionRef::new(
                id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367b93"),
                digest('d'),
            )
            .unwrap(),
        };
        let snapshot = RunBindingsSnapshot::build(
            ExactDeploymentRef::new(id("adep_0198f1c3-8f49-7c3e-b1f3-773c28367b94"), digest('e'))
                .unwrap(),
            PrincipalSnapshot::build(
                id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367b95"),
                id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367b96"),
                PrincipalKind::AgentRunner,
                PermissionSet::new(vec![Permission::AgentRun]).unwrap(),
                1,
                1,
                1,
            )
            .unwrap(),
            &closure,
        )
        .unwrap();
        assert_eq!(snapshot.schema_version, 1);
        assert!(snapshot.canonical_digest.as_str().starts_with("sha256:"));
        snapshot.validate().unwrap();
        let mut forged = snapshot;
        forged.canonical_digest = digest('0');
        assert_eq!(
            forged.validate(),
            Err(ResourceContractError::Canonicalization)
        );
    }

    #[test]
    fn pin_at_run_admission_requires_one_exact_dataset_view() {
        let agent_deployment_id = id("adep_0198f1c3-8f49-7c3e-b1f3-773c28367c01");
        let context_deployment =
            ExactDeploymentRef::new(id("xdep_0198f1c3-8f49-7c3e-b1f3-773c28367c02"), digest('1'))
                .unwrap();
        let authorization_policy =
            ExactVersionRef::new(id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367c03"), digest('2'))
                .unwrap();
        let ranking_policy =
            ExactVersionRef::new(id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367c04"), digest('3'))
                .unwrap();
        let dataset_id = id("dset_0198f1c3-8f49-7c3e-b1f3-773c28367c05");
        let binding = ContextBindingSnapshot::build(
            id("xcb_0198f1c3-8f49-7c3e-b1f3-773c28367c06"),
            agent_deployment_id.clone(),
            context_deployment,
            crate::ContextConsistencyPolicy::PinAtRunAdmission {
                dataset_id: dataset_id.clone(),
            },
            vec!["customer_id".to_owned()],
            authorization_policy.clone(),
            ranking_policy.clone(),
        )
        .unwrap();
        let closure = AgentDeploymentClosure {
            interface: ExactVersionRef::new(
                id("aif_0198f1c3-8f49-7c3e-b1f3-773c28367c07"),
                digest('4'),
            )
            .unwrap(),
            plan: ExactVersionRef::new(
                id("arev_0198f1c3-8f49-7c3e-b1f3-773c28367c08"),
                digest('5'),
            )
            .unwrap(),
            slots: vec![FrozenSlotBinding {
                slot_id: "catalog".to_owned(),
                requirement_digest: digest('6'),
                target: FrozenSlotTarget::Context {
                    binding: Box::new(binding.clone()),
                },
                binding_digest: digest('7'),
            }],
            policies: vec![authorization_policy, ranking_policy],
            execution_profile: ExactVersionRef::new(
                id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367c09"),
                digest('8'),
            )
            .unwrap(),
        };
        let agent = ExactDeploymentRef::new(agent_deployment_id, digest('9')).unwrap();
        let principal = PrincipalSnapshot::build(
            id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367c0a"),
            id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367c0b"),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::ContextQuery]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap();

        assert_eq!(
            RunBindingsSnapshot::build(agent.clone(), principal.clone(), &closure),
            Err(ResourceContractError::InvalidContextContract)
        );

        let view = crate::RunContextDatasetView {
            context_binding_id: binding.context_binding_id.clone(),
            context_binding_digest: binding.binding_digest.clone(),
            generation: crate::ExactDatasetGenerationRef {
                dataset_id,
                generation_id: id("dgen_0198f1c3-8f49-7c3e-b1f3-773c28367c0c"),
                generation_digest: digest('a'),
            },
        };
        let snapshot = RunBindingsSnapshot::build_with_context_dataset_views(
            agent.clone(),
            principal.clone(),
            &closure,
            vec![view.clone()],
        )
        .unwrap();
        snapshot.validate().unwrap();
        assert_eq!(snapshot.context_dataset_views, vec![view.clone()]);

        let mut forged = view;
        forged.context_binding_digest = digest('0');
        assert_eq!(
            RunBindingsSnapshot::build_with_context_dataset_views(
                agent,
                principal,
                &closure,
                vec![forged],
            ),
            Err(ResourceContractError::InvalidContextContract)
        );
    }

    #[test]
    fn exact_refs_and_unknown_fields_fail_closed() {
        assert!(
            ExactVersionRef::new(id("adep_0198f1c3-8f49-7c3e-b1f3-773c28367b94"), digest('a'))
                .is_err()
        );
        let value = serde_json::json!({
            "display_name": "x",
            "document": {
                "resource_kind": "policy",
                "spec": {
                    "authoring_package": {
                        "artifact": {
                            "artifact_id": "art_0198f1c3-8f49-7c3e-b1f3-773c28367b90",
                            "content_digest": digest('a'),
                            "byte_length": 1,
                            "media_type": "application/json",
                            "classification": "internal",
                            "display_name": null
                        },
                        "manifest_digest": digest('b')
                    },
                    "contract_digest": digest('c'),
                    "dependency_versions": [],
                    "policy_versions": [],
                    "policy_kind": "authorization",
                    "rules_digest": digest('d'),
                    "unexpected": true
                }
            },
            "validation": null
        });
        assert!(serde_json::from_value::<ResourceDraftPayload>(value).is_err());
    }

    #[test]
    fn artifact_retention_policy_is_closed_and_rules_digest_bound() {
        let retention = ArtifactRetentionPolicy {
            version: 1,
            minimum_retention_seconds: 3_600,
            gc_grace_seconds: 86_400,
            tombstone_retention_seconds: 2_592_000,
            retain_provenance_sources: true,
            delete_requires_approval: true,
        };
        let rules_digest = retention.canonical_digest().unwrap();
        let spec = PolicyResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: ArtifactRef::new(
                    id("art_0198f1c3-8f49-7c3e-b1f3-773c28367ba0"),
                    digest('a'),
                    16,
                    "application/json",
                    DataClassification::Internal,
                    Some("retention-policy.json".to_owned()),
                )
                .unwrap(),
                manifest_digest: digest('b'),
            },
            contract_digest: digest('c'),
            dependency_versions: vec![],
            policy_versions: vec![],
            policy_kind: PolicyKind::Retention,
            rules_digest,
            scheduling: None,
            retention: Some(retention.clone()),
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        };
        spec.validate().unwrap();

        let mut forged = spec.clone();
        forged.rules_digest = digest('0');
        assert_eq!(
            forged.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );

        let mut unbounded = retention;
        unbounded.gc_grace_seconds = 0;
        assert_eq!(
            unbounded.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
    }

    #[test]
    fn shared_network_policy_allows_generic_or_sandbox_typed_documents() {
        let mut spec = PolicyResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: ArtifactRef::new(
                    id("art_0198f1c3-8f49-7c3e-b1f3-773c28367ba1"),
                    digest('a'),
                    16,
                    "application/json",
                    DataClassification::Internal,
                    Some("network-policy.json".to_owned()),
                )
                .unwrap(),
                manifest_digest: digest('b'),
            },
            contract_digest: digest('c'),
            dependency_versions: vec![],
            policy_versions: vec![],
            policy_kind: PolicyKind::Network,
            rules_digest: digest('d'),
            scheduling: None,
            retention: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        };
        spec.validate().unwrap();

        let sandbox_network = crate::SandboxNetworkPolicyDocument {
            schema_version: 1,
            destinations: vec![],
            maximum_redirects: 0,
            maximum_dns_answers: 8,
            maximum_response_bytes: 65_536,
            require_https: true,
            require_tls12_or_newer: true,
            deny_private_addresses: true,
            deny_link_local_addresses: true,
            deny_metadata_addresses: true,
            deny_proxy_environment: true,
            deny_connect_tunnel: true,
            deny_listen: true,
            deny_udp: true,
        };
        spec.rules_digest = sandbox_network.canonical_digest().unwrap();
        spec.sandbox_network = Some(sandbox_network);
        spec.validate().unwrap();

        spec.rules_digest = digest('0');
        assert_eq!(
            spec.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
    }

    #[test]
    fn capability_interface_resource_keeps_full_schema_and_contract_closure() {
        let schema = ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "value": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 100,
                    "description": "bounded value",
                    "x-platform-classification": "internal"
                }
            },
            "required": ["value"],
            "additionalProperties": false
        }))
        .unwrap();
        let artifact = ArtifactRef::new(
            id("art_0198f1c3-8f49-7c3e-b1f3-773c28367bb0"),
            digest('a'),
            16,
            "application/json",
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let spec = CapabilityInterfaceResourceSpec {
            authoring_package: AuthoringPackage {
                artifact,
                manifest_digest: digest('b'),
            },
            contract_digest: digest('c'),
            dependency_versions: vec![],
            policy_versions: vec![],
            qualified_name: CapabilityName::new("fixture.validate").unwrap(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            error_schema: schema,
            artifacts: CapabilityArtifactContract {
                ports: vec![CapabilityArtifactPort {
                    name: "source".to_owned(),
                    direction: CapabilityArtifactDirection::Input,
                    media_types: vec!["application/json".to_owned()],
                    maximum_count: 1,
                    maximum_single_bytes: 1_024,
                    maximum_total_bytes: 1_024,
                    maximum_classification: DataClassification::Internal,
                }],
            },
            data_policy: CapabilityDataFlowPolicy {
                maximum_input_classification: DataClassification::Internal,
                maximum_output_classification: DataClassification::Internal,
                allowed_regions: vec!["cn-east-1".parse::<DataRegion>().unwrap()],
                declassification_policy: None,
            },
            execution_limits: CapabilityInterfaceLimits {
                maximum_input_bytes: 1_024,
                maximum_output_bytes: 1_024,
                maximum_artifacts: 1,
                maximum_execution_milliseconds: 1_000,
            },
            effect: Effect::Pure,
            idempotency: CapabilityIdempotencyKind::Intrinsic,
            cancellation: CapabilityCancellationKind::BestEffort,
            progress: CapabilityProgressContract {
                mode: CapabilityProgressMode::None,
                schema_digest: None,
                max_events: 0,
                max_bytes_per_event: 0,
                minimum_interval_milliseconds: 0,
                durability: CapabilityProgressDurability::None,
            },
        };
        ResourceDocument::CapabilityInterface(spec.clone())
            .validate()
            .unwrap();

        let mut mismatched = spec;
        mismatched.execution_limits.maximum_artifacts = 0;
        assert_eq!(
            ResourceDocument::CapabilityInterface(mismatched).validate(),
            Err(ResourceContractError::InvalidCapabilityContract)
        );
    }
}
