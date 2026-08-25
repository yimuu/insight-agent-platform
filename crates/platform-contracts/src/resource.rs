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
    McpTransportBinding, McpTransportKind, ModelCatalogEvidence, ModelLimits, ModelModalities,
    ModelToolContract, ModelUsageContract, PlanNodeKind, PolicyKind, PrincipalSnapshot,
    ProviderDataHandlingContract, ProviderModelIdentity, ProviderRequestLimits, ResourceId,
    ResourceKind, SandboxAbiVersion, SandboxCleanupPolicy, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxRuntimeFamily, SecretPurpose, Sha256Digest,
    SkillInstructionAudience, SkillInstructionPhase, SkillPackageEntryKind,
    StructuredOutputContract,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

pub const MAX_RESOURCE_DEPENDENCIES: usize = 512;
pub const MAX_RESOURCE_POLICIES: usize = 64;
pub const MAX_FROZEN_SLOTS: usize = 512;
pub const MAX_CODE_BYTES: usize = 128;
pub const MAX_SANDBOX_RUNTIME_BUNDLE_BYTES: u64 = 67_108_864;
pub const MAX_SKILL_PACKAGE_ENTRIES: usize = 512;
pub const MAX_SKILL_INSTRUCTION_SECTIONS: usize = 128;
pub const MAX_SKILL_REQUIREMENTS: usize = 256;
pub const MAX_SKILL_PURPOSE_CHARS: usize = 1_024;
pub const MAX_SKILL_PURPOSE_BYTES: usize = 4_096;
pub const MAX_SKILL_MEDIA_TYPE_BYTES: usize = 128;
pub const MAX_SKILL_PACKAGE_BYTES: u64 = 16_777_216;
pub const MAX_SKILL_INSTRUCTION_TOKENS: u32 = 131_072;

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
            | Self::Skill
            | Self::CapabilityInterface
            | Self::ContextSourceInterface
            | Self::McpServer
            | Self::ModelProvider
            | Self::ModelProfile
            | Self::Policy
            | Self::SandboxProfile => ActivationTargetKind::Deployment,
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
            Self::Skill => Some(ResourceKind::SkillDeployment),
            Self::CapabilityInterface => Some(ResourceKind::CapabilityDeployment),
            Self::ContextSourceInterface => Some(ResourceKind::ContextDeployment),
            Self::McpServer => Some(ResourceKind::McpDeployment),
            Self::ModelProvider => Some(ResourceKind::ModelProviderDeployment),
            Self::ModelProfile => Some(ResourceKind::ModelDeployment),
            Self::Policy => Some(ResourceKind::PolicyDeployment),
            Self::SandboxProfile => Some(ResourceKind::SandboxProfileDeployment),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceSpec {
    pub authoring_package: AuthoringPackage,
    pub contract_digest: Sha256Digest,
    pub dependency_versions: Vec<ExactVersionRef>,
    pub policy_versions: Vec<ExactVersionRef>,
    pub input_schema: ClosedJsonSchema,
    pub output_schema: ClosedJsonSchema,
    pub error_schema: ClosedJsonSchema,
    pub typed_plan_artifact_id: ResourceId,
    pub typed_plan_digest: Sha256Digest,
}

impl AgentResourceSpec {
    fn validate(&self) -> Result<(), ResourceContractError> {
        self.authoring_package.validate()?;
        validate_exact_versions(&self.dependency_versions, MAX_RESOURCE_DEPENDENCIES)?;
        validate_policy_versions(&self.policy_versions)?;
        validate_agent_interface_schema(&self.input_schema, false)?;
        validate_agent_interface_schema(&self.output_schema, false)?;
        validate_agent_interface_schema(&self.error_schema, true)?;
        require_kind(&self.typed_plan_artifact_id, ResourceKind::Artifact)
    }
}

fn validate_agent_interface_schema(
    schema: &ClosedJsonSchema,
    allow_failure_nominal: bool,
) -> Result<(), ResourceContractError> {
    schema
        .validate()
        .map_err(|_| ResourceContractError::InvalidAgentContract)?;
    let object_root = schema
        .schema
        .get("type")
        .and_then(serde_json::Value::as_str)
        == Some("object");
    let failure_root = allow_failure_nominal
        && schema
            .schema
            .get("$ref")
            .and_then(serde_json::Value::as_str)
            == crate::pinned_nominal_reference("Failure").as_deref();
    if !object_root && !failure_root {
        return Err(ResourceContractError::InvalidAgentContract);
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillInterface {
    pub qualified_name: String,
    pub purpose: String,
    pub task_input_schema: ClosedJsonSchema,
    pub produced_guidance_schema: ClosedJsonSchema,
    pub compatible_agent_interfaces: Vec<ResourceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPackageEntry {
    pub path: String,
    pub kind: SkillPackageEntryKind,
    pub media_type: String,
    pub byte_length: u64,
    pub content_digest: Sha256Digest,
    pub data_classification: crate::DataClassification,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPackageManifest {
    pub schema_version: u32,
    pub entries: Vec<SkillPackageEntry>,
    pub total_byte_length: u64,
    pub canonical_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillArtifactSliceRef {
    pub path: String,
    pub content_digest: Sha256Digest,
    pub byte_offset: u64,
    pub byte_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillInstructionSection {
    pub section_id: String,
    pub phase: SkillInstructionPhase,
    pub audience: SkillInstructionAudience,
    pub body: SkillArtifactSliceRef,
    pub max_tokens: u32,
    pub data_classification: crate::DataClassification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillCapabilityFeature {
    Deferred,
    InputRequired,
    Callback,
    Poll,
    Progress,
    Cancellation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillModelFeature {
    ToolUse,
    ParallelToolUse,
    StructuredOutput,
    CombinedToolAndMessage,
    TextInput,
    ImageInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilityRequirement {
    pub alias: String,
    pub interface_revision: ExactVersionRef,
    pub required_effect_ceiling: Effect,
    pub required_features: Vec<SkillCapabilityFeature>,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillContextRequirement {
    pub alias: String,
    pub interface_revision: ExactVersionRef,
    pub required_classification_ceiling: crate::DataClassification,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillModelRequirement {
    pub alias: String,
    pub required_features: Vec<SkillModelFeature>,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillResourceSpec {
    pub authoring_package: AuthoringPackage,
    pub contract_digest: Sha256Digest,
    pub dependency_versions: Vec<ExactVersionRef>,
    pub policy_versions: Vec<ExactVersionRef>,
    pub interface: SkillInterface,
    pub manifest: SkillPackageManifest,
    pub instruction_sections: Vec<SkillInstructionSection>,
    pub skill_dependencies: Vec<ExactVersionRef>,
    pub capability_requirements: Vec<SkillCapabilityRequirement>,
    pub context_requirements: Vec<SkillContextRequirement>,
    pub model_requirements: Vec<SkillModelRequirement>,
    pub instruction_set_digest: Sha256Digest,
    pub requirement_set_digest: Sha256Digest,
}

impl SkillResourceSpec {
    fn validate(&self) -> Result<(), ResourceContractError> {
        self.authoring_package.validate()?;
        validate_exact_versions(&self.dependency_versions, MAX_RESOURCE_DEPENDENCIES)?;
        validate_policy_versions(&self.policy_versions)?;
        self.interface.validate()?;
        self.manifest.validate(&self.authoring_package)?;
        validate_exact_versions(&self.skill_dependencies, MAX_RESOURCE_DEPENDENCIES)?;
        if self
            .skill_dependencies
            .iter()
            .any(|dependency| dependency.resource_kind != ResourceKind::SkillRevision)
            || self.instruction_sections.is_empty()
            || self.instruction_sections.len() > MAX_SKILL_INSTRUCTION_SECTIONS
            || self.capability_requirements.len() > MAX_SKILL_REQUIREMENTS
            || self.context_requirements.len() > MAX_SKILL_REQUIREMENTS
            || self.model_requirements.len() > MAX_SKILL_REQUIREMENTS
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
        let entries = self
            .manifest
            .entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut section_ids = BTreeSet::new();
        let mut instruction_tokens = 0_u32;
        for section in &self.instruction_sections {
            let entry = entries
                .get(section.body.path.as_str())
                .ok_or(ResourceContractError::InvalidSkillContract)?;
            instruction_tokens = instruction_tokens
                .checked_add(section.max_tokens)
                .ok_or(ResourceContractError::UnboundedValue)?;
            if !section_ids.insert(&section.section_id)
                || !is_code(&section.section_id)
                || section.max_tokens == 0
                || instruction_tokens > MAX_SKILL_INSTRUCTION_TOKENS
                || entry.kind != SkillPackageEntryKind::Instruction
                || entry.content_digest != section.body.content_digest
                || entry.data_classification != section.data_classification
                || section.body.byte_length == 0
                || section
                    .body
                    .byte_offset
                    .checked_add(section.body.byte_length)
                    .is_none_or(|end| end > entry.byte_length)
            {
                return Err(ResourceContractError::InvalidSkillContract);
            }
        }
        validate_skill_requirements(self)?;
        let instruction_set_digest: Sha256Digest = canonical_digest(
            &serde_json::to_value(&self.instruction_sections)
                .map_err(|_| ResourceContractError::Canonicalization)?,
        )
        .map_err(|_| ResourceContractError::Canonicalization)?
        .parse()
        .map_err(|_| ResourceContractError::Canonicalization)?;
        let requirement_set_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "capability": self.capability_requirements,
            "context": self.context_requirements,
            "model": self.model_requirements,
            "skill_dependencies": self.skill_dependencies,
        }))
        .map_err(|_| ResourceContractError::Canonicalization)?
        .parse()
        .map_err(|_| ResourceContractError::Canonicalization)?;
        if instruction_set_digest != self.instruction_set_digest
            || requirement_set_digest != self.requirement_set_digest
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
        Ok(())
    }
}

impl SkillInterface {
    fn validate(&self) -> Result<(), ResourceContractError> {
        if !is_qualified_skill_name(&self.qualified_name)
            || !is_name(&self.purpose, MAX_SKILL_PURPOSE_CHARS)
            || self.purpose.len() > MAX_SKILL_PURPOSE_BYTES
            || self.compatible_agent_interfaces.is_empty()
            || self.compatible_agent_interfaces.len() > MAX_FROZEN_SLOTS
            || !self
                .compatible_agent_interfaces
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .compatible_agent_interfaces
                .iter()
                .any(|interface| interface.kind() != ResourceKind::AgentInterfaceRevision)
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
        self.task_input_schema
            .validate()
            .map_err(|_| ResourceContractError::InvalidSkillContract)?;
        self.produced_guidance_schema
            .validate()
            .map_err(|_| ResourceContractError::InvalidSkillContract)
    }
}

impl SkillPackageManifest {
    fn validate(&self, package: &AuthoringPackage) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || self.entries.is_empty()
            || self.entries.len() > MAX_SKILL_PACKAGE_ENTRIES
            || self.total_byte_length == 0
            || self.total_byte_length > MAX_SKILL_PACKAGE_BYTES
            || self.canonical_digest != package.manifest_digest
            || !self
                .entries
                .windows(2)
                .all(|pair| pair[0].path < pair[1].path)
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
        let mut total = 0_u64;
        let mut manifest_count = 0_u8;
        for entry in &self.entries {
            total = total
                .checked_add(entry.byte_length)
                .ok_or(ResourceContractError::UnboundedValue)?;
            if entry.kind == SkillPackageEntryKind::Manifest {
                manifest_count = manifest_count.saturating_add(1);
            }
            if !valid_skill_entry(entry) {
                return Err(ResourceContractError::InvalidSkillContract);
            }
        }
        let digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "entries": self.entries,
            "schema_version": self.schema_version,
            "total_byte_length": self.total_byte_length,
        }))
        .map_err(|_| ResourceContractError::Canonicalization)?
        .parse()
        .map_err(|_| ResourceContractError::Canonicalization)?;
        if total > self.total_byte_length || manifest_count != 1 || digest != self.canonical_digest
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
        Ok(())
    }
}

fn valid_skill_entry(entry: &SkillPackageEntry) -> bool {
    let prefix_matches = match entry.kind {
        SkillPackageEntryKind::Manifest => entry.path == "skill.json",
        SkillPackageEntryKind::Instruction => entry.path.starts_with("instructions/"),
        SkillPackageEntryKind::Reference => entry.path.starts_with("references/"),
        SkillPackageEntryKind::Example => entry.path.starts_with("examples/"),
        SkillPackageEntryKind::Asset => entry.path.starts_with("assets/"),
    };
    let media_allowed = match entry.kind {
        SkillPackageEntryKind::Manifest | SkillPackageEntryKind::Example => {
            entry.media_type == "application/json"
        }
        SkillPackageEntryKind::Instruction => entry.media_type == "text/markdown",
        SkillPackageEntryKind::Reference => matches!(
            entry.media_type.as_str(),
            "application/json" | "application/pdf" | "text/markdown" | "text/plain"
        ),
        SkillPackageEntryKind::Asset => matches!(
            entry.media_type.as_str(),
            "application/json"
                | "image/jpeg"
                | "image/png"
                | "image/webp"
                | "text/markdown"
                | "text/plain"
        ),
    };
    prefix_matches
        && media_allowed
        && is_relative_path(&entry.path)
        && !entry.path.contains('\\')
        && entry
            .path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
        && entry.byte_length > 0
        && entry.byte_length <= MAX_SKILL_PACKAGE_BYTES
        && !entry.executable
        && !entry.media_type.is_empty()
        && entry.media_type.len() <= MAX_SKILL_MEDIA_TYPE_BYTES
        && entry
            .media_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'-' | b'.'))
}

fn validate_skill_requirements(spec: &SkillResourceSpec) -> Result<(), ResourceContractError> {
    let mut aliases = BTreeSet::new();
    for requirement in &spec.capability_requirements {
        if !aliases.insert(requirement.alias.as_str())
            || !is_code(&requirement.alias)
            || requirement.interface_revision.resource_kind
                != ResourceKind::CapabilityInterfaceRevision
            || requirement.interface_revision.validate().is_err()
            || !requirement
                .required_features
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
    }
    for requirement in &spec.context_requirements {
        if !aliases.insert(requirement.alias.as_str())
            || !is_code(&requirement.alias)
            || requirement.interface_revision.resource_kind
                != ResourceKind::ContextSourceInterfaceRevision
            || requirement.interface_revision.validate().is_err()
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
    }
    for requirement in &spec.model_requirements {
        if !aliases.insert(requirement.alias.as_str())
            || !is_code(&requirement.alias)
            || requirement.required_features.is_empty()
            || !requirement
                .required_features
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            return Err(ResourceContractError::InvalidSkillContract);
        }
    }
    Ok(())
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateSelectionMode {
    OnlyCandidate,
    OrderedFirst,
    RouteHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateSelectionPolicyDocument {
    pub schema_version: u32,
    pub mode: CandidateSelectionMode,
    pub route_schema_digest: Option<Sha256Digest>,
}

impl CandidateSelectionPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || (self.mode == CandidateSelectionMode::RouteHash)
                != self.route_schema_digest.is_some()
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
    pub selection: Option<CandidateSelectionPolicyDocument>,
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
        let document_count = [
            self.selection.is_some(),
            self.scheduling.is_some(),
            self.retention.is_some(),
            self.mcp_protocol.is_some(),
            self.mcp_auth.is_some(),
            self.sandbox_isolation.is_some(),
            self.sandbox_resource.is_some(),
            self.sandbox_network.is_some(),
            self.sandbox_artifact_io.is_some(),
            self.sandbox_secret_resolution.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        match self.policy_kind {
            PolicyKind::Selection if document_count == 1 && self.selection.is_some() => {
                let document = self.selection.as_ref().expect("guarded above");
                document.validate()?;
                if document.canonical_digest()? != self.rules_digest {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            PolicyKind::Scheduling if document_count == 1 && self.scheduling.is_some() => {
                let document = self.scheduling.as_ref().expect("guarded above");
                document.validate()?;
                if document.canonical_digest()? != self.rules_digest {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            PolicyKind::Retention if document_count == 1 && self.retention.is_some() => {
                let document = self.retention.as_ref().expect("guarded above");
                document.validate()?;
                if document.canonical_digest()? != self.rules_digest {
                    return Err(ResourceContractError::InvalidPolicyDocument);
                }
                Ok(())
            }
            PolicyKind::Protocol if document_count == 1 && self.mcp_protocol.is_some() => {
                let document = self.mcp_protocol.as_ref().expect("guarded above");
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
            PolicyKind::McpAuth if document_count == 1 && self.mcp_auth.is_some() => {
                let document = self.mcp_auth.as_ref().expect("guarded above");
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
            PolicyKind::Isolation if document_count == 1 && self.sandbox_isolation.is_some() => {
                validate_sandbox_policy(
                    self.sandbox_isolation.as_ref().expect("guarded above"),
                    &self.rules_digest,
                )
            }
            PolicyKind::Resource if document_count == 1 && self.sandbox_resource.is_some() => {
                validate_sandbox_policy(
                    self.sandbox_resource.as_ref().expect("guarded above"),
                    &self.rules_digest,
                )
            }
            PolicyKind::Network if document_count == 1 && self.sandbox_network.is_some() => {
                validate_sandbox_policy(
                    self.sandbox_network.as_ref().expect("guarded above"),
                    &self.rules_digest,
                )
            }
            PolicyKind::ArtifactIo if document_count == 1 && self.sandbox_artifact_io.is_some() => {
                validate_sandbox_policy(
                    self.sandbox_artifact_io.as_ref().expect("guarded above"),
                    &self.rules_digest,
                )
            }
            PolicyKind::SecretResolution
                if document_count == 1 && self.sandbox_secret_resolution.is_some() =>
            {
                validate_sandbox_policy(
                    self.sandbox_secret_resolution
                        .as_ref()
                        .expect("guarded above"),
                    &self.rules_digest,
                )
            }
            PolicyKind::Protocol | PolicyKind::Network if document_count == 0 => Ok(()),
            PolicyKind::Scheduling
            | PolicyKind::Selection
            | PolicyKind::Retention
            | PolicyKind::McpAuth
            | PolicyKind::Isolation
            | PolicyKind::Resource
            | PolicyKind::ArtifactIo
            | PolicyKind::SecretResolution => Err(ResourceContractError::InvalidPolicyDocument),
            _ if document_count == 0 => Ok(()),
            _ => Err(ResourceContractError::InvalidPolicyDocument),
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
#[serde(deny_unknown_fields)]
pub struct ExactPolicyBinding {
    pub deployment: ExactDeploymentRef,
    pub revision: ExactVersionRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactSandboxProfileBinding {
    pub deployment: ExactDeploymentRef,
    pub revision: ExactVersionRef,
}

impl ExactSandboxProfileBinding {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_deployment_kind(&self.deployment, ResourceKind::SandboxProfileDeployment)?;
        require_kind(
            &self.revision.revision_id,
            ResourceKind::SandboxProfileRevision,
        )
    }
}

impl ExactPolicyBinding {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_deployment_kind(&self.deployment, ResourceKind::PolicyDeployment)?;
        require_kind(&self.revision.revision_id, ResourceKind::PolicyRevision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillDeploymentClosure {
    pub skill_revision: ExactVersionRef,
    pub requirements: Vec<FrozenSlotBinding>,
    pub selection_policy: ExactPolicyBinding,
    pub qualification_evidence: ArtifactRef,
}

impl SkillDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.skill_revision.revision_id,
            ResourceKind::SkillRevision,
        )?;
        self.selection_policy.validate()?;
        if self.requirements.len() > MAX_FROZEN_SLOTS {
            return Err(ResourceContractError::UnboundedValue);
        }
        let mut slots = BTreeSet::new();
        for requirement in &self.requirements {
            requirement.validate()?;
            if !slots.insert(requirement.slot_id.as_str()) {
                return Err(ResourceContractError::DuplicateValue);
            }
        }
        self.qualification_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDeploymentClosure {
    pub policy_revision: ExactVersionRef,
    pub applicability_digest: Sha256Digest,
    pub qualification_evidence: ArtifactRef,
}

impl PolicyDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.policy_revision.revision_id,
            ResourceKind::PolicyRevision,
        )?;
        self.qualification_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProfileDeploymentClosure {
    pub profile_revision: ExactVersionRef,
    pub runtime_revision: ExactVersionRef,
    pub policy_bindings: Vec<ExactPolicyBinding>,
    pub qualification_evidence: ArtifactRef,
}

impl SandboxProfileDeploymentClosure {
    fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.profile_revision.revision_id,
            ResourceKind::SandboxProfileRevision,
        )?;
        require_kind(
            &self.runtime_revision.revision_id,
            ResourceKind::SandboxRuntimeRevision,
        )?;
        if self.policy_bindings.len() > MAX_RESOURCE_DEPENDENCIES {
            return Err(ResourceContractError::UnboundedValue);
        }
        let mut deployments = BTreeSet::new();
        let mut revisions = BTreeSet::new();
        for binding in &self.policy_bindings {
            binding.validate()?;
            if !deployments.insert(&binding.deployment.deployment_id)
                || !revisions.insert(&binding.revision.revision_id)
            {
                return Err(ResourceContractError::DuplicateValue);
            }
        }
        self.qualification_evidence
            .validate()
            .map_err(|_| ResourceContractError::InvalidArtifact)
    }
}

fn require_deployment_kind(
    reference: &ExactDeploymentRef,
    expected: ResourceKind,
) -> Result<(), ResourceContractError> {
    reference.validate()?;
    if reference.resource_kind != expected {
        return Err(ResourceContractError::WrongResourceIdKind);
    }
    Ok(())
}

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
                if spec.runtime_bundle_artifact.byte_length() == 0
                    || spec.runtime_bundle_artifact.byte_length() > MAX_SANDBOX_RUNTIME_BUNDLE_BYTES
                {
                    return Err(ResourceContractError::InvalidSandboxContract);
                }
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

    pub fn typed_plan_artifact(&self) -> Option<(&ResourceId, &Sha256Digest)> {
        match self {
            Self::Agent(spec) => Some((&spec.typed_plan_artifact_id, &spec.typed_plan_digest)),
            _ => None,
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
            Self::Skill(spec) => {
                common!(spec);
                refs.extend(spec.skill_dependencies.iter());
                refs.extend(
                    spec.capability_requirements
                        .iter()
                        .map(|requirement| &requirement.interface_revision),
                );
                refs.extend(
                    spec.context_requirements
                        .iter()
                        .map(|requirement| &requirement.interface_revision),
                );
            }
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
        selection_policy: ExactPolicyBinding,
    },
    Capability {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
        tool_alias: Option<String>,
    },
    Context {
        binding: Box<ContextBindingSnapshot>,
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
                selection_policy.validate()
            }
            FrozenSlotTarget::Capability {
                candidates,
                selection_policy,
                tool_alias,
            } => {
                validate_deployments(candidates, ResourceKind::CapabilityDeployment)?;
                selection_policy.validate()?;
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
                selection_policy.validate()
            }
            FrozenSlotTarget::Skill {
                candidates,
                selection_policy,
            } => {
                validate_deployments(candidates, ResourceKind::SkillDeployment)?;
                selection_policy.validate()
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
    Skill(SkillDeploymentClosure),
    CapabilityInterface(CapabilityDeploymentClosure),
    ContextSourceInterface(ContextDeploymentClosure),
    McpServer(McpDeploymentClosure),
    ModelProvider(ModelProviderDeploymentClosure),
    ModelProfile(ModelDeploymentClosure),
    Policy(PolicyDeploymentClosure),
    SandboxProfile(SandboxProfileDeploymentClosure),
}

impl DeploymentClosure {
    pub const fn resource_kind(&self) -> RegistryResourceKind {
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

    pub fn validate(&self) -> Result<(), ResourceContractError> {
        match self {
            Self::Agent(closure) => closure.validate(),
            Self::Skill(closure) => closure.validate(),
            Self::CapabilityInterface(closure) => closure.validate(),
            Self::ContextSourceInterface(closure) => closure.validate(),
            Self::McpServer(closure) => closure.validate(),
            Self::ModelProvider(closure) => closure.validate(),
            Self::ModelProfile(closure) => closure.validate(),
            Self::Policy(closure) => closure.validate(),
            Self::SandboxProfile(closure) => closure.validate(),
        }
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        let mut refs = Vec::new();
        match self {
            Self::Agent(closure) => {
                refs.extend([
                    &closure.interface,
                    &closure.plan,
                    &closure.execution_profile.revision,
                ]);
                refs.extend(closure.policies.iter().map(|binding| &binding.revision));
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
                            refs.push(&selection_policy.revision);
                        }
                        FrozenSlotTarget::Skill {
                            selection_policy, ..
                        } => {
                            refs.push(&selection_policy.revision);
                        }
                        FrozenSlotTarget::Context { binding } => {
                            refs.extend([&binding.authorization_policy, &binding.ranking_policy])
                        }
                    }
                }
            }
            Self::Skill(closure) => {
                refs.push(&closure.skill_revision);
                refs.push(&closure.selection_policy.revision);
                for requirement in &closure.requirements {
                    match &requirement.target {
                        FrozenSlotTarget::Model {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::Capability {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::ChildAgent {
                            selection_policy, ..
                        } => refs.push(&selection_policy.revision),
                        FrozenSlotTarget::Skill {
                            selection_policy, ..
                        } => {
                            refs.push(&selection_policy.revision);
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
                    &closure.chunker_policy,
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
            Self::Policy(closure) => refs.push(&closure.policy_revision),
            Self::SandboxProfile(closure) => {
                refs.extend([&closure.profile_revision, &closure.runtime_revision]);
                refs.extend(
                    closure
                        .policy_bindings
                        .iter()
                        .map(|binding| &binding.revision),
                );
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
                        FrozenSlotTarget::Model {
                            candidates,
                            selection_policy,
                        }
                        | FrozenSlotTarget::Capability {
                            candidates,
                            selection_policy,
                            ..
                        }
                        | FrozenSlotTarget::ChildAgent {
                            candidates,
                            selection_policy,
                        } => {
                            refs.extend(candidates.iter());
                            refs.push(&selection_policy.deployment);
                        }
                        FrozenSlotTarget::Context { binding } => {
                            refs.push(&binding.context_deployment);
                        }
                        FrozenSlotTarget::Skill {
                            candidates,
                            selection_policy,
                        } => {
                            refs.extend(candidates.iter());
                            refs.push(&selection_policy.deployment);
                        }
                    }
                }
                refs.extend(closure.policies.iter().map(|binding| &binding.deployment));
                refs.push(&closure.execution_profile.deployment);
            }
            Self::CapabilityInterface(closure) => {
                refs.extend(closure.backend.exact_deployment_refs())
            }
            Self::Skill(closure) => {
                refs.push(&closure.selection_policy.deployment);
                for requirement in &closure.requirements {
                    match &requirement.target {
                        FrozenSlotTarget::Model { candidates, .. }
                        | FrozenSlotTarget::Capability { candidates, .. }
                        | FrozenSlotTarget::ChildAgent { candidates, .. } => {
                            refs.extend(candidates.iter());
                        }
                        FrozenSlotTarget::Context { binding } => {
                            refs.push(&binding.context_deployment)
                        }
                        FrozenSlotTarget::Skill { candidates, .. } => {
                            refs.extend(candidates.iter());
                        }
                    }
                }
            }
            Self::ModelProfile(closure) => refs.push(&closure.provider_deployment),
            Self::ContextSourceInterface(closure) => {
                refs.extend(closure.embedding_model_deployment.iter());
                if let ContextBackendBinding::McpResources { mcp_deployment, .. } = &closure.backend
                {
                    refs.push(mcp_deployment);
                }
            }
            Self::SandboxProfile(closure) => refs.extend(
                closure
                    .policy_bindings
                    .iter()
                    .map(|binding| &binding.deployment),
            ),
            Self::McpServer(_) | Self::ModelProvider(_) | Self::Policy(_) => {}
        }
        refs
    }

    pub fn exact_policy_bindings(&self) -> Vec<&ExactPolicyBinding> {
        match self {
            Self::Agent(closure) => {
                let mut bindings = closure.policies.iter().collect::<Vec<_>>();
                bindings.push(&closure.execution_profile);
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
                        }
                        | FrozenSlotTarget::Skill {
                            selection_policy, ..
                        } => bindings.push(selection_policy),
                        FrozenSlotTarget::Context { .. } => {}
                    }
                }
                bindings
            }
            Self::Skill(closure) => {
                let mut bindings = vec![&closure.selection_policy];
                for requirement in &closure.requirements {
                    match &requirement.target {
                        FrozenSlotTarget::Model {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::Capability {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::ChildAgent {
                            selection_policy, ..
                        }
                        | FrozenSlotTarget::Skill {
                            selection_policy, ..
                        } => bindings.push(selection_policy),
                        FrozenSlotTarget::Context { .. } => {}
                    }
                }
                bindings
            }
            Self::SandboxProfile(closure) => closure.policy_bindings.iter().collect(),
            Self::CapabilityInterface(_)
            | Self::ContextSourceInterface(_)
            | Self::McpServer(_)
            | Self::ModelProvider(_)
            | Self::ModelProfile(_)
            | Self::Policy(_) => Vec::new(),
        }
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
            | Self::Policy(_)
            | Self::SandboxProfile(_)
            | Self::ContextSourceInterface(_)
            | Self::McpServer(_)
            | Self::ModelProvider(_)
            | Self::ModelProfile(_) => Vec::new(),
            Self::Skill(closure) => closure
                .requirements
                .iter()
                .filter_map(|requirement| match &requirement.target {
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
        }
    }

    pub fn secret_bindings(&self) -> &[crate::ExactSecretBindingRef] {
        match self {
            Self::CapabilityInterface(closure) => &closure.secret_bindings,
            Self::ContextSourceInterface(closure) => &closure.secret_bindings,
            Self::McpServer(closure) => &closure.secret_bindings,
            Self::ModelProvider(closure) => &closure.secret_bindings,
            Self::Agent(_)
            | Self::Skill(_)
            | Self::ModelProfile(_)
            | Self::Policy(_)
            | Self::SandboxProfile(_) => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDeploymentClosure {
    pub interface: ExactVersionRef,
    pub plan: ExactVersionRef,
    pub entry_node_id: String,
    pub entry_node_kind: PlanNodeKind,
    pub slots: Vec<FrozenSlotBinding>,
    pub policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
}

impl AgentDeploymentClosure {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        require_kind(
            &self.interface.revision_id,
            ResourceKind::AgentInterfaceRevision,
        )?;
        require_kind(&self.plan.revision_id, ResourceKind::AgentPlanRevision)?;
        if !is_code(&self.entry_node_id) {
            return Err(ResourceContractError::UnboundedValue);
        }
        validate_policy_bindings(&self.policies)?;
        self.execution_profile.validate()?;
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
    pub chunker_policy: ExactVersionRef,
    pub embedding_model_deployment: Option<ExactDeploymentRef>,
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
        if self
            .embedding_model_deployment
            .as_ref()
            .is_some_and(|deployment| {
                deployment.resource_kind != ResourceKind::ModelDeployment
                    || deployment.validate().is_err()
            })
        {
            return Err(ResourceContractError::InvalidContextContract);
        }
        let mut policies = vec![
            &self.parser_policy,
            &self.chunker_policy,
            &self.ranking_policy,
            &self.data_policy,
        ];
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
    pub policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
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
        policies.sort_by(|left, right| {
            left.deployment
                .deployment_id
                .cmp(&right.deployment.deployment_id)
        });
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
        {
            return Err(ResourceContractError::WrongResourceIdKind);
        }
        self.principal
            .validate()
            .map_err(|_| ResourceContractError::InvalidPrincipalSnapshot)?;
        validate_slot_bindings(&self.slots)?;
        validate_run_context_dataset_views(&self.slots, &self.context_dataset_views)?;
        self.execution_profile.validate()?;
        validate_policy_bindings(&self.policies)?;
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
                .all(|pair| pair[0].deployment.deployment_id < pair[1].deployment.deployment_id)
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
        let mut references = vec![
            &self.agent_interface,
            &self.plan,
            &self.execution_profile.revision,
        ];
        references.extend(self.policies.iter().map(|binding| &binding.revision));
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
                } => references.push(&selection_policy.revision),
                FrozenSlotTarget::Skill {
                    selection_policy, ..
                } => {
                    references.push(&selection_policy.revision);
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
                FrozenSlotTarget::Model {
                    candidates,
                    selection_policy,
                }
                | FrozenSlotTarget::Capability {
                    candidates,
                    selection_policy,
                    ..
                }
                | FrozenSlotTarget::ChildAgent {
                    candidates,
                    selection_policy,
                } => {
                    references.extend(candidates.iter());
                    references.push(&selection_policy.deployment);
                }
                FrozenSlotTarget::Context { binding } => {
                    references.push(&binding.context_deployment);
                }
                FrozenSlotTarget::Skill {
                    candidates,
                    selection_policy,
                } => {
                    references.extend(candidates.iter());
                    references.push(&selection_policy.deployment);
                }
            }
        }
        references.extend(self.policies.iter().map(|binding| &binding.deployment));
        references.push(&self.execution_profile.deployment);
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
    policies: &'a [ExactPolicyBinding],
    execution_profile: &'a ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceContractError {
    UnknownResourceKind(String),
    WrongResourceIdKind,
    KindMismatch,
    InvalidArtifact,
    InvalidAgentContract,
    InvalidSkillContract,
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
            Self::InvalidAgentContract => {
                formatter.write_str("Agent interface contract has an invalid closed shape")
            }
            Self::InvalidSkillContract => {
                formatter.write_str("Skill package contract has an invalid closed shape")
            }
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

fn validate_policy_bindings(values: &[ExactPolicyBinding]) -> Result<(), ResourceContractError> {
    if values.len() > MAX_RESOURCE_POLICIES {
        return Err(ResourceContractError::UnboundedValue);
    }
    let mut deployments = BTreeSet::new();
    let mut revisions = BTreeSet::new();
    for value in values {
        value.validate()?;
        if !deployments.insert(&value.deployment.deployment_id)
            || !revisions.insert(&value.revision.revision_id)
        {
            return Err(ResourceContractError::DuplicateValue);
        }
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

fn is_qualified_skill_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CODE_BYTES
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
                && bytes.all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-')
                })
        })
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

    fn closed_object_schema() -> ClosedJsonSchema {
        ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }))
        .unwrap()
    }

    fn qualification_artifact() -> ArtifactRef {
        ArtifactRef::new(
            id("art_0198f1c3-8f49-7c3e-b1f3-773c28367b80"),
            digest('f'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("qualification.json".to_owned()),
        )
        .unwrap()
    }

    fn skill_spec() -> SkillResourceSpec {
        let entries = vec![
            SkillPackageEntry {
                path: "instructions/review.md".to_owned(),
                kind: SkillPackageEntryKind::Instruction,
                media_type: "text/markdown".to_owned(),
                byte_length: 64,
                content_digest: digest('1'),
                data_classification: DataClassification::Internal,
                executable: false,
            },
            SkillPackageEntry {
                path: "skill.json".to_owned(),
                kind: SkillPackageEntryKind::Manifest,
                media_type: "application/json".to_owned(),
                byte_length: 64,
                content_digest: digest('2'),
                data_classification: DataClassification::Internal,
                executable: false,
            },
        ];
        let manifest_digest: Sha256Digest = canonical_digest(&json!({
            "entries": entries,
            "schema_version": 1,
            "total_byte_length": 128,
        }))
        .unwrap()
        .parse()
        .unwrap();
        let sections = vec![SkillInstructionSection {
            section_id: "review".to_owned(),
            phase: SkillInstructionPhase::Validation,
            audience: SkillInstructionAudience::Validator,
            body: SkillArtifactSliceRef {
                path: "instructions/review.md".to_owned(),
                content_digest: digest('1'),
                byte_offset: 0,
                byte_length: 64,
            },
            max_tokens: 32,
            data_classification: DataClassification::Internal,
        }];
        let instruction_set_digest: Sha256Digest =
            canonical_digest(&serde_json::to_value(&sections).unwrap())
                .unwrap()
                .parse()
                .unwrap();
        let requirement_set_digest: Sha256Digest = canonical_digest(&json!({
            "capability": [],
            "context": [],
            "model": [],
            "skill_dependencies": [],
        }))
        .unwrap()
        .parse()
        .unwrap();
        SkillResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: qualification_artifact(),
                manifest_digest: manifest_digest.clone(),
            },
            contract_digest: digest('3'),
            dependency_versions: vec![],
            policy_versions: vec![],
            interface: SkillInterface {
                qualified_name: "review.method".to_owned(),
                purpose: "Review a bounded result".to_owned(),
                task_input_schema: closed_object_schema(),
                produced_guidance_schema: closed_object_schema(),
                compatible_agent_interfaces: vec![id("aif_0198f1c3-8f49-7c3e-b1f3-773c28367b81")],
            },
            manifest: SkillPackageManifest {
                schema_version: 1,
                entries,
                total_byte_length: 128,
                canonical_digest: manifest_digest,
            },
            instruction_sections: sections,
            skill_dependencies: vec![],
            capability_requirements: vec![],
            context_requirements: vec![],
            model_requirements: vec![],
            instruction_set_digest,
            requirement_set_digest,
        }
    }

    #[test]
    fn skill_package_is_digest_slice_and_execution_closed() {
        let spec = skill_spec();
        ResourceDocument::Skill(spec.clone()).validate().unwrap();

        let mut traversal = spec.clone();
        traversal.manifest.entries[0].path = "instructions/../escape.md".to_owned();
        traversal.manifest.canonical_digest = canonical_digest(&json!({
            "entries": traversal.manifest.entries,
            "schema_version": traversal.manifest.schema_version,
            "total_byte_length": traversal.manifest.total_byte_length,
        }))
        .unwrap()
        .parse()
        .unwrap();
        traversal.authoring_package.manifest_digest = traversal.manifest.canonical_digest.clone();
        assert_eq!(
            ResourceDocument::Skill(traversal).validate(),
            Err(ResourceContractError::InvalidSkillContract)
        );

        let mut executable = spec.clone();
        executable.manifest.entries[0].executable = true;
        executable.manifest.canonical_digest = canonical_digest(&json!({
            "entries": executable.manifest.entries,
            "schema_version": executable.manifest.schema_version,
            "total_byte_length": executable.manifest.total_byte_length,
        }))
        .unwrap()
        .parse()
        .unwrap();
        executable.authoring_package.manifest_digest = executable.manifest.canonical_digest.clone();
        assert_eq!(
            ResourceDocument::Skill(executable).validate(),
            Err(ResourceContractError::InvalidSkillContract)
        );

        let mut escaped_slice = spec;
        escaped_slice.instruction_sections[0].body.byte_offset = 1;
        escaped_slice.instruction_set_digest =
            canonical_digest(&serde_json::to_value(&escaped_slice.instruction_sections).unwrap())
                .unwrap()
                .parse()
                .unwrap();
        assert_eq!(
            ResourceDocument::Skill(escaped_slice).validate(),
            Err(ResourceContractError::InvalidSkillContract)
        );
    }

    fn policy_binding(
        deployment_suffix: &str,
        revision_suffix: &str,
        marker: char,
    ) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id(&format!(
                    "pdep_0198f1c3-8f49-7c3e-b1f3-773c2836{deployment_suffix}"
                )),
                digest(marker),
            )
            .unwrap(),
            revision: ExactVersionRef::new(
                id(&format!(
                    "prev_0198f1c3-8f49-7c3e-b1f3-773c2836{revision_suffix}"
                )),
                digest(marker),
            )
            .unwrap(),
        }
    }

    #[test]
    fn definition_deployment_closures_are_nominal_and_closed() {
        let policy_deployment =
            ExactDeploymentRef::new(id("pdep_0198f1c3-8f49-7c3e-b1f3-773c28367b81"), digest('a'))
                .unwrap();
        let policy_binding_pair = ExactPolicyBinding {
            deployment: policy_deployment.clone(),
            revision: ExactVersionRef::new(
                id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367b89"),
                digest('9'),
            )
            .unwrap(),
        };
        let skill = DeploymentClosure::Skill(SkillDeploymentClosure {
            skill_revision: ExactVersionRef::new(
                id("srev_0198f1c3-8f49-7c3e-b1f3-773c28367b82"),
                digest('b'),
            )
            .unwrap(),
            requirements: vec![FrozenSlotBinding {
                slot_id: "summarizer".to_owned(),
                requirement_digest: digest('1'),
                target: FrozenSlotTarget::Skill {
                    candidates: vec![ExactDeploymentRef::new(
                        id("skdep_0198f1c3-8f49-7c3e-b1f3-773c28367b87"),
                        digest('2'),
                    )
                    .unwrap()],
                    selection_policy: policy_binding("7b8a", "7b88", '3'),
                },
                binding_digest: digest('4'),
            }],
            selection_policy: policy_binding_pair.clone(),
            qualification_evidence: qualification_artifact(),
        });
        let policy = DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(
                id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367b83"),
                digest('c'),
            )
            .unwrap(),
            applicability_digest: digest('d'),
            qualification_evidence: qualification_artifact(),
        });
        let sandbox = DeploymentClosure::SandboxProfile(SandboxProfileDeploymentClosure {
            profile_revision: ExactVersionRef::new(
                id("sxrev_0198f1c3-8f49-7c3e-b1f3-773c28367b84"),
                digest('e'),
            )
            .unwrap(),
            runtime_revision: ExactVersionRef::new(
                id("srrev_0198f1c3-8f49-7c3e-b1f3-773c28367b85"),
                digest('f'),
            )
            .unwrap(),
            policy_bindings: vec![policy_binding_pair],
            qualification_evidence: qualification_artifact(),
        });
        assert!(skill.validate().is_ok());
        assert!(policy.validate().is_ok());
        assert!(sandbox.validate().is_ok());
        assert_eq!(skill.resource_kind(), RegistryResourceKind::Skill);
        assert!(skill
            .exact_deployment_refs()
            .iter()
            .any(|reference| reference.resource_kind == ResourceKind::SkillDeployment));
        assert_eq!(
            skill
                .exact_version_refs()
                .iter()
                .filter(|reference| reference.resource_kind == ResourceKind::SkillRevision)
                .count(),
            1
        );
        assert_eq!(policy.resource_kind(), RegistryResourceKind::Policy);
        assert_eq!(
            sandbox.resource_kind(),
            RegistryResourceKind::SandboxProfile
        );

        let mut wrong: SkillDeploymentClosure = match skill {
            DeploymentClosure::Skill(closure) => closure,
            _ => unreachable!(),
        };
        wrong.selection_policy.deployment =
            ExactDeploymentRef::new(id("adep_0198f1c3-8f49-7c3e-b1f3-773c28367b86"), digest('1'))
                .unwrap();
        assert_eq!(
            wrong.validate(),
            Err(ResourceContractError::WrongResourceIdKind)
        );
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
            ActivationTargetKind::Deployment
        );
        assert_eq!(
            RegistryResourceKind::Skill.deployment_kind(),
            Some(ResourceKind::SkillDeployment)
        );
        assert_eq!(
            RegistryResourceKind::Policy.deployment_kind(),
            Some(ResourceKind::PolicyDeployment)
        );
        assert_eq!(
            RegistryResourceKind::SandboxProfile.deployment_kind(),
            Some(ResourceKind::SandboxProfileDeployment)
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
            entry_node_id: "start".to_owned(),
            entry_node_kind: PlanNodeKind::Start,
            slots: vec![],
            policies: vec![],
            execution_profile: policy_binding("7b97", "7b93", 'd'),
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
            entry_node_id: "start".to_owned(),
            entry_node_kind: PlanNodeKind::Start,
            slots: vec![FrozenSlotBinding {
                slot_id: "catalog".to_owned(),
                requirement_digest: digest('6'),
                target: FrozenSlotTarget::Context {
                    binding: Box::new(binding.clone()),
                },
                binding_digest: digest('7'),
            }],
            policies: vec![
                ExactPolicyBinding {
                    deployment: ExactDeploymentRef::new(
                        id("pdep_0198f1c3-8f49-7c3e-b1f3-773c28367c0d"),
                        digest('2'),
                    )
                    .unwrap(),
                    revision: authorization_policy,
                },
                ExactPolicyBinding {
                    deployment: ExactDeploymentRef::new(
                        id("pdep_0198f1c3-8f49-7c3e-b1f3-773c28367c0e"),
                        digest('3'),
                    )
                    .unwrap(),
                    revision: ranking_policy,
                },
            ],
            execution_profile: policy_binding("7c0f", "7c09", '8'),
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

        let agent_schema = ClosedJsonSchema::build(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }))
        .unwrap();
        let invalid_agent = ResourceDocument::Agent(AgentResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: ArtifactRef::new(
                    id("art_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
                    digest('a'),
                    16,
                    "application/json".to_owned(),
                    DataClassification::Internal,
                    None,
                )
                .unwrap(),
                manifest_digest: digest('b'),
            },
            contract_digest: digest('c'),
            dependency_versions: vec![],
            policy_versions: vec![],
            input_schema: agent_schema.clone(),
            output_schema: agent_schema.clone(),
            error_schema: agent_schema,
            typed_plan_artifact_id: id("val_0198f1c3-8f49-7c3e-b1f3-773c28367b92"),
            typed_plan_digest: digest('e'),
        });
        assert_eq!(
            invalid_agent.validate(),
            Err(ResourceContractError::WrongResourceIdKind)
        );
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
            selection: None,
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

        let selection = CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::RouteHash,
            route_schema_digest: Some(digest('9')),
        };
        let mut selection_spec = spec;
        selection_spec.policy_kind = PolicyKind::Selection;
        selection_spec.rules_digest = selection.canonical_digest().unwrap();
        selection_spec.selection = Some(selection.clone());
        selection_spec.retention = None;
        selection_spec.validate().unwrap();

        selection_spec.rules_digest = digest('0');
        assert_eq!(
            selection_spec.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
        let mut missing_route_schema = selection;
        missing_route_schema.route_schema_digest = None;
        assert_eq!(
            missing_route_schema.validate(),
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
            selection: None,
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
    fn sandbox_package_runtime_bundle_is_bounded_at_publication() {
        let artifact = |suffix: &str, character: char, byte_length| {
            ArtifactRef::new(
                id(&format!("art_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
                digest(character),
                byte_length,
                "application/octet-stream",
                DataClassification::Internal,
                None,
            )
            .unwrap()
        };
        let runtime_revision = ExactVersionRef::new(
            id("srrev_0198f1c3-8f49-7c3e-b1f3-773c28367baa"),
            digest('1'),
        )
        .unwrap();
        let spec = SandboxPackageResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: artifact("7bab", '2', 16),
                manifest_digest: digest('3'),
            },
            contract_digest: digest('4'),
            dependency_versions: vec![runtime_revision.clone()],
            policy_versions: vec![],
            source_artifact: artifact("7bac", '5', 16),
            source_digest: digest('5'),
            runtime_revision,
            entrypoint_kind: SandboxEntrypointKind::WasmExport,
            entrypoint: "run".to_owned(),
            dependency_lock_digest: digest('6'),
            runtime_bundle_artifact: artifact("7bad", '7', MAX_SANDBOX_RUNTIME_BUNDLE_BYTES),
            build_evidence: artifact("7bae", '8', 16),
            trust_class: CodeTrustClass::BuiltIn,
            package_digest: digest('9'),
        };
        ResourceDocument::SandboxPackage(spec.clone())
            .validate()
            .unwrap();

        let mut empty = spec.clone();
        empty.runtime_bundle_artifact = artifact("7baf", 'a', 0);
        assert_eq!(
            ResourceDocument::SandboxPackage(empty).validate(),
            Err(ResourceContractError::InvalidSandboxContract)
        );

        let mut oversized = spec;
        oversized.runtime_bundle_artifact =
            artifact("7bb0", 'b', MAX_SANDBOX_RUNTIME_BUNDLE_BYTES + 1);
        assert_eq!(
            ResourceDocument::SandboxPackage(oversized).validate(),
            Err(ResourceContractError::InvalidSandboxContract)
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
