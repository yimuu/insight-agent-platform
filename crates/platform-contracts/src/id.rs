use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};
use uuid::{Uuid, Variant, Version};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceKind {
    Tenant,
    Principal,
    InstallationService,
    WorkerProcessGeneration,
    SecretProvider,
    SecretBinding,
    Agent,
    AgentInterfaceRevision,
    AgentPlanRevision,
    AgentDeployment,
    Skill,
    SkillRevision,
    SkillDeployment,
    SkillActivation,
    CapabilityInterface,
    CapabilityInterfaceRevision,
    CapabilityImplementation,
    CapabilityImplementationRevision,
    CapabilityDeployment,
    ContextSourceInterface,
    ContextSourceInterfaceRevision,
    ContextSourceImplementation,
    ContextSourceImplementationRevision,
    ContextDeployment,
    ContextBinding,
    ContextDataset,
    DatasetGeneration,
    ContextQuery,
    ContextObservation,
    ContextItem,
    McpServer,
    McpServerRevision,
    McpDeployment,
    McpDiscoverySnapshot,
    McpAuthorizationBinding,
    McpOperation,
    ModelProvider,
    ModelProviderRevision,
    ModelProviderDeployment,
    ModelProfile,
    ModelProfileRevision,
    ModelDeployment,
    ModelTurn,
    UsageReservation,
    Policy,
    PolicyRevision,
    PolicyDeployment,
    SandboxRuntime,
    SandboxRuntimeRevision,
    SandboxPackage,
    SandboxPackageRevision,
    SandboxProfile,
    SandboxProfileRevision,
    SandboxProfileDeployment,
    Run,
    RunValue,
    NodeExecution,
    ScopeInstance,
    CapabilityInvocation,
    ChildRunLink,
    Artifact,
    ArtifactGrant,
    InternalBlob,
    EncryptionDomain,
    Interaction,
    ApprovalTask,
    Job,
    Task,
    Receipt,
    OutboxEvent,
    QuotaAccount,
    QuotaLedgerEntry,
    Event,
    ServerRequest,
    ArtifactLink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResourceKindDescriptor {
    pub prefix: &'static str,
    pub name: &'static str,
    pub exposure: ResourceIdExposure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceIdExposure {
    Public,
    Internal,
    CorrelationOnly,
}

macro_rules! descriptors {
    ($($kind:ident => ($prefix:literal, $name:literal, $exposure:ident)),+ $(,)?) => {
        pub const RESOURCE_KIND_DESCRIPTORS: &[ResourceKindDescriptor] = &[
            $(ResourceKindDescriptor {
                prefix: $prefix,
                name: $name,
                exposure: ResourceIdExposure::$exposure,
            }),+
        ];

        impl ResourceKind {
            pub const ALL: &'static [Self] = &[$(Self::$kind),+];

            pub const fn descriptor(self) -> ResourceKindDescriptor {
                match self {
                    $(Self::$kind => ResourceKindDescriptor {
                        prefix: $prefix,
                        name: $name,
                        exposure: ResourceIdExposure::$exposure,
                    }),+
                }
            }

            pub fn from_prefix(prefix: &str) -> Option<Self> {
                match prefix {
                    $($prefix => Some(Self::$kind)),+,
                    _ => None,
                }
            }
        }
    };
}

descriptors! {
    Tenant => ("ten", "tenant", Public),
    Principal => ("prn", "principal", Public),
    InstallationService => ("svc", "installation_service", Internal),
    WorkerProcessGeneration => ("wrk", "worker_process_generation", Internal),
    SecretProvider => ("spr", "secret_provider", Public),
    SecretBinding => ("sbd", "secret_binding", Public),
    Agent => ("agt", "agent", Public),
    AgentInterfaceRevision => ("aif", "agent_interface_revision", Public),
    AgentPlanRevision => ("arev", "agent_plan_revision", Public),
    AgentDeployment => ("adep", "agent_deployment", Public),
    Skill => ("skl", "skill", Public),
    SkillRevision => ("srev", "skill_revision", Public),
    SkillDeployment => ("skdep", "skill_deployment", Public),
    SkillActivation => ("sact", "skill_activation", Public),
    CapabilityInterface => ("cap", "capability_interface", Public),
    CapabilityInterfaceRevision => ("cirev", "capability_interface_revision", Public),
    CapabilityImplementation => ("cim", "capability_implementation", Public),
    CapabilityImplementationRevision => ("cimp", "capability_implementation_revision", Public),
    CapabilityDeployment => ("cdep", "capability_deployment", Public),
    ContextSourceInterface => ("ctx", "context_source_interface", Public),
    ContextSourceInterfaceRevision => ("xirev", "context_source_interface_revision", Public),
    ContextSourceImplementation => ("xim", "context_source_implementation", Public),
    ContextSourceImplementationRevision => ("ximp", "context_source_implementation_revision", Public),
    ContextDeployment => ("xdep", "context_deployment", Public),
    ContextBinding => ("xcb", "context_binding", Public),
    ContextDataset => ("dset", "context_dataset", Public),
    DatasetGeneration => ("dgen", "dataset_generation", Public),
    ContextQuery => ("cqry", "context_query", Public),
    ContextObservation => ("cobs", "context_observation", Public),
    ContextItem => ("cit", "context_item", Public),
    McpServer => ("mcp", "mcp_server", Public),
    McpServerRevision => ("mrev", "mcp_server_revision", Public),
    McpDeployment => ("mcdep", "mcp_deployment", Public),
    McpDiscoverySnapshot => ("mdsc", "mcp_discovery_snapshot", Public),
    McpAuthorizationBinding => ("mab", "mcp_authorization_binding", Public),
    McpOperation => ("mop", "mcp_operation", Public),
    ModelProvider => ("mpr", "model_provider", Public),
    ModelProviderRevision => ("mprev", "model_provider_revision", Public),
    ModelProviderDeployment => ("mpdep", "model_provider_deployment", Public),
    ModelProfile => ("mdl", "model_profile", Public),
    ModelProfileRevision => ("mdrev", "model_profile_revision", Public),
    ModelDeployment => ("mdep", "model_deployment", Public),
    ModelTurn => ("mturn", "model_turn", Public),
    UsageReservation => ("ures", "usage_reservation", Public),
    Policy => ("pol", "policy", Public),
    PolicyRevision => ("prev", "policy_revision", Public),
    PolicyDeployment => ("pdep", "policy_deployment", Public),
    SandboxRuntime => ("srt", "sandbox_runtime", Public),
    SandboxRuntimeRevision => ("srrev", "sandbox_runtime_revision", Public),
    SandboxPackage => ("spk", "sandbox_package", Public),
    SandboxPackageRevision => ("sprev", "sandbox_package_revision", Public),
    SandboxProfile => ("sxp", "sandbox_profile", Public),
    SandboxProfileRevision => ("sxrev", "sandbox_profile_revision", Public),
    SandboxProfileDeployment => ("sxdep", "sandbox_profile_deployment", Public),
    Run => ("run", "run", Public),
    RunValue => ("val", "run_value", Internal),
    NodeExecution => ("nod", "node_execution", Public),
    ScopeInstance => ("scp", "scope_instance", Internal),
    CapabilityInvocation => ("inv", "capability_invocation", Public),
    ChildRunLink => ("crun", "child_run_link", Public),
    Artifact => ("art", "artifact", Public),
    ArtifactGrant => ("grt", "artifact_grant", Internal),
    InternalBlob => ("blb", "internal_blob", Internal),
    EncryptionDomain => ("enc", "encryption_domain", Internal),
    Interaction => ("int", "interaction", Public),
    ApprovalTask => ("apr", "approval_task", Public),
    Job => ("job", "job", Internal),
    Task => ("tsk", "task", Internal),
    Receipt => ("rcp", "receipt", Internal),
    OutboxEvent => ("obx", "outbox_event", Internal),
    QuotaAccount => ("qac", "quota_account", Internal),
    QuotaLedgerEntry => ("qle", "quota_ledger_entry", Internal),
    Event => ("evt", "event", Internal),
    ServerRequest => ("req", "server_request", CorrelationOnly),
    ArtifactLink => ("lnk", "artifact_link", Internal),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceId {
    kind: ResourceKind,
    uuid: Uuid,
}

impl ResourceId {
    pub fn from_uuid_v7(kind: ResourceKind, uuid: Uuid) -> Result<Self, ResourceIdError> {
        if uuid.get_version() != Some(Version::SortRand) || uuid.get_variant() != Variant::RFC4122 {
            return Err(ResourceIdError::NotUuidV7);
        }
        Ok(Self { kind, uuid })
    }

    pub const fn kind(&self) -> ResourceKind {
        self.kind
    }

    pub const fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn parse_expected(value: &str, expected: ResourceKind) -> Result<Self, ResourceIdError> {
        let parsed: Self = value.parse()?;
        if parsed.kind != expected {
            return Err(ResourceIdError::WrongKind {
                expected,
                actual: parsed.kind,
            });
        }
        Ok(parsed)
    }
}

impl ResourceKind {
    pub const fn is_revision(self) -> bool {
        matches!(
            self,
            Self::AgentInterfaceRevision
                | Self::AgentPlanRevision
                | Self::SkillRevision
                | Self::CapabilityInterfaceRevision
                | Self::CapabilityImplementationRevision
                | Self::ContextSourceInterfaceRevision
                | Self::ContextSourceImplementationRevision
                | Self::DatasetGeneration
                | Self::McpServerRevision
                | Self::ModelProviderRevision
                | Self::ModelProfileRevision
                | Self::PolicyRevision
                | Self::SandboxRuntimeRevision
                | Self::SandboxPackageRevision
                | Self::SandboxProfileRevision
        )
    }

    pub const fn is_deployment(self) -> bool {
        matches!(
            self,
            Self::AgentDeployment
                | Self::SkillDeployment
                | Self::CapabilityDeployment
                | Self::ContextDeployment
                | Self::McpDeployment
                | Self::ModelProviderDeployment
                | Self::ModelDeployment
                | Self::PolicyDeployment
                | Self::SandboxProfileDeployment
        )
    }

    pub const fn supports_exact_registry_projection(self) -> bool {
        matches!(
            self,
            Self::Artifact
                | Self::AgentInterfaceRevision
                | Self::AgentPlanRevision
                | Self::AgentDeployment
                | Self::SkillRevision
                | Self::SkillDeployment
                | Self::CapabilityInterfaceRevision
                | Self::CapabilityImplementationRevision
                | Self::CapabilityDeployment
                | Self::ContextSourceInterfaceRevision
                | Self::ContextSourceImplementationRevision
                | Self::ContextDeployment
                | Self::ContextBinding
                | Self::ContextDataset
                | Self::DatasetGeneration
                | Self::McpServerRevision
                | Self::McpDeployment
                | Self::McpDiscoverySnapshot
                | Self::ModelProviderRevision
                | Self::ModelProviderDeployment
                | Self::ModelProfileRevision
                | Self::ModelDeployment
                | Self::PolicyDeployment
                | Self::SandboxRuntimeRevision
                | Self::SandboxPackageRevision
                | Self::SandboxProfileRevision
                | Self::SandboxProfileDeployment
        )
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.descriptor().name)
    }
}

impl Serialize for ResourceKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.descriptor().name)
    }
}

impl<'de> Deserialize<'de> for ResourceKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.descriptor().name == value)
            .ok_or_else(|| de::Error::custom(format!("unknown resource kind {value:?}")))
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}_{}",
            self.kind.descriptor().prefix,
            self.uuid.hyphenated()
        )
    }
}

impl FromStr for ResourceId {
    type Err = ResourceIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((prefix, raw_uuid)) = value.split_once('_') else {
            return Err(ResourceIdError::InvalidFormat);
        };
        if prefix.is_empty() || raw_uuid.is_empty() || raw_uuid.contains('_') {
            return Err(ResourceIdError::InvalidFormat);
        }
        let kind = ResourceKind::from_prefix(prefix)
            .ok_or_else(|| ResourceIdError::UnknownPrefix(prefix.to_owned()))?;
        let uuid = Uuid::parse_str(raw_uuid).map_err(|_| ResourceIdError::InvalidUuid)?;
        if uuid.get_version() != Some(Version::SortRand) || uuid.get_variant() != Variant::RFC4122 {
            return Err(ResourceIdError::NotUuidV7);
        }
        let parsed = Self { kind, uuid };
        if parsed.to_string() != value {
            return Err(ResourceIdError::NonCanonical);
        }
        Ok(parsed)
    }
}

impl Serialize for ResourceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ResourceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceIdError {
    InvalidFormat,
    UnknownPrefix(String),
    InvalidUuid,
    NotUuidV7,
    NonCanonical,
    WrongKind {
        expected: ResourceKind,
        actual: ResourceKind,
    },
}

impl fmt::Display for ResourceIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => formatter.write_str("resource ID must be {prefix}_{uuid-v7}"),
            Self::UnknownPrefix(prefix) => {
                write!(formatter, "unknown resource ID prefix {prefix:?}")
            }
            Self::InvalidUuid => formatter.write_str("resource ID UUID is invalid"),
            Self::NotUuidV7 => formatter.write_str("resource ID UUID must be RFC 9562 UUIDv7"),
            Self::NonCanonical => {
                formatter.write_str("resource ID must use canonical lowercase form")
            }
            Self::WrongKind { expected, actual } => write!(
                formatter,
                "resource ID kind mismatch: expected {}, got {}",
                expected.descriptor().name,
                actual.descriptor().name
            ),
        }
    }
}

impl Error for ResourceIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID_V7: &str = "0198f1c3-8f49-7c3e-b1f3-773c28367b7e";

    #[test]
    fn registry_has_unique_prefixes_and_names() {
        let prefixes = RESOURCE_KIND_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.prefix)
            .collect::<std::collections::BTreeSet<_>>();
        let names = RESOURCE_KIND_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(prefixes.len(), ResourceKind::ALL.len());
        assert_eq!(names.len(), ResourceKind::ALL.len());
        assert_eq!(RESOURCE_KIND_DESCRIPTORS.len(), ResourceKind::ALL.len());
    }

    #[test]
    fn parses_only_canonical_uuid_v7_with_known_prefix() {
        let value = format!("agt_{UUID_V7}");
        let parsed: ResourceId = value.parse().unwrap();
        assert_eq!(parsed.kind(), ResourceKind::Agent);
        assert_eq!(parsed.to_string(), value);

        assert!(format!("unknown_{UUID_V7}").parse::<ResourceId>().is_err());
        assert!(format!("AGT_{}", UUID_V7.to_uppercase())
            .parse::<ResourceId>()
            .is_err());
        assert!("agt_550e8400-e29b-41d4-a716-446655440000"
            .parse::<ResourceId>()
            .is_err());
    }

    #[test]
    fn constructs_only_from_rfc_uuid_v7() {
        let uuid = Uuid::parse_str(UUID_V7).unwrap();
        let id = ResourceId::from_uuid_v7(ResourceKind::Job, uuid).unwrap();
        assert_eq!(id.to_string(), format!("job_{UUID_V7}"));
        assert!(matches!(
            ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::new_v4()),
            Err(ResourceIdError::NotUuidV7)
        ));
    }

    #[test]
    fn field_kind_is_checked_in_addition_to_prefix_syntax() {
        let value = format!("mdep_{UUID_V7}");
        let failure =
            ResourceId::parse_expected(&value, ResourceKind::AgentDeployment).unwrap_err();
        assert!(matches!(failure, ResourceIdError::WrongKind { .. }));
    }
}
