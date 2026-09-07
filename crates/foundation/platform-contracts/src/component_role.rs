//! Closed process/image identity vocabulary; no release or promotion authority.

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};

/// Closed first-release process/image role used by startup and CI/CD manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComponentRole {
    ManagementApi,
    RuntimeApi,
    SchedulerRecovery,
    ModelWorker,
    CapabilityNativeWorker,
    CapabilityRemoteWorker,
    RegistryValidationWorker,
    ContextWorker,
    McpHost,
    SandboxDispatcher,
    OpenSandboxServer,
    OpenSandboxController,
    ArtifactGateway,
    ArtifactDataWorker,
    ArtifactMaintenance,
    EgressSecretBroker,
    OutboxWorker,
    HistoryMaintenance,
}

impl ComponentRole {
    pub const ALL: &'static [Self] = &[
        Self::ManagementApi,
        Self::RuntimeApi,
        Self::SchedulerRecovery,
        Self::ModelWorker,
        Self::CapabilityNativeWorker,
        Self::CapabilityRemoteWorker,
        Self::RegistryValidationWorker,
        Self::ContextWorker,
        Self::McpHost,
        Self::SandboxDispatcher,
        Self::OpenSandboxServer,
        Self::OpenSandboxController,
        Self::ArtifactGateway,
        Self::ArtifactDataWorker,
        Self::ArtifactMaintenance,
        Self::EgressSecretBroker,
        Self::OutboxWorker,
        Self::HistoryMaintenance,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManagementApi => "management_api",
            Self::RuntimeApi => "runtime_api",
            Self::SchedulerRecovery => "scheduler_recovery",
            Self::ModelWorker => "model_worker",
            Self::CapabilityNativeWorker => "capability_native_worker",
            Self::CapabilityRemoteWorker => "capability_remote_worker",
            Self::RegistryValidationWorker => "registry_validation_worker",
            Self::ContextWorker => "context_worker",
            Self::McpHost => "mcp_host",
            Self::SandboxDispatcher => "sandbox_dispatcher",
            Self::OpenSandboxServer => "opensandbox_server",
            Self::OpenSandboxController => "opensandbox_controller",
            Self::ArtifactGateway => "artifact_gateway",
            Self::ArtifactDataWorker => "artifact_data_worker",
            Self::ArtifactMaintenance => "artifact_maintenance",
            Self::EgressSecretBroker => "egress_secret_broker",
            Self::OutboxWorker => "outbox_worker",
            Self::HistoryMaintenance => "history_maintenance",
        }
    }
}

impl FromStr for ComponentRole {
    type Err = ComponentRoleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.as_str() == value)
            .ok_or(ComponentRoleError)
    }
}

impl fmt::Display for ComponentRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for ComponentRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ComponentRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentRoleError;

impl fmt::Display for ComponentRoleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("component role is not registered")
    }
}

impl Error for ComponentRoleError {}
