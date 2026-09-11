//! Shared development configuration producers. No PostgreSQL, RPC or CLI dependency.
pub mod base_profile;
pub mod bootstrap;
pub mod compose;
pub mod dependency_profile;
pub mod dev_profile;
pub mod full_profile;
pub mod identity;
pub mod installation;
pub mod kubernetes;
pub mod model_profile;
pub mod native;
pub mod openbao_profile;
pub mod private_state;
pub mod process_environment;
pub mod provider_config;
#[cfg(test)]
mod remote_context_tests;
pub mod renderer;
pub mod role_material;
pub mod role_output;
pub mod s3_profile;
pub mod tls;
pub mod worker_profile;
pub use dev_profile::DevProfile;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
#[derive(Debug)]
pub enum DeploymentError {
    Configuration(String),
}
impl std::fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for DeploymentError {}
pub(crate) fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 15)]),
            ]
        })
        .collect()
}
pub fn base_runtime_binary_paths(release: &Path) -> BTreeMap<&'static str, PathBuf> {
    let suffix = std::env::consts::EXE_SUFFIX;
    BTreeMap::from([
        (
            "platform-outbox-worker",
            release.join(format!("platform-outbox-worker{suffix}")),
        ),
        (
            "platform-jetstream-provision",
            release.join(format!("platform-jetstream-provision{suffix}")),
        ),
        (
            "platform-database-role",
            release.join(format!("platform-database-role{suffix}")),
        ),
        (
            "platform-history-maintenance",
            release.join(format!("platform-history-maintenance{suffix}")),
        ),
        (
            "platform-schema",
            release.join(format!("platform-schema{suffix}")),
        ),
        (
            "platform-dev-bootstrap",
            release.join(format!("platform-dev-bootstrap{suffix}")),
        ),
        (
            "platform-registry-validation-worker",
            release.join(format!("platform-registry-validation-worker{suffix}")),
        ),
        (
            "platform-gateway",
            release.join(format!("platform-gateway{suffix}")),
        ),
        (
            "platform-artifact-gateway",
            release.join(format!("platform-artifact-gateway{suffix}")),
        ),
        (
            "platform-artifact-data-worker",
            release.join(format!("platform-artifact-data-worker{suffix}")),
        ),
        (
            "platform-orchestration-worker",
            release.join(format!("platform-orchestration-worker{suffix}")),
        ),
        (
            "platform-capability-native-worker",
            release.join(format!("platform-capability-native-worker{suffix}")),
        ),
    ])
}

pub fn runtime_binary_paths(
    release: &Path,
    profile: DevProfile,
) -> BTreeMap<&'static str, PathBuf> {
    let mut binaries = base_runtime_binary_paths(release);
    let suffix = std::env::consts::EXE_SUFFIX;
    let mut include = |name: &'static str| {
        binaries.insert(name, release.join(format!("{name}{suffix}")));
    };
    if profile.has_context() {
        for name in [
            "platform-context-worker",
            "platform-context-dataset-worker",
            "platform-remote-context-worker",
            "platform-subscription-context-worker",
            "platform-mcp-resource-host",
        ] {
            include(name);
        }
    }
    if profile.needs_egress() {
        include("platform-security-authority");
        include("platform-egress-broker");
    }
    if profile.has_model() {
        include("platform-model-worker");
    }
    if profile.has_mcp() {
        for name in [
            "platform-mcp-host",
            "platform-mcp-resource-host",
            "platform-mcp-discovery-worker",
            "platform-mcp-subscription-worker",
            "platform-mcp-cleanup-worker",
            "platform-callback-api",
        ] {
            include(name);
        }
    }
    if profile.has_remote_capability() {
        include("platform-capability-remote-worker");
    }
    binaries
}

impl From<insight_platform_deployment_contracts::installation::InstallationError>
    for DeploymentError
{
    fn from(error: insight_platform_deployment_contracts::installation::InstallationError) -> Self {
        Self::Configuration(error.to_string())
    }
}
