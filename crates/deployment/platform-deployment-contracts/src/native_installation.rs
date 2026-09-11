//! Current native deployment handoff. This contains file identities, never credential values.
use crate::installation::{
    InstallationError, InstallationInputV1, InstallationProcess, InstallationTopology,
};
use insight_platform_contracts::Sha256Digest;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Component, Path},
};

pub const NATIVE_PLAN_VERSION: u32 = 1;
pub const NATIVE_MAX_ARTIFACTS: usize = 256;
pub const NATIVE_MAX_ARTIFACT_BYTES: u64 = 536_870_912;
pub const NATIVE_MIN_NODE_VERSION: [u32; 3] = [24, 11, 1];

/// Servers become ready before a consumer performs its eager authenticated connection.
/// Filter this order by the installation's selected processes; never infer it in a launcher.
pub const NATIVE_START_ORDER: &[InstallationProcess] = &[
    InstallationProcess::SecurityAuthority,
    InstallationProcess::ArtifactGateway,
    InstallationProcess::ArtifactData,
    InstallationProcess::ArtifactMaintenance,
    InstallationProcess::EgressBroker,
    InstallationProcess::McpHost,
    InstallationProcess::McpResourceHost,
    InstallationProcess::GatewayManagement,
    InstallationProcess::GatewayRuntime,
    InstallationProcess::Orchestration,
    InstallationProcess::RegistryValidation,
    InstallationProcess::CapabilityNative,
    InstallationProcess::CapabilityRemote,
    InstallationProcess::ModelWorker,
    InstallationProcess::ContextNative,
    InstallationProcess::ContextRemote,
    InstallationProcess::ContextDataset,
    InstallationProcess::ContextSubscription,
    InstallationProcess::McpDiscovery,
    InstallationProcess::McpSubscription,
    InstallationProcess::McpCleanup,
    InstallationProcess::CallbackApi,
    InstallationProcess::Outbox,
    InstallationProcess::HistoryMaintenance,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArtifactV1 {
    pub path: String,
    pub bytes_digest: Sha256Digest,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessLaunchV1 {
    pub process: InstallationProcess,
    pub executable_file: String,
    pub environment_file: String,
    pub temporary_directory: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConsoleLaunchV1 {
    pub node_file: String,
    pub entrypoint_file: String,
    pub configuration_file: String,
    pub bundle_directory: String,
    pub minimum_node_version: [u32; 3],
    pub maximum_node_major: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLaunchPlanV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub host_os: String,
    pub host_arch: String,
    pub installation_binary: String,
    pub preparation_directories: Vec<String>,
    pub processes: Vec<NativeProcessLaunchV1>,
    pub console: NativeConsoleLaunchV1,
    pub artifacts: Vec<NativeArtifactV1>,
}

pub fn native_absolute_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 2048
        && !path.chars().any(char::is_control)
        && Path::new(path).is_absolute()
        && Path::new(path)
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
        && !path.ends_with('/')
        && !path.contains("//")
        && !path.split('/').any(|part| matches!(part, "." | ".."))
}

impl NativeLaunchPlanV1 {
    pub fn validate_for(&self, input: &InstallationInputV1) -> Result<(), InstallationError> {
        input.validate()?;
        let expected = input
            .network
            .processes
            .iter()
            .map(|entry| entry.process)
            .collect::<BTreeSet<_>>();
        let actual = self
            .processes
            .iter()
            .map(|entry| entry.process)
            .collect::<BTreeSet<_>>();
        if input.network.topology != InstallationTopology::Native
            || self.schema_version != NATIVE_PLAN_VERSION
            || self.input_digest != input.digest()?
            || !matches!(self.host_os.as_str(), "linux" | "macos")
            || !matches!(self.host_arch.as_str(), "x86_64" | "aarch64")
            || actual != expected
            || actual.len() != self.processes.len()
            || self
                .processes
                .iter()
                .map(|entry| entry.process)
                .collect::<Vec<_>>()
                != NATIVE_START_ORDER
                    .iter()
                    .copied()
                    .filter(|process| expected.contains(process))
                    .collect::<Vec<_>>()
            || self.artifacts.is_empty()
            || self.artifacts.len() > NATIVE_MAX_ARTIFACTS
            || self.console.minimum_node_version != NATIVE_MIN_NODE_VERSION
            || self.console.maximum_node_major != 24
        {
            return Err(InstallationError::InvalidInput);
        }
        let mut files = BTreeSet::new();
        for artifact in &self.artifacts {
            if !native_absolute_path(&artifact.path) || !files.insert(&artifact.path) {
                return Err(InstallationError::InvalidPath);
            }
        }
        let executable = |path: &str| {
            self.artifacts
                .iter()
                .any(|entry| entry.path == path && entry.executable)
        };
        if !executable(&self.installation_binary)
            || !executable(&self.console.node_file)
            || !files.contains(&self.console.entrypoint_file)
            || !native_absolute_path(&self.console.configuration_file)
            || !native_absolute_path(&self.console.bundle_directory)
        {
            return Err(InstallationError::InvalidPath);
        }
        let output = input
            .paths
            .first()
            .and_then(|paths| Path::new(&paths.configuration_directory).parent())
            .and_then(Path::parent)
            .and_then(Path::parent)
            .ok_or(InstallationError::InvalidPath)?;
        let console_root = Path::new(&self.console.entrypoint_file)
            .parent()
            .and_then(Path::parent)
            .ok_or(InstallationError::InvalidPath)?;
        let expected_directories = std::iter::once(output.to_path_buf())
            .chain(
                [
                    "dependencies",
                    "roles",
                    "postgres-data",
                    "nats-data",
                    "s3-data",
                    "openbao-data",
                ]
                .into_iter()
                .map(|name| output.join(name)),
            )
            .collect::<Vec<_>>();
        if self
            .preparation_directories
            .iter()
            .map(Path::new)
            .collect::<Vec<_>>()
            != expected_directories
                .iter()
                .map(|path| path.as_path())
                .collect::<Vec<_>>()
        {
            return Err(InstallationError::InvalidPath);
        }
        let bundle = console_root.join("dist");
        if Path::new(&self.console.configuration_file) != output.join("roles/console/config.json")
            || Path::new(&self.console.entrypoint_file) != console_root.join("server-dist/main.js")
            || Path::new(&self.console.bundle_directory) != bundle
            || !["main.js", "config.js", "gateway-server.js", "process.js"]
                .iter()
                .all(|name| {
                    self.artifacts.iter().any(|entry| {
                        !entry.executable
                            && Path::new(&entry.path) == console_root.join("server-dist").join(name)
                    })
                })
            || !self.artifacts.iter().any(|entry| {
                !entry.executable && Path::new(&entry.path) == bundle.join("index.html")
            })
            || !self.artifacts.iter().any(|entry| {
                !entry.executable
                    && Path::new(&entry.path).starts_with(&bundle)
                    && entry.path.ends_with(".wasm")
            })
            || !self.artifacts.iter().any(|entry| {
                !entry.executable
                    && Path::new(&entry.path).starts_with(&bundle)
                    && Path::new(&entry.path)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| {
                            name.starts_with("compiler.worker-") && name.ends_with(".js")
                        })
            })
        {
            return Err(InstallationError::InvalidPath);
        }
        for process in &self.processes {
            let paths = input
                .paths
                .iter()
                .find(|entry| entry.process == process.process)
                .ok_or(InstallationError::InvalidRoleClosure)?;
            if !executable(&process.executable_file)
                || !native_absolute_path(&process.environment_file)
                || !native_absolute_path(&process.temporary_directory)
                || Path::new(&paths.configuration_directory)
                    .parent()
                    .map(|root| root.join("environment"))
                    != Some(Path::new(&process.environment_file).to_path_buf())
                || process.temporary_directory != paths.temporary_directory
                || Path::new(&process.executable_file)
                    .file_name()
                    .and_then(|name| name.to_str())
                    != Some(process.process.binary())
            {
                return Err(InstallationError::InvalidPath);
            }
        }
        Ok(())
    }
}
