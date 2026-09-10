//! Worker identities are derived from the selected executable files before any config is hashed.
use crate::{DeploymentError, DevProfile};
use insight_platform_contracts::{
    Sha256Digest, WorkClass, WorkerExecutionCapabilities, WorkerExecutionCapability,
    WorkerManifest, EXECUTION_REQUIREMENT_VERSION, WORKER_MANIFEST_VERSION,
    WORKER_PROTOCOL_VERSION,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Read, path::Path};

pub struct WorkerBuilds(BTreeMap<&'static str, Sha256Digest>);
impl WorkerBuilds {
    pub fn read(directory: &Path, selected: DevProfile) -> Result<Self, DeploymentError> {
        let mut builds = BTreeMap::new();
        for (name, path) in crate::runtime_binary_paths(directory, selected) {
            let mut file = std::fs::File::open(&path).map_err(|error| {
                DeploymentError::Configuration(format!(
                    "cannot read selected worker executable {}: {error}",
                    path.display()
                ))
            })?;
            if !file
                .metadata()
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
            {
                return Err(DeploymentError::Configuration(format!(
                    "worker executable {} must be a nonempty file",
                    path.display()
                )));
            }
            let mut hash = Sha256::new();
            let mut buffer = [0_u8; 65536];
            loop {
                let count = file.read(&mut buffer).map_err(|error| {
                    DeploymentError::Configuration(format!(
                        "cannot hash worker executable: {error}"
                    ))
                })?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            builds.insert(
                name,
                format!("sha256:{}", crate::lower_hex(&hash.finalize()))
                    .parse()
                    .expect("SHA256 executable hash"),
            );
        }
        Ok(Self(builds))
    }
    pub fn read_processes(
        directory: &Path,
        processes: &[insight_platform_deployment_contracts::installation::InstallationProcess],
    ) -> Result<Self, DeploymentError> {
        let mut builds = BTreeMap::new();
        for process in processes {
            let name = process.binary();
            if builds.contains_key(name) {
                continue;
            }
            let path = directory.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
            let metadata = std::fs::symlink_metadata(&path).map_err(|_| {
                DeploymentError::Configuration("selected executable missing".into())
            })?;
            if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
                return Err(DeploymentError::Configuration(
                    "selected executable must be a nonempty regular file".into(),
                ));
            }
            let mut file = std::fs::File::open(&path).map_err(|_| {
                DeploymentError::Configuration("selected executable unreadable".into())
            })?;
            let mut hash = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let count = file
                    .read(&mut buffer)
                    .map_err(|_| DeploymentError::Configuration("executable hash failed".into()))?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            builds.insert(
                name,
                format!("sha256:{}", crate::lower_hex(&hash.finalize()))
                    .parse()
                    .expect("SHA256 executable hash"),
            );
        }
        Ok(Self(builds))
    }
    pub fn executable(&self, binary: &str) -> Option<&Sha256Digest> {
        self.0.get(binary)
    }
    pub fn manifest(
        &self,
        binary: &str,
        role: &str,
        work_class: WorkClass,
        adapter: &str,
        capacity: (u16, u16),
        execution_capabilities: WorkerExecutionCapabilities,
    ) -> Option<serde_json::Value> {
        // Optional only while assembling an unselected feature; selected executable reads above
        // fail closed. No incomplete manifest is ever persisted.
        let worker_build_digest = self.0.get(binary)?.clone();
        let (concurrency, reserved) = capacity;
        let manifest = WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: role.to_owned(),
            work_class,
            adapter_runtime_digest: adapter.parse().expect("closed local adapter digest"),
            worker_build_digest,
            execution_capabilities,
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: concurrency,
            critical_control_reserved_slots: reserved,
        };
        manifest
            .validate()
            .expect("local owner-generated WorkerManifest");
        Some(serde_json::to_value(manifest).expect("typed WorkerManifest JSON"))
    }
}
pub fn programs() -> WorkerExecutionCapabilities {
    insight_platform_plan::execution::program_execution_capabilities()
}
pub fn registry() -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: vec![
            WorkerExecutionCapability::AgentCompilation {
                compiler_semantic_identity:
                    insight_platform_agent_compiler::compiler_semantic_identity(),
            },
            insight_platform_registry::registry_resource_validation_execution_capability(),
        ],
    }
}
#[cfg(test)]
pub fn fixture_binaries(root: &Path) -> std::path::PathBuf {
    let directory = root.join("worker-binary-fixtures");
    std::fs::create_dir_all(&directory).unwrap();
    for name in crate::base_runtime_binary_paths(&directory)
        .keys()
        .copied()
        .chain(crate::full_profile::INITIAL_BINARY_NAMES)
        .chain(["platform-sandbox-dispatcher"])
    {
        std::fs::write(
            directory.join(name),
            format!("dedicated test executable bytes: {name}\n"),
        )
        .unwrap();
    }
    directory
}

pub fn subscription() -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: vec![
            insight_platform_context::execution::subscription_execution_capability(),
        ],
    }
}
pub fn dataset(adapter: &str) -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: vec![
            insight_platform_context::execution::dataset_execution_capability(
                &adapter.parse().expect("Context adapter protocol identity"),
            ),
        ],
    }
}

fn with_adapters(
    adapters: impl IntoIterator<Item = WorkerExecutionCapability>,
) -> WorkerExecutionCapabilities {
    let mut catalog = programs();
    for adapter in adapters {
        if !catalog.capabilities.contains(&adapter) {
            catalog.capabilities.push(adapter);
        }
    }
    catalog.validate().expect("closed local execution catalog");
    catalog
}
pub fn model_manifest(builds: &WorkerBuilds) -> Option<serde_json::Value> {
    builds.manifest(
        "platform-model-worker",
        "model-worker",
        WorkClass::Model,
        &crate::base_profile::local_digest("model-worker-adapter")
            .expect("closed local Model runtime identity"),
        (4, 1),
        model(),
    )
}

pub fn model() -> WorkerExecutionCapabilities {
    use insight_platform_contracts::ModelProviderWireProtocol;
    with_adapters(
        [
            ModelProviderWireProtocol::AnthropicMessages,
            ModelProviderWireProtocol::OpenAiResponses,
        ]
        .map(|protocol| {
            insight_platform_contracts::model_adapter_execution_capability_from_contract(
                protocol.qualified_name(),
                &protocol.adapter_contract_digest(),
            )
            .expect("closed Model adapter")
        }),
    )
}
pub fn native_capability(module: &str) -> WorkerExecutionCapabilities {
    with_adapters([
        insight_platform_contracts::native_capability_adapter_execution_capability(
            &module.parse().expect("native module digest"),
        )
        .expect("closed native capability"),
    ])
}
pub fn native_context(contract: &str, installed: &str) -> WorkerExecutionCapabilities {
    with_adapters([
        insight_platform_contracts::native_context_adapter_execution_capability(
            &contract.parse().expect("Context contract digest"),
            &installed.parse().expect("Context installed digest"),
        )
        .expect("closed native Context"),
    ])
}
pub fn remote_capability(closure: &serde_json::Value) -> WorkerExecutionCapabilities {
    let adapters = ["http", "grpc", "mcp"].into_iter().flat_map(|kind| closure[kind].as_array().expect("closed codec set").iter().map(move |codec| {
        let descriptor: insight_platform_contracts::InstalledCapabilityCodecRef = serde_json::from_value(serde_json::json!({
            "schema_version": 1, "backend_kind": kind, "codec_id": codec["codec_id"], "codec_version": codec["codec_version"],
            "module_digest": codec["module_digest"], "worker_protocol_version": codec["worker_protocol_version"], "descriptor_digest": codec["descriptor_digest"],
        })).expect("closed remote codec descriptor");
        insight_platform_contracts::remote_capability_adapter_execution_capability(&descriptor).expect("closed remote codec")
    }));
    with_adapters(adapters)
}

pub fn remote_context() -> WorkerExecutionCapabilities {
    with_adapters([
        insight_platform_contracts::remote_context_adapter_execution_capability(
            &insight_platform_context::remote_context_protocol_contract_digest(),
            &insight_platform_context::remote_context_result_mapping_digest(),
        )
        .expect("closed Remote Context wire"),
    ])
}
