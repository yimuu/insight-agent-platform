//! Physical executable-to-manifest composition for signed deployment closure.
use insight_platform_contracts::WorkClass;

#[derive(Debug, Clone, Copy)]
pub struct WorkerExecutable {
    pub binary: &'static str,
    pub worker_role: &'static str,
    pub work_class: WorkClass,
    pub manifest_pointer: &'static str,
}
/// Every Job-claiming physical executable requires its own exact deployment manifest.
pub const WORKER_EXECUTABLES: &[WorkerExecutable] = &[
    WorkerExecutable {
        binary: "platform-mcp-cleanup-worker",
        worker_role: "mcp-cleanup-worker",
        work_class: WorkClass::Recovery,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-orchestration-worker",
        worker_role: "orchestration-worker",
        work_class: WorkClass::Orchestration,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-registry-validation-worker",
        worker_role: "registry-validation-worker",
        work_class: WorkClass::RegistryValidation,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-model-worker",
        worker_role: "model-worker",
        work_class: WorkClass::Model,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-capability-native-worker",
        worker_role: "capability.native",
        work_class: WorkClass::CapabilityNative,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-capability-remote-worker",
        worker_role: "capability.remote",
        work_class: WorkClass::CapabilityRemote,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-context-worker",
        worker_role: "context-worker",
        work_class: WorkClass::Context,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-remote-context-worker",
        worker_role: "context-worker",
        work_class: WorkClass::Context,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-subscription-context-worker",
        worker_role: "context-worker",
        work_class: WorkClass::Context,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-context-dataset-worker",
        worker_role: "context-dataset-worker",
        work_class: WorkClass::Context,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-sandbox-dispatcher",
        worker_role: "sandbox-dispatcher",
        work_class: WorkClass::Sandbox,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-artifact-data-worker",
        worker_role: "artifact-data-worker",
        work_class: WorkClass::Artifact,
        manifest_pointer: "/scan_worker/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-artifact-maintenance",
        worker_role: "artifact-maintenance",
        work_class: WorkClass::Artifact,
        manifest_pointer: "/worker/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-mcp-discovery-worker",
        worker_role: "mcp-discovery-worker",
        work_class: WorkClass::Mcp,
        manifest_pointer: "/worker_manifest",
    },
    WorkerExecutable {
        binary: "platform-mcp-subscription-worker",
        worker_role: "mcp-subscription-worker",
        work_class: WorkClass::Mcp,
        manifest_pointer: "/worker_manifest",
    },
];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutableEvidenceV1 {
    pub schema_version: u32,
    pub runtime_image_digest: insight_platform_contracts::Sha256Digest,
    pub workers: Vec<WorkerExecutableEvidenceEntryV1>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutableEvidenceEntryV1 {
    pub binary: String,
    pub worker_manifest_digest: insight_platform_contracts::Sha256Digest,
    pub worker_build_digest: insight_platform_contracts::Sha256Digest,
    pub process_config_digest: insight_platform_contracts::Sha256Digest,
}
impl WorkerExecutableEvidenceV1 {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1 || self.workers.len() != WORKER_EXECUTABLES.len() {
            return Err("worker evidence does not cover the executable closure");
        }
        let mut seen = std::collections::BTreeSet::new();
        for worker in &self.workers {
            if !seen.insert(&worker.binary)
                || !WORKER_EXECUTABLES
                    .iter()
                    .any(|known| known.binary == worker.binary)
            {
                return Err("worker evidence contains an unknown or duplicate executable");
            }
        }
        Ok(())
    }
}
