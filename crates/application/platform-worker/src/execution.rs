//! Startup verification of physical worker identity against compiled owner catalogs.
use insight_platform_contracts::{
    Sha256Digest, WorkClass, WorkerExecutionCapabilities, WorkerManifest,
};
use sha2::{Digest, Sha256};
use std::{fmt, io::Read, path::Path};

#[derive(Debug)]
pub enum InstalledWorkerError {
    InvalidManifest,
    UnsupportedCapability,
    WrongRole,
    BinaryUnavailable,
    BinaryDigestMismatch,
}
impl fmt::Display for InstalledWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidManifest => "worker manifest or compiled catalog is invalid",
            Self::UnsupportedCapability => {
                "worker declares an execution capability absent from its compiled owner catalog"
            }
            Self::WrongRole => "worker manifest does not match the physical role and work class",
            Self::BinaryUnavailable => "worker executable bytes are unavailable",
            Self::BinaryDigestMismatch => {
                "worker executable digest differs from the deployment manifest"
            }
        })
    }
}
impl std::error::Error for InstalledWorkerError {}

pub fn executable_digest(path: &Path) -> Result<Sha256Digest, InstalledWorkerError> {
    let mut file =
        std::fs::File::open(path).map_err(|_| InstalledWorkerError::BinaryUnavailable)?;
    if !file
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    {
        return Err(InstalledWorkerError::BinaryUnavailable);
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| InstalledWorkerError::BinaryUnavailable)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let mut encoded = String::from("sha256:");
    for byte in digest.finalize() {
        encoded.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        encoded.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    encoded
        .parse()
        .map_err(|_| InstalledWorkerError::BinaryUnavailable)
}
/// Validates declared capabilities against a catalog passed by physical role composition, never
/// a catalog reconstructed from the input manifest. Exact subsetting permits deliberate rollout.
pub fn validate_worker_catalog(
    manifest: &WorkerManifest,
    supported_catalog: &WorkerExecutionCapabilities,
    expected_role: &str,
    expected_class: WorkClass,
) -> Result<(), InstalledWorkerError> {
    manifest
        .validate()
        .map_err(|_| InstalledWorkerError::InvalidManifest)?;
    supported_catalog
        .validate()
        .map_err(|_| InstalledWorkerError::InvalidManifest)?;
    if manifest.worker_role != expected_role || manifest.work_class != expected_class {
        return Err(InstalledWorkerError::WrongRole);
    }
    if manifest
        .execution_capabilities
        .capabilities
        .iter()
        .any(|capability| !supported_catalog.capabilities.contains(capability))
    {
        return Err(InstalledWorkerError::UnsupportedCapability);
    }
    Ok(())
}
pub fn validate_installed_worker(
    manifest: &WorkerManifest,
    supported_catalog: &WorkerExecutionCapabilities,
    expected_role: &str,
    expected_class: WorkClass,
) -> Result<(), InstalledWorkerError> {
    validate_worker_catalog(manifest, supported_catalog, expected_role, expected_class)?;
    let path = std::env::current_exe().map_err(|_| InstalledWorkerError::BinaryUnavailable)?;
    if executable_digest(&path)? != manifest.worker_build_digest {
        return Err(InstalledWorkerError::BinaryDigestMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> WorkerManifest {
        let identity: Sha256Digest = format!("sha256:{}", "1".repeat(64)).parse().unwrap();
        WorkerManifest {
            manifest_version: insight_platform_contracts::WORKER_MANIFEST_VERSION,
            worker_role: "fixture-worker".to_owned(),
            work_class: WorkClass::Artifact,
            adapter_runtime_digest: identity.clone(),
            worker_build_digest: executable_digest(&std::env::current_exe().unwrap()).unwrap(),
            execution_capabilities: WorkerExecutionCapabilities {
                schema_version: 1,
                capabilities: vec![
                    insight_platform_contracts::WorkerExecutionCapability::DomainOperation {
                        operation_abi_identity: identity,
                        adapter: None,
                    },
                ],
            },
            protocol_version: 1,
            max_concurrency: 1,
            critical_control_reserved_slots: 1,
        }
    }
    #[test]
    fn startup_rejects_fabricated_capability_and_different_executable() {
        let mut manifest = manifest();
        let supported = manifest.execution_capabilities.clone();
        validate_installed_worker(&manifest, &supported, "fixture-worker", WorkClass::Artifact)
            .unwrap();
        manifest.worker_build_digest = format!("sha256:{}", "0".repeat(64)).parse().unwrap();
        assert!(matches!(
            validate_installed_worker(&manifest, &supported, "fixture-worker", WorkClass::Artifact),
            Err(InstalledWorkerError::BinaryDigestMismatch)
        ));
        manifest.execution_capabilities.capabilities.push(
            insight_platform_contracts::WorkerExecutionCapability::AgentCompilation {
                compiler_semantic_identity: manifest.adapter_runtime_digest.clone(),
            },
        );
        assert!(matches!(
            validate_worker_catalog(&manifest, &supported, "fixture-worker", WorkClass::Artifact),
            Err(InstalledWorkerError::UnsupportedCapability)
        ));
        assert!(matches!(
            validate_worker_catalog(&manifest, &supported, "another-worker", WorkClass::Artifact),
            Err(InstalledWorkerError::WrongRole)
        ));
    }
}
