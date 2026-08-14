use crate::{canonical_digest, registry::WorkClass, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, str::FromStr};

pub const WORKER_MANIFEST_VERSION: u32 = 1;
pub const WORKER_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerManifest {
    pub manifest_version: u32,
    pub worker_role: String,
    pub work_class: WorkClass,
    pub adapter_runtime_digest: Sha256Digest,
    pub protocol_version: u32,
    pub max_concurrency: u16,
    pub critical_control_reserved_slots: u16,
}

impl WorkerManifest {
    pub fn validate(&self) -> Result<(), WorkerManifestError> {
        if self.manifest_version != WORKER_MANIFEST_VERSION {
            return Err(invalid(
                "manifest_version",
                "must equal the closed worker manifest version",
            ));
        }
        if !is_stable_worker_role(&self.worker_role) {
            return Err(invalid(
                "worker_role",
                "must be a bounded lowercase stable role code",
            ));
        }
        if self.protocol_version != WORKER_PROTOCOL_VERSION {
            return Err(invalid(
                "protocol_version",
                "must equal the closed worker protocol version",
            ));
        }
        if self.max_concurrency == 0 {
            return Err(invalid("max_concurrency", "must be positive"));
        }
        if self.critical_control_reserved_slots == 0 {
            return Err(invalid(
                "critical_control_reserved_slots",
                "every worker role must reserve independent control capacity",
            ));
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, WorkerManifestError> {
        self.validate()?;
        let value = serde_json::to_value(self)
            .map_err(|_| invalid("worker_manifest", "cannot be serialized canonically"))?;
        let digest = canonical_digest(&value)
            .map_err(|_| invalid("worker_manifest", "cannot be canonicalized"))?;
        Sha256Digest::from_str(&digest)
            .map_err(|_| invalid("worker_manifest", "canonical digest is invalid"))
    }
}

fn is_stable_worker_role(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_lowercase()
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(*byte, b'_' | b'-' | b'.')
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerManifestError {
    pub field: &'static str,
    pub message: &'static str,
}

impl fmt::Display for WorkerManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid worker manifest {}: {}",
            self.field, self.message
        )
    }
}

impl Error for WorkerManifestError {}

fn invalid(field: &'static str, message: &'static str) -> WorkerManifestError {
    WorkerManifestError { field, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn manifest() -> WorkerManifest {
        WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: "orchestration.primary".to_owned(),
            work_class: WorkClass::Orchestration,
            adapter_runtime_digest: digest('a'),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 64,
            critical_control_reserved_slots: 2,
        }
    }

    #[test]
    fn manifest_is_closed_and_canonically_digestible() {
        let manifest = manifest();
        manifest.validate().unwrap();
        assert_eq!(manifest.canonical_digest(), manifest.canonical_digest());

        let mut value = serde_json::to_value(&manifest).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<WorkerManifest>(value).is_err());
    }

    #[test]
    fn manifest_requires_exact_protocol_and_reserved_control_capacity() {
        let mut candidate = manifest();
        candidate.protocol_version += 1;
        assert_eq!(candidate.validate().unwrap_err().field, "protocol_version");

        candidate = manifest();
        candidate.critical_control_reserved_slots = 0;
        assert_eq!(
            candidate.validate().unwrap_err().field,
            "critical_control_reserved_slots"
        );

        candidate = manifest();
        candidate.worker_role = "Sandbox Primary".to_owned();
        assert_eq!(candidate.validate().unwrap_err().field, "worker_role");
    }
}
