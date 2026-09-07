//! Artifact work interpreter identities. Deployment build identity is deliberately separate.
use crate::{ArtifactJobPayload, ArtifactUploadOperationSnapshot, ArtifactWorkError};
use insight_platform_contracts::{
    canonical_digest, DomainOperationRequirements, ExecutionRequirement, Sha256Digest,
    WorkerExecutionCapabilities, WorkerExecutionCapability, EXECUTION_REQUIREMENT_VERSION,
};
use serde_json::{json, Value};

fn digest(value: Value) -> Sha256Digest {
    canonical_digest(&value)
        .expect("bounded Artifact execution contract")
        .parse()
        .expect("canonical SHA-256")
}
fn abi(operation: &str) -> Sha256Digest {
    digest(json!({"owner":"artifact", "operation":operation, "interpreter_version":1}))
}
fn object_adapter() -> Sha256Digest {
    digest(
        json!({"owner":"artifact", "protocol":"exact-generation-object-store", "schema_version":1}),
    )
}
fn scan_adapter(scanner: &Sha256Digest) -> Sha256Digest {
    digest(json!({"scanner_contract":scanner, "object_adapter":object_adapter()}))
}
fn scan_requirement(
    policy: &insight_platform_contracts::ExactVersionRef,
    scanner: &Sha256Digest,
    ruleset: &Sha256Digest,
    evidence_ttl: u64,
    retry: u64,
) -> ExecutionRequirement {
    ExecutionRequirement::DomainOperation {
        operation_abi_identity: abi("scan-payload-v2"),
        requirements: DomainOperationRequirements::Adapter {
            protocol_adapter_identity: scan_adapter(scanner),
            policy_inputs_digest: digest(
                json!({"scan_policy_revision":policy,"scanner_contract_digest":scanner,
                "ruleset_digest":ruleset,"evidence_ttl_milliseconds":evidence_ttl,"retry_backoff_milliseconds":retry}),
            ),
        },
    }
}
impl ArtifactUploadOperationSnapshot {
    pub fn execution_requirement(&self) -> Result<ExecutionRequirement, ArtifactWorkError> {
        self.canonical_digest()
            .map_err(|_| ArtifactWorkError::InvalidJobPayload)?;
        Ok(scan_requirement(
            &self.scan_policy_revision,
            &self.scanner_contract_digest,
            &self.ruleset_digest,
            self.evidence_ttl_milliseconds,
            self.retry_backoff_milliseconds,
        ))
    }
}
impl ArtifactJobPayload {
    pub fn execution_requirement(&self) -> Result<ExecutionRequirement, ArtifactWorkError> {
        let (operation, policy) = match self {
            Self::AwaitingStage { stage } => {
                stage.validate()?;
                return Ok(scan_requirement(
                    &stage.artifact_io_policy_revision,
                    &stage.scanner_contract_digest,
                    &stage.ruleset_digest,
                    stage.evidence_ttl_milliseconds,
                    stage.retry_backoff_milliseconds,
                ));
            }
            Self::Scan { scan } | Self::Rescan { scan } => {
                self.validate_for_owner(&scan.artifact_id)?;
                return Ok(scan_requirement(
                    &scan.scan_policy_revision,
                    &scan.scanner_contract_digest,
                    &scan.ruleset_digest,
                    scan.evidence_ttl_milliseconds,
                    scan.retry_backoff_milliseconds,
                ));
            }
            Self::Delete { deletion } => {
                self.validate_for_owner(&deletion.artifact_id)?;
                (
                    "deletion-payload-v1",
                    json!({"mode":deletion.mode,"retry_backoff_milliseconds":deletion.retry_backoff_milliseconds}),
                )
            }
            Self::BlobCleanup { cleanup } => {
                self.validate_for_owner(&cleanup.discarded_blob_id)?;
                (
                    "blob-cleanup-payload-v1",
                    json!({"verification_evidence_digest":cleanup.verification_evidence_digest,
                    "retry_backoff_milliseconds":cleanup.retry_backoff_milliseconds}),
                )
            }
        };
        Ok(ExecutionRequirement::DomainOperation {
            operation_abi_identity: abi(operation),
            requirements: DomainOperationRequirements::Adapter {
                protocol_adapter_identity: object_adapter(),
                policy_inputs_digest: digest(policy),
            },
        })
    }
}
pub fn execution_capabilities(
    scanner_contract_digest: &Sha256Digest,
) -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: vec![
            WorkerExecutionCapability::DomainOperation {
                operation_abi_identity: abi("scan-payload-v2"),
                adapter: Some(scan_adapter(scanner_contract_digest)),
            },
            WorkerExecutionCapability::DomainOperation {
                operation_abi_identity: abi("deletion-payload-v1"),
                adapter: Some(object_adapter()),
            },
            WorkerExecutionCapability::DomainOperation {
                operation_abi_identity: abi("blob-cleanup-payload-v1"),
                adapter: Some(object_adapter()),
            },
        ],
    }
}

pub fn data_worker_execution_capabilities(scanner: &Sha256Digest) -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: vec![WorkerExecutionCapability::DomainOperation {
            operation_abi_identity: abi("scan-payload-v2"),
            adapter: Some(scan_adapter(scanner)),
        }],
    }
}
pub fn maintenance_execution_capabilities() -> WorkerExecutionCapabilities {
    WorkerExecutionCapabilities {
        schema_version: EXECUTION_REQUIREMENT_VERSION,
        capabilities: ["deletion-payload-v1", "blob-cleanup-payload-v1"]
            .into_iter()
            .map(|operation| WorkerExecutionCapability::DomainOperation {
                operation_abi_identity: abi(operation),
                adapter: Some(object_adapter()),
            })
            .collect(),
    }
}

/// The sole built-in data-worker scanner semantics. Ruleset/policy inputs remain frozen
/// independently; configuration cannot rename this physical interpreter.
pub fn integrity_scanner_contract_digest() -> Sha256Digest {
    digest(
        json!({"owner":"artifact", "scanner":"integrity", "semantic_version":1,
        "exact_generation_read":"size-and-sha256-verified", "media_probe_order":["png","jpeg","pdf","zip","strict-json","utf8-without-nul","opaque"],
        "json_limits":{"max_depth":64,"max_items_per_array":65536,"max_properties_per_object":65536,"max_bytes":"input-length","max_string_bytes":"input-length"},
        "reject_signatures":["ELF","MZ","shebang","EICAR-STANDARD-ANTIVIRUS-TEST-FILE"],
        "archive":"quarantine", "declared_media":"exact-or-octet-stream-or-verified-application-plus-json",
        "mismatch":"quarantine", "remaining":"verified"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scanner_identity_is_not_build_or_policy_identity() {
        let scanner = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let catalog = execution_capabilities(&scanner);
        catalog.validate().unwrap();
        assert_ne!(scan_adapter(&scanner), scanner);
        let changed = format!("sha256:{}", "b".repeat(64)).parse().unwrap();
        assert_ne!(catalog, execution_capabilities(&changed));
        assert_eq!(
            catalog.capabilities[1],
            execution_capabilities(&changed).capabilities[1]
        );
    }
}
