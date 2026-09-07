//! Bounded offline recovery validation. External attestations remain the deployer's evidence.
use chrono::{DateTime, Utc};
use insight_platform_contracts::{parse_strict_json, JsonLimits, Sha256Digest};
use insight_platform_deployment_contracts::recovery::{
    RecoveryManifestV1, RecoveryVerificationReportV1, RECOVERY_EVIDENCE_MAX_BYTES,
    RECOVERY_MANIFEST_LIMITS, RECOVERY_REPORT_LIMITS, RECOVERY_SET_MAX_BYTES,
};
use serde::de::DeserializeOwned;
use std::{io::Read, path::Path};

fn read<T: DeserializeOwned>(path: &Path, limits: JsonLimits) -> Result<T, String> {
    let metadata = path
        .symlink_metadata()
        .map_err(|_| "recovery_input_missing")?;
    if !metadata.is_file() || metadata.len() > limits.max_bytes as u64 {
        return Err("recovery_input_bounds".to_owned());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "recovery_input_unreadable")?
        .take(limits.max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "recovery_input_unreadable")?;
    serde_json::from_value(parse_strict_json(&bytes, limits).map_err(|_| "recovery_input_json")?)
        .map_err(|_| "recovery_input_schema".to_owned())
}

pub fn validate(
    manifest: &Path,
    report: &Path,
    now: DateTime<Utc>,
) -> Result<serde_json::Value, String> {
    let manifest: RecoveryManifestV1 = read(manifest, RECOVERY_MANIFEST_LIMITS)?;
    let report: RecoveryVerificationReportV1 = read(report, RECOVERY_REPORT_LIMITS)?;
    let evidence = report
        .validate_for(&manifest, now)
        .map_err(|error| error.to_string())?;
    let current = insight_platform_plan::execution::program_execution_capabilities();
    if manifest.program_capabilities != current {
        return Err("recovery_program_not_installed".to_owned());
    }
    let executable = std::env::current_exe().map_err(|_| "recovery_executable_unavailable")?;
    let current_build: Sha256Digest =
        insight_platform_worker::execution::executable_digest(&executable)
            .map_err(|_| "recovery_executable_unreadable")?;
    if manifest.recovery_tool_build_digest != current_build {
        return Err("recovery_tool_build_mismatch".to_owned());
    }
    Ok(serde_json::json!({
        "schema_version": 1, "manifest_digest": report.manifest_digest,
        "recovery_tool_build_digest": current_build,
        "evidence_digests": evidence, "evidence_max_bytes": RECOVERY_EVIDENCE_MAX_BYTES,
        "set_max_bytes": RECOVERY_SET_MAX_BYTES,
        "quarantined_effect_count": report.unrecovered_effects.len(),
        "declaration_consistency_verified": true,
        "external_state_verified_by_tool": false,
    }))
}

pub fn validate_envelope(path: &Path) -> Result<(), String> {
    let envelope: insight_platform_deployment_contracts::recovery::RecoverySetV1 = read(
        path,
        insight_platform_deployment_contracts::recovery::RECOVERY_SET_LIMITS,
    )?;
    envelope.validate().map_err(|error| error.to_string())
}
