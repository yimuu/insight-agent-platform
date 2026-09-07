use insight_platform_contracts::{parse_strict_json, JsonLimits};
use insight_platform_deployment_contracts::{
    CandidateManifest, CapacityProfile, QualificationArtifactLink, QualificationEvidenceManifest,
    QualificationProfile,
};
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use std::{
    env, fs,
    io::{BufReader, Read as _},
    path::Path,
    process,
};

fn main() {
    if let Err(failure) = run(env::args().skip(1).collect()) {
        eprintln!("platform qualification rejected input: {failure}");
        process::exit(1);
    }
}

fn history_evidence(
    config_path: &str,
    binary_path: &str,
    image: &str,
) -> Result<
    insight_platform_deployment_contracts::history::HistoryMaintenanceExecutableEvidenceV1,
    String,
> {
    let path = Path::new(config_path);
    if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        return Err("history config must be a real regular file".to_owned());
    }
    let limits =
        insight_platform_deployment_contracts::history::HISTORY_MAINTENANCE_CONFIG_JSON_LIMITS;
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|error| error.to_string())?
        .take(limits.max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    let value = parse_strict_json(&bytes, limits).map_err(|error| error.to_string())?;
    let config: insight_platform_deployment_contracts::history::HistoryMaintenanceConfigV1 =
        serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
    config.validate().map_err(str::to_owned)?;
    let executable_digest =
        insight_platform_worker::execution::executable_digest(Path::new(binary_path))
            .map_err(|error| error.to_string())?;
    if executable_digest != config.executable_digest {
        return Err(
            "history maintenance executable differs from signed process configuration".to_owned(),
        );
    }
    let evidence =
        insight_platform_deployment_contracts::history::HistoryMaintenanceExecutableEvidenceV1 {
            schema_version: 1,
            component_role: config.component_role,
            runtime_image_digest: image.parse().map_err(|_| "invalid runtime image digest")?,
            executable_digest,
            process_config_digest: insight_platform_contracts::canonical_digest(&value)
                .map_err(|error| error.to_string())?
                .parse()
                .map_err(|_| "invalid config digest")?,
        };
    evidence.validate().map_err(str::to_owned)?;
    Ok(evidence)
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    match arguments.as_slice() {
        [command, envelope] if command == "validate-recovery-envelope" => insight_platform_contract_tooling::recovery_validation::validate_envelope(Path::new(envelope)),
        [command, manifest, report, now] if command == "validate-recovery-set" => {
            let now = chrono::DateTime::parse_from_rfc3339(now).map_err(|_| "recovery_verification_time")?.with_timezone(&chrono::Utc);
            let result = insight_platform_contract_tooling::recovery_validation::validate(Path::new(manifest), Path::new(report), now)?;
            println!("{}", serde_json::to_string(&result).map_err(|error| error.to_string())?);
            Ok(())
        }
        [command, config_path, binary_path, image] if command == "validate-history-maintenance-deployment" => {
            let evidence = history_evidence(config_path, binary_path, image)?;
            println!("{}", serde_json::to_string(&evidence).map_err(|error| error.to_string())?); Ok(())
        }
        [command, config, binary, evidence] if command == "verify-history-maintenance-deployment" => {
            let expected: insight_platform_deployment_contracts::history::HistoryMaintenanceExecutableEvidenceV1 = read_closed_json(evidence)?;
            expected.validate().map_err(str::to_owned)?;
            let actual = history_evidence(config, binary, expected.runtime_image_digest.as_str())?;
            if actual != expected { return Err("history maintenance release evidence mismatch".to_owned()); }
            Ok(())
        }

        [command, schema, runner, image] if command == "build-schema-executable-evidence" => {
            let evidence = insight_platform_contract_tooling::schema_deployment::evidence(Path::new(schema), Path::new(runner), image.parse().map_err(|_| "invalid runtime image digest")?)?;
            println!("{}", serde_json::to_string(&evidence).map_err(|error| error.to_string())?); Ok(())
        }
        [command, schema, runner, evidence] if command == "verify-schema-executable-evidence" => {
            let evidence = read_closed_json(evidence)?;
            insight_platform_contract_tooling::schema_deployment::verify(Path::new(schema), Path::new(runner), &evidence)
        }
        [command] if command == "print-builtin-worker-protocols" => {
            println!("{}", insight_platform_contract_tooling::worker_deployment::builtin_worker_protocols()); Ok(())
        }
        [command] if command == "print-worker-executables" => {
            let records = insight_platform_deployment_contracts::workers::WORKER_EXECUTABLES.iter().map(|worker| serde_json::json!({
                "binary": worker.binary, "worker_role": worker.worker_role, "work_class": worker.work_class, "manifest_pointer": worker.manifest_pointer,
            })).collect::<Vec<_>>();
            println!("{}", serde_json::to_string(&records).map_err(|error| error.to_string())?); Ok(())
        }
        [command, binary, config] if command == "print-worker-execution-capabilities" => {
            let config: serde_json::Value = read_closed_json(config)?;
            let catalog = insight_platform_contract_tooling::worker_deployment::catalog(binary, &config)?;
            catalog.validate().map_err(|error| error.to_string())?;
            println!("{}", serde_json::to_string(&catalog).map_err(|error| error.to_string())?); Ok(())
        }
        [command, manifests, configs, binaries, image] if command == "validate-worker-deployment" => {
            let evidence = insight_platform_contract_tooling::worker_deployment::validate(Path::new(manifests), Path::new(configs), Path::new(binaries), image.parse().map_err(|_| "invalid runtime image digest")?)?;
            println!("{}", serde_json::to_string(&evidence).map_err(|error| error.to_string())?); Ok(())
        }
        [command] if command == "print-program-execution-capabilities" => {
            let catalog = insight_platform_plan::execution::program_execution_capabilities();
            catalog.validate().map_err(|failure| failure.to_string())?;
            println!("{}", serde_json::to_string(&catalog).map_err(|failure| failure.to_string())?);
            Ok(())
        }
        [command, capacity_path] if command == "validate-capacity-profile" => {
            let profile: CapacityProfile = read_closed_json(capacity_path)?;
            let digest = profile.canonical_digest().map_err(|failure| failure.to_string())?;
            println!("capacity profile valid but not thereby production-qualified ({digest})");
            Ok(())
        }
        [command, capacity_path, candidate_path] if command == "validate-candidate-capacity" => {
            let profile: CapacityProfile = read_closed_json(capacity_path)?;
            let candidate: CandidateManifest = read_closed_json(candidate_path)?;
            profile.validate_against_candidate(&candidate).map_err(|failure| failure.to_string())?;
            let digest = profile.canonical_digest().map_err(|failure| failure.to_string())?;
            println!("candidate capacity closure valid but not thereby production-qualified ({digest})");
            Ok(())
        }
        [command, capacity_path, candidate_path] if command == "validate-production-capacity" => {
            let profile: CapacityProfile = read_closed_json(capacity_path)?;
            let candidate: CandidateManifest = read_closed_json(candidate_path)?;
            profile
                .validate_for_production_release(&candidate)
                .map_err(|failure| failure.to_string())?;
            let digest = profile.canonical_digest().map_err(|failure| failure.to_string())?;
            println!("production capacity input closure valid but not thereby qualified ({digest})");
            Ok(())
        }
        [command, profile_path] if command == "validate-profile" => {
            let profile: QualificationProfile = read_closed_json(profile_path)?;
            let digest = profile
                .canonical_digest()
                .map_err(|failure| failure.to_string())?;
            println!("qualification profile valid ({digest})");
            Ok(())
        }
        [command, profile_path] if command == "validate-production-profile" => {
            let profile: QualificationProfile = read_closed_json(profile_path)?;
            profile
                .validate_for_production_release()
                .map_err(|failure| failure.to_string())?;
            let digest = profile
                .canonical_digest()
                .map_err(|failure| failure.to_string())?;
            println!("production qualification profile valid ({digest})");
            Ok(())
        }
        [command, profile_path, candidate_path] if command == "validate-production-candidate" => {
            let profile: QualificationProfile = read_closed_json(profile_path)?;
            let candidate: CandidateManifest = read_closed_json(candidate_path)?;
            candidate
                .validate_for_production_release(&profile)
                .map_err(|failure| failure.to_string())?;
            let digest = candidate
                .canonical_digest()
                .map_err(|failure| failure.to_string())?;
            println!("production candidate closure valid ({digest})");
            Ok(())
        }
        [
            command,
            profile_path,
            capacity_path,
            candidate_path,
            evidence_path,
            artifact_root,
        ]
            if command == "validate-release-evidence" =>
        {
            let profile: QualificationProfile = read_closed_json(profile_path)?;
            let capacity: CapacityProfile = read_closed_json(capacity_path)?;
            let candidate: CandidateManifest = read_closed_json(candidate_path)?;
            let evidence: QualificationEvidenceManifest = read_closed_json(evidence_path)?;
            profile
                .validate_for_production_release()
                .map_err(|failure| failure.to_string())?;
            candidate
                .validate_for_production_release(&profile)
                .map_err(|failure| failure.to_string())?;
            evidence
                .validate_with_capacity(&profile, &capacity, &candidate)
                .map_err(|failure| failure.to_string())?;
            if !evidence.passed() {
                return Err("one or more required qualification gates failed".to_owned());
            }
            verify_artifact_files(&evidence, artifact_root)?;
            let digest = evidence
                .canonical_digest(&profile, &candidate)
                .map_err(|failure| failure.to_string())?;
            println!(
                "production release evidence manifest is structurally valid and all declared gates passed ({digest})"
            );
            Ok(())
        }
        _ => Err(
            "usage: platform-qualification validate-capacity-profile <capacity.json> | validate-candidate-capacity <capacity.json> <candidate.json> | validate-production-capacity <capacity.json> <candidate.json> | validate-profile <profile.json> | validate-production-profile <profile.json> | validate-production-candidate <profile.json> <candidate.json> | validate-release-evidence <profile.json> <capacity.json> <candidate.json> <evidence.json> <artifact-root>"
                .to_owned(),
        ),
    }
}

fn verify_artifact_files(
    evidence: &QualificationEvidenceManifest,
    artifact_root: impl AsRef<Path>,
) -> Result<(), String> {
    let artifact_root = artifact_root.as_ref();
    let metadata = fs::symlink_metadata(artifact_root).map_err(|failure| {
        format!(
            "cannot inspect artifact root {}: {failure}",
            artifact_root.display()
        )
    })?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(format!(
            "artifact root must be a real directory: {}",
            artifact_root.display()
        ));
    }
    for artifact in &evidence.artifact_links {
        verify_artifact_file(artifact_root, artifact)?;
    }
    Ok(())
}

fn verify_artifact_file(
    artifact_root: &Path,
    artifact: &QualificationArtifactLink,
) -> Result<(), String> {
    let path = artifact_root.join(&artifact.name);
    let metadata = fs::symlink_metadata(&path).map_err(|failure| {
        format!(
            "cannot inspect evidence artifact {}: {failure}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!(
            "evidence artifact must be a real regular file: {}",
            path.display()
        ));
    }
    if metadata.len() != artifact.byte_length {
        return Err(format!(
            "evidence artifact byte length does not match manifest: {}",
            path.display()
        ));
    }

    let file = fs::File::open(&path).map_err(|failure| {
        format!(
            "cannot read evidence artifact {}: {failure}",
            path.display()
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(|failure| {
            format!(
                "cannot read evidence artifact {}: {failure}",
                path.display()
            )
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual_digest = sha256_digest_string(hasher.finalize().as_slice());
    if actual_digest != artifact.content_digest.as_str() {
        return Err(format!(
            "evidence artifact digest does not match manifest: {}",
            path.display()
        ));
    }
    Ok(())
}

fn sha256_digest_string(digest: &[u8]) -> String {
    let mut value = String::with_capacity("sha256:".len() + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn read_closed_json<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T, String> {
    let path = path.as_ref();
    let bytes =
        fs::read(path).map_err(|failure| format!("cannot read {}: {failure}", path.display()))?;
    let value = parse_strict_json(&bytes, JsonLimits::CONTRACT_FIXTURE)
        .map_err(|failure| format!("{}: {failure}", path.display()))?;
    serde_json::from_value(value)
        .map_err(|failure| format!("{} has the wrong closed shape: {failure}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::Sha256Digest;
    use std::{fs, str::FromStr as _};

    fn artifact(bytes: &[u8]) -> QualificationArtifactLink {
        QualificationArtifactLink {
            name: "gate-evidence.json".to_owned(),
            content_digest: Sha256Digest::from_str(&sha256_digest_string(
                Sha256::digest(bytes).as_slice(),
            ))
            .unwrap(),
            media_type: "application/json".to_owned(),
            byte_length: u64::try_from(bytes.len()).unwrap(),
        }
    }

    #[test]
    fn artifact_verification_binds_bytes_length_and_digest() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = br#"{"outcome":"passed"}"#;
        fs::write(directory.path().join("gate-evidence.json"), bytes).unwrap();
        verify_artifact_file(directory.path(), &artifact(bytes)).unwrap();

        let changed = br#"{"outcome":"failed"}"#;
        fs::write(directory.path().join("gate-evidence.json"), changed).unwrap();
        let failure = verify_artifact_file(directory.path(), &artifact(bytes)).unwrap_err();
        assert!(failure.contains("digest does not match"));

        let mut wrong_length = artifact(changed);
        wrong_length.byte_length += 1;
        let failure = verify_artifact_file(directory.path(), &wrong_length).unwrap_err();
        assert!(failure.contains("byte length does not match"));
    }

    #[cfg(unix)]
    #[test]
    fn artifact_verification_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let bytes = b"passed";
        fs::write(directory.path().join("target.json"), bytes).unwrap();
        symlink(
            directory.path().join("target.json"),
            directory.path().join("gate-evidence.json"),
        )
        .unwrap();
        let failure = verify_artifact_file(directory.path(), &artifact(bytes)).unwrap_err();
        assert!(failure.contains("real regular file"));
    }
}
