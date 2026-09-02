use insight_platform_contracts::{
    parse_strict_json, CandidateManifest, CapacityProfile, JsonLimits, QualificationArtifactLink,
    QualificationEvidenceManifest, QualificationProfile,
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

fn run(arguments: Vec<String>) -> Result<(), String> {
    match arguments.as_slice() {
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
