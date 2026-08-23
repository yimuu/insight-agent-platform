use insight_platform_contracts::{
    parse_strict_json, CandidateManifest, JsonLimits, QualificationEvidenceManifest,
    QualificationProfile,
};
use serde::de::DeserializeOwned;
use std::{env, fs, path::Path, process};

fn main() {
    if let Err(failure) = run(env::args().skip(1).collect()) {
        eprintln!("platform qualification rejected input: {failure}");
        process::exit(1);
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    match arguments.as_slice() {
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
        [command, profile_path, candidate_path, evidence_path]
            if command == "validate-release-evidence" =>
        {
            let profile: QualificationProfile = read_closed_json(profile_path)?;
            let candidate: CandidateManifest = read_closed_json(candidate_path)?;
            let evidence: QualificationEvidenceManifest = read_closed_json(evidence_path)?;
            profile
                .validate_for_production_release()
                .map_err(|failure| failure.to_string())?;
            candidate
                .validate_for_production_release(&profile)
                .map_err(|failure| failure.to_string())?;
            evidence
                .validate_against(&profile, &candidate)
                .map_err(|failure| failure.to_string())?;
            if !evidence.passed() {
                return Err("one or more required qualification gates failed".to_owned());
            }
            let digest = evidence
                .canonical_digest(&profile, &candidate)
                .map_err(|failure| failure.to_string())?;
            println!("production release evidence valid and passed ({digest})");
            Ok(())
        }
        _ => Err(
            "usage: platform-qualification validate-profile <profile.json> | validate-production-profile <profile.json> | validate-production-candidate <profile.json> <candidate.json> | validate-release-evidence <profile.json> <candidate.json> <evidence.json>"
                .to_owned(),
        ),
    }
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
