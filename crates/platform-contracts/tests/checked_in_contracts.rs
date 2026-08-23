use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile,
    machine::{check_contract_tree, repository_root_from_manifest},
    parse_strict_json, CandidateManifest, JsonLimits, NewCandidateManifest,
    QualificationArtifactLink, QualificationEvidenceManifest, QualificationGateEvidence,
    QualificationOutcome, QualificationProfile, Sha256Digest, WorkClass, WorkerManifest,
    QUALIFICATION_EVIDENCE_VERSION, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
};
use sha2::{Digest as _, Sha256};
use std::fs;
use std::{collections::BTreeMap, str::FromStr};

#[test]
fn checked_in_machine_contracts_match_the_rust_authority() {
    check_contract_tree(&repository_root_from_manifest()).unwrap();
}

#[test]
fn contract_manifest_closes_over_exact_checked_in_bytes() {
    let root = repository_root_from_manifest();
    let bytes = fs::read(root.join("contracts/platform-v1/manifest.json")).unwrap();
    let manifest = parse_strict_json(&bytes, JsonLimits::CONTRACT_FIXTURE).unwrap();
    for item in manifest["files"].as_array().unwrap() {
        let raw = fs::read(root.join(item["path"].as_str().unwrap())).unwrap();
        assert_eq!(raw.len() as u64, item["bytes"].as_u64().unwrap());
        assert_eq!(
            lowercase_sha256(&raw),
            item["sha256"].as_str().unwrap(),
            "{}",
            item["path"]
        );
    }
    assert_eq!(
        canonical_digest(&manifest["files"]).unwrap(),
        manifest["contract_digest"]
    );
}

#[test]
fn worker_manifest_example_matches_schema_and_typed_contract() {
    let root = repository_root_from_manifest();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("contracts/platform-v1/schemas/worker-manifest.schema.json")).unwrap(),
    )
    .unwrap();
    let example: serde_json::Value = serde_json::from_slice(
        &fs::read(
            root.join("contracts/platform-v1/examples/q1-orchestration-worker-manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .unwrap();
    assert!(validator.is_valid(&example));

    let manifest: WorkerManifest = serde_json::from_value(example.clone()).unwrap();
    manifest.validate().unwrap();
    manifest.canonical_digest().unwrap();

    let mut invalid = example;
    invalid["critical_control_reserved_slots"] = serde_json::json!(0);
    assert!(!validator.is_valid(&invalid));
    assert!(serde_json::from_value::<WorkerManifest>(invalid)
        .unwrap()
        .validate()
        .is_err());
}

#[test]
fn candidate_manifest_schema_matches_the_closed_rust_contract() {
    let root = repository_root_from_manifest();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("contracts/platform-v1/schemas/candidate-manifest.schema.json"))
            .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .unwrap();
    let worker = WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "orchestration.primary".to_owned(),
        work_class: WorkClass::Orchestration,
        adapter_runtime_digest: sha('a'),
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 64,
        critical_control_reserved_slots: 2,
    };
    let candidate = CandidateManifest::build(NewCandidateManifest {
        git_commit: format!("sha1:{}", "b".repeat(40)).parse().unwrap(),
        contract_digest: sha('c'),
        database_schema_version: 7,
        component_images: BTreeMap::from([("runtime_api".parse().unwrap(), sha('d'))]),
        worker_manifests: std::slice::from_ref(&worker),
        deployment_config_digest: sha('e'),
        hard_limit_profile: &checked_in_hard_limit_profile(),
        policy_baseline_digest: sha('f'),
        qualification_profile_digest: sha('9'),
        created_at: "2026-08-14T12:00:00.000000Z".parse().unwrap(),
    })
    .unwrap();
    let value = serde_json::to_value(&candidate).unwrap();
    assert!(validator.is_valid(&value));
    candidate
        .validate_against(&checked_in_hard_limit_profile(), &[worker])
        .unwrap();

    let mut invalid = value;
    invalid["git_commit"] = serde_json::json!("latest");
    assert!(!validator.is_valid(&invalid));
    assert!(serde_json::from_value::<CandidateManifest>(invalid).is_err());
}

#[test]
fn qualification_profile_and_evidence_schemas_match_rust_semantics() {
    let root = repository_root_from_manifest();
    let profile_schema: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("contracts/platform-v1/schemas/qualification-profile.schema.json"))
            .unwrap(),
    )
    .unwrap();
    let evidence_schema: serde_json::Value = serde_json::from_slice(
        &fs::read(
            root.join("contracts/platform-v1/schemas/qualification-evidence-manifest.schema.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let profile_value: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("contracts/platform-v1/qualification/production-release-profile.json"))
            .unwrap(),
    )
    .unwrap();
    let profile_validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&profile_schema)
        .unwrap();
    let evidence_validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&evidence_schema)
        .unwrap();
    assert!(profile_validator.is_valid(&profile_value));
    let profile: QualificationProfile = serde_json::from_value(profile_value).unwrap();
    profile.validate_for_production_release().unwrap();

    let worker = WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "orchestration.primary".to_owned(),
        work_class: WorkClass::Orchestration,
        adapter_runtime_digest: sha('a'),
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 64,
        critical_control_reserved_slots: 2,
    };
    let candidate = CandidateManifest::build(NewCandidateManifest {
        git_commit: format!("sha1:{}", "b".repeat(40)).parse().unwrap(),
        contract_digest: sha('c'),
        database_schema_version: 7,
        component_images: BTreeMap::from([("runtime_api".parse().unwrap(), sha('d'))]),
        worker_manifests: &[worker],
        deployment_config_digest: sha('e'),
        hard_limit_profile: &checked_in_hard_limit_profile(),
        policy_baseline_digest: sha('f'),
        qualification_profile_digest: profile.canonical_digest().unwrap(),
        created_at: "2026-08-23T00:00:00.000000Z".parse().unwrap(),
    })
    .unwrap();
    let evidence = QualificationEvidenceManifest {
        schema_version: QUALIFICATION_EVIDENCE_VERSION,
        qualification_profile_digest: profile.canonical_digest().unwrap(),
        candidate_manifest_digest: candidate.canonical_digest().unwrap(),
        topology_digest: sha('8'),
        seed: 42,
        started_at: "2026-08-23T00:00:00.000000Z".parse().unwrap(),
        completed_at: "2026-08-24T00:00:00.000000Z".parse().unwrap(),
        tool_versions: BTreeMap::from([("kubectl".to_owned(), "v1.35.0".to_owned())]),
        results: profile
            .required_gates
            .iter()
            .copied()
            .map(|gate| QualificationGateEvidence {
                gate,
                layer: gate.layer(),
                outcome: QualificationOutcome::Passed,
                evidence_digests: vec![sha('7')],
            })
            .collect(),
        artifact_links: vec![QualificationArtifactLink {
            name: "qualification-bundle".to_owned(),
            content_digest: sha('7'),
            media_type: "application/zstd".to_owned(),
            byte_length: 4096,
        }],
    };
    let evidence_value = serde_json::to_value(&evidence).unwrap();
    assert!(evidence_validator.is_valid(&evidence_value));
    evidence.validate_against(&profile, &candidate).unwrap();

    let mut incomplete = evidence;
    incomplete.results.pop();
    assert!(incomplete.validate_against(&profile, &candidate).is_err());
}

fn sha(character: char) -> Sha256Digest {
    Sha256Digest::from_str(&format!("sha256:{}", character.to_string().repeat(64))).unwrap()
}

fn lowercase_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
