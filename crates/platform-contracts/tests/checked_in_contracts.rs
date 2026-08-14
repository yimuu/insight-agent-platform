use insight_platform_contracts::{
    canonical_digest,
    machine::{check_contract_tree, repository_root_from_manifest},
    parse_strict_json, JsonLimits, WorkerManifest,
};
use sha2::{Digest as _, Sha256};
use std::fs;

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

fn lowercase_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
