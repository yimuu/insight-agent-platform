//! Offline verification of the exact current schema release files and provisioning binary.
use insight_platform_contracts::Sha256Digest;
use insight_platform_deployment_contracts::schema::{
    SchemaExecutableEvidenceV1, SCHEMA_RELEASE_FILES, SCHEMA_RUNNER_BINARY,
};
use insight_platform_worker::execution::executable_digest;
use std::{collections::BTreeSet, path::Path};

fn digest(path: &Path) -> Result<Sha256Digest, String> {
    let metadata = path.symlink_metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(format!(
            "schema release input {} must be a nonempty physical file",
            path.display()
        ));
    }
    executable_digest(path).map_err(|error| error.to_string())
}

pub fn evidence(
    schema: &Path,
    runner: &Path,
    image: Sha256Digest,
) -> Result<SchemaExecutableEvidenceV1, String> {
    let expected = SCHEMA_RELEASE_FILES
        .iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let observed = std::fs::read_dir(schema)
        .map_err(|error| error.to_string())?
        .map(|entry| {
            let entry = entry.map_err(|error| error.to_string())?;
            if !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
            {
                return Err("schema release closure must contain only physical files".to_owned());
            }
            entry
                .file_name()
                .into_string()
                .map_err(|_| "schema release filename must be UTF8".to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if observed != expected {
        return Err("schema release closure contains missing or extra files".to_owned());
    }
    let evidence = SchemaExecutableEvidenceV1 {
        schema_version: 1,
        runtime_image_digest: image,
        runner_binary: SCHEMA_RUNNER_BINARY.to_owned(),
        runner_build_digest: digest(runner)?,
        schema_snapshot_digest: digest(&schema.join("schema.sql"))?,
        schema_inventory_digest: digest(&schema.join("schema-inventory.json"))?,
        schema_contract_digest: digest(&schema.join("schema-contract.json"))?,
    };
    evidence.validate().map_err(str::to_owned)?;
    Ok(evidence)
}

pub fn verify(
    schema: &Path,
    runner: &Path,
    expected: &SchemaExecutableEvidenceV1,
) -> Result<(), String> {
    expected.validate().map_err(str::to_owned)?;
    let actual = evidence(schema, runner, expected.runtime_image_digest.clone())?;
    if actual != *expected {
        return Err("schema release bytes differ from signed executable evidence".to_owned());
    }
    Ok(())
}
