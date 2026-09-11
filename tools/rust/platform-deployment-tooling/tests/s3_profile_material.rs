//! Test-only export of the actual closed S3 producer for the isolated IAM harness.
#![cfg(unix)]
use insight_platform_deployment_tooling::{
    private_state::InstallationDirectory,
    s3_profile::{render_s3_profile, S3IdentityRole, S3RoleCredentials},
};
use serde::Deserialize;
use std::{collections::BTreeMap, fs, os::unix::fs::MetadataExt, path::Path};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema_version: u32,
    bucket: String,
}

fn export(root: &Path) -> Result<(), &'static str> {
    if fs::symlink_metadata(root)
        .map_err(|_| "fixture_directory")?
        .uid()
        != unsafe { libc::geteuid() }
    {
        return Err("fixture_directory");
    }
    let directory = InstallationDirectory::open(root, true).map_err(|_| "fixture_directory")?;
    for name in [
        "profile-s3.json",
        "profile-security.toml",
        "profile-arguments.json",
    ] {
        if directory
            .read(name, 16_384)
            .map_err(|_| "fixture_output")?
            .is_some()
        {
            return Err("fixture_output_exists");
        }
    }
    let input: Input = serde_json::from_slice(
        &directory
            .read("profile-input.json", 4096)
            .map_err(|_| "fixture_input")?
            .ok_or("fixture_input")?,
    )
    .map_err(|_| "fixture_input")?;
    if input.schema_version != 1 {
        return Err("fixture_input");
    }
    let mut credentials = BTreeMap::new();
    for role in S3IdentityRole::ALL {
        let bytes = directory
            .read(role.credential_filename(), 256)
            .map_err(|_| "fixture_credentials")?
            .ok_or("fixture_credentials")?;
        credentials.insert(
            role,
            S3RoleCredentials::decode(&bytes).map_err(|_| "fixture_credentials")?,
        );
    }
    let profile = render_s3_profile(&input.bucket, &credentials).map_err(|_| "fixture_profile")?;
    for (name, bytes) in [
        ("profile-s3.json", profile.configuration_json),
        ("profile-security.toml", profile.security_toml),
        (
            "profile-arguments.json",
            serde_json::to_vec(&profile.arguments).map_err(|_| "fixture_profile")?,
        ),
    ] {
        directory
            .write_immutable(name, &bytes)
            .map_err(|_| "fixture_output")?;
    }
    Ok(())
}

#[test]
#[ignore = "requires the private, task-only static IAM qualification input directory"]
fn export_actual_s3_profile() {
    let root = std::env::var_os("INSIGHT_S3_PROFILE_FIXTURE_DIRECTORY")
        .expect("explicit fixture directory required");
    export(Path::new(&root)).expect("S3 profile fixture export failed with safe code");
}

#[test]
fn actual_profile_export_rejects_public_directory_and_never_replaces_outputs() {
    use std::os::unix::fs::PermissionsExt;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(export(&root).is_err());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    {
        let directory = InstallationDirectory::open(&root, true).unwrap();
        directory.write_immutable("profile-input.json", br#"{"schema_version":1,"bucket":"insight-platform-artifacts-0123456789abcdef0123456789abcdef"}"#).unwrap();
        for (index, role) in S3IdentityRole::ALL.into_iter().enumerate() {
            let bytes = format!(
                "[default]\naws_access_key_id={}\naws_secret_access_key={}\n",
                index.to_string().repeat(32),
                (index + 4).to_string().repeat(64)
            );
            directory
                .write_immutable(role.credential_filename(), bytes.as_bytes())
                .unwrap();
        }
    }
    export(&root).unwrap();
    let before = fs::read(root.join("profile-s3.json")).unwrap();
    assert!(export(&root).is_err());
    assert_eq!(fs::read(root.join("profile-s3.json")).unwrap(), before);
}
