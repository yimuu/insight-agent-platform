//! Observe the actual source or prebuilt cache across the public stop/start journey.
//! Signature, CLI and runtime identity validation remains in the CLI's owning restart path.
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

#[derive(Debug, PartialEq, Eq)]
struct FileEvidence {
    bytes: u64,
    digest: String,
    modified: SystemTime,
}

fn file(path: &Path) -> Result<FileEvidence, String> {
    let metadata = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > 256 * 1024 * 1024
    {
        return Err(format!("invalid restart cache file {}", path.display()));
    }
    let mut input = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65_536];
    loop {
        let count = input.read(&mut buffer).map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(FileEvidence {
        bytes: metadata.len(),
        digest: format!(
            "sha256:{}",
            hash.finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        modified: metadata.modified().map_err(|e| e.to_string())?,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct RuntimeRestartCache(BTreeMap<PathBuf, FileEvidence>);

impl RuntimeRestartCache {
    pub(super) fn capture(project: &Path) -> Result<Self, String> {
        let runtime = project.join(".insight/runtime");
        let profile: serde_json::Value = serde_json::from_slice(
            &fs::read(runtime.join("profile.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let identity = profile["release_identity"]
            .as_str()
            .ok_or("missing release identity")?;
        let mut files = BTreeMap::new();
        let source_cache = runtime.join("build.json");
        if identity.starts_with("source:") {
            files.insert(source_cache.clone(), file(&source_cache)?);
        } else {
            if fs::symlink_metadata(&source_cache).is_ok() {
                return Err("prebuilt restart must never create a source build cache".into());
            }
            let (version, digest) = identity
                .strip_prefix("release:")
                .and_then(|v| v.split_once(':'))
                .ok_or("invalid prebuilt release identity")?;
            let parts: Vec<_> = version.split('.').collect();
            if parts.len() != 3
                || parts.iter().any(|part| {
                    part.parse::<u64>()
                        .map(|v| v.to_string() != *part)
                        .unwrap_or(true)
                })
                || digest
                    .parse::<insight_platform_contracts::Sha256Digest>()
                    .is_err()
                || profile["source_fingerprint"].as_str() != Some(digest)
            {
                return Err("invalid exact prebuilt cache identity".into());
            }
            let release_cache = project.join(".insight/cache/releases").join(version);
            let bundle_path = release_cache.join("release-bundle.json");
            let bundle = file(&bundle_path)?;
            if bundle.digest != digest {
                return Err("cached bundle differs from runtime identity".into());
            }
            files.insert(bundle_path, bundle);
            let signature = release_cache.join("release-bundle.signature.json");
            files.insert(signature.clone(), file(&signature)?);
            let binaries = runtime.join("releases").join(&digest[7..]).join("bin");
            let metadata = fs::symlink_metadata(&binaries).map_err(|e| e.to_string())?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err("prebuilt binary cache must be a real directory".into());
            }
            let entries: Vec<_> = fs::read_dir(&binaries)
                .map_err(|e| e.to_string())?
                .take(65)
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
            if entries.is_empty() || entries.len() > 64 {
                return Err("invalid prebuilt binary closure size".into());
            }
            for entry in entries {
                let path = entry.path();
                files.insert(path.clone(), file(&path)?);
            }
        }
        Ok(Self(files))
    }

    pub(super) fn verify_unchanged(&self, project: &Path) -> Result<(), String> {
        if *self != Self::capture(project)? {
            return Err("restart changed the selected release or build cache".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_cache_is_required_and_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path().join(".insight/runtime");
        fs::create_dir_all(&runtime).unwrap();
        fs::write(
            runtime.join("profile.json"),
            br#"{"release_identity":"source:fixture"}"#,
        )
        .unwrap();
        assert!(RuntimeRestartCache::capture(root.path()).is_err());
        fs::write(runtime.join("build.json"), b"source build identity").unwrap();
        let before = RuntimeRestartCache::capture(root.path()).unwrap();
        before.verify_unchanged(root.path()).unwrap();
        fs::write(runtime.join("build.json"), b"another build identity").unwrap();
        assert!(before.verify_unchanged(root.path()).is_err());
    }

    #[test]
    fn prebuilt_cache_tracks_real_files_and_never_requires_a_source_record() {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path().join(".insight/runtime");
        let cache = root.path().join(".insight/cache/releases/0.2.0");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir_all(&cache).unwrap();
        let bytes = br#"{"version":"0.2.0"}"#;
        let digest = super::super::digest_bytes(bytes);
        fs::write(
            runtime.join("profile.json"),
            serde_json::to_vec(&serde_json::json!({
                "release_identity": format!("release:0.2.0:{digest}"), "source_fingerprint": digest,
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(cache.join("release-bundle.json"), bytes).unwrap();
        // This helper observes bytes; the actual CLI independently verifies the signed bundle.
        fs::write(
            cache.join("release-bundle.signature.json"),
            b"fixture signature bytes",
        )
        .unwrap();
        let binaries = runtime.join("releases").join(&digest[7..]).join("bin");
        fs::create_dir_all(&binaries).unwrap();
        fs::write(binaries.join("platform-gateway"), b"installed executable").unwrap();
        let before = RuntimeRestartCache::capture(root.path()).unwrap();
        before.verify_unchanged(root.path()).unwrap();
        fs::write(runtime.join("build.json"), b"unexpected source compilation").unwrap();
        assert!(before.verify_unchanged(root.path()).is_err());
        fs::remove_file(runtime.join("build.json")).unwrap();
        fs::write(binaries.join("platform-gateway"), b"replaced executable").unwrap();
        assert!(before.verify_unchanged(root.path()).is_err());
        fs::write(binaries.join("platform-gateway"), b"installed executable").unwrap();
        fs::write(cache.join("release-bundle.json"), b"different bundle").unwrap();
        assert!(RuntimeRestartCache::capture(root.path()).is_err());
    }
}
