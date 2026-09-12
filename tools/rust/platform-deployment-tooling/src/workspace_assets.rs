//! Shared, traversal-safe locator for checked-in workspace assets.

use std::path::{Component, Path, PathBuf};

pub fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|candidate| {
            candidate.join("Cargo.toml").is_file()
                && candidate.join("apps/insight-cli/Cargo.toml").is_file()
                && candidate
                    .join("contracts/platform-v1/manifest.json")
                    .is_file()
        })
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not locate workspace root from {}",
                manifest_dir.display()
            )
        })
}

pub fn workspace_path(relative: impl AsRef<Path>) -> PathBuf {
    let relative = relative.as_ref();
    assert!(
        relative.is_relative()
            && relative
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "workspace asset path must be relative and contain no traversal components"
    );
    let root = workspace_root()
        .canonicalize()
        .expect("workspace root must be canonicalizable");
    let candidate = root.join(relative);
    assert!(
        candidate.exists(),
        "workspace asset does not exist: {}",
        candidate.display()
    );
    let canonical = candidate
        .canonicalize()
        .expect("workspace asset must be canonicalizable");
    assert!(
        canonical.starts_with(&root),
        "workspace asset must remain inside the workspace root"
    );
    canonical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_existing_asset_inside_workspace() {
        assert!(
            workspace_path("compose.yaml").starts_with(workspace_root().canonicalize().unwrap())
        );
    }

    #[test]
    fn rejects_absolute_and_traversal_paths() {
        for path in [
            "/tmp/compose.yaml",
            "../compose.yaml",
            "deploy/../../compose.yaml",
        ] {
            assert!(std::panic::catch_unwind(|| workspace_path(path)).is_err());
        }
    }
}
