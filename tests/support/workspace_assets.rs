//! Shared workspace-asset locator for member integration tests.

#![allow(dead_code)]

use std::path::{Component, Path, PathBuf};

/// Embeds a workspace-root asset from an integration test owned by a package
/// under `crates/*`. `include_str!` is the compile-time existence gate.
#[allow(unused_macros)]
macro_rules! workspace_asset_str {
    ($workspace_relative:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../",
            $workspace_relative
        ))
    };
}

pub(crate) fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|candidate| {
            candidate.join("Cargo.toml").is_file()
                && candidate.join("crates/engine/Cargo.toml").is_file()
                && candidate.join("tests/fixtures/dsl").is_dir()
        })
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not locate workspace root from {}",
                manifest_dir.display()
            )
        })
}

pub(crate) fn workspace_path(relative: impl AsRef<Path>) -> PathBuf {
    let relative = relative.as_ref();
    assert!(
        relative.is_relative(),
        "workspace asset path must be relative"
    );
    assert!(
        relative
            .components()
            .all(|component| matches!(component, Component::Normal(_))),
        "workspace asset path must not contain traversal components"
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
