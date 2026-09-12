//! CLI test assets. Workspace path validation is shared with deployment tooling.
pub(crate) use insight_platform_deployment_tooling::workspace_assets::workspace_path;
use std::path::Path;

#[cfg(test)]
pub fn worker_binary_fixtures(root: &Path) -> std::path::PathBuf {
    let directory = root.join("worker-binary-fixtures");
    std::fs::create_dir_all(&directory).unwrap();
    for name in insight_platform_deployment_tooling::base_runtime_binary_paths(&directory)
        .keys()
        .copied()
        .chain(insight_platform_deployment_tooling::full_profile::INITIAL_BINARY_NAMES)
        .chain(["platform-sandbox-dispatcher"])
    {
        std::fs::write(
            directory.join(name),
            format!("dedicated test executable bytes: {name}\n"),
        )
        .unwrap();
    }
    directory
}
