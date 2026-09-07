#[path = "support/fixture_directory.rs"]
mod fixture_directory;
use fixture_directory::FixtureDirectory;

#[test]
fn fixture_directories_are_unique_and_removed_after_success() {
    let first = FixtureDirectory::new("lifecycle");
    let second = FixtureDirectory::new("lifecycle");
    assert_ne!(first.path(), second.path());
    let first_path = first.path().to_owned();
    std::fs::write(first.path().join("credential"), b"fixture-only").unwrap();
    drop(first);
    assert!(!first_path.exists());
    assert!(second.path().exists());
}

#[test]
fn fixture_directory_cleans_up_during_unwind() {
    // A subprocess gives the explicit retention switch isolated environment ownership.
    const MARKER: &str = "INSIGHT_FIXTURE_UNWIND_CHILD";
    if let Ok(root) = std::env::var(MARKER) {
        let outcome = std::panic::catch_unwind(|| {
            let fixture = FixtureDirectory::new("unwind");
            std::fs::write(&root, fixture.path().to_str().unwrap()).unwrap();
            panic!("fixture assertion failure");
        });
        assert!(outcome.is_err());
        return;
    }
    let owner = tempfile::tempdir().unwrap();
    let marker = owner.path().join("path");
    for keep in [false, true] {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fixture_directory_cleans_up_during_unwind"])
            .env(MARKER, &marker)
            .env(
                "INSIGHT_TEST_KEEP_FAILED_RESOURCES",
                if keep { "1" } else { "0" },
            )
            .output()
            .unwrap();
        assert!(result.status.success(), "{:?}", result);
        let path = std::path::PathBuf::from(std::fs::read_to_string(&marker).unwrap());
        assert_eq!(path.exists(), keep);
        if keep {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
