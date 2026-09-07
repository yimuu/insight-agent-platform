use std::path::Path;

/// Owns a unique fixture directory. Declare before child guards so workers stop first.
pub struct FixtureDirectory {
    directory: Option<tempfile::TempDir>,
    keep_failed: bool,
}

impl FixtureDirectory {
    pub fn new(purpose: &str) -> Self {
        Self {
            directory: Some(
                tempfile::Builder::new()
                    .prefix(&format!("insight-{purpose}-"))
                    .tempdir()
                    .expect("create private fixture directory"),
            ),
            keep_failed: std::env::var("INSIGHT_TEST_KEEP_FAILED_RESOURCES")
                .is_ok_and(|value| value == "1"),
        }
    }

    pub fn path(&self) -> &Path {
        self.directory.as_ref().unwrap().path()
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let directory = self.directory.take().unwrap();
        if self.keep_failed && std::thread::panicking() {
            eprintln!(
                "failed fixture explicitly retained at {}",
                directory.keep().display()
            );
        } else if let Err(error) = directory.close() {
            if std::thread::panicking() {
                eprintln!("fixture cleanup failed during panic: {error}");
            } else {
                panic!("fixture cleanup failed: {error}");
            }
        }
    }
}
