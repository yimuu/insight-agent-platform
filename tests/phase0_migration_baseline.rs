use std::{collections::BTreeMap, fmt::Write as _, fs, path::Path};

use insight_agent_platform::engine::repository::migration_manifest::{
    SqliteMigrationGuard, DURABLE_V3_MIGRATIONS,
};
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct Baseline<'a> {
    version: u64,
    name: &'a str,
    postgres_sha256: &'a str,
    postgres_checksum: &'a str,
    sqlite_sha256: &'a str,
    sqlite_guard: &'a str,
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn baselines() -> Vec<Baseline<'static>> {
    include_str!("baselines/durable-v3-migrations.tsv")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let columns = line.split('\t').collect::<Vec<_>>();
            assert_eq!(columns.len(), 6, "invalid migration baseline row: {line}");
            Baseline {
                version: columns[0].parse().unwrap(),
                name: columns[1],
                postgres_sha256: columns[2],
                postgres_checksum: columns[3],
                sqlite_sha256: columns[4],
                sqlite_guard: columns[5],
            }
        })
        .collect()
}

fn directory_files(path: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            (
                entry.file_name().into_string().unwrap(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

#[test]
fn phase0_freezes_all_migration_bytes_checksums_guards_and_execution_manifest() {
    let baseline = baselines();
    assert_eq!(baseline.len(), 23);
    assert_eq!(DURABLE_V3_MIGRATIONS.len(), baseline.len());

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/durable_v3");
    let postgres_files = directory_files(&root.join("postgres"));
    let sqlite_files = directory_files(&root.join("sqlite"));
    assert_eq!(postgres_files.len(), baseline.len());
    assert_eq!(sqlite_files.len(), baseline.len());

    for (migration, expected) in DURABLE_V3_MIGRATIONS.iter().zip(&baseline) {
        assert_eq!(migration.version, expected.version);
        assert_eq!(migration.name, expected.name);

        let postgres_disk = &postgres_files[expected.name];
        let sqlite_disk = &sqlite_files[expected.name];
        assert_eq!(migration.postgres_sql.as_bytes(), postgres_disk);
        assert_eq!(migration.sqlite_sql.as_bytes(), sqlite_disk);
        assert_eq!(sha256(postgres_disk), expected.postgres_sha256);
        assert_eq!(sha256(sqlite_disk), expected.sqlite_sha256);
        assert_eq!(migration.postgres_checksum(), expected.postgres_checksum);

        let guard = match migration.sqlite_guard {
            SqliteMigrationGuard::Always => "always".to_owned(),
            SqliteMigrationGuard::WhenQueryMissing(query) => {
                format!("when_query_missing:{query}")
            }
        };
        assert_eq!(guard, expected.sqlite_guard);
    }

    let names = baseline
        .iter()
        .map(|entry| entry.name.to_owned())
        .collect::<Vec<_>>();
    assert_eq!(postgres_files.keys().cloned().collect::<Vec<_>>(), names);
    assert_eq!(sqlite_files.keys().cloned().collect::<Vec<_>>(), names);
}
