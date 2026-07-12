use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use insight_agent_platform::config::{
    AuthConfig, HistoryConfig, PlatformConfig, PlatformConfigError,
};
use tempfile::tempdir;

fn base_yaml(auth: &str) -> String {
    format!(
        r#"
version: 1
bind_addr: 127.0.0.1:3000
auth:
{auth}
agents:
  directory: ../agents
models:
  config: models/resources.yaml
actions:
  enabled: [current_time, example.text_metrics]
history:
  provider: sqlite
  path: ../data/history.sqlite3
runtime:
  max_concurrent_runs: 8
  max_fork_branches: 32
  max_parallel_node_executions: 32
  max_parallel_branches_per_run: 8
  default_node_timeout: 30s
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 64
  journal_capacity: 512
  journal_batch_size: 32
  journal_operation_timeout: 5s
"#,
    )
}

fn write_config(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempdir().unwrap();
    let config_dir = directory.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    let path = config_dir.join("platform.yaml");
    fs::write(&path, yaml).unwrap();
    (directory, path)
}

fn load(
    path: &Path,
    environment: BTreeMap<String, String>,
) -> Result<PlatformConfig, PlatformConfigError> {
    PlatformConfig::load_with_env(path, |name| environment.get(name).cloned())
}

#[test]
fn explicit_missing_platform_file_is_an_error() {
    let directory = tempdir().unwrap();
    let error = load(&directory.path().join("missing.yaml"), BTreeMap::new()).unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_NOT_FOUND");
}

#[test]
fn strict_parser_rejects_unknown_top_level_and_nested_fields() {
    let (_directory, path) = write_config(&format!(
        "{}\nunknown: true\n",
        base_yaml("  mode: disabled")
    ));
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let yaml = base_yaml("  mode: disabled").replace(
        "  directory: ../agents",
        "  directory: ../agents\n  default_public: true",
    );
    let (_directory, path) = write_config(&yaml);
    let error = load(&path, BTreeMap::new()).unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(!error.to_string().is_empty());
}

#[test]
fn relative_agent_model_and_history_paths_resolve_from_platform_parent() {
    let (directory, path) = write_config(&base_yaml("  mode: disabled"));
    let config = load(&path, BTreeMap::new()).unwrap();

    assert_eq!(config.runtime.max_fork_branches, 32);
    assert_eq!(config.runtime.max_parallel_node_executions, 32);
    assert_eq!(config.runtime.max_parallel_branches_per_run, 8);
    assert_eq!(
        config.runtime.sse_keep_alive_interval,
        Duration::from_secs(5)
    );
    assert_eq!(config.agents.directory, directory.path().join("agents"));
    assert_eq!(
        config.models.config,
        directory.path().join("config/models/resources.yaml")
    );
    assert_eq!(
        config.history,
        HistoryConfig::Sqlite {
            path: directory.path().join("data/history.sqlite3")
        }
    );
}

#[test]
fn disabled_auth_is_explicit_and_agent_enablement_defaults_to_none() {
    let yaml = base_yaml("  mode: disabled").replace(
        "actions:\n  enabled: [current_time, example.text_metrics]",
        "actions:\n  enabled: []",
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();

    assert_eq!(config.auth, AuthConfig::Disabled);
    assert!(config.agents.enabled.is_empty());
    assert!(config.actions.enabled.is_empty());
}

#[test]
fn bearer_auth_requires_a_named_nonempty_environment_secret() {
    let (_directory, path) =
        write_config(&base_yaml("  mode: bearer_env\n  token_env: RUNTIME_TOKEN"));
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_SECRET_MISSING"
    );
    assert_eq!(
        load(
            &path,
            BTreeMap::from([("RUNTIME_TOKEN".to_string(), "   ".to_string())])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_SECRET_EMPTY"
    );

    let config = load(
        &path,
        BTreeMap::from([("RUNTIME_TOKEN".to_string(), "super-secret".to_string())]),
    )
    .unwrap();
    assert_eq!(config.auth.bearer_token(), Some("super-secret"));
    let debug = format!("{config:?}");
    assert!(!debug.contains("super-secret"));
    assert!(debug.contains("REDACTED"));
}

#[test]
fn postgres_history_secret_is_resolved_and_redacted() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@database/private?sslmode=verify-full";
    let config = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap();

    assert_eq!(config.history.database_url(), Some(secret));
    assert!(!format!("{config:?}").contains(secret));
}

#[test]
fn postgres_history_requires_verify_full_for_remote_tcp() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for sslmode in [
        None,
        Some("prefer"),
        Some("allow"),
        Some("disable"),
        Some("require"),
        Some("verify-ca"),
    ] {
        let suffix = sslmode
            .map(|mode| format!("?sslmode={mode}"))
            .unwrap_or_default();
        let secret = format!("postgres://user:password@database/private{suffix}");
        let error = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.clone())]),
        )
        .unwrap_err();

        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
        assert!(error.to_string().contains("sslmode=verify-full"));
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains(&secret));
    }
}

#[test]
fn postgres_history_allows_exact_local_development_plaintext() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for secret in [
        "postgres://user:password@localhost/private",
        "postgres://user:password@127.0.0.1/private",
        "postgres://user:password@[::1]/private",
    ] {
        let config = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
        )
        .unwrap();

        assert_eq!(config.history.database_url(), Some(secret));
    }
}

#[test]
fn postgres_history_rejects_loopback_aliases_without_verify_full() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for secret in [
        "postgres://user:password@127.1/private",
        "postgres://user:password@0:0:0:0:0:0:0:1/private",
    ] {
        let error = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
        )
        .unwrap_err();

        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    }
}

#[test]
fn zero_capacities_and_durations_are_rejected() {
    for (from, to) in [
        ("max_concurrent_runs: 8", "max_concurrent_runs: 0"),
        ("max_fork_branches: 32", "max_fork_branches: 0"),
        (
            "max_parallel_node_executions: 32",
            "max_parallel_node_executions: 0",
        ),
        (
            "max_parallel_branches_per_run: 8",
            "max_parallel_branches_per_run: 0",
        ),
        ("default_node_timeout: 30s", "default_node_timeout: 0s"),
        ("run_timeout: 5m", "run_timeout: 0s"),
        ("sse_keep_alive_interval: 5s", "sse_keep_alive_interval: 0s"),
        ("subscriber_capacity: 64", "subscriber_capacity: 0"),
        ("journal_batch_size: 32", "journal_batch_size: 0"),
        (
            "journal_operation_timeout: 5s",
            "journal_operation_timeout: 0s",
        ),
    ] {
        let yaml = base_yaml("  mode: disabled").replace(from, to);
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }
}

#[test]
fn removed_sse_recovery_settings_are_unknown() {
    for removed in [
        "  attached_reconnect_grace: 10s\n",
        "  replay_ring_capacity: 256\n",
    ] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  sse_keep_alive_interval: 5s\n",
            &format!("  sse_keep_alive_interval: 5s\n{removed}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONFIG_INVALID"
        );
    }
}

#[test]
fn invalid_sse_keep_alive_duration_is_rejected() {
    let yaml = base_yaml("  mode: disabled").replace(
        "sse_keep_alive_interval: 5s",
        "sse_keep_alive_interval: soon",
    );
    let (_directory, path) = write_config(&yaml);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_RUNTIME_INVALID"
    );
}

#[test]
fn journal_batch_must_fit_the_queue() {
    let yaml =
        base_yaml("  mode: disabled").replace("journal_capacity: 512", "journal_capacity: 16");
    let (_directory, path) = write_config(&yaml);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_RUNTIME_INVALID"
    );
}

#[test]
fn public_and_default_public_are_not_part_of_formal_schema() {
    for field in ["public: true", "default_public: true"] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  directory: ../agents",
            &format!("  directory: ../agents\n  {field}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONFIG_INVALID"
        );
    }
}
