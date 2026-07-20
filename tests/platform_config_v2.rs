use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use insight_agent_platform::{
    config::{
        ArtifactStoreProvider, AuthConfig, DeploymentMode, HistoryConfig,
        LiveResponseBrokerProvider, PlatformConfig, PlatformConfigError,
    },
    resources::config::load_model_registry_with_env,
};
use tempfile::tempdir;

fn base_yaml(auth: &str) -> String {
    format!(
        r#"
version: 1
deployment_mode: single_process_development
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
  max_concurrent_operations: 32
  max_concurrent_operations_per_run: 256
  operation_timeout: 30s
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 64
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
fn deployment_mode_is_required_and_production_rejects_sqlite() {
    let missing =
        base_yaml("  mode: disabled").replace("deployment_mode: single_process_development\n", "");
    let (_directory, path) = write_config(&missing);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let production = base_yaml("  mode: disabled").replace(
        "deployment_mode: single_process_development",
        "deployment_mode: production",
    );
    let (_directory, path) = write_config(&production);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_PRODUCTION_REQUIRES_POSTGRES"
    );
}

#[test]
fn relative_agent_model_and_history_paths_resolve_from_platform_parent() {
    let (directory, path) = write_config(&base_yaml("  mode: disabled"));
    let config = load(&path, BTreeMap::new()).unwrap();

    assert_eq!(
        config.deployment_mode,
        DeploymentMode::SingleProcessDevelopment
    );
    assert_eq!(config.runtime.max_concurrent_operations, 32);
    assert_eq!(config.runtime.max_concurrent_operations_per_run, 256);
    assert_eq!(config.runtime.operation_timeout, Duration::from_secs(30));
    assert_eq!(
        config.runtime.sse_keep_alive_interval,
        Duration::from_secs(5)
    );
    assert_eq!(
        config.runtime.readiness_probe_timeout,
        Duration::from_secs(2)
    );
    assert_eq!(
        config.runtime.shutdown_grace_period,
        Duration::from_secs(30)
    );
    assert_eq!(
        config.runtime.shutdown_hard_deadline,
        Duration::from_secs(35)
    );
    assert_eq!(
        config.runtime.public_event_retention,
        Duration::from_secs(24 * 60 * 60)
    );
    assert_eq!(
        config.runtime.public_event_prune_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        config.runtime.response_stream.broker,
        LiveResponseBrokerProvider::InProcess
    );
    assert_eq!(config.runtime.response_stream.body_queue_capacity, 256);
    assert_eq!(config.runtime.response_stream.control_queue_capacity, 32);
    assert_eq!(config.runtime.response_stream.max_frame_bytes, 4 * 1024);
    assert_eq!(
        config.runtime.response_stream.max_item_bytes,
        4 * 1024 * 1024
    );
    assert_eq!(
        config.runtime.response_stream.max_run_bytes,
        16 * 1024 * 1024
    );
    assert_eq!(
        config.runtime.response_stream.terminal_barrier_timeout,
        Duration::from_secs(2)
    );
    assert_eq!(
        config.runtime.response_stream.outbound_write_timeout,
        Duration::from_secs(10)
    );
    assert_eq!(config.runtime.max_llm_tool_rounds, 16);
    assert_eq!(config.runtime.max_llm_tool_calls, 64);
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
    assert_eq!(
        config.artifacts.directory,
        directory.path().join("data/artifacts")
    );
    assert_eq!(config.artifacts.inline_threshold_bytes, 64 * 1024);
    assert_eq!(config.artifacts.max_read_bytes, 64 * 1024 * 1024);
    assert_eq!(
        config.artifacts.provider,
        ArtifactStoreProvider::LocalFilesystem
    );
    assert_eq!(config.artifacts.namespace, None);
    assert_eq!(
        config.artifacts.orphan_retention,
        Duration::from_secs(86_400)
    );
    assert_eq!(
        config.artifacts.reference_retention,
        Duration::from_secs(30 * 86_400)
    );
    assert_eq!(config.artifacts.gc_interval, Duration::from_secs(60));
    assert_eq!(config.artifacts.deletion_claim_seconds, 60);
}

#[test]
fn artifact_store_policy_is_strict_resolved_and_bounded() {
    let explicit = base_yaml("  mode: disabled").replace(
        "runtime:",
        "artifacts:\n  provider: local_filesystem\n  directory: objects\n  inline_threshold_bytes: 1024\n  max_read_bytes: 4096\n  orphan_retention: 2h\n  reference_retention: 7d\n  gc_interval: 15s\n  deletion_claim_seconds: 30\nruntime:",
    );
    let (directory, path) = write_config(&explicit);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.artifacts.directory,
        directory.path().join("config/objects")
    );
    assert_eq!(config.artifacts.inline_threshold_bytes, 1024);
    assert_eq!(config.artifacts.max_read_bytes, 4096);
    assert_eq!(
        config.artifacts.orphan_retention,
        Duration::from_secs(7_200)
    );
    assert_eq!(
        config.artifacts.reference_retention,
        Duration::from_secs(7 * 86_400)
    );
    assert_eq!(config.artifacts.gc_interval, Duration::from_secs(15));
    assert_eq!(config.artifacts.deletion_claim_seconds, 30);

    for (valid, invalid) in [
        (
            "  inline_threshold_bytes: 1024",
            "  inline_threshold_bytes: 0",
        ),
        ("  max_read_bytes: 4096", "  max_read_bytes: 0"),
        ("  max_read_bytes: 4096", "  max_read_bytes: 268435457"),
        ("  orphan_retention: 2h", "  orphan_retention: 0s"),
        ("  reference_retention: 7d", "  reference_retention: 0s"),
        ("  reference_retention: 7d", "  reference_retention: 500ms"),
        ("  reference_retention: 7d", "  reference_retention: 3651d"),
        (
            "  deletion_claim_seconds: 30",
            "  deletion_claim_seconds: 0",
        ),
        (
            "  deletion_claim_seconds: 30",
            "  deletion_claim_seconds: 3601",
        ),
    ] {
        let yaml = explicit.replace(valid, invalid);
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_ARTIFACTS_INVALID"
        );
    }
}

#[test]
fn artifact_provider_contract_is_explicit_and_production_requires_shared_namespace() {
    let explicit_local = base_yaml("  mode: disabled").replace(
        "runtime:",
        "artifacts:\n  provider: local_filesystem\n  directory: objects\n  inline_threshold_bytes: 1024\n  orphan_retention: 2h\n  gc_interval: 15s\n  deletion_claim_seconds: 30\nruntime:",
    );
    let (_directory, path) = write_config(&explicit_local);
    let local = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        local.artifacts.provider,
        ArtifactStoreProvider::LocalFilesystem
    );
    assert_eq!(local.artifacts.namespace, None);

    let missing_provider = explicit_local.replace("  provider: local_filesystem\n", "");
    let (_directory, path) = write_config(&missing_provider);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let local_with_namespace = explicit_local.replace(
        "  provider: local_filesystem",
        "  provider: local_filesystem\n  namespace: forbidden",
    );
    let (_directory, path) = write_config(&local_with_namespace);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_ARTIFACTS_INVALID"
    );

    let shared_without_namespace = explicit_local.replace(
        "  provider: local_filesystem",
        "  provider: shared_filesystem",
    );
    let (_directory, path) = write_config(&shared_without_namespace);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_ARTIFACTS_INVALID"
    );

    let shared_with_invalid_namespace = shared_without_namespace.replace(
        "  provider: shared_filesystem",
        "  provider: shared_filesystem\n  namespace: ../forged",
    );
    let (_directory, path) = write_config(&shared_with_invalid_namespace);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_ARTIFACTS_INVALID"
    );

    let production_local = explicit_local
        .replace(
            "deployment_mode: single_process_development",
            "deployment_mode: production",
        )
        .replace(
            "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
            "history:\n  provider: postgres\n  database_url_env: DATABASE_URL",
        );
    let (_directory, path) = write_config(&production_local);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([(
                "DATABASE_URL".to_owned(),
                "postgres://localhost/platform".to_owned(),
            )]),
        )
        .unwrap_err()
        .code(),
        "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE"
    );

    let production_shared = production_local.replace(
        "  provider: local_filesystem",
        "  provider: shared_filesystem\n  namespace: production",
    );
    let (_directory, path) = write_config(&production_shared);
    let shared = load(
        &path,
        BTreeMap::from([(
            "DATABASE_URL".to_owned(),
            "postgres://localhost/platform".to_owned(),
        )]),
    )
    .unwrap();
    assert_eq!(
        shared.artifacts.provider,
        ArtifactStoreProvider::SharedFilesystem
    );
    assert_eq!(shared.artifacts.namespace.as_deref(), Some("production"));
}

#[test]
fn lifecycle_durations_are_configurable_and_hard_deadline_exceeds_grace_period() {
    let yaml = base_yaml("  mode: disabled").replace(
        "  subscriber_capacity: 64",
        "  subscriber_capacity: 64\n  readiness_probe_timeout: 250ms\n  shutdown_grace_period: 2s\n  shutdown_hard_deadline: 3s",
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.readiness_probe_timeout,
        Duration::from_millis(250)
    );
    assert_eq!(config.runtime.shutdown_grace_period, Duration::from_secs(2));
    assert_eq!(
        config.runtime.shutdown_hard_deadline,
        Duration::from_secs(3)
    );

    for invalid in [
        "  readiness_probe_timeout: 0s\n  shutdown_grace_period: 2s\n  shutdown_hard_deadline: 3s",
        "  readiness_probe_timeout: 1s\n  shutdown_grace_period: 3s\n  shutdown_hard_deadline: 3s",
        "  readiness_probe_timeout: 1s\n  shutdown_grace_period: 4s\n  shutdown_hard_deadline: 3s",
    ] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  subscriber_capacity: 64",
            &format!("  subscriber_capacity: 64\n{invalid}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }
}

#[test]
fn response_stream_transport_and_bounds_are_closed_and_validated() {
    let response_stream = "  subscriber_capacity: 64\n  max_llm_tool_rounds: 8\n  max_llm_tool_calls: 32\n  response_stream:\n    broker: in_process\n    body_queue_capacity: 17\n    control_queue_capacity: 5\n    max_frame_bytes: 8192\n    max_item_bytes: 65536\n    max_run_bytes: 262144\n    terminal_barrier_timeout: 750ms\n    outbound_write_timeout: 3s";
    let yaml = base_yaml("  mode: disabled").replace("  subscriber_capacity: 64", response_stream);
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.response_stream.broker,
        LiveResponseBrokerProvider::InProcess
    );
    assert_eq!(config.runtime.response_stream.body_queue_capacity, 17);
    assert_eq!(config.runtime.response_stream.control_queue_capacity, 5);
    assert_eq!(config.runtime.response_stream.max_frame_bytes, 8192);
    assert_eq!(
        config.runtime.response_stream.terminal_barrier_timeout,
        Duration::from_millis(750)
    );
    assert_eq!(config.runtime.max_llm_tool_rounds, 8);
    assert_eq!(config.runtime.max_llm_tool_calls, 32);

    for (valid, invalid) in [
        ("    body_queue_capacity: 17", "    body_queue_capacity: 0"),
        (
            "    control_queue_capacity: 5",
            "    control_queue_capacity: 0",
        ),
        ("    max_frame_bytes: 8192", "    max_frame_bytes: 128"),
        ("    max_item_bytes: 65536", "    max_item_bytes: 4096"),
        ("    max_run_bytes: 262144", "    max_run_bytes: 32768"),
        (
            "    terminal_barrier_timeout: 750ms",
            "    terminal_barrier_timeout: 0s",
        ),
        (
            "    outbound_write_timeout: 3s",
            "    outbound_write_timeout: 0s",
        ),
        ("  max_llm_tool_rounds: 8", "  max_llm_tool_rounds: 0"),
        ("  max_llm_tool_calls: 32", "  max_llm_tool_calls: 4"),
    ] {
        let invalid_yaml = yaml.replace(valid, invalid);
        let (_directory, path) = write_config(&invalid_yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }

    let postgres_oversized = yaml.replace("    broker: in_process", "    broker: postgres_notify");
    let (_directory, path) = write_config(&postgres_oversized);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_RUNTIME_INVALID"
    );
}

#[test]
fn public_event_retention_is_explicit_bounded_and_independent() {
    let yaml = base_yaml("  mode: disabled").replace(
        "  subscriber_capacity: 64",
        "  subscriber_capacity: 64\n  public_event_retention: 7d\n  public_event_prune_interval: 15s",
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.public_event_retention,
        Duration::from_secs(7 * 24 * 60 * 60)
    );
    assert_eq!(
        config.runtime.public_event_prune_interval,
        Duration::from_secs(15)
    );

    for invalid in [
        "  public_event_retention: 0s\n  public_event_prune_interval: 15s",
        "  public_event_retention: 500ms\n  public_event_prune_interval: 15s",
        "  public_event_retention: 3651d\n  public_event_prune_interval: 15s",
        "  public_event_retention: 7d\n  public_event_prune_interval: 0s",
    ] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  subscriber_capacity: 64",
            &format!("  subscriber_capacity: 64\n{invalid}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }
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
fn quickstart_configs_load_without_secrets() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = PlatformConfig::load(&root.join("config/platform.quickstart.yaml")).unwrap();

    assert_eq!(
        config.deployment_mode,
        DeploymentMode::SingleProcessDevelopment
    );
    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:3000");
    assert_eq!(config.auth, AuthConfig::Disabled);
    assert_eq!(config.agents.directory, root.join("agents"));
    assert_eq!(
        config.agents.enabled.iter().cloned().collect::<Vec<_>>(),
        vec!["action_demo".to_string()]
    );
    assert_eq!(
        config.models.config,
        root.join("config/models.quickstart.yaml")
    );
    assert_eq!(
        config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        vec!["example.text_metrics".to_string()]
    );

    let registry = load_model_registry_with_env(&config.models.config, |_| None).unwrap();
    registry.resolve("unused_quickstart_model").unwrap();
}

#[test]
fn stock_production_config_requires_and_resolves_postgres() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let database_url = "postgres://insight:secret@localhost:5432/insight_agent_platform";
    let config = PlatformConfig::load_with_env(&root.join("config/platform.yaml"), |name| {
        (name == "RUN_HISTORY_POSTGRES_URL").then(|| database_url.to_owned())
    })
    .unwrap();

    assert_eq!(config.deployment_mode, DeploymentMode::Production);
    assert_eq!(config.history.database_url(), Some(database_url));
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
fn human_task_credentials_map_two_environment_secrets_to_distinct_principals() {
    let yaml = base_yaml(
        "  mode: disabled\n  human_task_credentials:\n    - identity: alice\n      groups: [medical, triage]\n      token_env: HUMAN_ALICE_TOKEN\n    - identity: bob\n      groups: [legal]\n      token_env: HUMAN_BOB_TOKEN",
    );
    let (_directory, path) = write_config(&yaml);
    let alice_token = "human-alice-secret";
    let bob_token = "human-bob-secret";
    let config = load(
        &path,
        BTreeMap::from([
            ("HUMAN_ALICE_TOKEN".to_owned(), alice_token.to_owned()),
            ("HUMAN_BOB_TOKEN".to_owned(), bob_token.to_owned()),
        ]),
    )
    .unwrap();

    assert_eq!(config.human_task_credentials.len(), 2);
    assert_eq!(config.human_task_credentials[0].identity(), "alice");
    assert_eq!(
        config.human_task_credentials[0]
            .groups()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["medical", "triage"]
    );
    assert_eq!(
        config.human_task_credentials[0].token().expose(),
        alice_token
    );
    assert_eq!(config.human_task_credentials[1].identity(), "bob");
    assert_eq!(config.human_task_credentials[1].token().expose(), bob_token);
    let debug = format!("{config:?}");
    assert!(!debug.contains(alice_token));
    assert!(!debug.contains(bob_token));
    assert!(debug.matches("REDACTED").count() >= 2);
}

#[test]
fn human_task_credentials_fail_closed_on_missing_empty_or_duplicate_secrets() {
    let yaml = base_yaml(
        "  mode: disabled\n  human_task_credentials:\n    - identity: alice\n      groups: [medical]\n      token_env: HUMAN_ALICE_TOKEN\n    - identity: bob\n      groups: [legal]\n      token_env: HUMAN_BOB_TOKEN",
    );
    let (_directory, path) = write_config(&yaml);

    assert_eq!(
        load(
            &path,
            BTreeMap::from([("HUMAN_ALICE_TOKEN".to_owned(), "alice-secret".to_owned())])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_SECRET_MISSING"
    );
    assert_eq!(
        load(
            &path,
            BTreeMap::from([
                ("HUMAN_ALICE_TOKEN".to_owned(), "alice-secret".to_owned()),
                ("HUMAN_BOB_TOKEN".to_owned(), "   ".to_owned()),
            ])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_SECRET_EMPTY"
    );

    let duplicate_secret = "shared-human-secret";
    let error = load(
        &path,
        BTreeMap::from([
            ("HUMAN_ALICE_TOKEN".to_owned(), duplicate_secret.to_owned()),
            ("HUMAN_BOB_TOKEN".to_owned(), duplicate_secret.to_owned()),
        ]),
    )
    .unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(!error.to_string().contains(duplicate_secret));

    let admin_collision = base_yaml(
        "  mode: bearer_env\n  token_env: RUNTIME_TOKEN\n  human_task_credentials:\n    - identity: alice\n      groups: [medical]\n      token_env: HUMAN_ALICE_TOKEN",
    );
    let (_directory, path) = write_config(&admin_collision);
    let shared_secret = "shared-admin-human-secret";
    let error = load(
        &path,
        BTreeMap::from([
            ("RUNTIME_TOKEN".to_owned(), shared_secret.to_owned()),
            ("HUMAN_ALICE_TOKEN".to_owned(), shared_secret.to_owned()),
        ]),
    )
    .unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(!error.to_string().contains(shared_secret));
}

#[test]
fn human_task_credentials_reject_duplicate_identity_and_inline_plaintext_token() {
    let duplicate_identity = base_yaml(
        "  mode: disabled\n  human_task_credentials:\n    - identity: alice\n      token_env: HUMAN_ALICE_TOKEN\n    - identity: alice\n      token_env: HUMAN_ALICE_SECOND_TOKEN",
    );
    let (_directory, path) = write_config(&duplicate_identity);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([
                ("HUMAN_ALICE_TOKEN".to_owned(), "first-secret".to_owned()),
                (
                    "HUMAN_ALICE_SECOND_TOKEN".to_owned(),
                    "second-secret".to_owned(),
                ),
            ])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let inline_secret = "must-never-be-inline";
    let plaintext = base_yaml(&format!(
        "  mode: disabled\n  human_task_credentials:\n    - identity: alice\n      groups: [medical]\n      token: {inline_secret}"
    ));
    let (_directory, path) = write_config(&plaintext);
    let error = load(&path, BTreeMap::new()).unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(!error.to_string().contains(inline_secret));
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
fn postgres_history_rejects_later_remote_hostaddr_query_override_without_verify_full() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@database/private?host=localhost&hostaddr=8.8.8.8";

    let error = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap_err();

    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(error.to_string().contains("sslmode=verify-full"));
    assert!(!error.to_string().contains("password"));
    assert!(!error.to_string().contains(secret));
}

#[test]
fn postgres_history_rejects_percent_encoded_remote_hostaddr_query_without_verify_full() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@localhost/private?h%6Fstaddr=8.8.8.8";

    let error = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap_err();

    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(error.to_string().contains("sslmode=verify-full"));
    assert!(!error.to_string().contains("password"));
    assert!(!error.to_string().contains(secret));
}

#[test]
fn postgres_history_allows_percent_encoded_exact_local_hostaddr_query_without_tls() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@database/private?hostaddr=127%2E0%2E0%2E1";

    let config = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap();

    assert_eq!(config.history.database_url(), Some(secret));
}

#[test]
fn postgres_history_rejects_later_remote_host_query_override_without_verify_full() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@database/private?host=localhost&host=database";

    let error = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap_err();

    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    assert!(error.to_string().contains("sslmode=verify-full"));
    assert!(!error.to_string().contains("password"));
    assert!(!error.to_string().contains(secret));
}

#[test]
fn postgres_history_allows_later_exact_local_host_query_override_without_tls() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);
    let secret = "postgres://user:password@database/private?host=database&host=localhost";

    let config = load(
        &path,
        BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
    )
    .unwrap();

    assert_eq!(config.history.database_url(), Some(secret));
}

#[test]
fn postgres_history_rejects_non_postgres_schemes() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for secret in [
        "mysql://user:password@localhost/private",
        "mysql://user:password@database/private?sslmode=verify-full",
    ] {
        let error = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
        )
        .unwrap_err();

        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
        assert!(error.to_string().contains("postgres:// or postgresql://"));
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains(secret));
    }
}

#[test]
fn zero_capacities_and_durations_are_rejected() {
    for (from, to) in [
        ("max_concurrent_runs: 8", "max_concurrent_runs: 0"),
        (
            "max_concurrent_operations: 32",
            "max_concurrent_operations: 0",
        ),
        (
            "max_concurrent_operations_per_run: 256",
            "max_concurrent_operations_per_run: 0",
        ),
        ("operation_timeout: 30s", "operation_timeout: 0s"),
        ("run_timeout: 5m", "run_timeout: 0s"),
        ("sse_keep_alive_interval: 5s", "sse_keep_alive_interval: 0s"),
        ("subscriber_capacity: 64", "subscriber_capacity: 0"),
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
fn removed_v2_runtime_settings_are_unknown() {
    for removed in [
        "  operation_cancel_grace_period: 1s\n",
        "  max_template_output_bytes: 1048576\n",
        "  journal_capacity: 512\n",
        "  journal_batch_size: 32\n",
        "  journal_operation_timeout: 5s\n",
    ] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  operation_timeout: 30s\n",
            &format!("  operation_timeout: 30s\n{removed}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONFIG_INVALID"
        );
    }
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
