use std::{collections::BTreeMap, fs, path::Path, time::Duration};

use insight_agent_platform::{
    config::{
        AuthConfig, DeploymentMode, HistoryConfig, LiveRunStreamBrokerConfig,
        LlmAttachmentDeliveryConfig, McpInteractionConfig, ModelInputModality, PlatformConfig,
        PlatformConfigError, ProviderExtensionSource, ProviderTransportPolicy, RunStreamTopology,
    },
    engine::PersistenceMode,
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
fn mcp_config_is_strict_bounded_and_secret_backed() {
    let yaml = format!(
        "{}{}",
        base_yaml("  mode: disabled"),
        r#"
management:
  version: 1
  enabled: true
  operator_credentials:
    - identity: platform-operator
      token_env: MCP_OPERATOR_TOKEN
      capabilities: [mcp.server.read, mcp.server.write, mcp.server.discover, mcp.server.publish]
  provider_secret_resolver:
    type: environment_reference
    allowed_names: [PROVIDER_TOKEN]
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
  client:
    enabled: true
    management_api:
      enabled: true
      discovery_workers: 4
      max_pending_discoveries: 128
    secret_encryption:
      active_key_version: v1
      keyring_env: MCP_SECRET_KEYRING
    secret_resolver:
      type: environment_reference
      allowed_names: [MCP_ENGINEERING_TOKEN]
    default_limits:
      max_request_bytes: 1048576
      max_response_bytes: 16777216
      max_sse_line_bytes: 65536
      max_sse_event_bytes: 1048576
      max_content_items: 128
      max_catalog_items: 4096
  server:
    enabled: false
    authorization:
      type: disabled
"#
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(
        &path,
        BTreeMap::from([
            (
                "MCP_OPERATOR_TOKEN".to_owned(),
                "operator-secret".to_owned(),
            ),
            (
                "MCP_SECRET_KEYRING".to_owned(),
                format!(r#"{{"v1":"{}"}}"#, "11".repeat(32)),
            ),
        ]),
    )
    .unwrap();
    assert_eq!(config.mcp.version, 2);
    assert!(config.mcp.client.management_api.enabled);
    assert_eq!(config.mcp.client.management_api.discovery_workers, 4);
    assert!(config
        .management
        .provider_secret_resolver_allowed_names
        .contains("PROVIDER_TOKEN"));
    assert!(config
        .mcp
        .client
        .secret_resolver
        .allowed_names
        .contains("MCP_ENGINEERING_TOKEN"));
    assert!(!format!("{:?}", config.management.operator_credentials).contains("operator-secret"));

    let invalid_provider_secret = yaml.replace("PROVIDER_TOKEN", "provider-token");
    let (_directory, path) = write_config(&invalid_provider_secret);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([
                (
                    "MCP_OPERATOR_TOKEN".to_owned(),
                    "operator-secret".to_owned()
                ),
                (
                    "MCP_SECRET_KEYRING".to_owned(),
                    format!(r#"{{"v1":"{}"}}"#, "11".repeat(32))
                ),
            ])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_MANAGEMENT_INVALID"
    );

    let invalid = yaml.replace(
        "      max_catalog_items: 4096",
        "      max_catalog_items: 0",
    );
    let (_directory, path) = write_config(&invalid);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([
                (
                    "MCP_OPERATOR_TOKEN".to_owned(),
                    "operator-secret".to_owned()
                ),
                (
                    "MCP_SECRET_KEYRING".to_owned(),
                    format!(r#"{{"v1":"{}"}}"#, "11".repeat(32))
                ),
            ])
        )
        .unwrap_err()
        .code(),
        "PLATFORM_MCP_INVALID"
    );
}

#[test]
fn mcp_config_rejects_unknown_fields_and_unsafe_remote_plaintext() {
    let base = format!(
        "{}{}",
        base_yaml("  mode: disabled"),
        r#"
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
  client:
    enabled: false
    management_api:
      enabled: false
    servers: {}
  server:
    enabled: false
    authorization:
      type: disabled
"#
    );
    let (_directory, path) = write_config(&base);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let unknown = base.replace("    servers: {}", "    magic: true");
    let (_directory, path) = write_config(&unknown);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
}

#[test]
fn legacy_mcp_operator_credentials_are_rejected_by_the_shared_management_cutover() {
    let yaml = format!(
        "{}{}",
        base_yaml("  mode: disabled"),
        r#"
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
  client:
    enabled: true
    management_api:
      enabled: true
      operator_credentials: []
  server:
    enabled: false
    authorization:
      type: disabled
"#
    );
    let (_directory, path) = write_config(&yaml);
    let error = load(&path, BTreeMap::new()).unwrap_err();
    assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
}

#[test]
fn mcp_oauth_resource_server_requires_explicit_scope_authority() {
    let yaml = format!(
        "{}{}",
        base_yaml("  mode: disabled"),
        r#"
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
  client:
    enabled: false
  server:
    enabled: true
    endpoint: /mcp
    authorization:
      type: oauth_resource_server
      resource: https://agents.example.com/mcp
      authorization_servers:
        - https://identity.example.com
      required_scopes: [mcp.invoke]
"#
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert!(matches!(
        &config.mcp.server.authorization,
        insight_agent_platform::config::McpServerAuthorizationConfig::OauthResourceServer {
            required_scopes,
            ..
        } if required_scopes.contains("mcp.invoke")
    ));

    let missing_scope = yaml.replace("      required_scopes: [mcp.invoke]\n", "");
    let (_directory, path) = write_config(&missing_scope);
    assert!(load(&path, BTreeMap::new()).is_err());
}

#[test]
fn mcp_server_public_resources_prompts_and_export_scopes_are_closed() {
    let yaml = format!(
        "{}{}",
        base_yaml("  mode: disabled"),
        r#"
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
  client:
    enabled: false
  server:
    enabled: true
    endpoint: /mcp
    authorization:
      type: oauth_resource_server
      resource: https://agents.example.com/mcp
      authorization_servers: [https://identity.example.com]
      required_scopes: [mcp.read]
    exports:
      agents:
        - agent: researcher
          as: researcher
          execution: task_preferred
          input_required: allowed
          required_scope: mcp.read
      resources:
        - uri: insight://public/guide
          name: public_guide
          title: Public guide
          mime_type: text/markdown
          text: "Safe public content."
          required_scope: mcp.read
      prompts:
        - name: review_topic
          description: Review a user-selected topic.
          required_scope: mcp.read
          arguments:
            - name: topic
              required: true
          messages:
            - role: user
              text: "Review {{topic}}."
          completions:
            topic: [security, reliability]
"#
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.mcp.server.exports.resources[0].uri,
        "insight://public/guide"
    );
    assert_eq!(
        config.mcp.server.exports.agents[0].input_required,
        McpInteractionConfig::Allowed
    );
    assert_eq!(
        config.mcp.server.exports.prompts[0].completions["topic"],
        ["security", "reliability"]
    );

    let missing_scope = yaml.replace("          required_scope: mcp.read\n", "");
    let (_directory, path) = write_config(&missing_scope);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_MCP_INVALID"
    );

    let hidden_argument = yaml.replace("Review {{topic}}.", "Review {{internal_prompt}}.");
    let (_directory, path) = write_config(&hidden_argument);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_MCP_INVALID"
    );

    let implicit_interaction_policy = yaml.replace("          input_required: allowed\n", "");
    let (_directory, path) = write_config(&implicit_interaction_policy);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
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
    assert_eq!(
        config.runtime.default_persistence_mode,
        PersistenceMode::Full
    );
    assert!(config.runtime.terminal_only.enabled);
    assert_eq!(
        config.runtime.terminal_only.owner_lease,
        Duration::from_secs(30)
    );
    assert_eq!(
        config.runtime.terminal_only.owner_heartbeat,
        Duration::from_secs(10)
    );
    assert_eq!(
        config.runtime.terminal_only.terminal_commit_retry,
        Duration::from_secs(10)
    );
    assert!(!config.runtime.terminal_only.allow_volatile_waits);
    assert_eq!(config.runtime.terminal_only.max_concurrent_runs, 50);
    assert_eq!(config.runtime.max_concurrent_operations_per_run, 256);
    assert_eq!(config.runtime.operation_timeout, Duration::from_secs(30));
    assert_eq!(
        config.runtime.sse_keep_alive_interval,
        Duration::from_secs(5)
    );
    assert_eq!(
        config.runtime.scheduler.active_poll_interval,
        Duration::from_millis(25)
    );
    assert_eq!(
        config.runtime.scheduler.idle_poll_min_interval,
        Duration::from_millis(100)
    );
    assert_eq!(config.runtime.scheduler.claim_batch_size, 8);
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
        config.runtime.run_stream.topology,
        RunStreamTopology::SingleRuntime
    );
    assert_eq!(
        config.runtime.run_stream.broker,
        LiveRunStreamBrokerConfig::InMemory
    );
    assert_eq!(config.runtime.run_stream.body_queue_capacity, 256);
    assert_eq!(config.runtime.run_stream.control_queue_capacity, 32);
    assert_eq!(config.runtime.run_stream.max_frame_bytes, 4 * 1024);
    assert_eq!(config.runtime.run_stream.max_item_bytes, 4 * 1024 * 1024);
    assert_eq!(config.runtime.run_stream.max_run_bytes, 16 * 1024 * 1024);
    assert_eq!(
        config.runtime.run_stream.terminal_barrier_timeout,
        Duration::from_secs(2)
    );
    assert_eq!(
        config.runtime.run_stream.outbound_write_timeout,
        Duration::from_secs(10)
    );
    assert_eq!(config.runtime.max_llm_tool_rounds, 16);
    assert_eq!(config.runtime.max_llm_tool_calls, 64);
    assert_eq!(config.agents.directory, directory.path().join("agents"));
    assert!(config.providers.extensions.is_empty());
    assert!(config.model_policy.is_none());
    assert_eq!(
        config.history,
        HistoryConfig::Sqlite {
            path: directory.path().join("data/history.sqlite3")
        }
    );
    assert_eq!(config.artifacts.inline_threshold_bytes, 64 * 1024);
    assert_eq!(config.artifacts.max_read_bytes, 64 * 1024 * 1024);
    assert_eq!(config.artifacts.namespace, "development");
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
    assert_eq!(
        config.object_storage.llm_attachment_delivery,
        LlmAttachmentDeliveryConfig::InlineData
    );
    assert!(config.conversations.enabled);
    assert_eq!(config.conversations.inline_content_max_bytes, 8_192);
    assert_eq!(config.conversations.message_page_size_default, 50);
    assert_eq!(config.conversations.message_page_size_max, 200);
    assert_eq!(config.conversations.summary_trigger_messages, 30);
    assert_eq!(config.conversations.summary_trigger_tokens, 24_000);
    assert_eq!(config.conversations.recent_context_messages, 20);
    assert_eq!(config.conversations.retention_days, 90);
    assert_eq!(config.runtime.terminal_only.run_retention_days, 30);
}

#[test]
fn llm_attachment_delivery_is_explicit_closed_and_defaults_to_inline() {
    let object_storage = "object_storage:\n  llm_attachment_delivery: presigned_url\n  s3:\n    endpoint: http://127.0.0.1:9000\n    public_endpoint: https://files.example.com\n    region: us-east-1\n    bucket: platform\n    force_path_style: true\n    access_key_env: S3_ACCESS_KEY\n    secret_key_env: S3_SECRET_KEY\nruntime:";
    let yaml = base_yaml("  mode: disabled").replace("runtime:", object_storage);
    let (_directory, path) = write_config(&yaml);
    assert_eq!(
        load(&path, BTreeMap::new())
            .unwrap()
            .object_storage
            .llm_attachment_delivery,
        LlmAttachmentDeliveryConfig::PresignedUrl
    );

    let invalid = yaml.replace("presigned_url", "automatic");
    let (_directory, path) = write_config(&invalid);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
}

#[test]
fn terminal_only_runtime_policy_is_closed_defaulted_and_bounded() {
    let terminal_only = "runtime:\n  default_persistence_mode: terminal_only\n  terminal_only:\n    enabled: true\n    owner_lease_seconds: 60\n    owner_heartbeat_seconds: 20\n    terminal_commit_retry_seconds: 15\n    run_retention_days: 45\n    allow_volatile_waits: true\n    max_concurrent_runs: 75";
    let yaml = base_yaml("  mode: disabled").replace("runtime:", terminal_only);
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.default_persistence_mode,
        PersistenceMode::TerminalOnly
    );
    assert_eq!(
        config.runtime.terminal_only.owner_lease,
        Duration::from_secs(60)
    );
    assert_eq!(
        config.runtime.terminal_only.owner_heartbeat,
        Duration::from_secs(20)
    );
    assert_eq!(
        config.runtime.terminal_only.terminal_commit_retry,
        Duration::from_secs(15)
    );
    assert_eq!(config.runtime.terminal_only.run_retention_days, 45);
    assert!(config.runtime.terminal_only.allow_volatile_waits);
    assert_eq!(config.runtime.terminal_only.max_concurrent_runs, 75);

    for (valid, invalid, code) in [
        (
            "  default_persistence_mode: terminal_only",
            "  default_persistence_mode: checkpointed",
            "PLATFORM_CONFIG_INVALID",
        ),
        (
            "    owner_lease_seconds: 60",
            "    owner_lease_seconds: 2",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "    owner_heartbeat_seconds: 20",
            "    owner_heartbeat_seconds: 21",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "    terminal_commit_retry_seconds: 15",
            "    terminal_commit_retry_seconds: 0",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "    run_retention_days: 45",
            "    run_retention_days: 0",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "    max_concurrent_runs: 75",
            "    max_concurrent_runs: 10001",
            "PLATFORM_RUNTIME_INVALID",
        ),
    ] {
        let invalid_yaml = yaml.replace(valid, invalid);
        let (_directory, path) = write_config(&invalid_yaml);
        assert_eq!(load(&path, BTreeMap::new()).unwrap_err().code(), code);
    }

    let unknown = yaml.replace(
        "    allow_volatile_waits: true",
        "    allow_volatile_waits: true\n    checkpoint_events: false",
    );
    let (_directory, path) = write_config(&unknown);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
}

#[test]
fn conversation_policy_is_closed_and_all_numeric_fields_are_bounded() {
    let conversations = "conversations:\n  enabled: true\n  inline_content_max_bytes: 16384\n  message_page_size_default: 25\n  message_page_size_max: 150\n  summary_trigger_messages: 40\n  summary_trigger_tokens: 32000\n  recent_context_messages: 10\n  retention_days: 365\nruntime:";
    let yaml = base_yaml("  mode: disabled").replace("runtime:", conversations);
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(config.conversations.inline_content_max_bytes, 16_384);
    assert_eq!(config.conversations.message_page_size_default, 25);
    assert_eq!(config.conversations.message_page_size_max, 150);
    assert_eq!(config.conversations.summary_trigger_messages, 40);
    assert_eq!(config.conversations.summary_trigger_tokens, 32_000);
    assert_eq!(config.conversations.recent_context_messages, 10);
    assert_eq!(config.conversations.retention_days, 365);

    for (valid, invalid) in [
        (
            "  inline_content_max_bytes: 16384",
            "  inline_content_max_bytes: 255",
        ),
        (
            "  message_page_size_default: 25",
            "  message_page_size_default: 151",
        ),
        (
            "  message_page_size_max: 150",
            "  message_page_size_max: 201",
        ),
        (
            "  summary_trigger_messages: 40",
            "  summary_trigger_messages: 1",
        ),
        (
            "  summary_trigger_tokens: 32000",
            "  summary_trigger_tokens: 255",
        ),
        (
            "  recent_context_messages: 10",
            "  recent_context_messages: 41",
        ),
        ("  retention_days: 365", "  retention_days: 0"),
    ] {
        let invalid_yaml = yaml.replace(valid, invalid);
        let (_directory, path) = write_config(&invalid_yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONVERSATIONS_INVALID"
        );
    }

    let unknown = yaml.replace(
        "  retention_days: 365",
        "  retention_days: 365\n  persist_sse_chunks: true",
    );
    let (_directory, path) = write_config(&unknown);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
}

#[test]
fn persistence_environment_surface_strictly_overrides_yaml_then_defaults() {
    let yaml = base_yaml("  mode: disabled").replace(
        "runtime:",
        "conversations:\n  enabled: false\n  inline_content_max_bytes: 4096\n  message_page_size_default: 10\n  message_page_size_max: 20\n  summary_trigger_messages: 25\n  summary_trigger_tokens: 12000\n  recent_context_messages: 5\n  retention_days: 30\nruntime:\n  default_persistence_mode: full\n  terminal_only:\n    enabled: false\n    owner_lease_seconds: 30\n    owner_heartbeat_seconds: 10\n    terminal_commit_retry_seconds: 10\n    run_retention_days: 30\n    allow_volatile_waits: false\n    max_concurrent_runs: 50",
    );
    let (_directory, path) = write_config(&yaml);
    let environment = BTreeMap::from([
        (
            "INSIGHT_RUNTIME__DEFAULT_PERSISTENCE_MODE".to_owned(),
            "terminal_only".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_LEASE_SECONDS".to_owned(),
            "60".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_HEARTBEAT_SECONDS".to_owned(),
            "20".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__TERMINAL_COMMIT_RETRY_SECONDS".to_owned(),
            "15".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__RUN_RETENTION_DAYS".to_owned(),
            "45".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__ALLOW_VOLATILE_WAITS".to_owned(),
            "true".to_owned(),
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS".to_owned(),
            "75".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__INLINE_CONTENT_MAX_BYTES".to_owned(),
            "16384".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_DEFAULT".to_owned(),
            "25".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_MAX".to_owned(),
            "150".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_MESSAGES".to_owned(),
            "40".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_TOKENS".to_owned(),
            "32000".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__RECENT_CONTEXT_MESSAGES".to_owned(),
            "10".to_owned(),
        ),
        (
            "INSIGHT_CONVERSATIONS__RETENTION_DAYS".to_owned(),
            "365".to_owned(),
        ),
    ]);
    let config = load(&path, environment).unwrap();
    assert_eq!(
        config.runtime.default_persistence_mode,
        PersistenceMode::TerminalOnly
    );
    assert!(config.runtime.terminal_only.enabled);
    assert_eq!(
        config.runtime.terminal_only.owner_lease,
        Duration::from_secs(60)
    );
    assert_eq!(
        config.runtime.terminal_only.owner_heartbeat,
        Duration::from_secs(20)
    );
    assert_eq!(
        config.runtime.terminal_only.terminal_commit_retry,
        Duration::from_secs(15)
    );
    assert_eq!(config.runtime.terminal_only.run_retention_days, 45);
    assert!(config.runtime.terminal_only.allow_volatile_waits);
    assert_eq!(config.runtime.terminal_only.max_concurrent_runs, 75);
    assert!(config.conversations.enabled);
    assert_eq!(config.conversations.inline_content_max_bytes, 16_384);
    assert_eq!(config.conversations.message_page_size_default, 25);
    assert_eq!(config.conversations.message_page_size_max, 150);
    assert_eq!(config.conversations.summary_trigger_messages, 40);
    assert_eq!(config.conversations.summary_trigger_tokens, 32_000);
    assert_eq!(config.conversations.recent_context_messages, 10);
    assert_eq!(config.conversations.retention_days, 365);
}

#[test]
fn persistence_environment_values_are_strict_and_share_yaml_bounds() {
    let (_directory, path) = write_config(&base_yaml("  mode: disabled"));
    for (name, value, code) in [
        (
            "INSIGHT_RUNTIME__DEFAULT_PERSISTENCE_MODE",
            "FULL",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__ENABLED",
            "1",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_HEARTBEAT_SECONDS",
            "11",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS",
            "-1",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS",
            "+1",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_RUNTIME__TERMINAL_ONLY__RUN_RETENTION_DAYS",
            "0",
            "PLATFORM_RUNTIME_INVALID",
        ),
        (
            "INSIGHT_CONVERSATIONS__ENABLED",
            "yes",
            "PLATFORM_CONVERSATIONS_INVALID",
        ),
        (
            "INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_MAX",
            "many",
            "PLATFORM_CONVERSATIONS_INVALID",
        ),
        (
            "INSIGHT_CONVERSATIONS__RECENT_CONTEXT_MESSAGES",
            "201",
            "PLATFORM_CONVERSATIONS_INVALID",
        ),
        (
            "INSIGHT_CONVERSATIONS__RETENTION_DAYS",
            "0",
            "PLATFORM_CONVERSATIONS_INVALID",
        ),
    ] {
        let error = load(&path, BTreeMap::from([(name.to_owned(), value.to_owned())])).unwrap_err();
        assert_eq!(error.code(), code, "{name}");
    }
}

#[test]
fn artifact_store_policy_is_strict_resolved_and_bounded() {
    let explicit = base_yaml("  mode: disabled").replace(
        "runtime:",
        "artifacts:\n  namespace: test\n  inline_threshold_bytes: 1024\n  max_read_bytes: 4096\n  orphan_retention: 2h\n  reference_retention: 7d\n  gc_interval: 15s\n  deletion_claim_seconds: 30\nruntime:",
    );
    let (_directory, path) = write_config(&explicit);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(config.artifacts.namespace, "test");
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
fn artifact_product_contract_is_s3_only_and_rejects_filesystem_fields() {
    let explicit = base_yaml("  mode: disabled").replace(
        "runtime:",
        "artifacts:\n  namespace: development\n  inline_threshold_bytes: 1024\n  orphan_retention: 2h\n  gc_interval: 15s\n  deletion_claim_seconds: 30\nruntime:",
    );
    for retired in [
        "  provider: local_filesystem\n",
        "  provider: shared_filesystem\n",
        "  directory: /data/artifacts\n",
        "  tenant_encryption: {}\n",
    ] {
        let retired = explicit.replace(
            "  namespace: development\n",
            &format!("  namespace: development\n{retired}"),
        );
        let (_directory, path) = write_config(&retired);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONFIG_INVALID"
        );
    }

    let invalid_namespace = explicit.replace("namespace: development", "namespace: ../forged");
    let (_directory, path) = write_config(&invalid_namespace);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_ARTIFACTS_INVALID"
    );

    let production = explicit
        .replace(
            "deployment_mode: single_process_development",
            "deployment_mode: production",
        )
        .replace(
            "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
            "history:\n  provider: postgres\n  database_url_env: DATABASE_URL",
        )
        .replace(
            "artifacts:",
            "object_storage:\n  s3:\n    endpoint: https://rustfs.internal\n    public_endpoint: https://files.example.com\n    region: us-east-1\n    bucket: platform\n    force_path_style: true\n    access_key_env: S3_ACCESS_KEY\n    secret_key_env: S3_SECRET_KEY\nartifacts:",
        )
        .replace("namespace: development", "namespace: production");
    let (_directory, path) = write_config(&production);
    let config = load(
        &path,
        BTreeMap::from([(
            "DATABASE_URL".to_owned(),
            "postgres://localhost/platform".to_owned(),
        )]),
    )
    .unwrap();
    assert_eq!(config.artifacts.namespace, "production");
}

#[test]
fn retired_artifact_encryption_is_rejected() {
    let yaml = base_yaml("  mode: disabled").replace(
        "runtime:",
        "artifacts:\n  namespace: development\n  inline_threshold_bytes: 1024\n  tenant_encryption:\n    active_key_version: v2\n    keyring_env: TENANT_KEYRING\n  orphan_retention: 2h\n  gc_interval: 15s\n  deletion_claim_seconds: 30\nruntime:",
    );
    let (_directory, path) = write_config(&yaml);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
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
fn run_stream_transport_and_bounds_are_closed_and_validated() {
    let run_stream = "  subscriber_capacity: 64\n  max_llm_tool_rounds: 8\n  max_llm_tool_calls: 32\n  run_stream:\n    topology: single_runtime\n    broker:\n      type: in_memory\n    body_queue_capacity: 17\n    control_queue_capacity: 5\n    max_frame_bytes: 8192\n    max_item_bytes: 65536\n    max_run_bytes: 262144\n    terminal_barrier_timeout: 750ms\n    outbound_write_timeout: 3s";
    let yaml = base_yaml("  mode: disabled").replace("  subscriber_capacity: 64", run_stream);
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.run_stream.broker,
        LiveRunStreamBrokerConfig::InMemory
    );
    assert_eq!(config.runtime.run_stream.body_queue_capacity, 17);
    assert_eq!(config.runtime.run_stream.control_queue_capacity, 5);
    assert_eq!(config.runtime.run_stream.max_frame_bytes, 8192);
    assert_eq!(
        config.runtime.run_stream.terminal_barrier_timeout,
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

    for legacy_broker in ["in_process", "postgres_notify", "nats_core"] {
        let legacy_yaml = yaml.replace(
            "    broker:\n      type: in_memory",
            &format!("    broker: {legacy_broker}"),
        );
        let (_directory, path) = write_config(&legacy_yaml);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_CONFIG_INVALID"
        );
    }

    let legacy = yaml.replace("  run_stream:", "  response_stream:");
    let (_directory, path) = write_config(&legacy);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
}

#[test]
fn nats_core_run_stream_config_is_strict_secret_backed_and_topology_aware() {
    let nats = "  subscriber_capacity: 64\n  run_stream:\n    topology: distributed\n    broker:\n      type: nats_core\n      servers: [tls://nats-a.internal:4222, tls://nats-b.internal:4222]\n      namespace: production_cn1\n      credentials_env: NATS_RUN_STREAM_CREDS\n      tls:\n        required: true\n        root_certificates: [../secrets/nats-ca.pem]\n        client_certificate: ../secrets/nats-client.pem\n        client_private_key: ../secrets/nats-client-key.pem\n      connect_timeout: 3s\n      subscription_ready_timeout: 2s\n      reconnect_min_delay: 100ms\n      reconnect_max_delay: 5s\n      max_pending_messages: 4096\n      max_pending_bytes: 16777216\n      drain_timeout: 10s\n    body_queue_capacity: 256\n    control_queue_capacity: 32\n    max_frame_bytes: 4096\n    max_item_bytes: 4194304\n    max_run_bytes: 16777216\n    terminal_barrier_timeout: 2s\n    outbound_write_timeout: 10s";
    let yaml = base_yaml("  mode: disabled")
        .replace(
            "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
            "history:\n  provider: postgres\n  database_url_env: DATABASE_URL",
        )
        .replace("  subscriber_capacity: 64", nats);
    let (directory, path) = write_config(&yaml);
    let config = load(
        &path,
        BTreeMap::from([
            (
                "NATS_RUN_STREAM_CREDS".to_owned(),
                "user-jwt-and-seed".to_owned(),
            ),
            (
                "DATABASE_URL".to_owned(),
                "postgres://localhost/platform".to_owned(),
            ),
        ]),
    )
    .unwrap();
    assert_eq!(
        config.runtime.run_stream.topology,
        RunStreamTopology::Distributed
    );
    let LiveRunStreamBrokerConfig::NatsCore(nats) = config.runtime.run_stream.broker else {
        panic!("expected nats_core broker")
    };
    assert_eq!(nats.servers.len(), 2);
    assert_eq!(nats.namespace, "production_cn1");
    assert_eq!(nats.credentials.unwrap().expose(), "user-jwt-and-seed");
    assert!(nats.tls.required);
    assert_eq!(
        nats.tls.root_certificates,
        vec![directory.path().join("secrets/nats-ca.pem")]
    );
    assert_eq!(nats.connect_timeout, Duration::from_secs(3));
    assert_eq!(nats.reconnect_min_delay, Duration::from_millis(100));
    assert_eq!(nats.max_pending_messages, 4096);
    assert_eq!(nats.max_pending_bytes, 16_777_216);

    let distributed_in_memory = yaml
        .replace("      type: nats_core", "      type: in_memory")
        .replace(
            "      servers: [tls://nats-a.internal:4222, tls://nats-b.internal:4222]\n      namespace: production_cn1\n      credentials_env: NATS_RUN_STREAM_CREDS\n      tls:\n        required: true\n        root_certificates: [../secrets/nats-ca.pem]\n        client_certificate: ../secrets/nats-client.pem\n        client_private_key: ../secrets/nats-client-key.pem\n      connect_timeout: 3s\n      subscription_ready_timeout: 2s\n      reconnect_min_delay: 100ms\n      reconnect_max_delay: 5s\n      max_pending_messages: 4096\n      max_pending_bytes: 16777216\n      drain_timeout: 10s\n",
            "",
        );
    let (_directory, path) = write_config(&distributed_in_memory);
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
        "PLATFORM_RUNTIME_INVALID"
    );

    for invalid in [
        yaml.replace("tls://nats-a.internal:4222", "http://nats-a.internal:4222"),
        yaml.replace(
            "tls://nats-a.internal:4222",
            "tls://user:password@nats-a.internal:4222",
        ),
        yaml.replace(
            "tls://nats-a.internal:4222",
            "tls://nats-a.internal:4222?token=secret",
        ),
        yaml.replace(
            "tls://nats-a.internal:4222",
            "tls://nats-a.internal:4222#fragment",
        ),
        yaml.replace("production_cn1", "Production.*"),
        yaml.replace("tls://nats-b.internal:4222", "tls://nats-a.internal:4222"),
        yaml.replace("max_pending_messages: 4096", "max_pending_messages: 0"),
        yaml.replace("reconnect_min_delay: 100ms", "reconnect_min_delay: 6s"),
        yaml.replace("max_frame_bytes: 4096", "max_frame_bytes: 65537"),
        yaml.replace(
            "        client_private_key: ../secrets/nats-client-key.pem\n",
            "",
        ),
    ] {
        let (_directory, path) = write_config(&invalid);
        assert_eq!(
            load(
                &path,
                BTreeMap::from([
                    (
                        "NATS_RUN_STREAM_CREDS".to_owned(),
                        "user-jwt-and-seed".to_owned(),
                    ),
                    (
                        "DATABASE_URL".to_owned(),
                        "postgres://localhost/platform".to_owned(),
                    ),
                ]),
            )
            .unwrap_err()
            .code(),
            "PLATFORM_RUNTIME_INVALID",
            "{invalid}"
        );
    }

    let unknown_backend_field = yaml.replace(
        "      drain_timeout: 10s",
        "      drain_timeout: 10s\n      unexpected: true",
    );
    let (_directory, path) = write_config(&unknown_backend_field);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([
                (
                    "NATS_RUN_STREAM_CREDS".to_owned(),
                    "user-jwt-and-seed".to_owned(),
                ),
                (
                    "DATABASE_URL".to_owned(),
                    "postgres://localhost/platform".to_owned(),
                ),
            ]),
        )
        .unwrap_err()
        .code(),
        "PLATFORM_CONFIG_INVALID"
    );

    let (_directory, path) = write_config(&yaml);
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
        "PLATFORM_SECRET_MISSING"
    );

    let sqlite_distributed = yaml.replace(
        "history:\n  provider: postgres\n  database_url_env: DATABASE_URL",
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
    );
    let (_directory, path) = write_config(&sqlite_distributed);
    assert_eq!(
        load(
            &path,
            BTreeMap::from([(
                "NATS_RUN_STREAM_CREDS".to_owned(),
                "user-jwt-and-seed".to_owned(),
            )]),
        )
        .unwrap_err()
        .code(),
        "PLATFORM_RUNTIME_INVALID"
    );

    let production = yaml
        .replace(
            "deployment_mode: single_process_development",
            "deployment_mode: production",
        )
        .replace(
            "runtime:",
            "object_storage:\n  s3:\n    endpoint: https://rustfs.internal\n    public_endpoint: https://files.example.com\n    region: us-east-1\n    bucket: platform\n    force_path_style: true\n    access_key_env: S3_ACCESS_KEY\n    secret_key_env: S3_SECRET_KEY\nartifacts:\n  namespace: production\n  inline_threshold_bytes: 65536\n  max_read_bytes: 67108864\n  orphan_retention: 1h\n  reference_retention: 1d\n  gc_interval: 1m\n  deletion_claim_seconds: 60\nruntime:",
        );
    for invalid in [
        production.replace("      credentials_env: NATS_RUN_STREAM_CREDS\n", ""),
        production.replace("        required: true", "        required: false"),
    ] {
        let (_directory, path) = write_config(&invalid);
        assert_eq!(
            load(
                &path,
                BTreeMap::from([
                    (
                        "NATS_RUN_STREAM_CREDS".to_owned(),
                        "user-jwt-and-seed".to_owned(),
                    ),
                    (
                        "DATABASE_URL".to_owned(),
                        "postgres://localhost/platform".to_owned(),
                    ),
                ]),
            )
            .unwrap_err()
            .code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }
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
    assert!(config.providers.extensions.is_empty());
    assert!(config.model_policy.is_none());
    assert_eq!(
        config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        vec!["example.text_metrics".to_string()]
    );

    let registry =
        load_model_registry_with_env(&config.providers, config.model_policy.as_ref(), |_| None)
            .unwrap();
    assert!(registry.selectors().count() >= 4);
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
    assert!(matches!(
        config.history,
        insight_agent_platform::config::HistoryConfig::Postgres {
            max_connections: 10,
            ..
        }
    ));
    assert!(!format!("{config:?}").contains(secret));
}

#[test]
fn postgres_history_pool_bound_is_strict_and_bounded() {
    for max_connections in [0, 3, 257] {
        let yaml = base_yaml("  mode: disabled").replace(
            "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
            &format!(
                "history:\n  provider: postgres\n  database_url_env: HISTORY_URL\n  max_connections: {max_connections}"
            ),
        );
        let (_directory, path) = write_config(&yaml);
        let error = load(
            &path,
            BTreeMap::from([(
                "HISTORY_URL".to_owned(),
                "postgres://localhost/platform".to_owned(),
            )]),
        )
        .unwrap_err();
        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    }

    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL\n  max_connections: 8",
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(
        &path,
        BTreeMap::from([(
            "HISTORY_URL".to_owned(),
            "postgres://localhost/platform".to_owned(),
        )]),
    )
    .unwrap();
    assert!(matches!(
        config.history,
        insight_agent_platform::config::HistoryConfig::Postgres {
            max_connections: 8,
            ..
        }
    ));
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
fn scheduler_polling_contract_is_strict_ordered_and_bounded() {
    let explicit = base_yaml("  mode: disabled").replace(
        "  subscriber_capacity: 64",
        "  subscriber_capacity: 64\n  scheduler:\n    active_poll_interval: 10ms\n    idle_poll_min_interval: 20ms\n    idle_poll_max_interval: 1s\n    safety_poll_interval: 3s\n    claim_batch_size: 12\n    notification_reconnect_interval: 50ms",
    );
    let (_directory, path) = write_config(&explicit);
    let config = load(&path, BTreeMap::new()).unwrap();
    assert_eq!(
        config.runtime.scheduler.active_poll_interval,
        Duration::from_millis(10)
    );
    assert_eq!(config.runtime.scheduler.claim_batch_size, 12);

    for invalid in [
        explicit.replace("active_poll_interval: 10ms", "active_poll_interval: 30ms"),
        explicit.replace("claim_batch_size: 12", "claim_batch_size: 0"),
        explicit.replace(
            "notification_reconnect_interval: 50ms",
            "notification_reconnect_interval: 0ms",
        ),
    ] {
        let (_directory, path) = write_config(&invalid);
        assert_eq!(
            load(&path, BTreeMap::new()).unwrap_err().code(),
            "PLATFORM_RUNTIME_INVALID"
        );
    }
    let unknown = explicit.replace(
        "notification_reconnect_interval: 50ms",
        "notification_reconnect_interval: 50ms\n    unknown_scheduler_key: true",
    );
    let (_directory, path) = write_config(&unknown);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_CONFIG_INVALID"
    );
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

#[test]
fn provider_extensions_and_model_policy_are_strict_platform_configuration() {
    let yaml = base_yaml("  mode: disabled").replace(
        "actions:",
        r#"providers:
  company-llm:
    type: open_ai_compatible
    endpoint: http://127.0.0.1:11434/v1
    credential:
      type: bearer
      env: COMPANY_LLM_API_KEY
    models:
      vendor/internal-chat/v1:
        input: [text, image]
    connect_timeout: 2s
    request_timeout: 45s
    transport:
      plaintext_http: loopback
  dashscope-cn-team-a:
    extends: dashscope-cn
    credential:
      env: TEAM_A_DASHSCOPE_API_KEY
    models:
      qwen-new-model:
        input: [text]
model_policy:
  allow:
    - provider: company-llm
      id: vendor/internal-chat/v1
    - provider: dashscope-cn-team-a
      id: qwen-new-model
actions:"#,
    );
    let (_directory, path) = write_config(&yaml);
    let config = load(&path, BTreeMap::new()).unwrap();

    let custom = &config.providers.extensions["company-llm"];
    assert_eq!(custom.source, ProviderExtensionSource::OpenAiCompatible);
    assert_eq!(
        custom.endpoint.as_deref(),
        Some("http://127.0.0.1:11434/v1")
    );
    assert_eq!(
        custom.credential_env.as_deref(),
        Some("COMPANY_LLM_API_KEY")
    );
    assert_eq!(custom.connect_timeout, Duration::from_secs(2));
    assert_eq!(custom.request_timeout, Duration::from_secs(45));
    assert_eq!(custom.transport, ProviderTransportPolicy::AllowLoopbackHttp);
    let model = &custom.models["vendor/internal-chat/v1"];
    assert_eq!(
        model.input,
        std::collections::BTreeSet::from([ModelInputModality::Text, ModelInputModality::Image,])
    );
    let inherited = &config.providers.extensions["dashscope-cn-team-a"];
    assert_eq!(
        inherited.source,
        ProviderExtensionSource::Extends {
            provider: "dashscope-cn".to_owned()
        }
    );
    assert_eq!(config.model_policy.unwrap().allow.len(), 2);
}

#[test]
fn legacy_model_registry_and_malformed_provider_extensions_fail_closed() {
    let cases = [
        (
            "models:\n  config: models.yaml\nactions:",
            "PLATFORM_CONFIG_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: COMPANY_LLM_API_KEY}\n    models: {chat: {input: [text]}}\n    enabled: true\nactions:",
            "PLATFORM_CONFIG_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: COMPANY_LLM_API_KEY}\n    models: {chat: {input: [text], native_structured_output: json_object}}\nactions:",
            "PLATFORM_CONFIG_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: COMPANY_LLM_API_KEY}\n    models: {chat: {input: [text]}}\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    extends: dashscope-cn\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: COMPANY_LLM_API_KEY}\n    models: {chat: {input: [text]}}\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    endpoint: https://llm.example/v1\n    models: {chat: {input: [text]}}\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "providers:\n  dashscope-cn-team-a:\n    extends: dashscope-cn\n    endpoint: https://override.example/v1\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: 1INVALID}\n    models: {chat: {input: [text]}}\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "providers:\n  company-llm:\n    type: open_ai_compatible\n    endpoint: https://llm.example/v1\n    credential: {type: bearer, env: COMPANY_LLM_API_KEY}\n    models: {chat: {input: [image]}}\nactions:",
            "PLATFORM_PROVIDER_INVALID",
        ),
        (
            "model_policy:\n  allow:\n    - {provider: dashscope-cn, id: qwen3.6-flash}\n    - {provider: dashscope-cn, id: qwen3.6-flash}\nactions:",
            "PLATFORM_MODEL_POLICY_INVALID",
        ),
    ];

    for (replacement, code) in cases {
        let yaml = base_yaml("  mode: disabled").replace("actions:", replacement);
        let (_directory, path) = write_config(&yaml);
        assert_eq!(load(&path, BTreeMap::new()).unwrap_err().code(), code);
    }
}
