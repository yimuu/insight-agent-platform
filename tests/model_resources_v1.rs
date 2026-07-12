use std::fs;

use insight_agent_platform::resources::{
    config::load_model_registry_with_env, models::ModelCapability, openai_chat::OpenAiChatLimits,
};
use tempfile::tempdir;

fn model_yaml(extra: &str) -> String {
    format!(
        r#"version: 1
models:
  primary:
    type: open_ai_chat
    base_url: https://models.example.test/v1
    model: example-chat
    api_key_env: MODEL_API_KEY
    capabilities: [vision]
    connect_timeout: 2s
    request_timeout: 30s
{extra}"#
    )
}

fn write_config(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempdir().unwrap();
    let path = directory.path().join("models.yaml");
    fs::write(&path, yaml).unwrap();
    (directory, path)
}

#[test]
fn strict_model_resources_resolve_alias_capability_and_redacted_secret() {
    let (_directory, path) = write_config(&model_yaml(""));
    let registry = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "never-log-this-key".to_string())
    })
    .unwrap();
    let model = registry.resolve("primary").unwrap();

    assert!(model.capabilities().contains(&ModelCapability::Vision));
    assert!(!format!("{model:?}").contains("never-log-this-key"));
}

#[test]
fn model_resources_default_and_override_response_limits() {
    let defaults = OpenAiChatLimits::default();
    let (_directory, path) = write_config(&model_yaml(""));
    let default_registry = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "never-log-this-key".to_string())
    })
    .unwrap();
    let default_model = default_registry.resolve("primary").unwrap();
    assert_eq!(
        default_model.max_accumulated_text_bytes(),
        defaults.max_accumulated_text_bytes
    );

    let (_directory, path) = write_config(&model_yaml(
        "    limits:\n      max_accumulated_text_bytes: 7\n",
    ));
    let overridden = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "never-log-this-key".to_string())
    })
    .unwrap();
    let model = overridden.resolve("primary").unwrap();
    assert_eq!(model.max_accumulated_text_bytes(), 7);
}

#[test]
fn model_resources_reject_zero_response_limits() {
    for field in [
        "max_upstream_bytes",
        "max_buffered_line_bytes",
        "max_event_payload_bytes",
        "max_chunk_text_bytes",
        "max_usage_json_bytes",
        "max_accumulated_text_bytes",
    ] {
        let yaml = model_yaml(&format!("    limits:\n      {field}: 0\n"));
        let (_directory, path) = write_config(&yaml);
        let error = load_model_registry_with_env(&path, |_| Some("secret".to_string()))
            .err()
            .expect("zero limit must fail model configuration");
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID", "{field}: {error}");
    }
}

#[test]
fn model_resources_reject_unknown_fields_versions_and_invalid_durations() {
    for (yaml, code) in [
        (model_yaml("    unexpected: true\n"), "MODEL_CONFIG_INVALID"),
        (
            model_yaml("").replacen("version: 1", "version: 2", 1),
            "MODEL_CONFIG_VERSION_UNSUPPORTED",
        ),
        (
            model_yaml("").replacen("connect_timeout: 2s", "connect_timeout: 0s", 1),
            "MODEL_CONFIG_INVALID",
        ),
    ] {
        let (_directory, path) = write_config(&yaml);
        let error = load_model_registry_with_env(&path, |_| Some("secret".to_string()))
            .err()
            .expect("invalid model configuration must fail");
        assert_eq!(error.code(), code, "{error}");
    }
}

#[test]
fn named_model_secrets_are_required_and_must_not_be_empty() {
    let (_directory, path) = write_config(&model_yaml(""));
    let missing = load_model_registry_with_env(&path, |_| None).err().unwrap();
    assert_eq!(missing.code(), "MODEL_SECRET_MISSING");

    let empty = load_model_registry_with_env(&path, |_| Some("  ".to_string()))
        .err()
        .unwrap();
    assert_eq!(empty.code(), "MODEL_SECRET_EMPTY");
}

#[test]
fn model_resources_reject_plaintext_http_by_default_without_leaking_secrets() {
    let yaml = model_yaml("").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://models.example.test/v1?token=url-secret",
    );
    let (_directory, path) = write_config(&yaml);
    let error = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .err()
    .expect("plaintext HTTP must fail by default");

    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("api-key-secret"));
    assert!(!rendered.contains("url-secret"));
}

#[test]
fn model_resources_allow_explicit_loopback_and_trusted_private_http() {
    let loopback = model_yaml("    transport:\n      plaintext_http: loopback\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://localhost:11434/v1",
    );
    let (_directory, path) = write_config(&loopback);
    load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap();

    let trusted_private = model_yaml("    transport:\n      plaintext_http: trusted_private\n")
        .replace(
            "base_url: https://models.example.test/v1",
            "base_url: http://model-service.internal:8080/v1",
        );
    let (_directory, path) = write_config(&trusted_private);
    load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap();
}

#[test]
fn model_resources_reject_non_loopback_http_with_loopback_policy() {
    let yaml = model_yaml("    transport:\n      plaintext_http: loopback\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://10.0.0.10:8080/v1",
    );
    let (_directory, path) = write_config(&yaml);
    let error = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .err()
    .expect("non-loopback HTTP must fail with loopback policy");

    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
}

#[test]
fn model_resources_reject_non_exact_loopback_aliases() {
    for base_url in [
        "http://127.1:8080/v1",
        "http://127.000.000.001:8080/v1",
        "http://2130706433:8080/v1",
        "http://[0:0:0:0:0:0:0:1]:8080/v1",
    ] {
        let yaml = model_yaml("    transport:\n      plaintext_http: loopback\n").replace(
            "base_url: https://models.example.test/v1",
            &format!("base_url: {base_url}"),
        );
        let (_directory, path) = write_config(&yaml);
        let error = load_model_registry_with_env(&path, |name| {
            (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
        })
        .err()
        .expect("non-exact loopback alias must fail with loopback policy");

        assert_eq!(error.code(), "MODEL_CONFIG_INVALID", "{base_url}");
    }
}

#[test]
fn model_resources_reject_unknown_transport_policy_and_url_userinfo() {
    let unknown = model_yaml("    transport:\n      plaintext_http: internet\n");
    let (_directory, path) = write_config(&unknown);
    let error = load_model_registry_with_env(&path, |_| Some("secret".to_string()))
        .err()
        .expect("unknown transport policy must fail");
    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");

    let userinfo = model_yaml("    transport:\n      plaintext_http: trusted_private\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://user:pass@model-service.internal:8080/v1",
    );
    let (_directory, path) = write_config(&userinfo);
    let error = load_model_registry_with_env(&path, |_| Some("api-key-secret".to_string()))
        .err()
        .expect("URL userinfo must fail");
    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("user:pass"));
    assert!(!rendered.contains("api-key-secret"));
}
