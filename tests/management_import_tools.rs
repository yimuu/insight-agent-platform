use std::{fs, path::Path, process::Command};

use serde_json::Value;

#[test]
fn agentctl_dry_run_compiles_package_without_reading_token_or_emitting_content() {
    let output = Command::new(env!("CARGO_BIN_EXE_agentctl"))
        .args([
            "import",
            "--server",
            "http://127.0.0.1:1",
            "--token-env",
            "TOKEN_MUST_NOT_BE_READ",
            "--agent-dir",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("agents/action_demo")
                .to_str()
                .unwrap(),
            "--dry-run",
        ])
        .env_remove("TOKEN_MUST_NOT_BE_READ")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["operation"], "agent_import");
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["agent_id"], "action_demo");
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains("api_version:"));
    assert!(!rendered.contains("workflow:"));
}

#[test]
fn providerctl_dry_run_expands_static_extension_without_reading_secret() {
    let temporary = tempfile::tempdir().unwrap();
    let config_path = temporary.path().join("platform.yaml");
    let quickstart = include_str!("../config/platform.quickstart.yaml");
    let with_provider = quickstart.replace(
        "actions:\n",
        "providers:\n  imported-company:\n    type: open_ai_compatible\n    endpoint: https://llm.company.test/v1\n    credential:\n      type: bearer\n      env: PROVIDER_SECRET_MUST_NOT_BE_READ\n    models:\n      chat-v1:\n        input: [text]\n\nactions:\n",
    );
    fs::write(&config_path, with_provider).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_providerctl"))
        .args([
            "import-extensions",
            "--server",
            "http://127.0.0.1:1",
            "--token-env",
            "TOKEN_MUST_NOT_BE_READ",
            "--platform-config",
            config_path.to_str().unwrap(),
            "--catalog",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("catalog/provider-catalog.yaml")
                .to_str()
                .unwrap(),
            "--dry-run",
        ])
        .env_remove("TOKEN_MUST_NOT_BE_READ")
        .env_remove("PROVIDER_SECRET_MUST_NOT_BE_READ")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["operation"], "provider_extension_import");
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["providers"][0]["provider_id"], "imported-company");
    assert_eq!(report["providers"][0]["model_count"], 1);
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains("PROVIDER_SECRET_MUST_NOT_BE_READ"));
    assert!(!rendered.contains("llm.company.test"));
}
