# Release Quickstart and Binary Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a no-key local quickstart and a real binary smoke test that proves the production executable can boot, serve HTTP, run `code_node_demo`, persist completion, and shut down.

**Architecture:** Keep runtime contracts unchanged and validate the existing stable baseline from the outside. The quickstart path enables only the action-only `code_node_demo` Agent and uses a dummy but syntactically valid model alias because model resource YAML currently requires at least one alias. The binary smoke test launches `CARGO_BIN_EXE_insight-agent-platform` with temporary config and SQLite history, then verifies `/health`, `/v1/agents`, detached Run creation, and completed Run lookup.

**Tech Stack:** Rust 1.94.1, Cargo integration tests, Tokio test runtime, Reqwest client, Tempfile, SQLite history, existing Formal V1 HTTP API, existing `code_node_demo` Agent, existing `example.text_metrics` built-in Action.

## Global Constraints

- Provide a local quickstart path that does not require `OPENAI_API_KEY` or any external model service.
- Add a binary smoke test that launches the real `insight-agent-platform` executable.
- Prove the production startup chain works end to end: platform YAML loading, model resource YAML loading, enabled Agent compilation, SQLite repository initialization, HTTP listener binding, `/health`, `/v1/agents`, detached Run creation, Run lookup until `completed`, and graceful process shutdown.
- Keep the smoke test deterministic, fast, and network-local.
- Avoid changing Formal V1 HTTP, DSL, Run lifecycle, SSE, history, model, or Action interfaces.
- Do not add real model-provider integration testing.
- Do not require external API keys in CI.
- Do not cover PostgreSQL in this smoke test.
- Do not re-enable SSE replay or event recovery routes.
- Do not test attached SSE behavior here.
- Do not introduce new runtime dependencies or dev-dependencies unless implementation proves an existing dependency is insufficient.
- Do not change default production `config/platform.yaml` semantics.
- No HTTP route changes.
- No request/response schema changes.
- No DSL syntax changes.
- No model resource schema changes.
- No platform config schema changes.
- No Run lifecycle changes.
- No SSE behavior changes.
- No database migration changes.

---

## File Structure

- Create `config/platform.quickstart.yaml`: no-key local platform config for humans and documentation.
- Create `config/models.quickstart.yaml`: dummy loadable model resource config with no secret.
- Modify `tests/platform_config_v1.rs`: regression test that quickstart configs load with an empty environment.
- Modify `README.md`: document no-key quickstart separately from model-backed examples.
- Create `tests/binary_smoke.rs`: integration test that launches the real binary and verifies end-to-end behavior over HTTP.

## Task 1: Add No-Key Quickstart Configs and Config Regression

**Files:**
- Create: `config/platform.quickstart.yaml`
- Create: `config/models.quickstart.yaml`
- Modify: `tests/platform_config_v1.rs`

**Interfaces:**
- Consumes: `PlatformConfig::load(path: &Path) -> Result<PlatformConfig, PlatformConfigError>`.
- Consumes: `load_model_registry_with_env(path: &Path, get_env: impl Fn(&str) -> Option<String>) -> Result<ModelRegistry, ResourceConfigError>`.
- Produces: `config/platform.quickstart.yaml` that enables only `code_node_demo`.
- Produces: `config/models.quickstart.yaml` that defines `unused_quickstart_model` without `api_key_env`.
- Produces: `quickstart_configs_load_without_secrets` regression test.

- [ ] **Step 1: Write the failing config regression test**

In `tests/platform_config_v1.rs`, change the imports at the top from:

```rust
use insight_agent_platform::config::{
    AuthConfig, HistoryConfig, PlatformConfig, PlatformConfigError,
};
```

to:

```rust
use insight_agent_platform::{
    config::{AuthConfig, HistoryConfig, PlatformConfig, PlatformConfigError},
    resources::config::load_model_registry_with_env,
};
```

Then add this test after `disabled_auth_is_explicit_and_agent_enablement_defaults_to_none`:

```rust
#[test]
fn quickstart_configs_load_without_secrets() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = PlatformConfig::load(&root.join("config/platform.quickstart.yaml")).unwrap();

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:3000");
    assert_eq!(config.auth, AuthConfig::Disabled);
    assert_eq!(config.agents.directory, root.join("agents"));
    assert_eq!(
        config.agents.enabled.iter().cloned().collect::<Vec<_>>(),
        vec!["code_node_demo".to_string()]
    );
    assert_eq!(config.models.config, root.join("config/models.quickstart.yaml"));
    assert_eq!(
        config.actions.enabled.iter().cloned().collect::<Vec<_>>(),
        vec!["example.text_metrics".to_string()]
    );

    let registry = load_model_registry_with_env(&config.models.config, |_| None).unwrap();
    registry.resolve("unused_quickstart_model").unwrap();
}
```

- [ ] **Step 2: Run the focused test and verify it fails before config files exist**

Run:

```bash
cargo test --test platform_config_v1 quickstart_configs_load_without_secrets -- --exact --nocapture
```

Expected: FAIL with `PLATFORM_CONFIG_NOT_FOUND` or a missing-file panic for `config/platform.quickstart.yaml`.

- [ ] **Step 3: Create `config/platform.quickstart.yaml`**

Create `config/platform.quickstart.yaml` with exactly:

```yaml
version: 1
bind_addr: 127.0.0.1:3000

auth:
  mode: disabled

agents:
  directory: ../agents
  enabled:
    - code_node_demo

models:
  config: models.quickstart.yaml

actions:
  enabled:
    - example.text_metrics

history:
  provider: sqlite
  path: ../data/quickstart.sqlite3

runtime:
  max_concurrent_runs: 4
  max_fork_branches: 4
  max_parallel_node_executions: 4
  max_parallel_branches_per_run: 2
  default_node_timeout: 30s
  run_timeout: 1m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 32
  journal_capacity: 128
  journal_batch_size: 16
  journal_operation_timeout: 5s
```

- [ ] **Step 4: Create `config/models.quickstart.yaml`**

Create `config/models.quickstart.yaml` with exactly:

```yaml
version: 1

models:
  unused_quickstart_model:
    type: open_ai_chat
    base_url: https://models.example.invalid/v1
    model: unused-quickstart-model
    capabilities: []
    connect_timeout: 1s
    request_timeout: 5s
```

- [ ] **Step 5: Run the focused test and verify it passes**

Run:

```bash
cargo test --test platform_config_v1 quickstart_configs_load_without_secrets -- --exact --nocapture
```

Expected: PASS. The test must pass without `OPENAI_API_KEY` or any other model secret.

- [ ] **Step 6: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 7: Commit Task 1**

Run:

```bash
git diff -- config/platform.quickstart.yaml config/models.quickstart.yaml tests/platform_config_v1.rs
git add config/platform.quickstart.yaml config/models.quickstart.yaml tests/platform_config_v1.rs
git commit -m "test: add no-key quickstart config"
```

Expected: one commit containing the two quickstart config files and one config regression test.

## Task 2: Document the No-Key Quickstart

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: `config/platform.quickstart.yaml`.
- Consumes: `config/models.quickstart.yaml`.
- Consumes: HTTP routes `GET /health`, `GET /v1/agents`, `POST /v1/agents/{agent_id}/runs`, and `GET /v1/runs/{run_id}`.
- Produces: README instructions that distinguish no-key local smoke from model-backed examples.

- [ ] **Step 1: Replace the opening startup instructions**

In `README.md`, under `## 配置与启动`, replace this block:

````markdown
复制环境变量模板并填写模型密钥：

```bash
cp .env.example .env
# 编辑 .env，设置 OPENAI_API_KEY
cargo run
```

默认配置只监听 `127.0.0.1:3000`，并显式关闭鉴权。`/health` 始终公开；运行时可接受请求时返回 `200/OK`，journal 永久失败或服务关停后返回 `503/RUNTIME_UNHEALTHY`。`/v1` 可切换到从环境变量读取的 Bearer token：
````

with:

````markdown
无密钥本地 quickstart：

```bash
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

该配置只启用 `code_node_demo`，使用 SQLite 本地历史和内置 `example.text_metrics` Action。它引用 `config/models.quickstart.yaml` 中的未使用 dummy 模型别名，因此不需要 `OPENAI_API_KEY`，也不会调用外部模型服务。

另开一个终端验证健康状态和 Agent 列表：

```bash
curl --silent http://127.0.0.1:3000/health
curl --silent http://127.0.0.1:3000/v1/agents
```

创建一个 detached Run：

```bash
curl --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs
```

复制响应里的 `data.run_id`，再查询 Run：

```bash
RUN_ID=<paste-run-id>
curl --silent "http://127.0.0.1:3000/v1/runs/${RUN_ID}"
```

模型能力示例需要配置真实模型密钥：

```bash
cp .env.example .env
# 编辑 .env，设置 OPENAI_API_KEY
cargo run
```

默认配置只监听 `127.0.0.1:3000`，并显式关闭鉴权。`/health` 始终公开；运行时可接受请求时返回 `200/OK`，journal 永久失败或服务关停后返回 `503/RUNTIME_UNHEALTHY`。`/v1` 可切换到从环境变量读取的 Bearer token：
````

- [ ] **Step 2: Verify README references the quickstart files and action-only Agent**

Run:

```bash
rg -n "platform.quickstart.yaml|models.quickstart.yaml|code_node_demo|example.text_metrics|OPENAI_API_KEY" README.md
```

Expected: matches show the no-key quickstart, `code_node_demo`, `example.text_metrics`, and the separate model-key path.

- [ ] **Step 3: Verify Markdown diff**

Run:

```bash
git diff -- README.md
git diff --check
```

Expected: README diff is limited to startup documentation, and `git diff --check` passes.

- [ ] **Step 4: Commit Task 2**

Run:

```bash
git add README.md
git commit -m "docs: add no-key quickstart"
```

Expected: one documentation-only commit.

## Task 3: Add Real Binary Smoke Test

**Files:**
- Create: `tests/binary_smoke.rs`

**Interfaces:**
- Consumes: `CARGO_BIN_EXE_insight-agent-platform`.
- Consumes: repository `agents/code_node_demo/agent.yaml`.
- Consumes: Formal V1 response envelope `{"code":"OK","message":"ok","data":...}`.
- Produces: `binary_starts_and_completes_code_node_demo_run`.

- [ ] **Step 1: Confirm the smoke test target is currently absent**

Run:

```bash
cargo test --test binary_smoke -- --nocapture
```

Expected: FAIL with `no test target named 'binary_smoke'`. This confirms the missing binary-level coverage.

- [ ] **Step 2: Create `tests/binary_smoke.rs`**

Create `tests/binary_smoke.rs` with exactly:

```rust
//! Real binary smoke test for the formal V1 runtime.

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tempfile::TempDir;

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test]
async fn binary_starts_and_completes_code_node_demo_run() {
    let temp = TempDir::new().unwrap();
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_temp_configs(temp.path(), bind_addr);
    let base_url = format!("http://{bind_addr}");
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let mut child = ChildGuard::spawn(&platform_config);
    wait_for_health(&client, &base_url, &mut child).await;

    let agents = expect_json(
        client.get(format!("{base_url}/v1/agents")),
        StatusCode::OK,
    )
    .await;
    let agents = agents["data"].as_array().expect("agents data must be array");
    assert_eq!(agents.len(), 1, "quickstart smoke should expose one Agent");
    assert_eq!(agents[0]["id"], "code_node_demo");

    let created = expect_json(
        client
            .post(format!("{base_url}/v1/agents/code_node_demo/runs"))
            .json(&json!({"text":"hello rust world"})),
        StatusCode::ACCEPTED,
    )
    .await;
    let run_id = created["data"]["run_id"]
        .as_str()
        .expect("create response must contain run_id")
        .to_string();

    let completed = wait_for_completed_run(&client, &base_url, &run_id).await;
    assert_eq!(completed["data"]["agent_id"], "code_node_demo");
    assert_eq!(completed["data"]["status"], "completed");
    assert_eq!(completed["data"]["output"]["format"], "text");
    assert_eq!(completed["data"]["output"]["data"], Value::Null);
    let content = completed["data"]["output"]["content"]
        .as_str()
        .expect("completed output must contain text content");
    assert!(content.contains("Text metrics:"), "{content}");
    assert!(content.contains("- characters: 16"), "{content}");
    assert!(content.contains("- words: 3"), "{content}");
    assert!(content.contains("- lines: 1"), "{content}");

    let output = child.shutdown();
    assert!(
        shutdown_was_graceful(&output),
        "platform should exit cleanly after shutdown request\n{}",
        format_output(&output)
    );
}

fn reserve_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn write_temp_configs(root: &Path, bind_addr: SocketAddr) -> PathBuf {
    let platform_config = root.join("platform.yaml");
    let models_config = root.join("models.yaml");
    let history_path = root.join("history.sqlite3");
    let agents_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");

    fs::write(
        &models_config,
        r#"version: 1

models:
  unused_smoke_model:
    type: open_ai_chat
    base_url: https://models.example.invalid/v1
    model: unused-smoke-model
    capabilities: []
    connect_timeout: 1s
    request_timeout: 5s
"#,
    )
    .unwrap();

    fs::write(
        &platform_config,
        format!(
            r#"version: 1
bind_addr: {bind_addr}

auth:
  mode: disabled

agents:
  directory: {}
  enabled:
    - code_node_demo

models:
  config: models.yaml

actions:
  enabled:
    - example.text_metrics

history:
  provider: sqlite
  path: {}

runtime:
  max_concurrent_runs: 4
  max_fork_branches: 4
  max_parallel_node_executions: 4
  max_parallel_branches_per_run: 2
  default_node_timeout: 30s
  run_timeout: 1m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 32
  journal_capacity: 128
  journal_batch_size: 16
  journal_operation_timeout: 5s
"#,
            agents_dir.display(),
            history_path.display(),
        ),
    )
    .unwrap();

    platform_config
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(platform_config: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_insight-agent-platform"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PLATFORM_CONFIG", platform_config)
            .env_remove("OPENAI_API_KEY")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn insight-agent-platform binary");
        Self { child: Some(child) }
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()
            .expect("child already collected")
            .try_wait()
            .expect("failed to poll child process")
    }

    fn shutdown(mut self) -> Output {
        let mut child = self.child.take().expect("child already collected");
        request_graceful_shutdown(&mut child);
        for _ in 0..30 {
            if child
                .try_wait()
                .expect("failed to poll child during shutdown")
                .is_some()
            {
                return child
                    .wait_with_output()
                    .expect("failed to collect child output");
            }
            thread::sleep(Duration::from_millis(100));
        }
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("failed to collect killed child output")
    }

    fn terminate_and_collect(&mut self) -> Output {
        let mut child = self.child.take().expect("child already collected");
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("failed to collect child output")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(unix)]
fn request_graceful_shutdown(child: &mut Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("failed to invoke kill -TERM");
    assert!(status.success(), "kill -TERM failed with status {status}");
}

#[cfg(not(unix))]
fn request_graceful_shutdown(child: &mut Child) {
    let _ = child.kill();
}

#[cfg(unix)]
fn shutdown_was_graceful(output: &Output) -> bool {
    output.status.success()
}

#[cfg(not(unix))]
fn shutdown_was_graceful(_output: &Output) -> bool {
    true
}

async fn wait_for_health(client: &Client, base_url: &str, child: &mut ChildGuard) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_error = String::new();

    loop {
        if child.try_wait().is_some() {
            let output = child.terminate_and_collect();
            panic!(
                "platform exited before readiness; last error: {last_error}\n{}",
                format_output(&output)
            );
        }

        match client.get(format!("{base_url}/health")).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read body: {error}"));
                if status == StatusCode::OK {
                    let body: Value = serde_json::from_str(&body)
                        .unwrap_or_else(|error| panic!("health body is not JSON: {error}"));
                    if body["code"] == "OK" {
                        return;
                    }
                    last_error = format!("unexpected health body: {body}");
                } else {
                    last_error = format!("health returned {status}: {body}");
                }
            }
            Err(error) => {
                last_error = error.to_string();
            }
        }

        if Instant::now() >= deadline {
            let output = child.terminate_and_collect();
            panic!(
                "platform did not become ready within {READY_TIMEOUT:?}; last error: {last_error}\n{}",
                format_output(&output)
            );
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn expect_json(request: reqwest::RequestBuilder, expected_status: StatusCode) -> Value {
    let response = request.send().await.expect("HTTP request failed");
    let status = response.status();
    let body = response.text().await.expect("failed to read response body");
    assert_eq!(
        status, expected_status,
        "unexpected HTTP status; body:\n{body}"
    );
    serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("response body is not JSON: {error}\n{body}"))
}

async fn wait_for_completed_run(client: &Client, base_url: &str, run_id: &str) -> Value {
    let deadline = Instant::now() + RUN_TIMEOUT;
    let mut last_record = Value::Null;

    loop {
        let record = expect_json(
            client.get(format!("{base_url}/v1/runs/{run_id}")),
            StatusCode::OK,
        )
        .await;
        match record["data"]["status"].as_str() {
            Some("completed") => return record,
            Some("failed" | "cancelled" | "interrupted") => {
                panic!("run reached terminal non-success state:\n{record}")
            }
            Some("created" | "running") => {
                last_record = record;
            }
            other => {
                panic!("run returned unknown status {other:?}:\n{record}");
            }
        }

        if Instant::now() >= deadline {
            panic!("run {run_id} did not complete within {RUN_TIMEOUT:?}:\n{last_record}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn format_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
```

- [ ] **Step 3: Run the binary smoke test**

Run:

```bash
cargo test --test binary_smoke -- --nocapture
```

Expected: PASS. The test must not require `OPENAI_API_KEY`, PostgreSQL, external model services, or external network access.

- [ ] **Step 4: Run API regression tests that cover the same public routes in-process**

Run:

```bash
cargo test --test api -- --nocapture formal_v1_path_captures_match_after_axum_upgrade event_replay_route_and_recovery_headers_are_not_supported
```

Expected: PASS. This confirms the binary smoke did not require route contract changes.

- [ ] **Step 5: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git diff -- tests/binary_smoke.rs
git add tests/binary_smoke.rs
git commit -m "test: smoke test platform binary"
```

Expected: one test-only commit.

## Task 4: Full Verification

**Files:**
- No required file changes.

**Interfaces:**
- Consumes: all files changed by Tasks 1-3.
- Produces: final verification evidence for handoff.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 2: Run all tests**

Run:

```bash
cargo test --all-targets --all-features -- --nocapture --test-threads=1
```

Expected: PASS, including `tests/binary_smoke.rs`.

- [ ] **Step 3: Run Clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Run dependency advisory audit**

Run:

```bash
cargo audit
```

Expected: PASS.

- [ ] **Step 5: Run dependency policy check**

Run:

```bash
cargo deny check
```

Expected: PASS, with any duplicate dependency output matching the already accepted residual duplicate policy.

- [ ] **Step 6: Check patch hygiene**

Run:

```bash
git diff --check
git status --short --branch
```

Expected:

- `git diff --check` passes.
- `git status --short --branch` shows `main` ahead of `origin/main` only by the new local commits and no uncommitted files.

- [ ] **Step 7: Report verification evidence**

In the final handoff, report the exact commands that passed:

```text
cargo fmt --check
cargo test --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Do not claim a command passed unless its output was observed in this execution session.
