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

    let agents = expect_json(client.get(format!("{base_url}/v1/agents")), StatusCode::OK).await;
    let agents = agents["data"]
        .as_array()
        .expect("agents data must be array");
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
