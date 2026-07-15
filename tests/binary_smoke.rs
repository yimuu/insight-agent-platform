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
async fn binary_starts_and_observes_success_and_workflow_failure_runs() {
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

    let ready_url = format!("{base_url}/health/ready");
    let ready = expect_json(
        format!("GET {ready_url}"),
        client.get(ready_url),
        StatusCode::OK,
    )
    .await;
    let health_url = format!("{base_url}/health");
    let health = expect_json(
        format!("GET {health_url}"),
        client.get(health_url),
        StatusCode::OK,
    )
    .await;
    assert_eq!(health, ready, "/health must remain the readiness alias");
    let live_url = format!("{base_url}/health/live");
    let live = expect_json(
        format!("GET {live_url}"),
        client.get(live_url),
        StatusCode::OK,
    )
    .await;
    assert_eq!(live["data"]["status"], "live");

    let agents_url = format!("{base_url}/v1/agents");
    let agents = expect_json(
        format!("GET {agents_url}"),
        client.get(agents_url),
        StatusCode::OK,
    )
    .await;
    let agents = agents["data"]
        .as_array()
        .expect("agents data must be array");
    assert_eq!(agents.len(), 2, "binary smoke should expose two Agents");
    assert_eq!(agents[0]["id"], "code_node_demo");
    assert_eq!(agents[1]["id"], "workflow_failure_demo");
    assert_eq!(
        agents[0]["input_schema"],
        json!({
            "type": "object",
            "required": ["text"],
            "additionalProperties": false,
            "properties": {"text": {"type": "string"}}
        })
    );
    let required_field = agents[0]["input_schema"]["required"]
        .as_array()
        .and_then(|required| required.first())
        .and_then(Value::as_str)
        .expect("code_node_demo discovery contract must identify its required input");
    let discovered_input = Value::Object(serde_json::Map::from_iter([(
        required_field.to_string(),
        Value::String("hello rust world".to_string()),
    )]));
    let agent_url = format!("{base_url}/v1/agents/code_node_demo");
    let detail = expect_json(
        format!("GET {agent_url}"),
        client.get(agent_url),
        StatusCode::OK,
    )
    .await;
    assert_eq!(detail["data"], agents[0]);
    let disabled_url = format!("{base_url}/v1/agents/researcher");
    let disabled = expect_json(
        format!("GET {disabled_url}"),
        client.get(disabled_url),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(disabled["code"], "AGENT_NOT_FOUND");

    let completed = create_and_wait(&client, &base_url, "code_node_demo", discovered_input).await;
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

    let failed = create_and_wait(&client, &base_url, "workflow_failure_demo", json!({})).await;
    assert_eq!(failed["data"]["agent_id"], "workflow_failure_demo");
    assert_eq!(failed["data"]["status"], "failed");
    assert_eq!(failed["data"]["error"]["kind"], "workflow");
    assert_eq!(failed["data"]["error"]["code"], "WORKFLOW_DEMO_REJECTED");
    assert!(failed["data"].get("output").is_none());

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
    - workflow_failure_demo

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

trait RecoverableChild {
    fn kill_for_drop(&mut self);
    fn wait_for_drop(&mut self);
}

impl RecoverableChild for Child {
    fn kill_for_drop(&mut self) {
        let _ = self.kill();
    }

    fn wait_for_drop(&mut self) {
        let _ = self.wait();
    }
}

struct ChildGuard<C: RecoverableChild = Child> {
    child: Option<C>,
}

impl<C: RecoverableChild> ChildGuard<C> {
    fn new(child: C) -> Self {
        Self { child: Some(child) }
    }

    fn run_guarded_shutdown_step<R>(&mut self, step: impl FnOnce(&mut C) -> R) -> R {
        step(self.child.as_mut().expect("child already collected"))
    }
}

impl ChildGuard<Child> {
    fn spawn(platform_config: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_insight-agent-platform"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PLATFORM_CONFIG", platform_config)
            .env_remove("OPENAI_API_KEY")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn insight-agent-platform binary");
        Self::new(child)
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.run_guarded_shutdown_step(|child| {
            child.try_wait().expect("failed to poll child process")
        })
    }

    fn shutdown(mut self) -> Output {
        self.run_guarded_shutdown_step(request_graceful_shutdown);
        for _ in 0..30 {
            if let Some(output) = self.try_collect_exited_output(
                "failed to poll child during shutdown",
                "failed to collect child output",
            ) {
                return output;
            }
            thread::sleep(Duration::from_millis(100));
        }
        self.run_guarded_shutdown_step(|child| {
            child
                .kill()
                .expect("failed to kill child after shutdown timeout");
            child
                .wait()
                .expect("failed to wait for killed child after shutdown timeout");
        });
        self.try_collect_exited_output(
            "failed to confirm killed child exit",
            "failed to collect killed child output",
        )
        .expect("killed child must be exited before output collection")
    }

    fn terminate_and_collect(&mut self) -> Output {
        if let Some(output) = self.try_collect_exited_output(
            "failed to poll child before termination",
            "failed to collect already-exited child output",
        ) {
            return output;
        }

        self.run_guarded_shutdown_step(|child| {
            child.kill().expect("failed to terminate child");
            child.wait().expect("failed to wait for terminated child");
        });
        self.try_collect_exited_output(
            "failed to confirm terminated child exit",
            "failed to collect terminated child output",
        )
        .expect("terminated child must be exited before output collection")
    }

    fn try_collect_exited_output(
        &mut self,
        poll_context: &str,
        collect_context: &str,
    ) -> Option<Output> {
        let exited =
            self.run_guarded_shutdown_step(|child| child.try_wait().expect(poll_context).is_some());
        if !exited {
            return None;
        }

        let child = self.child.take().expect("child already collected");
        Some(child.wait_with_output().expect(collect_context))
    }
}

impl<C: RecoverableChild> Drop for ChildGuard<C> {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            child.kill_for_drop();
            child.wait_for_drop();
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
    let mut last_error = health_diagnostic_message(base_url, "request not attempted", None);

    loop {
        if child.try_wait().is_some() {
            let output = child.terminate_and_collect();
            panic!(
                "platform exited before readiness; last error: {last_error}\n{}",
                format_output(&output)
            );
        }

        match client.get(format!("{base_url}/health/ready")).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_else(|error| {
                    let detail = format!("failed to read response body: {error}");
                    panic!("{}", health_diagnostic_message(base_url, &detail, None));
                });
                if status == StatusCode::OK {
                    let body: Value = serde_json::from_str(&body).unwrap_or_else(|error| {
                        let detail = format!("health body is not JSON: {error}");
                        panic!(
                            "{}",
                            health_diagnostic_message(base_url, &detail, Some(&body))
                        );
                    });
                    if body["code"] == "OK" {
                        return;
                    }
                    last_error = health_diagnostic_message(
                        base_url,
                        "unexpected health response body",
                        Some(&body.to_string()),
                    );
                } else {
                    last_error = health_diagnostic_message(
                        base_url,
                        &format!(
                            "unexpected HTTP status {status}; expected {}",
                            StatusCode::OK
                        ),
                        Some(&body),
                    );
                }
            }
            Err(error) => {
                last_error = health_diagnostic_message(
                    base_url,
                    &format!("HTTP request failed: {error}"),
                    None,
                );
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

async fn expect_json(
    label: String,
    request: reqwest::RequestBuilder,
    expected_status: StatusCode,
) -> Value {
    let response = request.send().await.unwrap_or_else(|error| {
        let detail = format!("HTTP request failed: {error}");
        panic!("{}", http_diagnostic_message(&label, &detail, None));
    });
    let status = response.status();
    let body = response.text().await.unwrap_or_else(|error| {
        let detail = format!("failed to read response body after HTTP status {status}: {error}");
        panic!("{}", http_diagnostic_message(&label, &detail, None));
    });
    assert_eq!(
        status,
        expected_status,
        "{}",
        http_diagnostic_message(
            &label,
            &format!("unexpected HTTP status {status}; expected {expected_status}"),
            Some(&body)
        )
    );
    serde_json::from_str(&body).unwrap_or_else(|error| {
        let detail = format!("response body for HTTP status {status} is not JSON: {error}");
        panic!("{}", http_diagnostic_message(&label, &detail, Some(&body)));
    })
}

async fn create_and_wait(client: &Client, base_url: &str, agent_id: &str, input: Value) -> Value {
    let create_run_url = format!("{base_url}/v1/agents/{agent_id}/runs");
    let created = expect_json(
        format!("POST {create_run_url}"),
        client.post(create_run_url).json(&input),
        StatusCode::ACCEPTED,
    )
    .await;
    let run_id = created["data"]["run_id"]
        .as_str()
        .expect("create response must contain run_id");
    wait_for_terminal_run(client, base_url, run_id).await
}

async fn wait_for_terminal_run(client: &Client, base_url: &str, run_id: &str) -> Value {
    let deadline = Instant::now() + RUN_TIMEOUT;

    loop {
        let run_url = format!("{base_url}/v1/runs/{run_id}");
        let record = expect_json(
            format!("GET {run_url}"),
            client.get(run_url),
            StatusCode::OK,
        )
        .await;
        let last_record = match record["data"]["status"].as_str() {
            Some("completed" | "failed" | "cancelled" | "interrupted") => return record,
            Some("created" | "running") => record,
            other => {
                panic!("run returned unknown status {other:?}:\n{record}");
            }
        };

        if Instant::now() >= deadline {
            panic!("run {run_id} did not terminate within {RUN_TIMEOUT:?}:\n{last_record}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn http_diagnostic_message(label: &str, detail: &str, body: Option<&str>) -> String {
    match body {
        Some(body) => format!("{label}: {detail}\nbody:\n{body}"),
        None => format!("{label}: {detail}"),
    }
}

fn health_diagnostic_message(base_url: &str, detail: &str, body: Option<&str>) -> String {
    http_diagnostic_message(&format!("GET {base_url}/health/ready"), detail, body)
}

fn format_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn http_diagnostic_message_includes_request_context_and_body() {
    let message = http_diagnostic_message(
        "GET http://127.0.0.1:3000/v1/runs/run-123",
        "unexpected HTTP status 500 Internal Server Error; expected 200 OK",
        Some("{\"code\":\"ERR\"}"),
    );

    assert_eq!(
        message,
        "GET http://127.0.0.1:3000/v1/runs/run-123: unexpected HTTP status 500 Internal Server Error; expected 200 OK\nbody:\n{\"code\":\"ERR\"}"
    );
}

#[test]
fn shutdown_step_panic_keeps_child_guard_armed_for_drop() {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    struct RecoveryProbe {
        kill_calls: Arc<AtomicUsize>,
        wait_calls: Arc<AtomicUsize>,
    }

    impl RecoverableChild for RecoveryProbe {
        fn kill_for_drop(&mut self) {
            self.kill_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn wait_for_drop(&mut self) {
            self.wait_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    let kill_calls = Arc::new(AtomicUsize::new(0));
    let wait_calls = Arc::new(AtomicUsize::new(0));
    let result = catch_unwind(AssertUnwindSafe({
        let kill_calls = Arc::clone(&kill_calls);
        let wait_calls = Arc::clone(&wait_calls);
        move || {
            let mut guard = ChildGuard::new(RecoveryProbe {
                kill_calls,
                wait_calls,
            });
            guard.run_guarded_shutdown_step(|_| panic!("simulated shutdown step panic"));
        }
    }));

    assert!(result.is_err(), "the injected shutdown step must panic");
    assert_eq!(kill_calls.load(Ordering::SeqCst), 1);
    assert_eq!(wait_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn health_diagnostic_includes_exact_get_readiness_request() {
    let message = health_diagnostic_message(
        "http://127.0.0.1:3000",
        "unexpected HTTP status 503 Service Unavailable; expected 200 OK",
        Some("{\"code\":\"STARTING\"}"),
    );

    assert_eq!(
        message,
        "GET http://127.0.0.1:3000/health/ready: unexpected HTTP status 503 Service Unavailable; expected 200 OK\nbody:\n{\"code\":\"STARTING\"}"
    );
}
