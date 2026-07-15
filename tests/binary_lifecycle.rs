//! Real-process lifecycle coverage for startup reconciliation and bounded shutdown.

#![cfg(unix)]

use std::{
    fs,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventType},
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunAttachment, RunLifecycle, RunRecord, RunStatus},
    },
};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(8);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_terminalizes_attached_and_detached_runs_and_restart_preserves_them() {
    let temp = TempDir::new().unwrap();
    let mut model = BlockingModelServer::spawn(2).await;
    let files = LifecycleFiles::new(temp.path(), model.address());
    let client = test_client();

    let first_addr = reserve_loopback_addr();
    let platform_config =
        files.write_platform(first_addr, Duration::from_secs(3), Duration::from_secs(5));
    let first_base_url = format!("http://{first_addr}");
    let mut child = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &first_base_url, &mut child).await;

    let detached_id = create_detached(&client, &first_base_url).await;
    model.wait_for_accepted(1).await;
    wait_for_status(&client, &first_base_url, &detached_id, RunStatus::Running).await;

    let (attached_id, attached_stream) = create_attached(&client, &first_base_url).await;
    model.wait_for_accepted(1).await;
    wait_for_status(&client, &first_base_url, &attached_id, RunStatus::Running).await;

    child.signal_term();
    model.wait_for_closed(2).await;
    let output = child.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        output.status.success(),
        "SIGTERM drain must exit zero\n{}",
        format_output(&output)
    );

    let attached_sse = tokio::time::timeout(PROCESS_TIMEOUT, attached_stream)
        .await
        .expect("Attached SSE did not close after SIGTERM")
        .expect("Attached SSE reader panicked")
        .expect("Attached SSE body failed");
    assert!(
        attached_sse.contains("\"type\":\"run.cancelled\""),
        "Attached SSE did not contain run.cancelled:\n{attached_sse}"
    );
    assert!(
        attached_sse.contains("\"code\":\"RUN_CANCELLED\""),
        "Attached SSE did not contain RUN_CANCELLED:\n{attached_sse}"
    );

    let repository = SqliteRunRepository::connect_path(&files.history_path)
        .await
        .unwrap();
    let attached_before = get_record(&repository, &attached_id).await;
    let detached_before = get_record(&repository, &detached_id).await;
    assert_stop_terminal(
        &attached_before,
        RunAttachment::Attached,
        RunStatus::Cancelled,
        "RUN_CANCELLED",
    );
    assert_stop_terminal(
        &detached_before,
        RunAttachment::Detached,
        RunStatus::Interrupted,
        "RUN_INTERRUPTED",
    );
    let attached_events_before = events(&repository, &attached_id).await;
    let detached_events_before = events(&repository, &detached_id).await;
    assert_single_terminal(&attached_events_before, RunEventType::RunCancelled);
    assert_single_terminal(&detached_events_before, RunEventType::RunInterrupted);
    drop(repository);

    let restart_addr = reserve_loopback_addr();
    let platform_config =
        files.write_platform(restart_addr, Duration::from_secs(3), Duration::from_secs(5));
    let restart_base_url = format!("http://{restart_addr}");
    let mut restarted = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &restart_base_url, &mut restarted).await;
    assert_api_status(
        &client,
        &restart_base_url,
        &attached_id,
        RunStatus::Cancelled,
        "RUN_CANCELLED",
    )
    .await;
    assert_api_status(
        &client,
        &restart_base_url,
        &detached_id,
        RunStatus::Interrupted,
        "RUN_INTERRUPTED",
    )
    .await;
    restarted.signal_term();
    let restart_output = restarted.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        restart_output.status.success(),
        "idle restart must exit zero\n{}",
        format_output(&restart_output)
    );

    let repository = SqliteRunRepository::connect_path(&files.history_path)
        .await
        .unwrap();
    assert_eq!(get_record(&repository, &attached_id).await, attached_before);
    assert_eq!(get_record(&repository, &detached_id).await, detached_before);
    assert_eq!(
        events(&repository, &attached_id).await,
        attached_events_before
    );
    assert_eq!(
        events(&repository, &detached_id).await,
        detached_events_before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_running_run_is_reconciled_before_first_ready_and_second_restart_is_idempotent() {
    let temp = TempDir::new().unwrap();
    let mut model = BlockingModelServer::spawn(1).await;
    let files = LifecycleFiles::new(temp.path(), model.address());
    let client = test_client();

    let first_addr = reserve_loopback_addr();
    let platform_config =
        files.write_platform(first_addr, Duration::from_secs(3), Duration::from_secs(5));
    let first_base_url = format!("http://{first_addr}");
    let mut child = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &first_base_url, &mut child).await;
    let run_id = create_detached(&client, &first_base_url).await;
    model.wait_for_accepted(1).await;
    wait_for_status(&client, &first_base_url, &run_id, RunStatus::Running).await;

    let crash_output = child.kill_and_collect();
    assert!(
        !crash_output.status.success(),
        "forced process death unexpectedly exited zero\n{}",
        format_output(&crash_output)
    );
    model.wait_for_closed(1).await;

    let repository = SqliteRunRepository::connect_path(&files.history_path)
        .await
        .unwrap();
    assert_eq!(
        get_record(&repository, &run_id).await.status(),
        RunStatus::Running
    );
    assert_eq!(
        terminal_events(&events(&repository, &run_id).await).len(),
        0
    );
    drop(repository);

    let first_restart_addr = reserve_loopback_addr();
    let platform_config = files.write_platform(
        first_restart_addr,
        Duration::from_secs(3),
        Duration::from_secs(5),
    );
    let first_restart_url = format!("http://{first_restart_addr}");
    let mut first_restart = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &first_restart_url, &mut first_restart).await;
    assert_api_status(
        &client,
        &first_restart_url,
        &run_id,
        RunStatus::Interrupted,
        "RUN_INTERRUPTED",
    )
    .await;
    first_restart.signal_term();
    let first_restart_output = first_restart.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        first_restart_output.status.success(),
        "first recovery restart must exit zero\n{}",
        format_output(&first_restart_output)
    );

    let repository = SqliteRunRepository::connect_path(&files.history_path)
        .await
        .unwrap();
    let recovered_record = get_record(&repository, &run_id).await;
    assert_stop_terminal(
        &recovered_record,
        RunAttachment::Detached,
        RunStatus::Interrupted,
        "RUN_INTERRUPTED",
    );
    let recovered_events = events(&repository, &run_id).await;
    assert_single_terminal(&recovered_events, RunEventType::RunInterrupted);
    drop(repository);

    let second_restart_addr = reserve_loopback_addr();
    let platform_config = files.write_platform(
        second_restart_addr,
        Duration::from_secs(3),
        Duration::from_secs(5),
    );
    let second_restart_url = format!("http://{second_restart_addr}");
    let mut second_restart = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &second_restart_url, &mut second_restart).await;
    assert_api_status(
        &client,
        &second_restart_url,
        &run_id,
        RunStatus::Interrupted,
        "RUN_INTERRUPTED",
    )
    .await;
    second_restart.signal_term();
    let second_restart_output = second_restart.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        second_restart_output.status.success(),
        "second recovery restart must exit zero\n{}",
        format_output(&second_restart_output)
    );

    let repository = SqliteRunRepository::connect_path(&files.history_path)
        .await
        .unwrap();
    assert_eq!(get_record(&repository, &run_id).await, recovered_record);
    assert_eq!(events(&repository, &run_id).await, recovered_events);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_http_body_hits_configured_hard_deadline_and_creates_no_run() {
    let temp = TempDir::new().unwrap();
    let unused_model_addr = reserve_loopback_addr();
    let files = LifecycleFiles::new(temp.path(), unused_model_addr);
    let client = test_client();
    let bind_addr = reserve_loopback_addr();
    let grace_period = Duration::from_millis(200);
    let hard_deadline = Duration::from_millis(800);
    let platform_config = files.write_platform(bind_addr, grace_period, hard_deadline);
    let base_url = format!("http://{bind_addr}");
    let mut child = ChildGuard::spawn(&platform_config);
    wait_for_ready(&client, &base_url, &mut child).await;

    let mut socket = TcpStream::connect(bind_addr).await.unwrap();
    let request_headers = format!(
        "POST /v1/agents/lifecycle_blocker/runs HTTP/1.1\r\n\
         Host: {bind_addr}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: 1048576\r\n\
         Expect: 100-continue\r\n\
         Connection: close\r\n\r\n"
    );
    socket.write_all(request_headers.as_bytes()).await.unwrap();
    let mut interim = [0_u8; 256];
    let read = tokio::time::timeout(IO_TIMEOUT, socket.read(&mut interim))
        .await
        .expect("server did not acknowledge Expect: 100-continue")
        .unwrap();
    let interim = String::from_utf8_lossy(&interim[..read]);
    assert!(
        interim.starts_with("HTTP/1.1 100 Continue"),
        "unexpected interim response: {interim:?}"
    );
    socket.write_all(b"{").await.unwrap();

    let started = Instant::now();
    child.signal_term();
    let output = child.wait_for_exit(Duration::from_secs(3));
    let elapsed = started.elapsed();
    assert!(
        !output.status.success(),
        "hard-deadline shutdown unexpectedly exited zero\n{}",
        format_output(&output)
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "hard-deadline exit was not bounded: {elapsed:?}\n{}",
        format_output(&output)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("server shutdown exceeded its hard deadline"),
        "hard-deadline diagnostic was missing\n{}",
        format_output(&output)
    );

    let mut closed = [0_u8; 1];
    let closed = tokio::time::timeout(IO_TIMEOUT, socket.read(&mut closed))
        .await
        .expect("incomplete request socket stayed open after process exit");
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "incomplete request unexpectedly received more response bytes"
    );
    assert_eq!(run_count(&files.history_path).await, 0);
}

struct LifecycleFiles {
    platform_config: PathBuf,
    models_config: PathBuf,
    agents_dir: PathBuf,
    history_path: PathBuf,
}

impl LifecycleFiles {
    fn new(root: &Path, model_addr: SocketAddr) -> Self {
        let agents_dir = root.join("agents");
        let agent_dir = agents_dir.join("lifecycle_blocker");
        fs::create_dir_all(&agent_dir).unwrap();
        fs::write(
            agent_dir.join("agent.yaml"),
            r#"version: 1
id: lifecycle_blocker
name: Lifecycle Blocker
description: Keeps a real model stream in flight for process lifecycle tests.

input:
  schema:
    type: object
    additionalProperties: false

entry: block

nodes:
  block:
    type: core.chat
    next: result
    emit: content
    config:
      model: blocking_chat
      messages:
        - role: user
          content: wait until the process asks this Run to stop
      parameters: {}

  result:
    type: core.end
    config:
      outcome: success
      content:
        template: "{{ nodes.block.output.text }}"
      format: text
"#,
        )
        .unwrap();

        let models_config = root.join("models.yaml");
        fs::write(
            &models_config,
            format!(
                r#"version: 1

models:
  blocking_chat:
    type: open_ai_chat
    base_url: http://{model_addr}/v1
    model: lifecycle-blocking-model
    capabilities: []
    connect_timeout: 1s
    request_timeout: 2m
    transport:
      plaintext_http: loopback
"#
            ),
        )
        .unwrap();

        Self {
            platform_config: root.join("platform.yaml"),
            models_config,
            agents_dir,
            history_path: root.join("history.sqlite3"),
        }
    }

    fn write_platform(
        &self,
        bind_addr: SocketAddr,
        shutdown_grace_period: Duration,
        shutdown_hard_deadline: Duration,
    ) -> PathBuf {
        assert!(shutdown_hard_deadline > shutdown_grace_period);
        assert!(self.models_config.is_file());
        fs::write(
            &self.platform_config,
            format!(
                r#"version: 1
bind_addr: {bind_addr}

auth:
  mode: disabled

agents:
  directory: {}
  enabled:
    - lifecycle_blocker

models:
  config: {}

actions:
  enabled: []

history:
  provider: sqlite
  path: {}

runtime:
  max_concurrent_runs: 4
  max_fork_branches: 4
  max_parallel_node_executions: 4
  max_parallel_branches_per_run: 2
  default_node_timeout: 2m
  run_timeout: 2m
  sse_keep_alive_interval: 100ms
  subscriber_capacity: 32
  journal_capacity: 128
  journal_batch_size: 16
  journal_operation_timeout: 1s
  readiness_probe_timeout: 500ms
  shutdown_grace_period: {}
  shutdown_hard_deadline: {}
"#,
                self.agents_dir.display(),
                self.models_config.display(),
                self.history_path.display(),
                duration_yaml(shutdown_grace_period),
                duration_yaml(shutdown_hard_deadline),
            ),
        )
        .unwrap();
        self.platform_config.clone()
    }
}

fn duration_yaml(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        format!("{}s", duration.as_secs())
    } else {
        format!("{}ms", duration.as_millis())
    }
}

struct BlockingModelServer {
    address: SocketAddr,
    accepted: mpsc::UnboundedReceiver<()>,
    closed: mpsc::UnboundedReceiver<()>,
    task: JoinHandle<()>,
}

impl BlockingModelServer {
    async fn spawn(expected_connections: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted) = mpsc::unbounded_channel();
        let (closed_tx, closed) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut connections = Vec::with_capacity(expected_connections);
            for _ in 0..expected_connections {
                let (socket, _) = listener.accept().await.unwrap();
                let accepted_tx = accepted_tx.clone();
                let closed_tx = closed_tx.clone();
                connections.push(tokio::spawn(async move {
                    serve_blocking_model_connection(socket, accepted_tx, closed_tx).await;
                }));
            }
            drop(accepted_tx);
            drop(closed_tx);
            for connection in connections {
                connection.await.unwrap();
            }
        });
        Self {
            address,
            accepted,
            closed,
            task,
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    async fn wait_for_accepted(&mut self, count: usize) {
        for _ in 0..count {
            tokio::time::timeout(RUN_TIMEOUT, self.accepted.recv())
                .await
                .expect("model fixture was not contacted")
                .expect("model fixture stopped before accepting the expected request");
        }
    }

    async fn wait_for_closed(&mut self, count: usize) {
        for _ in 0..count {
            tokio::time::timeout(RUN_TIMEOUT, self.closed.recv())
                .await
                .expect("platform did not close the model connection")
                .expect("model fixture stopped before observing the expected close");
        }
    }
}

impl Drop for BlockingModelServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_blocking_model_connection(
    mut socket: TcpStream,
    accepted: mpsc::UnboundedSender<()>,
    closed: mpsc::UnboundedSender<()>,
) {
    read_complete_http_request(&mut socket).await;
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n\
              data: {\"choices\":[{\"delta\":{\"content\":\"waiting\"},\"finish_reason\":null}],\"usage\":null}\n\n",
        )
        .await
        .unwrap();
    socket.flush().await.unwrap();
    accepted.send(()).unwrap();

    let mut buffer = [0_u8; 1024];
    loop {
        match socket.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    let _ = closed.send(());
}

async fn read_complete_http_request(socket: &mut TcpStream) {
    const MAX_REQUEST_BYTES: usize = 1024 * 1024;
    let mut request = Vec::new();
    let header_end = loop {
        if let Some(index) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(
            read > 0,
            "model request closed before its headers completed"
        );
        request.extend_from_slice(&chunk[..read]);
        assert!(request.len() <= MAX_REQUEST_BYTES);
    };
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    let expected = header_end + content_length;
    while request.len() < expected {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "model request closed before its body completed");
        request.extend_from_slice(&chunk[..read]);
        assert!(request.len() <= MAX_REQUEST_BYTES);
    }
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
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy")
            .env("NO_PROXY", "127.0.0.1,localhost,::1")
            .env("no_proxy", "127.0.0.1,localhost,::1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn insight-agent-platform binary");
        Self { child: Some(child) }
    }

    fn signal_term(&mut self) {
        let child = self.child.as_ref().expect("child already collected");
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("failed to invoke kill -TERM");
        assert!(status.success(), "kill -TERM failed with status {status}");
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()
            .expect("child already collected")
            .try_wait()
            .expect("failed to poll child process")
    }

    fn collect_exited(&mut self) -> Output {
        self.child
            .take()
            .expect("child already collected")
            .wait_with_output()
            .expect("failed to collect child output")
    }

    fn wait_for_exit(mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        loop {
            if self.try_wait().is_some() {
                return self.collect_exited();
            }
            if Instant::now() >= deadline {
                let mut child = self.child.take().expect("child already collected");
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("failed to collect timed-out child output");
                panic!(
                    "platform did not exit within {timeout:?}\n{}",
                    format_output(&output)
                );
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn kill_and_collect(mut self) -> Output {
        let mut child = self.child.take().expect("child already collected");
        child.kill().expect("failed to kill child process");
        child
            .wait_with_output()
            .expect("failed to collect killed child output")
    }

    fn collect_if_exited(&mut self) -> Option<Output> {
        self.try_wait().map(|_| self.collect_exited())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn reserve_loopback_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn test_client() -> Client {
    Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(1))
        .build()
        .unwrap()
}

async fn wait_for_ready(client: &Client, base_url: &str, child: &mut ChildGuard) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let ready_url = format!("{base_url}/health/ready");
    let mut last_error = "request not attempted".to_string();
    loop {
        if let Some(output) = child.collect_if_exited() {
            panic!(
                "platform exited before readiness; last error: {last_error}\n{}",
                format_output(&output)
            );
        }
        match tokio::time::timeout(IO_TIMEOUT, client.get(&ready_url).send()).await {
            Ok(Ok(response)) if response.status() == StatusCode::OK => {
                let body = response.text().await.unwrap();
                let body: Value = serde_json::from_str(&body).unwrap();
                assert_eq!(body["code"], "OK");
                return;
            }
            Ok(Ok(response)) => {
                last_error = format!("unexpected status {}", response.status());
            }
            Ok(Err(error)) => last_error = error.to_string(),
            Err(_) => last_error = "readiness request timed out".to_string(),
        }
        if Instant::now() >= deadline {
            panic!("platform did not become ready: {last_error}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn create_detached(client: &Client, base_url: &str) -> String {
    let url = format!("{base_url}/v1/agents/lifecycle_blocker/runs");
    let body = expect_json(client.post(&url).json(&json!({})), StatusCode::ACCEPTED).await;
    body["data"]["run_id"]
        .as_str()
        .expect("Detached create response omitted run_id")
        .to_string()
}

async fn create_attached(
    client: &Client,
    base_url: &str,
) -> (String, JoinHandle<Result<String, reqwest::Error>>) {
    let url = format!("{base_url}/v1/agents/lifecycle_blocker/runs/stream");
    let response = tokio::time::timeout(IO_TIMEOUT, client.post(&url).json(&json!({})).send())
        .await
        .expect("Attached create request timed out")
        .expect("Attached create request failed");
    assert_eq!(response.status(), StatusCode::OK);
    let run_id = response
        .headers()
        .get("x-run-id")
        .expect("Attached response omitted x-run-id")
        .to_str()
        .unwrap()
        .to_string();
    let reader = tokio::spawn(async move { response.text().await });
    (run_id, reader)
}

async fn wait_for_status(
    client: &Client,
    base_url: &str,
    run_id: &str,
    expected: RunStatus,
) -> Value {
    let deadline = Instant::now() + RUN_TIMEOUT;
    loop {
        let body = get_run(client, base_url, run_id).await;
        if body["data"]["status"] == expected.as_str() {
            return body;
        }
        if Instant::now() >= deadline {
            panic!("Run {run_id} did not reach {expected:?}: {body}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn assert_api_status(
    client: &Client,
    base_url: &str,
    run_id: &str,
    expected: RunStatus,
    error_code: &str,
) {
    let body = get_run(client, base_url, run_id).await;
    assert_eq!(body["data"]["status"], expected.as_str(), "{body}");
    assert_eq!(body["data"]["error"]["code"], error_code, "{body}");
}

async fn get_run(client: &Client, base_url: &str, run_id: &str) -> Value {
    let url = format!("{base_url}/v1/runs/{run_id}");
    expect_json(client.get(url), StatusCode::OK).await
}

async fn expect_json(request: reqwest::RequestBuilder, expected: StatusCode) -> Value {
    let response = tokio::time::timeout(IO_TIMEOUT, request.send())
        .await
        .expect("HTTP request timed out")
        .expect("HTTP request failed");
    let status = response.status();
    let body = tokio::time::timeout(IO_TIMEOUT, response.text())
        .await
        .expect("HTTP response body timed out")
        .expect("HTTP response body failed");
    assert_eq!(status, expected, "unexpected response body: {body}");
    serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("HTTP response was not JSON: {error}; body: {body}"))
}

async fn get_record(repository: &SqliteRunRepository, run_id: &str) -> RunRecord {
    repository
        .get_run(run_id)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("Run {run_id} was not persisted"))
}

async fn events(repository: &SqliteRunRepository, run_id: &str) -> Vec<RunEvent> {
    repository.list_events_after(run_id, 0, 1024).await.unwrap()
}

fn terminal_events(events: &[RunEvent]) -> Vec<&RunEvent> {
    events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                RunEventType::RunCompleted
                    | RunEventType::RunFailed
                    | RunEventType::RunCancelled
                    | RunEventType::RunInterrupted
            )
        })
        .collect()
}

fn assert_single_terminal(events: &[RunEvent], expected: RunEventType) {
    let terminal = terminal_events(events);
    assert_eq!(terminal.len(), 1, "unexpected terminal events: {events:?}");
    assert_eq!(terminal[0].event_type, expected);
    assert_eq!(events.last().unwrap().event_type, expected);
}

fn assert_stop_terminal(
    record: &RunRecord,
    expected_attachment: RunAttachment,
    expected_status: RunStatus,
    expected_code: &str,
) {
    assert_eq!(record.attachment, expected_attachment);
    assert_eq!(record.status(), expected_status);
    assert!(record.started_at.is_some());
    assert!(record.ended_at.is_some());
    let code = match &record.lifecycle {
        RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => &error.code,
        lifecycle => panic!("expected stop terminal, got {lifecycle:?}"),
    };
    assert_eq!(code, expected_code);
}

async fn run_count(path: &Path) -> i64 {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM runs")
        .fetch_one(&pool)
        .await
        .unwrap();
    count.0
}

fn format_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
