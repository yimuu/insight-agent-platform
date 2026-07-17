//! Real-process coverage for PostgreSQL exclusive-store ownership.

#![cfg(unix)]

use std::{
    fs,
    net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const DATABASE_URL_ENV: &str = "BINARY_OWNERSHIP_DATABASE_URL";
const ADVISORY_LOCK_NAMESPACE: i64 = 0x4941_5001;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusive_store_gate_rejects_contender_releases_cleanly_and_fences_loss() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresStore::create(&database_url).await;
    let temp = TempDir::new().unwrap();
    let mut model = BlockingModelServer::spawn(2).await;
    let files = BinaryFiles::new(temp.path(), model.address());
    let client = test_client();

    let owner_application = unique_name("binary_owner");
    let owner_database_url = store.child_url(&owner_application);
    let owner_addr = reserve_loopback_addr();
    let platform_config = files.write_platform(owner_addr);
    let owner_base_url = format!("http://{owner_addr}");
    let mut owner = ChildGuard::spawn(&platform_config, &owner_database_url);
    wait_for_ready(&client, &owner_base_url, &mut owner).await;

    let run_id = create_detached(&client, &owner_base_url).await;
    model.wait_for_accepted(1).await;
    wait_for_status(&client, &owner_base_url, &run_id, "running").await;
    let run_before = store.run_snapshot(&run_id).await;
    assert_eq!(run_before.status, "running");
    assert_eq!(run_before.terminal_events, 0);
    let ownership_before = store.ownership_row().await;
    assert_eq!(ownership_before.generation, 1);

    let contender_application = unique_name("binary_contender");
    let contender_database_url = store.child_url(&contender_application);
    let contender_addr = reserve_loopback_addr();
    let contender_config = files.write_platform(contender_addr);
    let contender = ChildGuard::spawn(&contender_config, &contender_database_url);
    let contender_output =
        contender.wait_for_rejection_without_binding(contender_addr, PROCESS_TIMEOUT);
    assert!(
        !contender_output.status.success(),
        "same-store contender unexpectedly exited zero\n{}",
        format_output(&contender_output)
    );
    assert!(
        output_text(&contender_output).contains("HISTORY_STORE_ALREADY_OWNED"),
        "contender omitted the stable ownership error code\n{}",
        format_output(&contender_output)
    );

    assert_eq!(store.run_snapshot(&run_id).await, run_before);
    assert_eq!(store.ownership_row().await, ownership_before);
    assert_output_is_sanitized(
        "contender",
        &contender_output,
        &store,
        &[&contender_database_url, &owner_database_url],
        &[ownership_before.owner_id.as_str()],
    )
    .await;

    owner.signal_term();
    model.wait_for_closed(1).await;
    let owner_output = owner.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        owner_output.status.success(),
        "clean owner drain must exit zero\n{}",
        format_output(&owner_output)
    );
    let clean_terminal = store.run_snapshot(&run_id).await;
    assert_eq!(clean_terminal.status, "interrupted");
    assert_eq!(
        clean_terminal.error_code.as_deref(),
        Some("RUN_INTERRUPTED")
    );
    assert_eq!(clean_terminal.terminal_events, 1);

    let replacement_application = unique_name("binary_replacement");
    let replacement_database_url = store.child_url(&replacement_application);
    let replacement_addr = reserve_loopback_addr();
    let replacement_config = files.write_platform(replacement_addr);
    let replacement_base_url = format!("http://{replacement_addr}");
    let mut replacement = ChildGuard::spawn(&replacement_config, &replacement_database_url);
    wait_for_ready(&client, &replacement_base_url, &mut replacement).await;
    assert_api_status(
        &client,
        &replacement_base_url,
        &run_id,
        "interrupted",
        "RUN_INTERRUPTED",
    )
    .await;
    let ownership_after = store.ownership_row().await;
    assert_eq!(ownership_after.generation, ownership_before.generation + 1);
    assert_ne!(ownership_after.owner_id, ownership_before.owner_id);

    let abandoned_run_id = create_detached(&client, &replacement_base_url).await;
    model.wait_for_accepted(1).await;
    wait_for_status(&client, &replacement_base_url, &abandoned_run_id, "running").await;
    let running_before_loss = store.run_snapshot(&abandoned_run_id).await;
    assert_eq!(running_before_loss.status, "running");
    assert_eq!(running_before_loss.terminal_events, 0);

    let ownership_backend = store
        .wait_for_advisory_backend(&replacement_application)
        .await;
    store.terminate_backend(ownership_backend).await;

    let recovery_application = unique_name("binary_loss_recovery");
    let recovery_database_url = store.child_url(&recovery_application);
    let recovery_addr = reserve_loopback_addr();
    let recovery_config = files.write_platform(recovery_addr);
    let recovery_base_url = format!("http://{recovery_addr}");
    let mut recovery = ChildGuard::spawn(&recovery_config, &recovery_database_url);
    wait_for_ready(&client, &recovery_base_url, &mut recovery).await;
    assert_api_status(
        &client,
        &recovery_base_url,
        &abandoned_run_id,
        "interrupted",
        "RUN_INTERRUPTED",
    )
    .await;

    model.wait_for_closed(1).await;
    let replacement_output = replacement.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        !replacement_output.status.success(),
        "process that lost PostgreSQL ownership unexpectedly exited zero\n{}",
        format_output(&replacement_output)
    );
    assert!(
        output_text(&replacement_output).contains("HISTORY_OWNERSHIP_LOST"),
        "ownership-loss process omitted the stable error code\n{}",
        format_output(&replacement_output)
    );
    let recovered_terminal = store.run_snapshot(&abandoned_run_id).await;
    assert_eq!(recovered_terminal.status, "interrupted");
    assert_eq!(
        recovered_terminal.error_code.as_deref(),
        Some("RUN_INTERRUPTED")
    );
    assert_eq!(recovered_terminal.terminal_events, 1);
    assert_ne!(recovered_terminal, running_before_loss);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store.run_snapshot(&abandoned_run_id).await,
        recovered_terminal,
        "the fenced old runtime overwrote replacement reconciliation"
    );

    let ownership_recovered = store.ownership_row().await;
    assert_eq!(
        ownership_recovered.generation,
        ownership_after.generation + 1
    );
    assert_ne!(ownership_recovered.owner_id, ownership_after.owner_id);

    recovery.signal_term();
    let recovery_output = recovery.wait_for_exit(PROCESS_TIMEOUT);
    assert!(
        recovery_output.status.success(),
        "post-loss recovery owner must drain cleanly\n{}",
        format_output(&recovery_output)
    );
    assert_eq!(store.run_snapshot(&run_id).await, clean_terminal);
    assert_eq!(
        store.run_snapshot(&abandoned_run_id).await,
        recovered_terminal
    );

    let ownership_ids = [
        ownership_before.owner_id.as_str(),
        ownership_after.owner_id.as_str(),
        ownership_recovered.owner_id.as_str(),
    ];
    let database_urls = [
        owner_database_url.as_str(),
        contender_database_url.as_str(),
        replacement_database_url.as_str(),
        recovery_database_url.as_str(),
    ];
    assert_output_is_sanitized(
        "owner",
        &owner_output,
        &store,
        &database_urls,
        &ownership_ids,
    )
    .await;
    assert_output_is_sanitized(
        "ownership-loss process",
        &replacement_output,
        &store,
        &database_urls,
        &ownership_ids,
    )
    .await;
    assert_output_is_sanitized(
        "post-loss recovery",
        &recovery_output,
        &store,
        &database_urls,
        &ownership_ids,
    )
    .await;

    store.cleanup().await;
}

fn postgres_database_url() -> Option<String> {
    let database_url = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if std::env::var_os("CI").is_some() && database_url.is_none() {
        panic!("RUN_HISTORY_POSTGRES_URL is required in CI");
    }
    if database_url.is_none() {
        eprintln!("skipping PostgreSQL binary ownership test: RUN_HISTORY_POSTGRES_URL is not set");
    }
    database_url
}

struct PostgresStore {
    admin: PgPool,
    observer: PgPool,
    role: String,
    credential_sentinel: String,
    schema: String,
    role_database_url: String,
}

impl PostgresStore {
    async fn create(admin_database_url: &str) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let role = format!("binary_owner_{suffix}");
        let credential_sentinel = format!("credential_{suffix}_secret");
        let schema = format!("formal_v2_binary_{suffix}");
        let admin = PgPoolOptions::new()
            .max_connections(4)
            .connect(admin_database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{credential_sentinel}'"
        )))
        .execute(&admin)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {schema} AUTHORIZATION {role}"
        )))
        .execute(&admin)
        .await
        .unwrap();

        let observer_url = append_query(
            admin_database_url,
            &format!("options=-csearch_path%3D{schema}&application_name=binary_ownership_observer"),
        );
        let observer = PgPoolOptions::new()
            .max_connections(2)
            .connect(&observer_url)
            .await
            .unwrap();
        let role_database_url =
            replace_credentials(admin_database_url, &role, &credential_sentinel);
        Self {
            admin,
            observer,
            role,
            credential_sentinel,
            schema,
            role_database_url,
        }
    }

    fn child_url(&self, application_name: &str) -> String {
        append_query(
            &self.role_database_url,
            &format!(
                "options=-csearch_path%3D{}&application_name={application_name}",
                self.schema
            ),
        )
    }

    async fn ownership_row(&self) -> OwnershipRow {
        let (generation, owner_id, claimed_at): (i64, Option<String>, Option<DateTime<Utc>>) =
            sqlx::query_as(
                "SELECT generation, owner_id, claimed_at
             FROM runtime_ownership
             WHERE singleton = 1",
            )
            .fetch_one(&self.observer)
            .await
            .unwrap();
        OwnershipRow {
            generation,
            owner_id: owner_id.expect("claimed ownership row omitted owner_id"),
            claimed_at: claimed_at.expect("claimed ownership row omitted claimed_at"),
        }
    }

    async fn run_snapshot(&self, run_id: &str) -> DurableRunSnapshot {
        let (status, started_at, ended_at, updated_at, error_code) = sqlx::query_as(
            "SELECT status, started_at, ended_at, updated_at, error_code
             FROM runs
             WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_one(&self.observer)
        .await
        .unwrap();
        let (event_count, terminal_events) = sqlx::query_as(
            "SELECT
                 COUNT(*),
                 COUNT(*) FILTER (
                     WHERE event_type IN (
                         'run.completed', 'run.failed', 'run.cancelled', 'run.interrupted'
                     )
                 )
             FROM run_events
             WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_one(&self.observer)
        .await
        .unwrap();
        DurableRunSnapshot {
            status,
            started_at,
            ended_at,
            updated_at,
            error_code,
            event_count,
            terminal_events,
        }
    }

    async fn advisory_lock_key(&self) -> i64 {
        let schema_oid: i64 = sqlx::query_scalar(
            "SELECT oid::bigint FROM pg_namespace WHERE nspname = current_schema()",
        )
        .fetch_one(&self.observer)
        .await
        .unwrap();
        (ADVISORY_LOCK_NAMESPACE << 32) | schema_oid
    }

    async fn wait_for_advisory_backend(&self, application_name: &str) -> i32 {
        let deadline = Instant::now() + RUN_TIMEOUT;
        loop {
            let pid: Option<i32> = sqlx::query_scalar(
                "SELECT DISTINCT activity.pid
                 FROM pg_stat_activity activity
                 JOIN pg_locks lock ON lock.pid = activity.pid
                 WHERE activity.application_name = $1
                   AND activity.usename = $2
                   AND lock.locktype = 'advisory'
                   AND lock.granted",
            )
            .bind(application_name)
            .bind(&self.role)
            .fetch_optional(&self.admin)
            .await
            .unwrap();
            if let Some(pid) = pid {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "ownership advisory backend was not observed for {application_name}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn terminate_backend(&self, pid: i32) {
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&self.admin)
            .await
            .unwrap();
        assert!(terminated, "ownership backend was not terminated");

        let deadline = Instant::now() + RUN_TIMEOUT;
        loop {
            let still_connected: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                    .bind(pid)
                    .fetch_one(&self.admin)
                    .await
                    .unwrap();
            if !still_connected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "terminated ownership backend remained connected"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn cleanup(self) {
        self.observer.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!("DROP ROLE {}", self.role)))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnershipRow {
    generation: i64,
    owner_id: String,
    claimed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DurableRunSnapshot {
    status: String,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    error_code: Option<String>,
    event_count: i64,
    terminal_events: i64,
}

struct BinaryFiles {
    platform_config: PathBuf,
    models_config: PathBuf,
    agents_dir: PathBuf,
}

impl BinaryFiles {
    fn new(root: &Path, model_addr: SocketAddr) -> Self {
        let agents_dir = root.join("agents");
        let agent_dir = agents_dir.join("ownership_blocker");
        fs::create_dir_all(&agent_dir).unwrap();
        fs::write(
            agent_dir.join("agent.yaml"),
            r#"api_version: insight.agent/v2
kind: agent
metadata:
  id: ownership_blocker
  name: Ownership Blocker
  description: Keeps a real model stream in flight for PostgreSQL ownership tests.
inputs: {}
output: string
workflow:
  steps:
    - type: llm
      id: block
      model: blocking_chat
      messages:
        - role: user
          content:
            - text: wait until this process is stopped
      parameters: {}
      response: string
  result:
    return: $block
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
    model: ownership-blocking-model
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
        }
    }

    fn write_platform(&self, bind_addr: SocketAddr) -> PathBuf {
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
    - ownership_blocker

models:
  config: {}

actions:
  enabled: []

history:
  provider: postgres
  database_url_env: {DATABASE_URL_ENV}

runtime:
  max_concurrent_runs: 4
  max_concurrent_operations: 4
  max_concurrent_operations_per_run: 32
  operation_timeout: 2m
  operation_cancel_grace_period: 1s
  max_template_output_bytes: 1048576
  run_timeout: 2m
  sse_keep_alive_interval: 100ms
  subscriber_capacity: 32
  journal_capacity: 128
  journal_batch_size: 16
  journal_operation_timeout: 1s
  readiness_probe_timeout: 500ms
  shutdown_grace_period: 3s
  shutdown_hard_deadline: 6s
"#,
                self.agents_dir.display(),
                self.models_config.display(),
            ),
        )
        .unwrap();
        self.platform_config.clone()
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
        assert!(read > 0, "model request closed before headers completed");
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
        assert!(read > 0, "model request closed before body completed");
        request.extend_from_slice(&chunk[..read]);
        assert!(request.len() <= MAX_REQUEST_BYTES);
    }
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(platform_config: &Path, database_url: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_insight-agent-platform"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PLATFORM_CONFIG", platform_config)
            .env(DATABASE_URL_ENV, database_url)
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
                let output = self.kill_and_collect_inner();
                panic!(
                    "platform did not exit within {timeout:?}\n{}",
                    format_output(&output)
                );
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn wait_for_rejection_without_binding(
        mut self,
        bind_addr: SocketAddr,
        timeout: Duration,
    ) -> Output {
        let deadline = Instant::now() + timeout;
        let mut accepted_connection = false;
        loop {
            if StdTcpStream::connect_timeout(&bind_addr, Duration::from_millis(20)).is_ok() {
                accepted_connection = true;
            }
            if self.try_wait().is_some() {
                let output = self.collect_exited();
                assert!(
                    !accepted_connection,
                    "same-store contender bound {bind_addr} before failing\n{}",
                    format_output(&output)
                );
                return output;
            }
            if Instant::now() >= deadline {
                let output = self.kill_and_collect_inner();
                panic!(
                    "same-store contender did not fail within {timeout:?}\n{}",
                    format_output(&output)
                );
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn collect_if_exited(&mut self) -> Option<Output> {
        self.try_wait().map(|_| self.collect_exited())
    }

    fn kill_and_collect_inner(&mut self) -> Output {
        let mut child = self.child.take().expect("child already collected");
        child.kill().expect("failed to kill child process");
        child
            .wait_with_output()
            .expect("failed to collect killed child output")
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

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
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
                let body: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
                assert_eq!(body["code"], "OK");
                return;
            }
            Ok(Ok(response)) => last_error = format!("unexpected status {}", response.status()),
            Ok(Err(error)) => last_error = error.to_string(),
            Err(_) => last_error = "readiness request timed out".to_string(),
        }
        assert!(
            Instant::now() < deadline,
            "platform did not become ready: {last_error}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn create_detached(client: &Client, base_url: &str) -> String {
    let body = expect_json(
        client
            .post(format!("{base_url}/v1/agents/ownership_blocker/runs"))
            .json(&json!({})),
        StatusCode::ACCEPTED,
    )
    .await;
    body["data"]["run_id"]
        .as_str()
        .expect("detached create response omitted run_id")
        .to_string()
}

async fn wait_for_status(client: &Client, base_url: &str, run_id: &str, expected: &str) -> Value {
    let deadline = Instant::now() + RUN_TIMEOUT;
    loop {
        let body = get_run(client, base_url, run_id).await;
        if body["data"]["status"] == expected {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "Run {run_id} did not reach {expected}: {body}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn assert_api_status(
    client: &Client,
    base_url: &str,
    run_id: &str,
    expected: &str,
    error_code: &str,
) {
    let body = get_run(client, base_url, run_id).await;
    assert_eq!(body["data"]["status"], expected, "{body}");
    assert_eq!(body["data"]["error"]["code"], error_code, "{body}");
}

async fn get_run(client: &Client, base_url: &str, run_id: &str) -> Value {
    expect_json(
        client.get(format!("{base_url}/v1/runs/{run_id}")),
        StatusCode::OK,
    )
    .await
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

async fn assert_output_is_sanitized(
    label: &str,
    output: &Output,
    store: &PostgresStore,
    database_urls: &[&str],
    owner_ids: &[&str],
) {
    let text = output_text(output);
    let lock_key = store.advisory_lock_key().await.to_string();
    let mut forbidden = vec![
        store.role_database_url.as_str(),
        store.credential_sentinel.as_str(),
        lock_key.as_str(),
    ];
    forbidden.extend(database_urls.iter().copied());
    forbidden.extend(owner_ids.iter().copied());
    for secret in forbidden {
        assert!(
            !text.contains(secret),
            "{label} output leaked protected PostgreSQL ownership material {secret:?}\n{}",
            format_output(output)
        );
    }
}

fn replace_credentials(database_url: &str, username: &str, password: &str) -> String {
    let scheme_end = database_url
        .find("://")
        .map(|index| index + 3)
        .expect("PostgreSQL test URL omitted its scheme");
    let rest = &database_url[scheme_end..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!(
        "{}{}:{}@{}{}",
        &database_url[..scheme_end],
        username,
        password,
        host,
        &rest[authority_end..]
    )
}

fn append_query(database_url: &str, query: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}{query}")
}

fn output_text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn format_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
