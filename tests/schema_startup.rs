//! Real-process startup gates for a pre-provisioned durable SQLite Schema.

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use insight_storage::DATABASE_SCHEMA_NOT_INITIALIZED;
use tempfile::TempDir;

const STARTUP_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[test]
fn binary_rejects_missing_sqlite_before_http_bind_without_creating_the_file() {
    let temporary = TempDir::new().unwrap();
    let database = temporary.path().join("missing.sqlite3");
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_sqlite_platform_config(temporary.path(), bind_addr, &database);

    let output = run_failing_binary(&platform_config);

    assert_startup_schema_error(&output, DATABASE_SCHEMA_NOT_INITIALIZED);
    assert!(
        !database.exists(),
        "service startup must not create a missing SQLite database"
    );
    assert_http_address_is_available(bind_addr);
}

#[test]
fn binary_rejects_uninitialized_sqlite_before_http_bind() {
    let temporary = TempDir::new().unwrap();
    let database = temporary.path().join("uninitialized.sqlite3");
    fs::File::create(&database).unwrap();
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_sqlite_platform_config(temporary.path(), bind_addr, &database);

    let output = run_failing_binary(&platform_config);

    assert_startup_schema_error(&output, DATABASE_SCHEMA_NOT_INITIALIZED);
    assert!(
        database.is_file(),
        "startup failure must leave the pre-existing SQLite file in place"
    );
    assert_http_address_is_available(bind_addr);
}

fn reserve_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn assert_http_address_is_available(bind_addr: SocketAddr) {
    let rebound = TcpListener::bind(bind_addr)
        .expect("configured HTTP address must be reusable after startup rejection");
    drop(rebound);
}

fn run_failing_binary(platform_config: &Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_insight-agent-platform"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("PLATFORM_CONFIG", platform_config)
        .env_remove("DASHSCOPE_API_KEY")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn insight-agent-platform binary");

    let deadline = Instant::now() + STARTUP_EXIT_TIMEOUT;
    loop {
        if child
            .try_wait()
            .expect("failed to poll insight-agent-platform binary")
            .is_some()
        {
            return child
                .wait_with_output()
                .expect("failed to collect insight-agent-platform output");
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("failed to kill binary after startup rejection timeout");
            let output = child
                .wait_with_output()
                .expect("failed to collect timed-out binary output");
            panic!(
                "binary did not reject invalid SQLite startup within {STARTUP_EXIT_TIMEOUT:?}\n\
                 stdout:\n{}\n\
                 stderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        thread::sleep(STARTUP_POLL_INTERVAL);
    }
}

fn assert_startup_schema_error(output: &Output, expected_code: &str) {
    assert!(
        !output.status.success(),
        "invalid durable Schema must fail process startup"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains(expected_code) || stderr.contains(expected_code),
        "startup output must expose the stable Schema failure code {expected_code}\n\
         stdout:\n{stdout}\n\
         stderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("durable runtime listening")
            && !stderr.contains("durable runtime listening"),
        "invalid Schema must be rejected before the runtime reports HTTP readiness"
    );
}

fn write_sqlite_platform_config(root: &Path, bind_addr: SocketAddr, database: &Path) -> PathBuf {
    let platform_config = root.join("platform.yaml");
    let agents_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");

    fs::write(
        &platform_config,
        format!(
            r#"version: 1
deployment_mode: single_process_development
bind_addr: {bind_addr}

auth:
  mode: disabled

agents:
  directory: {}
  enabled:
    - action_demo

actions:
  enabled:
    - example.text_metrics

history:
  provider: sqlite
  path: {}

runtime:
  max_concurrent_runs: 1
  max_concurrent_operations: 1
  max_concurrent_operations_per_run: 1
  operation_timeout: 5s
  run_timeout: 10s
  sse_keep_alive_interval: 5s
  subscriber_capacity: 8
"#,
            agents_dir.display(),
            database.display(),
        ),
    )
    .unwrap();

    platform_config
}
