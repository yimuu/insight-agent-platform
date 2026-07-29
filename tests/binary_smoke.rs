//! Real binary smoke test for the durable workflow runtime.

#[path = "support/database.rs"]
mod database;

use std::{
    fs,
    io::{self, Read},
    net::{SocketAddr, TcpListener, TcpStream},
    panic::{resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use reqwest::{Client, StatusCode, Url};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use tempfile::TempDir;
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const RUN_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const BINARY_POSTGRES_URL_ENV: &str = "BINARY_SMOKE_POSTGRES_URL";
const BINARY_POSTGRES_ARTIFACT_NAMESPACE: &str = "binary-pg-restart";
static BINARY_STARTUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn binary_rejects_missing_sqlite_before_bind_without_creating_the_file() {
    let startup_guard = BINARY_STARTUP_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_temp_configs(temp.path(), bind_addr);
    let database_path = temp.path().join("history.sqlite3");
    assert!(!database_path.exists());

    let mut child = ChildGuard::spawn(&platform_config);
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().is_none() {
        assert!(
            Instant::now() < deadline,
            "binary did not fail after the required SQLite file was absent"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    let output = child.terminate_and_collect();
    drop(startup_guard);

    assert!(
        !output.status.success(),
        "binary must fail when Schema provisioning was skipped"
    );
    assert!(
        !database_path.exists(),
        "binary startup must not create the configured SQLite file"
    );
    assert!(
        TcpStream::connect_timeout(&bind_addr, Duration::from_millis(100)).is_err(),
        "business HTTP must not bind before the Schema contract gate"
    );
}

#[tokio::test]
async fn binary_starts_and_observes_success_and_workflow_failure_runs() {
    let startup_guard = BINARY_STARTUP_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_temp_configs(temp.path(), bind_addr);
    database::provision_sqlite_database(&temp.path().join("history.sqlite3")).await;
    let base_url = format!("http://{bind_addr}");
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let mut child = ChildGuard::spawn(&platform_config);
    wait_for_health(&client, &base_url, &mut child).await;
    drop(startup_guard);

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

    let human_tasks_url = format!("{base_url}/v1/human-tasks");
    let unauthorized_human_tasks = expect_json(
        format!("GET {human_tasks_url}"),
        client
            .get(&human_tasks_url)
            .bearer_auth("unconfigured-human-token"),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(unauthorized_human_tasks["code"], "UNAUTHORIZED");

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
    assert_eq!(agents[0]["id"], "action_demo");
    assert_eq!(agents[1]["id"], "workflow_failure_demo");
    assert_eq!(
        agents[0]["input_schema"],
        json!({
            "type": "object",
            "required": ["text"],
            "additionalProperties": false,
            "properties": {"text": {"type": "string"}},
            "$defs": {}
        })
    );
    let required_field = agents[0]["input_schema"]["required"]
        .as_array()
        .and_then(|required| required.first())
        .and_then(Value::as_str)
        .expect("action_demo discovery contract must identify its required input");
    let discovered_input = Value::Object(serde_json::Map::from_iter([(
        required_field.to_string(),
        Value::String("hello rust world".to_string()),
    )]));
    let agent_url = format!("{base_url}/v1/agents/action_demo");
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

    let completed = create_and_wait(&client, &base_url, "action_demo", discovered_input).await;
    assert_eq!(completed["data"]["agent_id"], "action_demo");
    assert_eq!(completed["data"]["status"], "completed");
    assert_eq!(
        completed["data"]["output"],
        json!({"data":{"characters":16,"words":3,"lines":1}})
    );

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

#[tokio::test]
async fn stock_binary_wires_two_environment_backed_human_principals() {
    let startup_guard = BINARY_STARTUP_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let bind_addr = reserve_loopback_addr();
    let platform_config = write_human_auth_configs(temp.path(), bind_addr);
    database::provision_sqlite_database(&temp.path().join("history.sqlite3")).await;
    let base_url = format!("http://{bind_addr}");
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let admin_token = "binary-admin-secret";
    let alice_token = "binary-alice-secret";
    let bob_token = "binary-bob-secret";

    let mut child = ChildGuard::spawn_with_env(
        &platform_config,
        &[
            ("BINARY_ADMIN_TOKEN", admin_token),
            ("BINARY_ALICE_TOKEN", alice_token),
            ("BINARY_BOB_TOKEN", bob_token),
        ],
    );
    wait_for_health(&client, &base_url, &mut child).await;
    drop(startup_guard);

    let human_tasks_url = format!("{base_url}/v1/human-tasks");
    for token in [alice_token, bob_token] {
        let tasks = expect_json(
            format!("GET {human_tasks_url}"),
            client.get(&human_tasks_url).bearer_auth(token),
            StatusCode::OK,
        )
        .await;
        assert_eq!(tasks["data"], json!([]));
    }
    let admin_human_tasks = expect_json(
        format!("GET {human_tasks_url}"),
        client.get(&human_tasks_url).bearer_auth(admin_token),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(admin_human_tasks["code"], "UNAUTHORIZED");

    let agents_url = format!("{base_url}/v1/agents");
    expect_json(
        format!("GET {agents_url}"),
        client.get(&agents_url).bearer_auth(alice_token),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    expect_json(
        format!("GET {agents_url}"),
        client.get(&agents_url).bearer_auth(admin_token),
        StatusCode::OK,
    )
    .await;

    let output = child.shutdown();
    assert!(
        shutdown_was_graceful(&output),
        "platform should exit cleanly after shutdown request\n{}",
        format_output(&output)
    );
    let rendered_output = format_output(&output);
    for secret in [admin_token, alice_token, bob_token] {
        assert!(!rendered_output.contains(secret));
    }
}

#[tokio::test]
async fn ordinary_process_restart_keeps_a_nonterminal_run_recoverable() {
    let first_startup_guard = BINARY_STARTUP_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let first_addr = reserve_loopback_addr();
    let platform_config = write_restart_configs(temp.path(), first_addr);
    database::provision_sqlite_database(&temp.path().join("restart.sqlite3")).await;
    let first_base = format!("http://{first_addr}");
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let mut first = ChildGuard::spawn(&platform_config);
    wait_for_health(&client, &first_base, &mut first).await;
    drop(first_startup_guard);
    let created = expect_json(
        format!("POST {first_base}/v1/agents/restart_waiter/runs"),
        client
            .post(format!("{first_base}/v1/agents/restart_waiter/runs"))
            .header("x-request-id", "restart-request-1")
            .json(&json!({})),
        StatusCode::ACCEPTED,
    )
    .await;
    assert!(matches!(
        created["data"]["status"].as_str(),
        Some("created" | "running")
    ));
    let run_id = created["data"]["run_id"].as_str().unwrap().to_owned();
    let first_output = first.shutdown();
    assert!(
        shutdown_was_graceful(&first_output),
        "first process should stop cleanly\n{}",
        format_output(&first_output)
    );

    let second_startup_guard = BINARY_STARTUP_LOCK.lock().await;
    let second_addr = reserve_loopback_addr();
    rewrite_bind_addr(&platform_config, first_addr, second_addr);
    let second_base = format!("http://{second_addr}");
    let mut second = ChildGuard::spawn(&platform_config);
    wait_for_health(&client, &second_base, &mut second).await;
    drop(second_startup_guard);
    let recovered = expect_json(
        format!("GET {second_base}/v1/runs/{run_id}"),
        client.get(format!("{second_base}/v1/runs/{run_id}")),
        StatusCode::OK,
    )
    .await;
    assert_eq!(recovered["data"]["status"], "running");
    assert_eq!(recovered["data"]["request_id"], "restart-request-1");
    assert_eq!(recovered["data"]["attachment"], "detached");
    assert!(recovered["data"]["agent_version"]
        .as_str()
        .is_some_and(|value| value.starts_with("deployrev_")));

    let second_output = second.shutdown();
    assert!(
        shutdown_was_graceful(&second_output),
        "second process should stop cleanly\n{}",
        format_output(&second_output)
    );
}

#[derive(Debug, PartialEq, Eq)]
struct PostgresStartupAuthoritySnapshot {
    schema_contract: (String, String, DateTime<Utc>),
    tables: Vec<String>,
    indexes: Vec<(String, String)>,
    triggers: Vec<(String, String)>,
}

#[tokio::test]
async fn stock_production_binary_runs_and_restarts_with_a_no_ddl_postgres_role() {
    let Some(database_url) = postgres_binary_test_url() else {
        return;
    };
    let schema = format!("binary_restart_{}", Uuid::new_v4().simple());
    let runtime_role = format!("binary_runtime_{}", Uuid::new_v4().simple());
    let runtime_password = format!("binary_password_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap_or_else(|_| panic!("TEST_POSTGRES_URL must be reachable"));

    let outcome = AssertUnwindSafe(async {
        let server_version =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::INT")
                .fetch_one(&admin)
                .await
                .expect("binary PostgreSQL gate must read the server version");
        assert_eq!(
            server_version / 10_000,
            16,
            "the no-DDL production binary gate must run against PostgreSQL 16"
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .expect("binary PostgreSQL gate must create its isolated schema");
        let scoped_admin_url = postgres_scoped_url(&database_url, &schema);
        let control = PgPoolOptions::new()
            .max_connections(2)
            .connect(&scoped_admin_url)
            .await
            .expect("isolated binary PostgreSQL schema must be reachable");
        database::provision_postgres_schema(&control).await;
        install_postgres_runtime_role(&admin, &schema, &runtime_role, &runtime_password).await;
        let runtime_url = postgres_role_url(&scoped_admin_url, &runtime_role, &runtime_password);
        verify_postgres_runtime_role(&admin, &runtime_url, &runtime_role).await;
        let provisioned_schema = postgres_startup_authority(&control).await;

        let first_startup_guard = BINARY_STARTUP_LOCK.lock().await;
        let temp = TempDir::new().unwrap();
        let first_addr = reserve_loopback_addr();
        let platform_config = write_postgres_action_configs(temp.path(), first_addr);
        let first_base = format!("http://{first_addr}");
        let client = Client::builder().timeout(RUN_TIMEOUT).build().unwrap();
        let process_environment = [(BINARY_POSTGRES_URL_ENV, runtime_url.as_str())];

        let mut first = ChildGuard::spawn_with_env(&platform_config, &process_environment);
        wait_for_health(&client, &first_base, &mut first).await;
        drop(first_startup_guard);
        let attached = create_attached_action_and_read_response(&client, &first_base).await;
        let run_id = attached.run_id;
        let completed = expect_json(
            format!("GET {first_base}/v1/runs/{run_id}"),
            client.get(format!("{first_base}/v1/runs/{run_id}")),
            StatusCode::OK,
        )
        .await;
        assert_eq!(completed["data"]["status"], "completed");
        assert_eq!(
            completed["data"]["output"],
            json!({"data":{"characters":16,"words":3,"lines":1}})
        );
        assert_completed_action_trace(&client, &first_base, &run_id).await;

        let first_output = first.shutdown();
        assert!(
            shutdown_was_graceful(&first_output),
            "first production PostgreSQL process should stop cleanly\n{}",
            format_output(&first_output)
        );
        let after_first_start = postgres_startup_authority(&control).await;
        assert_eq!(
            after_first_start, provisioned_schema,
            "first startup must not rewrite the contract or table/index/trigger inventory"
        );
        let first_artifact_authority = postgres_artifact_store_authority(&control).await;

        let second_startup_guard = BINARY_STARTUP_LOCK.lock().await;
        let second_addr = reserve_loopback_addr();
        rewrite_bind_addr(&platform_config, first_addr, second_addr);
        let second_base = format!("http://{second_addr}");
        let mut second = ChildGuard::spawn_with_env(&platform_config, &process_environment);
        wait_for_health(&client, &second_base, &mut second).await;
        drop(second_startup_guard);
        let recovered = expect_json(
            format!("GET {second_base}/v1/runs/{run_id}"),
            client.get(format!("{second_base}/v1/runs/{run_id}")),
            StatusCode::OK,
        )
        .await;
        assert_eq!(recovered["data"]["status"], "completed");
        assert_eq!(recovered["data"]["attachment"], "attached");
        assert_eq!(
            recovered["data"]["output"],
            json!({"data":{"characters":16,"words":3,"lines":1}})
        );
        assert_completed_action_trace(&client, &second_base, &run_id).await;
        let second_output = second.shutdown();
        assert!(
            shutdown_was_graceful(&second_output),
            "second production PostgreSQL process should stop cleanly\n{}",
            format_output(&second_output)
        );
        let after_restart = postgres_startup_authority(&control).await;
        assert_eq!(
            after_restart, provisioned_schema,
            "restart must not rewrite the contract or table/index/trigger inventory"
        );
        let second_artifact_authority = postgres_artifact_store_authority(&control).await;
        assert_eq!(
            second_artifact_authority, first_artifact_authority,
            "restart must preserve the durable Artifact-store authority row"
        );
        control.close().await;
    })
    .catch_unwind()
    .await;

    let schema_cleanup = sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await;
    let role_cleanup = sqlx::query(AssertSqlSafe(format!("DROP ROLE IF EXISTS {runtime_role}")))
        .execute(&admin)
        .await;
    admin.close().await;
    match outcome {
        Ok(()) => {
            schema_cleanup.expect("binary PostgreSQL gate must clean its isolated schema");
            role_cleanup.expect("binary PostgreSQL gate must clean its runtime role");
        }
        Err(payload) => {
            if let Err(error) = schema_cleanup {
                eprintln!("failed to clean binary PostgreSQL schema after panic: {error}");
            }
            if let Err(error) = role_cleanup {
                eprintln!("failed to clean binary PostgreSQL role after panic: {error}");
            }
            resume_unwind(payload);
        }
    };
}

fn postgres_binary_test_url() -> Option<String> {
    match std::env::var("TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(error) if std::env::var_os("CI").is_some() => {
            panic!("CI must set TEST_POSTGRES_URL for the stock production binary gate: {error}")
        }
        Err(_) => None,
    }
}

#[derive(Debug)]
struct AttachedActionEvidence {
    run_id: String,
}

async fn create_attached_action_and_read_response(
    client: &Client,
    base_url: &str,
) -> AttachedActionEvidence {
    let stream_url = format!("{base_url}/v1/agents/action_demo/runs/stream");
    let response = client
        .post(&stream_url)
        .header("x-request-id", "postgres-no-ddl-action-1")
        .json(&json!({"text":"hello rust world"}))
        .send()
        .await
        .unwrap_or_else(|error| panic!("POST {stream_url}: {error}"));
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "attached action run must open its response stream"
    );
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")),
        "attached action run must return an SSE response"
    );
    let run_id = response
        .headers()
        .get("x-run-id")
        .and_then(|value| value.to_str().ok())
        .expect("attached action response must expose x-run-id")
        .to_owned();
    assert!(!response.headers().contains_key("x-response-id"));
    let body = response
        .text()
        .await
        .unwrap_or_else(|error| panic!("failed to read {stream_url} to terminal EOF: {error}"));
    let events = decode_sse_json_events(&body);
    assert!(
        events.len() >= 3,
        "attached action response must expose created, in-progress, and terminal events: {body}"
    );
    assert_eq!(events[0]["type"], "run.lifecycle.created");
    assert_eq!(events[1]["type"], "run.lifecycle.running");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["type"], "run.lifecycle.completed");
    assert_eq!(terminal["run"]["id"], run_id);
    assert_eq!(terminal["run"]["status"], "completed");
    assert_eq!(
        terminal["run"]["result"],
        json!({"characters":16,"words":3,"lines":1})
    );
    let mut previous_sequence = None;
    for event in &events {
        let sequence = event["sequence_number"]
            .as_u64()
            .expect("every public response event must carry a sequence number");
        if let Some(previous) = previous_sequence {
            assert!(
                sequence > previous,
                "public response event sequence must be strictly increasing"
            );
        }
        previous_sequence = Some(sequence);
    }

    AttachedActionEvidence { run_id }
}

fn decode_sse_json_events(body: &str) -> Vec<Value> {
    let normalized = body.replace("\r\n", "\n");
    normalized
        .split("\n\n")
        .filter_map(|frame| {
            let mut event_name = None;
            let mut data = Vec::new();
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("event:") {
                    event_name = Some(value.trim());
                } else if let Some(value) = line.strip_prefix("data:") {
                    data.push(value.trim_start());
                }
            }
            let event_name = event_name?;
            assert!(
                !data.is_empty(),
                "SSE event {event_name} must carry JSON data"
            );
            let encoded = data.join("\n");
            let event: Value = serde_json::from_str(&encoded)
                .unwrap_or_else(|error| panic!("SSE event {event_name} is not JSON: {error}"));
            assert_eq!(
                event["type"], event_name,
                "SSE event name and closed JSON event type must agree"
            );
            Some(event)
        })
        .collect()
}

async fn assert_completed_action_trace(client: &Client, base_url: &str, run_id: &str) {
    let graph_url = format!("{base_url}/v1/runs/{run_id}/execution-graph");
    let graph = expect_json(
        format!("GET {graph_url}"),
        client.get(graph_url),
        StatusCode::OK,
    )
    .await;
    assert!(
        graph["data"]["nodes"]
            .as_array()
            .is_some_and(|nodes| !nodes.is_empty()),
        "completed action must expose its scheduled execution graph"
    );

    let trace_url = format!("{base_url}/v1/runs/{run_id}/trace");
    let trace = expect_json(
        format!("GET {trace_url}"),
        client.get(trace_url),
        StatusCode::OK,
    )
    .await;
    assert_eq!(trace["data"]["run_id"], run_id);
    let activations = trace["data"]["activations"]
        .as_array()
        .expect("completed action trace must expose activation events");
    assert!(
        !activations.is_empty(),
        "completed action trace must prove scheduler activity"
    );
    assert!(
        activations
            .iter()
            .all(|activation| activation["state"] == "succeeded"),
        "every terminal action activation must be succeeded: {activations:?}"
    );
}

fn postgres_scoped_url(database_url: &str, schema: &str) -> String {
    assert_postgres_identifier(schema);
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}options=-csearch_path%3D{schema}")
}

fn postgres_role_url(scoped_url: &str, role: &str, password: &str) -> String {
    assert_postgres_identifier(role);
    let mut url = Url::parse(scoped_url).expect("TEST_POSTGRES_URL must be a PostgreSQL URL");
    url.set_username(role)
        .expect("generated PostgreSQL role must be URL-safe");
    url.set_password(Some(password))
        .expect("generated PostgreSQL password must be URL-safe");
    url.to_string()
}

fn assert_postgres_identifier(identifier: &str) {
    assert!(
        identifier
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_lowercase()
                || byte == b'_'
                || (index > 0 && byte.is_ascii_digit())),
        "generated PostgreSQL identifier is not safely formattable: {identifier}"
    );
}

async fn install_postgres_runtime_role(admin: &PgPool, schema: &str, role: &str, password: &str) {
    assert_postgres_identifier(schema);
    assert_postgres_identifier(role);
    assert!(
        password
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_' || byte.is_ascii_digit()),
        "generated PostgreSQL password is not safely formattable"
    );
    let statements = [
        format!(
            "CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB \
             NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
        ),
        format!("REVOKE ALL PRIVILEGES ON SCHEMA {schema} FROM {role}"),
        format!("GRANT USAGE ON SCHEMA {schema} TO {role}"),
        format!("GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {schema} TO {role}"),
        format!("GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {role}"),
        format!("REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {schema} FROM PUBLIC"),
        format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role}"),
        format!(
            "REVOKE ALL PRIVILEGES ON TABLE \
             {schema}.durable_schema_contract FROM {role}"
        ),
        format!("GRANT SELECT ON TABLE {schema}.durable_schema_contract TO {role}"),
    ];
    for statement in statements {
        sqlx::query(AssertSqlSafe(statement))
            .execute(admin)
            .await
            .expect("provisioner must install the restricted runtime role");
    }
}

async fn verify_postgres_runtime_role(admin: &PgPool, runtime_url: &str, role: &str) {
    let role_attributes = sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,
                rolreplication,rolbypassrls
         FROM pg_roles WHERE rolname=$1",
    )
    .bind(role)
    .fetch_one(admin)
    .await
    .expect("provisioner must expose the generated runtime role");
    assert_eq!(
        role_attributes,
        (false, false, false, false, true, false, false),
        "runtime role must be LOGIN-only and inherit no owner/DDL authority"
    );

    let runtime = PgPoolOptions::new()
        .max_connections(2)
        .connect(runtime_url)
        .await
        .expect("restricted runtime LOGIN role must connect");
    let contract = sqlx::query_as::<_, (String, String)>(
        "SELECT contract_id,backend FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(&runtime)
    .await
    .expect("runtime role must read the Schema contract");
    assert_eq!(contract.0, database::DURABLE_SCHEMA_CONTRACT_ID);
    assert_eq!(contract.1, "postgres");

    let privileges = sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT
           has_schema_privilege(current_user,current_schema(),'USAGE'),
           has_schema_privilege(current_user,current_schema(),'CREATE'),
           has_table_privilege(current_user,'workflow_runs','SELECT,INSERT,UPDATE,DELETE'),
           has_table_privilege(current_user,'durable_schema_contract','SELECT'),
           has_table_privilege(current_user,'durable_schema_contract','INSERT'),
           has_table_privilege(current_user,'durable_schema_contract','UPDATE'),
           has_table_privilege(current_user,'durable_schema_contract','DELETE'),
           has_table_privilege(current_user,'durable_schema_contract','TRUNCATE')",
    )
    .fetch_one(&runtime)
    .await
    .expect("runtime role privileges must be inspectable");
    assert_eq!(
        privileges,
        (true, false, true, true, false, false, false, false),
        "runtime role needs DML while the contract remains read-only and schema CREATE is denied"
    );
    let owned_objects = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM pg_class class
         JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
         WHERE namespace.nspname=current_schema()
           AND class.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)",
    )
    .fetch_one(&runtime)
    .await
    .expect("runtime role object ownership must be inspectable");
    assert_eq!(
        owned_objects, 0,
        "runtime role must own no relation, so ALTER and DROP remain unavailable"
    );
    let all_functions_executable = sqlx::query_scalar::<_, Option<bool>>(
        "SELECT bool_and(has_function_privilege(current_user,procedure.oid,'EXECUTE'))
         FROM pg_proc procedure
         JOIN pg_namespace namespace ON namespace.oid=procedure.pronamespace
         WHERE namespace.nspname=current_schema()",
    )
    .fetch_one(&runtime)
    .await
    .expect("runtime function privileges must be inspectable");
    assert_eq!(
        all_functions_executable,
        Some(true),
        "runtime role must execute the pre-provisioned trigger functions"
    );

    for (statement, authority) in [
        (
            "CREATE TABLE runtime_must_not_create(singleton INTEGER)",
            "CREATE",
        ),
        (
            "ALTER TABLE workflow_runs \
             ADD COLUMN runtime_must_not_alter INTEGER",
            "ALTER",
        ),
        ("DROP TABLE workflow_runs", "DROP"),
        (
            "UPDATE durable_schema_contract SET contract_id=contract_id WHERE singleton=1",
            "contract UPDATE",
        ),
    ] {
        assert_postgres_statement_denied(&runtime, statement, authority).await;
    }
    runtime.close().await;
}

async fn assert_postgres_statement_denied(pool: &PgPool, statement: &str, authority: &str) {
    let mut transaction = pool
        .begin()
        .await
        .expect("runtime permission probe must begin a transaction");
    let result = sqlx::query(AssertSqlSafe(statement.to_owned()))
        .execute(&mut *transaction)
        .await;
    transaction
        .rollback()
        .await
        .expect("runtime permission probe must roll back");
    assert!(
        result.is_err(),
        "runtime role unexpectedly acquired {authority} authority"
    );
}

async fn postgres_startup_authority(pool: &PgPool) -> PostgresStartupAuthoritySnapshot {
    let schema_contract = sqlx::query_as::<_, (String, String, DateTime<Utc>)>(
        "SELECT contract_id,backend,installed_at
         FROM durable_schema_contract WHERE singleton=1",
    )
    .fetch_one(pool)
    .await
    .expect("stock binary must preserve the pre-provisioned Schema contract");
    assert_eq!(schema_contract.0, database::DURABLE_SCHEMA_CONTRACT_ID);
    assert_eq!(schema_contract.1, "postgres");
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT class.relname
         FROM pg_class class
         JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
         WHERE namespace.nspname=current_schema()
           AND class.relkind IN ('r','p')
         ORDER BY class.relname",
    )
    .fetch_all(pool)
    .await
    .expect("stock binary Schema table inventory must be readable");
    let indexes = sqlx::query_as::<_, (String, String)>(
        "SELECT table_class.relname,index_class.relname
         FROM pg_index index_metadata
         JOIN pg_class table_class ON table_class.oid=index_metadata.indrelid
         JOIN pg_class index_class ON index_class.oid=index_metadata.indexrelid
         JOIN pg_namespace namespace ON namespace.oid=table_class.relnamespace
         WHERE namespace.nspname=current_schema()
         ORDER BY table_class.relname,index_class.relname",
    )
    .fetch_all(pool)
    .await
    .expect("stock binary Schema index inventory must be readable");
    let triggers = sqlx::query_as::<_, (String, String)>(
        "SELECT class.relname,trigger.tgname
         FROM pg_trigger trigger
         JOIN pg_class class ON class.oid=trigger.tgrelid
         JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
         WHERE namespace.nspname=current_schema()
           AND NOT trigger.tgisinternal
         ORDER BY class.relname,trigger.tgname",
    )
    .fetch_all(pool)
    .await
    .expect("stock binary Schema trigger inventory must be readable");

    PostgresStartupAuthoritySnapshot {
        schema_contract,
        tables,
        indexes,
        triggers,
    }
}

async fn postgres_artifact_store_authority(
    pool: &PgPool,
) -> (String, String, String, DateTime<Utc>) {
    let artifact_store = sqlx::query_as::<_, (String, String, String, DateTime<Utc>)>(
        "SELECT backend,namespace,store_id,bound_at
         FROM artifact_store_authority WHERE singleton=TRUE",
    )
    .fetch_one(pool)
    .await
    .expect("stock binary must bind one shared Artifact-store authority");
    assert_eq!(artifact_store.0, "shared_filesystem");
    assert_eq!(artifact_store.1, BINARY_POSTGRES_ARTIFACT_NAMESPACE);
    assert!(artifact_store.2.starts_with("artifact_store_"));
    artifact_store
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
deployment_mode: single_process_development
bind_addr: {bind_addr}

auth:
  mode: disabled

agents:
  directory: {}
  enabled:
    - action_demo
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
  max_concurrent_operations: 4
  max_concurrent_operations_per_run: 32
  operation_timeout: 30s
  run_timeout: 1m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 32
"#,
            agents_dir.display(),
            history_path.display(),
        ),
    )
    .unwrap();

    platform_config
}

fn write_human_auth_configs(root: &Path, bind_addr: SocketAddr) -> PathBuf {
    let platform_config = write_temp_configs(root, bind_addr);
    let source = fs::read_to_string(&platform_config).unwrap();
    fs::write(
        &platform_config,
        source.replacen(
            "auth:\n  mode: disabled",
            "auth:\n  mode: bearer_env\n  token_env: BINARY_ADMIN_TOKEN\n  human_task_credentials:\n    - identity: alice\n      groups: [medical, triage]\n      token_env: BINARY_ALICE_TOKEN\n    - identity: bob\n      groups: [legal]\n      token_env: BINARY_BOB_TOKEN",
            1,
        ),
    )
    .unwrap();
    platform_config
}

fn write_restart_configs(root: &Path, bind_addr: SocketAddr) -> PathBuf {
    let platform_config = root.join("restart-platform.yaml");
    let models_config = root.join("restart-models.yaml");
    let history_path = root.join("restart.sqlite3");
    let agents_dir = root.join("agents");
    let agent_dir = agents_dir.join("restart_waiter");
    fs::create_dir_all(&agent_dir).unwrap();
    fs::write(
        agent_dir.join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent

metadata:
  id: restart_waiter
  name: Restart waiter
  description: Waits durably across an ordinary process restart.

inputs: {}
output: string

workflow:
  steps:
    - id: gate
      wait:
        signal: continue
        response: string
    - return: $gate
"#,
    )
    .unwrap();
    fs::write(
        &models_config,
        r#"version: 1

models:
  unused_restart_model:
    type: open_ai_chat
    base_url: https://models.example.invalid/v1
    model: unused-restart-model
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
deployment_mode: single_process_development
bind_addr: {bind_addr}

auth:
  mode: disabled

agents:
  directory: {}
  enabled: [restart_waiter]

models:
  config: restart-models.yaml

actions:
  enabled: []

history:
  provider: sqlite
  path: {}

runtime:
  max_concurrent_runs: 4
  max_concurrent_operations: 4
  max_concurrent_operations_per_run: 4
  operation_timeout: 30s
  run_timeout: 1m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 32
  shutdown_grace_period: 2s
  shutdown_hard_deadline: 3s
"#,
            agents_dir.display(),
            history_path.display(),
        ),
    )
    .unwrap();
    platform_config
}

fn write_postgres_action_configs(root: &Path, bind_addr: SocketAddr) -> PathBuf {
    let platform_config = root.join("postgres-platform.yaml");
    let models_config = root.join("models.yaml");
    let agents_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");
    let artifact_root = root.join("shared-artifacts");
    fs::write(
        &models_config,
        r#"version: 1

models:
  unused_postgres_smoke_model:
    type: open_ai_chat
    base_url: https://models.example.invalid/v1
    model: unused-postgres-smoke-model
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
deployment_mode: production
bind_addr: {}

auth:
  mode: disabled

agents:
  directory: {}
  enabled: [action_demo]

models:
  config: models.yaml

actions:
  enabled: [example.text_metrics]

history:
  provider: postgres
  database_url_env: {}

artifacts:
  provider: shared_filesystem
  namespace: {}
  directory: {}
  inline_threshold_bytes: 65536
  orphan_retention: 24h
  reference_retention: 30d
  gc_interval: 1m
  deletion_claim_seconds: 60

runtime:
  max_concurrent_runs: 4
  max_concurrent_operations: 4
  max_concurrent_operations_per_run: 4
  operation_timeout: 30s
  run_timeout: 1m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 32
  shutdown_grace_period: 2s
  shutdown_hard_deadline: 3s
"#,
            bind_addr,
            agents_dir.display(),
            BINARY_POSTGRES_URL_ENV,
            BINARY_POSTGRES_ARTIFACT_NAMESPACE,
            artifact_root.display(),
        ),
    )
    .unwrap();
    platform_config
}

fn rewrite_bind_addr(path: &Path, from: SocketAddr, to: SocketAddr) {
    let source = fs::read_to_string(path).unwrap();
    fs::write(
        path,
        source.replacen(
            &format!("bind_addr: {from}"),
            &format!("bind_addr: {to}"),
            1,
        ),
    )
    .unwrap();
}

trait RecoverableChild {
    fn kill_for_drop(&mut self);
    fn wait_for_drop(&mut self);
}

struct CapturedChild {
    child: Child,
    stdout: Option<thread::JoinHandle<io::Result<Vec<u8>>>>,
    stderr: Option<thread::JoinHandle<io::Result<Vec<u8>>>>,
}

impl CapturedChild {
    fn new(mut child: Child) -> Self {
        let stdout = child.stdout.take().expect("child stdout must be piped");
        let stderr = child.stderr.take().expect("child stderr must be piped");
        Self {
            child,
            stdout: Some(spawn_output_reader(stdout)),
            stderr: Some(spawn_output_reader(stderr)),
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait()
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn collect(mut self) -> io::Result<Output> {
        let status = self.child.wait()?;
        let stdout = join_output_reader(self.stdout.take(), "stdout")?;
        let stderr = join_output_reader(self.stderr.take(), "stderr")?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }

    fn join_output_readers(&mut self) {
        for reader in [&mut self.stdout, &mut self.stderr] {
            if let Some(reader) = reader.take() {
                let _ = reader.join();
            }
        }
    }
}

fn spawn_output_reader<R>(mut reader: R) -> thread::JoinHandle<io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut output = Vec::new();
        reader.read_to_end(&mut output)?;
        Ok(output)
    })
}

fn join_output_reader(
    reader: Option<thread::JoinHandle<io::Result<Vec<u8>>>>,
    stream: &str,
) -> io::Result<Vec<u8>> {
    reader
        .expect("output reader already collected")
        .join()
        .map_err(|_| io::Error::other(format!("child {stream} reader panicked")))?
}

impl RecoverableChild for CapturedChild {
    fn kill_for_drop(&mut self) {
        let _ = self.kill();
    }

    fn wait_for_drop(&mut self) {
        let _ = self.wait();
        self.join_output_readers();
    }
}

struct ChildGuard<C: RecoverableChild = CapturedChild> {
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

impl ChildGuard<CapturedChild> {
    fn spawn(platform_config: &Path) -> Self {
        Self::spawn_with_env(platform_config, &[])
    }

    fn spawn_with_env(platform_config: &Path, environment: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_insight-agent-platform"));
        command
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PLATFORM_CONFIG", platform_config)
            .envs(environment.iter().copied())
            .env_remove("OPENAI_API_KEY")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command
            .spawn()
            .expect("failed to spawn insight-agent-platform binary");
        Self::new(CapturedChild::new(child))
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
        Some(child.collect().expect(collect_context))
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
fn request_graceful_shutdown(child: &mut CapturedChild) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("failed to invoke kill -TERM");
    assert!(status.success(), "kill -TERM failed with status {status}");
}

#[cfg(not(unix))]
fn request_graceful_shutdown(child: &mut CapturedChild) {
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
