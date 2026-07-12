# HTTP Stack Majors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the platform HTTP stack to Axum 0.8 and Reqwest 0.13 while preserving the Formal V1 API/SSE contract and the existing model/action transport security contract.

**Architecture:** Treat Phase 3 as one delivery wave with two independent review gates: server routing/SSE first, client/TLS second. The Axum task changes only dependency resolution and route definitions needed by Axum 0.8. The Reqwest task changes only dependency features and project-owned client builders so TLS backend selection stays explicit.

**Tech Stack:** Rust 1.94.1, Cargo, Axum 0.8.9, Reqwest 0.13.4, Tokio, Tower 0.5, Rustls via Reqwest 0.13, SQLx 0.9.0, project-owned Formal V1 API/SSE tests.

## Global Constraints

- Scope is exactly dependency-governance `R5`: Axum 0.8 and Reqwest 0.13.
- Do not reintroduce SSE recovery or a public event replay route.
- Do not change API response envelopes, event payloads, Run status semantics, repository schema, migrations, or history persistence.
- Do not change DSL syntax, node execution semantics, scheduler behavior, or resource configuration version.
- Do not add custom CA bundle configuration in this phase.
- Do not switch model HTTP policy to automatic private-network detection.
- Do not upgrade Tokio, Tower, SQLx, JSON Schema, CEL, YAML parser, SHA-2, thiserror, Handlebars, UUID, or other direct lines as part of this phase.
- Do not add cargo-audit or cargo-deny suppressions as a substitute for dependency remediation.
- Reqwest must keep `default-features = false`.
- Reqwest must not enable `default-tls`, `native-tls`, `native-tls-*`, `system-proxy`, `http2`, `http3`, compression, cookies, socks, hickory DNS, multipart, or blocking features.
- Model plaintext HTTP policy remains: default HTTPS-only, explicit `loopback`, explicit `trusted_private`, and no URL username/password.

---

## File Structure

- `Cargo.toml`: bump direct Axum/Reqwest dependency declarations and keep Reqwest features narrow.
- `Cargo.lock`: resolve Axum 0.8.9 and Reqwest 0.13.4 with their required transitive updates.
- `src/api/formal/routes.rs`: change internal Axum route capture syntax from `/:param` to `/{param}`.
- `src/resources/openai_chat.rs`: add explicit `.tls_backend_rustls()` to the OpenAI-compatible Reqwest client builder.
- `src/resources/builtin_actions.rs`: add explicit `.tls_backend_rustls()` to the restricted HTTP action Reqwest client builder.
- `tests/api.rs`: add a focused route smoke test for all Formal V1 path templates after Axum capture migration.
- `tests/formal_resources.rs`: extend HTTP request capture to assert the OpenAI client still does not follow redirects and still sends bearer auth only on the initial approved request.
- `docs/formal-v1-breaking-changes.md`: document the dependency-contract change and interface-change rationale.

## Task 1: Axum 0.8 Server Migration

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/api/formal/routes.rs`
- Modify: `tests/api.rs`
- Modify: `docs/formal-v1-breaking-changes.md`

**Interfaces:**
- Consumes: `build_router(state: FormalApiState) -> Router`, `FormalApiState`, existing `tests/api.rs::fixture`.
- Produces: Axum 0.8-compatible route definitions using `/{agent_id}` and `/{run_id}` with unchanged public URL paths and unchanged response contracts.

- [ ] **Step 1: Write the focused route smoke test**

Append this test to `tests/api.rs` after `agent_metadata_omits_runtime_internals_and_unknown_agents_are_hidden`:

```rust
#[tokio::test]
async fn formal_v1_path_captures_match_after_axum_upgrade() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;

    let agent = app
        .clone()
        .oneshot(request(Method::GET, "/v1/agents/fast", None))
        .await
        .unwrap();
    assert_eq!(agent.status(), StatusCode::OK);
    assert_eq!(json_body(agent).await["data"]["id"], "fast");

    let stream = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/fast/runs/stream",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    assert!(stream.headers().contains_key("x-run-id"));
    let _ = to_bytes(stream.into_body(), usize::MAX).await.unwrap();

    let detached = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/fast/runs",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    assert_eq!(detached.status(), StatusCode::ACCEPTED);
    let detached = json_body(detached).await;
    let run_id = detached["data"]["run_id"].as_str().unwrap();
    wait_for_status(&service, run_id, RunStatus::Completed).await;

    let lookup = app
        .clone()
        .oneshot(request(Method::GET, &format!("/v1/runs/{run_id}"), None))
        .await
        .unwrap();
    assert_eq!(lookup.status(), StatusCode::OK);
    assert_eq!(json_body(lookup).await["data"]["run_id"], run_id);

    let cancel = app
        .oneshot(request(Method::DELETE, &format!("/v1/runs/{run_id}"), None))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::OK);
    assert_eq!(json_body(cancel).await["data"]["run_id"], run_id);
}
```

- [ ] **Step 2: Run the route smoke test before changing Axum**

Run:

```bash
cargo test --test api formal_v1_path_captures_match_after_axum_upgrade -- --exact --nocapture
```

Expected: PASS on Axum 0.7. This test freezes public path behavior before changing internal route syntax.

- [ ] **Step 3: Upgrade the direct Axum dependency**

In `Cargo.toml`, change:

```toml
axum = "0.7"
```

to:

```toml
axum = "0.8"
```

Run:

```bash
cargo update -p axum --precise 0.8.9
```

Expected: `Cargo.lock` resolves `axum v0.8.9`.

- [ ] **Step 4: Convert route capture syntax**

In `src/api/formal/routes.rs`, replace the route definitions inside `build_router` with:

```rust
    let v1 = Router::new()
        .route("/v1/agents", get(list_agents))
        .route("/v1/agents/{agent_id}", get(get_agent))
        .route(
            "/v1/agents/{agent_id}/runs/stream",
            post(create_attached_run),
        )
        .route("/v1/agents/{agent_id}/runs", post(create_detached_run))
        .route("/v1/runs/{run_id}", get(get_run).delete(cancel_run))
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return Err(ApiError::unauthorized());
                    }
                    Ok::<Response, ApiError>(next.run(request).await)
                }
            },
        ));
```

Do not add `without_v07_checks`.

- [ ] **Step 5: Document the Axum interface-change reason**

Append this section near the dependency-governance notes in `docs/formal-v1-breaking-changes.md`:

```markdown

## Dependency governance: Axum 0.8 route syntax

- Phase 3 upgrades the server framework from Axum 0.7 to Axum 0.8.
- Public Formal V1 paths are unchanged, but internal route definitions now use Axum 0.8 `{param}` captures instead of Axum 0.7 `:param` captures.
- This is intentional: relying on old route syntax after the major upgrade would hide framework-compatibility behavior inside the route table and make future route reviews less clear.
- API response envelopes, SSE event payloads, auth behavior, cancellation behavior, and unsupported replay/recovery routes are unchanged.
```

- [ ] **Step 6: Run focused Axum verification**

Run:

```bash
cargo test --test api -- --nocapture --test-threads=1
cargo test --test action_error_containment -- --nocapture --test-threads=1
cargo check --bin insight-agent-platform
```

Expected:

- `tests/api.rs` passes, including live-only SSE and route capture behavior.
- `tests/action_error_containment.rs` passes, proving API + runtime error containment did not regress.
- `cargo check --bin insight-agent-platform` passes, proving `axum::serve(...).with_graceful_shutdown(...).into_future()` still compiles.

- [ ] **Step 7: Inspect dependency diff for Task 1 scope**

Run:

```bash
git diff -- Cargo.toml Cargo.lock src/api/formal/routes.rs tests/api.rs docs/formal-v1-breaking-changes.md
cargo tree -i axum --locked
cargo tree -i reqwest --locked
```

Expected:

- `cargo tree -i axum --locked` shows `axum v0.8.9`.
- `cargo tree -i reqwest --locked` still shows `reqwest v0.12.28`.
- No Reqwest 0.13 change is present in Task 1.

- [ ] **Step 8: Commit Task 1**

Run:

```bash
git add Cargo.toml Cargo.lock src/api/formal/routes.rs tests/api.rs docs/formal-v1-breaking-changes.md
git commit -m "chore: upgrade axum to 0.8"
```

## Task 2: Reqwest 0.13 Client and TLS Migration

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/resources/openai_chat.rs`
- Modify: `src/resources/builtin_actions.rs`
- Modify: `tests/formal_resources.rs`
- Modify: `docs/formal-v1-breaking-changes.md`

**Interfaces:**
- Consumes: `OpenAiChatModel::new_with_limits_and_transport_policy(...) -> Result<OpenAiChatModel, CompileError>` and `RestrictedHttpGetAction::new(...) -> Result<RestrictedHttpGetAction, CompileError>`.
- Produces: Reqwest 0.13 clients built with `default-features = false`, features `json`, `stream`, `rustls`, and explicit `tls_backend_rustls()` calls.

- [ ] **Step 1: Add a redirect non-following regression test for the OpenAI-compatible client**

Append this test to `tests/formal_resources.rs` after `openai_errors_and_debug_never_expose_api_key_or_response_body`:

```rust
#[tokio::test]
async fn openai_client_does_not_follow_redirects_or_leak_authorization_to_location() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let second_request_seen = Arc::new(Notify::new());
    let server_second_request_seen = Arc::clone(&second_request_seen);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        loop {
            let mut chunk = [0_u8; 2048];
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8_lossy(&buffer);
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer api-key-secret"));
        socket
            .write_all(
                b"HTTP/1.1 302 Found\r\nlocation: /redirected\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await
            .unwrap();

        match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
            Ok(Ok((_socket, _))) => server_second_request_seen.notify_one(),
            Ok(Err(error)) => panic!("unexpected accept error: {error}"),
            Err(_) => {}
        }
    });

    let model = loopback_model(format!("http://{address}/v1"), Some("api-key-secret".to_string()));
    let error = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), "UPSTREAM_STATUS");
    assert!(error.to_string().contains("302"));
    assert!(tokio::time::timeout(Duration::from_millis(10), second_request_seen.notified())
        .await
        .is_err());
    server.await.unwrap();
}
```

- [ ] **Step 2: Run the new redirect test before changing Reqwest**

Run:

```bash
cargo test --test formal_resources openai_client_does_not_follow_redirects_or_leak_authorization_to_location -- --exact --nocapture
```

Expected: PASS on Reqwest 0.12. This freezes redirect behavior before the major upgrade.

- [ ] **Step 3: Upgrade the direct Reqwest dependency**

In `Cargo.toml`, change:

```toml
reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"], default-features = false }
```

to:

```toml
reqwest = { version = "0.13", features = ["json", "stream", "rustls"], default-features = false }
```

Run:

```bash
cargo update -p reqwest --precise 0.13.4
```

Expected: `Cargo.lock` resolves `reqwest v0.13.4`.

- [ ] **Step 4: Select the rustls backend explicitly for the OpenAI-compatible client**

In `src/resources/openai_chat.rs`, replace the client builder block with:

```rust
        let client = Client::builder()
            .tls_backend_rustls()
            .redirect(Policy::none())
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| {
                CompileError::new("MODEL_CONFIG_INVALID", "failed to build OpenAI HTTP client")
            })?;
```

- [ ] **Step 5: Select the rustls backend explicitly for the restricted HTTP action client**

In `src/resources/builtin_actions.rs`, replace the client builder block with:

```rust
        let client = reqwest::Client::builder()
            .tls_backend_rustls()
            .redirect(Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|_| {
                CompileError::new(
                    "ACTION_CONFIG_INVALID",
                    "failed to build restricted HTTP client",
                )
            })?;
```

- [ ] **Step 6: Document the Reqwest interface-change reason**

Append this section near the dependency-governance notes in `docs/formal-v1-breaking-changes.md`:

```markdown

## Dependency governance: Reqwest 0.13 TLS contract

- Phase 3 upgrades the HTTP client from Reqwest 0.12 to Reqwest 0.13.
- The direct feature selection changes from `rustls-tls` to `rustls` because Reqwest 0.13 changed its TLS feature contract.
- Reqwest default features remain disabled. The platform does not opt into implicit `default-tls`, system proxy behavior, HTTP/2, compression, cookies, SOCKS, HTTP/3, or alternate DNS behavior in this phase.
- Project-built Reqwest clients explicitly call `tls_backend_rustls()` so model and action traffic do not depend on a backend-abstract default TLS alias.
- HTTPS verification uses Reqwest 0.13's rustls/platform verifier path and the runtime environment's trusted roots. Private HTTPS model services should install private CA roots at the host/container layer until a future explicit per-model CA configuration exists.
- Existing `transport.plaintext_http` semantics are unchanged: default HTTPS-only, `loopback` for exact local hosts, and `trusted_private` when the deployment owner accepts a private-network plaintext model hop.
```

- [ ] **Step 7: Run focused Reqwest verification**

Run:

```bash
cargo test --test model_resources_v1 -- --nocapture
cargo test --test formal_resources -- --nocapture --test-threads=1
cargo test --test core_chat_action -- --nocapture --test-threads=1
cargo test --test observability openai_provider_logs_response_metadata_without_body_or_key -- --exact --nocapture
```

Expected:

- model resource transport policy tests pass;
- OpenAI-compatible stream, limits, redirect, redaction, and drop tests pass;
- restricted action and core chat/action behavior passes;
- observability does not log model body or API key.

- [ ] **Step 8: Verify Reqwest dependency features and absence of native TLS packages**

Run:

```bash
cargo tree -i reqwest --locked
cargo tree -e features -i reqwest --locked
cargo tree -i native-tls --locked
cargo tree -i hyper-tls --locked
cargo tree -i tokio-native-tls --locked
```

Expected:

- `cargo tree -i reqwest --locked` shows `reqwest v0.13.4`.
- Feature output includes the project path to `reqwest` with `json`, `stream`, and `rustls`.
- Feature output does not show direct project activation of `default-tls`, `native-tls`, `system-proxy`, `http2`, `gzip`, `brotli`, `deflate`, `zstd`, `cookies`, `socks`, `hickory-dns`, `multipart`, or `blocking`.
- `cargo tree -i native-tls --locked`, `cargo tree -i hyper-tls --locked`, and `cargo tree -i tokio-native-tls --locked` fail with package-not-found style output.

- [ ] **Step 9: Inspect dependency diff for Task 2 scope**

Run:

```bash
git diff -- Cargo.toml Cargo.lock src/resources/openai_chat.rs src/resources/builtin_actions.rs tests/formal_resources.rs docs/formal-v1-breaking-changes.md
cargo tree -i axum --locked
cargo tree -i reqwest --locked
```

Expected:

- Axum remains `0.8.9` from Task 1.
- Reqwest is `0.13.4`.
- No unrelated direct dependency major upgrade is present.

- [ ] **Step 10: Commit Task 2**

Run:

```bash
git add Cargo.toml Cargo.lock src/resources/openai_chat.rs src/resources/builtin_actions.rs tests/formal_resources.rs docs/formal-v1-breaking-changes.md
git commit -m "chore: upgrade reqwest to 0.13"
```

## Task 3: Final Dependency and Whole-Project Verification

**Files:**
- Read: `Cargo.toml`
- Read: `Cargo.lock`
- Read: `docs/superpowers/specs/2026-07-12-http-stack-majors-design.md`
- Modify only if verification reveals a scoped issue: files changed by Task 1 or Task 2.

**Interfaces:**
- Consumes: committed Axum 0.8 and Reqwest 0.13 changes from Tasks 1 and 2.
- Produces: final evidence that Phase 3 meets the spec and did not hide unrelated dependency changes.

- [ ] **Step 1: Run the full standard verification set**

Run:

```bash
cargo fmt --check
cargo test --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
```

Expected: all commands exit 0. If `cargo audit` or `cargo deny check` reports a new dependency issue caused by Axum/Reqwest migration, fix it in the smallest scoped change or stop and report the blocker.

- [ ] **Step 2: Verify direct dependency versions**

Run:

```bash
cargo tree -i axum --locked
cargo tree -i reqwest --locked
cargo tree --locked --duplicates
```

Expected:

- `axum v0.8.9` is the only direct Axum version used by the project.
- `reqwest v0.13.4` is the only direct Reqwest version used by the project.
- Duplicate output may still include known ecosystem duplicates from the dependency-governance review; do not treat unrelated duplicate warnings as Phase 3 failures unless a new Axum/Reqwest-caused duplicate is introduced.

- [ ] **Step 3: Verify Reqwest negative dependency evidence**

Run:

```bash
cargo tree -i native-tls --locked
cargo tree -i hyper-tls --locked
cargo tree -i tokio-native-tls --locked
```

Expected: each command fails with package-not-found style output. Record the exact output in the final handoff.

- [ ] **Step 4: Review the final diff against the spec**

Run:

```bash
git diff --stat HEAD~2..HEAD
git diff --name-only HEAD~2..HEAD
```

Expected changed files are limited to:

- `Cargo.toml`
- `Cargo.lock`
- `src/api/formal/routes.rs`
- `src/resources/openai_chat.rs`
- `src/resources/builtin_actions.rs`
- `tests/api.rs`
- `tests/formal_resources.rs`
- `docs/formal-v1-breaking-changes.md`

If additional files changed, verify each one is directly required by the spec before keeping it.

- [ ] **Step 5: Commit any verification-only corrections**

If Task 3 required code or documentation fixes, run:

```bash
git add <changed-files>
git commit -m "fix: complete http stack major migration"
```

If Task 3 required no changes, do not create an empty commit.

- [ ] **Step 6: Final handoff checklist**

Verify and report:

```text
Axum: 0.8.9
Reqwest: 0.13.4
Reqwest features: json, stream, rustls, default-features=false
Native TLS packages: absent
Formal V1 public routes: unchanged
SSE recovery: still unsupported
Attached SSE disconnect: still cancels the run
Model HTTP policy: unchanged
Restricted http_get policy: unchanged
```

Do not claim completion until the commands in Task 3 have been run fresh and their output has been read.

## Self-Review

- Spec coverage: Task 1 covers Axum route syntax, public API contract, SSE behavior, startup compile, and documentation. Task 2 covers Reqwest feature selection, explicit rustls backend selection, transport policy preservation, redirect behavior, and documentation. Task 3 covers whole-project verification, dependency evidence, and final scope audit.
- Placeholder scan: no placeholders are intentionally left for implementers; each step lists concrete files, code snippets, commands, and expected output.
- Type consistency: route handlers continue to consume `Path<String>` and `State<Arc<FormalApiState>>`; Reqwest builders remain `reqwest::Client::builder()` / imported `Client::builder()` and return existing `CompileError` boundaries.
