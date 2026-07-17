# Provider Memory Bounds Implementation Plan

> **Historical implementation record:** provider response bounds remain relevant, but `core.chat`/generic-operation YAML below is superseded. Current LLM authoring and request-budget contracts are defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `BASE-P1-011` by bounding OpenAI-compatible provider response memory and accumulated chat output memory.

**Architecture:** Add a defaulted `OpenAiChatLimits` value to the OpenAI-compatible model boundary, parse optional per-model overrides from `models.yaml`, enforce upstream/body/SSE/chunk/usage limits in `OpenAiChatModel`, and enforce accumulated text bytes in `ChatNode`. All limit failures use one stable sanitized runtime error while keeping existing HTTP, event, Run, repository, dependency, and migration shapes unchanged.

**Tech Stack:** Rust, Tokio, futures streams, Reqwest byte streams, serde YAML, serde JSON, existing `ChatModel`/`ChatNode`/`RunService` test harnesses.

## Global Constraints

- Implement only A1 Provider memory bounds from `docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md`.
- Do not implement A2-A8, dependency upgrades, HTTPS-only transport, clean-EOF semantics, or observability/metrics.
- Existing `models.yaml` without `limits` must remain valid and use defaults.
- Configured limit values are positive byte counts; zero fails startup with `MODEL_CONFIG_INVALID`.
- All A1 limit violations return exactly `MODEL_RESPONSE_TOO_LARGE` / `chat provider response exceeded the configured size limit`.
- Limit errors must not include provider body text, chunk text, usage JSON, URL query values, API keys, request messages, response headers, or numeric configured limits.
- Bounds are inclusive: exact limit accepted, first byte above rejected.
- Do not change `ChatStream` item shape, `ChatNode` output JSON, HTTP/SSE/event/Run/repository shapes, migrations, Cargo manifests, lockfile, or dependency policy.
- Follow TDD: every production behavior change must have a failing test observed before implementation.

---

## Files and Responsibilities

- `src/resources/openai_chat.rs`: owns `OpenAiChatLimits`, OpenAI constructor with limits, upstream byte accounting, SSE line/payload/chunk/usage enforcement, and stable too-large errors.
- `src/resources/config.rs`: parses optional `open_ai_chat.limits` from model YAML, merges defaults, validates positive values, and passes limits into `OpenAiChatModel`.
- `src/resources/models.rs`: exposes the generic accumulated-text limit hook and shared stable too-large error helper for model consumers.
- `src/nodes/chat.rs`: enforces accumulated chat text bytes before appending and before content emission.
- `tests/model_resources_v1.rs`: verifies model YAML defaulting, overrides, strict fields, and zero-limit rejection.
- `tests/formal_resources.rs`: verifies OpenAI streaming limits, exact boundaries, sanitized errors, and upstream body closure.
- `tests/core_chat_action.rs`: verifies direct `ChatNode` accumulated text success/failure and no over-limit content emission.
- `tests/chat_memory_bounds.rs`: verifies a bounded chat failure releases service capacity and a later Run completes through production runtime paths.
- `README.md`: documents optional per-model limits and default behavior.

---

### Task 1: Add provider limit configuration and model exposure

**Files:**
- Modify: `src/resources/openai_chat.rs`
- Modify: `src/resources/config.rs`
- Modify: `src/resources/models.rs`
- Modify: `tests/model_resources_v1.rs`

**Interfaces:**
- Produces: `pub struct OpenAiChatLimits` with public `usize` fields and `Default`.
- Produces: `OpenAiChatLimits::validate(self) -> Result<Self, CompileError>`.
- Produces: `OpenAiChatModel::new_with_limits(..., limits: OpenAiChatLimits) -> Result<Self, CompileError>`.
- Preserves: `OpenAiChatModel::new(...) -> Result<Self, CompileError>` as the default-limit constructor.
- Produces: `ChatModel::max_accumulated_text_bytes(&self) -> usize` with a default implementation.
- Produces: shared too-large helper in `src/resources/models.rs`: `pub fn model_response_too_large() -> RunError`.
- Produces: shared default constant in `src/resources/models.rs`: `pub const DEFAULT_MAX_ACCUMULATED_TEXT_BYTES: usize = 1024 * 1024`.

- [ ] **Step 1: Write failing config/default/override tests**

In `tests/model_resources_v1.rs`, update imports:

```rust
use insight_agent_platform::resources::{
    config::load_model_registry_with_env,
    models::ModelCapability,
    openai_chat::OpenAiChatLimits,
};
```

Add this test after `strict_model_resources_resolve_alias_capability_and_redacted_secret`:

```rust
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
```

- [ ] **Step 2: Write failing zero-limit rejection test**

Add this test to `tests/model_resources_v1.rs`:

```rust
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
```

- [ ] **Step 3: Run RED for model resource tests**

Run:

```bash
cargo test --test model_resources_v1 -- --nocapture
```

Expected: FAIL to compile because `OpenAiChatLimits` and `ChatModel::max_accumulated_text_bytes` do not exist, or fail loading because `limits` is still an unknown field.

- [ ] **Step 4: Add `OpenAiChatLimits` and default constructor path**

In `src/resources/openai_chat.rs`, add below imports:

```rust
pub const DEFAULT_MAX_UPSTREAM_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_BUFFERED_LINE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_EVENT_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_CHUNK_TEXT_BYTES: usize = 256 * 1024;
pub const DEFAULT_MAX_USAGE_JSON_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiChatLimits {
    pub max_upstream_bytes: usize,
    pub max_buffered_line_bytes: usize,
    pub max_event_payload_bytes: usize,
    pub max_chunk_text_bytes: usize,
    pub max_usage_json_bytes: usize,
    pub max_accumulated_text_bytes: usize,
}

impl Default for OpenAiChatLimits {
    fn default() -> Self {
        Self {
            max_upstream_bytes: DEFAULT_MAX_UPSTREAM_BYTES,
            max_buffered_line_bytes: DEFAULT_MAX_BUFFERED_LINE_BYTES,
            max_event_payload_bytes: DEFAULT_MAX_EVENT_PAYLOAD_BYTES,
            max_chunk_text_bytes: DEFAULT_MAX_CHUNK_TEXT_BYTES,
            max_usage_json_bytes: DEFAULT_MAX_USAGE_JSON_BYTES,
            max_accumulated_text_bytes: DEFAULT_MAX_ACCUMULATED_TEXT_BYTES,
        }
    }
}

impl OpenAiChatLimits {
    pub fn validate(self) -> Result<Self, CompileError> {
        if [
            self.max_upstream_bytes,
            self.max_buffered_line_bytes,
            self.max_event_payload_bytes,
            self.max_chunk_text_bytes,
            self.max_usage_json_bytes,
            self.max_accumulated_text_bytes,
        ]
        .contains(&0)
        {
            return Err(CompileError::new(
                "MODEL_CONFIG_INVALID",
                "OpenAI response limits must be greater than zero",
            ));
        }
        Ok(self)
    }
}
```

Before this block, update the existing `super::models` import in `src/resources/openai_chat.rs` to include `DEFAULT_MAX_ACCUMULATED_TEXT_BYTES`. Do not define `DEFAULT_MAX_ACCUMULATED_TEXT_BYTES` in `src/resources/openai_chat.rs`; it is owned by `src/resources/models.rs` so the generic model trait does not depend on the concrete OpenAI provider.

Add `limits: OpenAiChatLimits` to `OpenAiChatModel`.

Replace the constructor body with a delegating default constructor and a new limit-aware constructor:

```rust
pub fn new(
    api_key: Option<String>,
    base_url: String,
    model: String,
    capabilities: BTreeSet<ModelCapability>,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<Self, CompileError> {
    Self::new_with_limits(
        api_key,
        base_url,
        model,
        capabilities,
        connect_timeout,
        request_timeout,
        OpenAiChatLimits::default(),
    )
}

pub fn new_with_limits(
    api_key: Option<String>,
    base_url: String,
    model: String,
    capabilities: BTreeSet<ModelCapability>,
    connect_timeout: Duration,
    request_timeout: Duration,
    limits: OpenAiChatLimits,
) -> Result<Self, CompileError> {
    if model.trim().is_empty() || connect_timeout.is_zero() || request_timeout.is_zero() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI model and timeouts must be non-empty",
        ));
    }
    let limits = limits.validate()?;
    let mut endpoint = Url::parse(&base_url)
        .map_err(|_| CompileError::new("MODEL_CONFIG_INVALID", "OpenAI base URL is invalid"))?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must use HTTP or HTTPS and include a host",
        ));
    }
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    let path = format!("{}/chat/completions", endpoint.path().trim_end_matches('/'));
    endpoint.set_path(&path);
    let client = Client::builder()
        .redirect(Policy::none())
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .map_err(|_| {
            CompileError::new("MODEL_CONFIG_INVALID", "failed to build OpenAI HTTP client")
        })?;
    let parameter_validator = JSONSchema::compile(&parameter_schema()).map_err(|_| {
        CompileError::new(
            "MODEL_CONFIG_INVALID",
            "failed to compile OpenAI parameter schema",
        )
    })?;
    Ok(Self {
        client,
        api_key,
        endpoint,
        model,
        capabilities,
        parameter_validator: std::sync::Arc::new(parameter_validator),
        limits,
    })
}
```

- [ ] **Step 5: Add generic model limit hook and stable error helper**

In `src/resources/models.rs`, add:

```rust
pub const DEFAULT_MAX_ACCUMULATED_TEXT_BYTES: usize = 1024 * 1024;
pub const MODEL_RESPONSE_TOO_LARGE_CODE: &str = "MODEL_RESPONSE_TOO_LARGE";
pub const MODEL_RESPONSE_TOO_LARGE_MESSAGE: &str =
    "chat provider response exceeded the configured size limit";

pub fn model_response_too_large() -> RunError {
    RunError::new(MODEL_RESPONSE_TOO_LARGE_CODE, MODEL_RESPONSE_TOO_LARGE_MESSAGE)
}
```

Extend the `ChatModel` trait:

```rust
fn max_accumulated_text_bytes(&self) -> usize {
    DEFAULT_MAX_ACCUMULATED_TEXT_BYTES
}
```

In `src/resources/openai_chat.rs`, import `DEFAULT_MAX_ACCUMULATED_TEXT_BYTES` from `models`, use it in `OpenAiChatLimits::default()`, and add this method to `impl ChatModel for OpenAiChatModel`:

```rust
fn max_accumulated_text_bytes(&self) -> usize {
    self.limits.max_accumulated_text_bytes
}
```

- [ ] **Step 6: Parse optional YAML limits**

In `src/resources/config.rs`, update imports:

```rust
openai_chat::{OpenAiChatLimits, OpenAiChatModel},
```

Extend `ModelYaml::OpenAiChat`:

```rust
limits: Option<OpenAiChatLimitsYaml>,
```

Add below `ModelYaml`:

```rust
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiChatLimitsYaml {
    max_upstream_bytes: Option<usize>,
    max_buffered_line_bytes: Option<usize>,
    max_event_payload_bytes: Option<usize>,
    max_chunk_text_bytes: Option<usize>,
    max_usage_json_bytes: Option<usize>,
    max_accumulated_text_bytes: Option<usize>,
}

impl OpenAiChatLimitsYaml {
    fn resolve(self) -> Result<OpenAiChatLimits, ResourceConfigError> {
        let defaults = OpenAiChatLimits::default();
        OpenAiChatLimits {
            max_upstream_bytes: self.max_upstream_bytes.unwrap_or(defaults.max_upstream_bytes),
            max_buffered_line_bytes: self
                .max_buffered_line_bytes
                .unwrap_or(defaults.max_buffered_line_bytes),
            max_event_payload_bytes: self
                .max_event_payload_bytes
                .unwrap_or(defaults.max_event_payload_bytes),
            max_chunk_text_bytes: self
                .max_chunk_text_bytes
                .unwrap_or(defaults.max_chunk_text_bytes),
            max_usage_json_bytes: self
                .max_usage_json_bytes
                .unwrap_or(defaults.max_usage_json_bytes),
            max_accumulated_text_bytes: self
                .max_accumulated_text_bytes
                .unwrap_or(defaults.max_accumulated_text_bytes),
        }
        .validate()
        .map_err(|error| ResourceConfigError::new(error.code(), error.to_string()))
    }
}
```

In the `match` arm, bind `limits` and resolve before construction:

```rust
let limits = limits.unwrap_or_default().resolve()?;
let model = OpenAiChatModel::new_with_limits(
    api_key,
    base_url,
    model,
    capabilities,
    positive_duration(&connect_timeout, "connect_timeout")?,
    positive_duration(&request_timeout, "request_timeout")?,
    limits,
)
```

- [ ] **Step 7: Run GREEN for model resource tests**

Run:

```bash
cargo test --test model_resources_v1 -- --nocapture
```

Expected: PASS; default config resolves default accumulated text limit, override resolves `7`, zero limits fail with `MODEL_CONFIG_INVALID`.

- [ ] **Step 8: Commit Task 1**

Run:

```bash
git add src/resources/openai_chat.rs src/resources/config.rs src/resources/models.rs tests/model_resources_v1.rs
git commit -m "feat: configure chat provider memory limits"
```

---

### Task 2: Enforce OpenAI-compatible provider stream limits

**Files:**
- Modify: `src/resources/openai_chat.rs`
- Modify: `tests/formal_resources.rs`

**Interfaces:**
- Consumes: `OpenAiChatLimits`, `OpenAiChatModel::new_with_limits`, and `model_response_too_large()`.
- Produces: incremental enforcement for total upstream bytes, buffered line bytes, event payload bytes, chunk text bytes, and usage JSON bytes.

- [ ] **Step 1: Add limit-aware model helper to OpenAI tests**

In `tests/formal_resources.rs`, update imports:

```rust
openai_chat::{OpenAiChatLimits, OpenAiChatModel},
```

Add helper below `model(...)`:

```rust
fn model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        limits,
    )
    .unwrap()
}

fn default_chat_request() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
        parameters: json!({}),
    }
}

async fn next_stream_error(model: OpenAiChatModel) -> RunError {
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    stream
        .next()
        .await
        .expect("stream should yield an error")
        .expect_err("limit violation must be an error")
}

fn assert_too_large(error: &RunError, forbidden: &[&str]) {
    assert_eq!(error.code(), "MODEL_RESPONSE_TOO_LARGE");
    assert_eq!(
        error.message(),
        "chat provider response exceeded the configured size limit"
    );
    let rendered = format!("{error:?} {error}");
    for value in forbidden {
        assert!(
            !rendered.contains(value),
            "limit error leaked forbidden value {value}: {rendered}"
        );
    }
}
```

- [ ] **Step 2: Add exact-boundary streaming regression**

Add this test to `tests/formal_resources.rs`:

```rust
#[tokio::test]
async fn openai_stream_accepts_exact_configured_response_limits() {
    let payload =
        r#"{"choices":[{"delta":{"content":"abc"},"finish_reason":"stop"}],"usage":{"u":"xy"}}"#;
    let data_line = format!("data: {payload}\n");
    let done_line = "data: [DONE]\n";
    let body = format!("{data_line}\n{done_line}\n");
    let usage = json!({"u":"xy"});
    let mut limits = OpenAiChatLimits::default();
    limits.max_upstream_bytes = body.as_bytes().len();
    limits.max_buffered_line_bytes = data_line.trim_end_matches('\n').as_bytes().len();
    limits.max_event_payload_bytes = payload.as_bytes().len();
    limits.max_chunk_text_bytes = "abc".len();
    limits.max_usage_json_bytes = serde_json::to_vec(&usage).unwrap().len();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(body.as_bytes()).await.unwrap();
    });

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    let chunk = stream.next().await.unwrap().unwrap();
    assert_eq!(chunk.text, "abc");
    assert_eq!(chunk.finish_reason.as_deref(), Some("stop"));
    assert_eq!(chunk.usage, Some(usage));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
```

Run:

```bash
cargo test --test formal_resources openai_stream_accepts_exact_configured_response_limits -- --nocapture
```

Expected before Step 7/8: this may already PASS because the old implementation is unbounded. Keep it as off-by-one acceptance coverage; the RED tests in Steps 3-6 drive the production change.

- [ ] **Step 3: Write RED upstream byte limit test**

Add this test:

```rust
#[tokio::test]
async fn openai_stream_rejects_total_upstream_bytes_without_echoing_body() {
    let body_secret = "upstream-body-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{body_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_upstream_bytes = body.as_bytes().len() - 1;

    let model = model_with_limits(
        format!("http://{address}/v1?token=url-secret"),
        Some("api-key-secret".to_string()),
        limits,
    );
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[body_secret, "url-secret", "api-key-secret"]);
    server.await.unwrap();
}
```

Run:

```bash
cargo test --test formal_resources openai_stream_rejects_total_upstream_bytes_without_echoing_body -- --nocapture
```

Expected: FAIL because oversized upstream bodies are currently accepted until parsed.

- [ ] **Step 4: Write RED no-LF buffered-line test**

Add this test:

```rust
#[tokio::test]
async fn openai_stream_rejects_no_lf_buffer_growth() {
    let line_secret = "line-buffer-secret";
    let body = format!("data: {line_secret}");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_buffered_line_bytes = body.as_bytes().len() - 1;

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[line_secret]);
    server.await.unwrap();
}
```

Run:

```bash
cargo test --test formal_resources openai_stream_rejects_no_lf_buffer_growth -- --nocapture
```

Expected: FAIL because the decoder currently allows the buffer to grow until EOF.

- [ ] **Step 5: Write RED event payload, chunk text, and usage tests**

Add these tests:

```rust
#[tokio::test]
async fn openai_stream_rejects_oversized_event_payload_without_parsing_secret() {
    let payload_secret = "event-payload-secret";
    let payload = format!(
        "{{\"choices\":[{{\"delta\":{{\"content\":\"{payload_secret}\"}},\"finish_reason\":null}}]}}"
    );
    let body = format!("data: {payload}\n\n");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_event_payload_bytes = payload.as_bytes().len() - 1;

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[payload_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_oversized_chunk_text_without_echoing_text() {
    let text_secret = "chunk-text-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_chunk_text_bytes = text_secret.len() - 1;

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[text_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_oversized_usage_json_without_echoing_usage() {
    let usage_secret = "usage-json-secret";
    let usage = json!({"detail": usage_secret});
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}],\"usage\":{usage}}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_usage_json_bytes = serde_json::to_vec(&usage).unwrap().len() - 1;

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[usage_secret]);
    server.await.unwrap();
}
```

Run:

```bash
cargo test --test formal_resources openai_stream_rejects_ -- --nocapture
```

Expected: FAIL for the new oversized limit cases.

- [ ] **Step 6: Write RED upstream-close-on-limit test**

Add this test:

```rust
#[tokio::test]
async fn openai_limit_error_drops_the_in_flight_http_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let closed = Arc::new(Notify::new());
    let server_closed = Arc::clone(&closed);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket
            .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"too-large\"},\"finish_reason\":null}]}\n\n")
            .await
            .unwrap();
        let mut byte = [0_u8; 1];
        if socket.read(&mut byte).await.unwrap_or(0) == 0 {
            server_closed.notify_one();
        }
    });
    let mut limits = OpenAiChatLimits::default();
    limits.max_chunk_text_bytes = "too-large".len() - 1;
    let model = model_with_limits(format!("http://{address}"), None, limits);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_too_large(&error, &["too-large"]);

    drop(stream);

    tokio::time::timeout(Duration::from_secs(1), closed.notified())
        .await
        .unwrap();
    server.await.unwrap();
}
```

Run:

```bash
cargo test --test formal_resources openai_limit_error_drops_the_in_flight_http_body -- --nocapture
```

Expected: FAIL until the stream returns the limit error early and dropping it closes the body.

- [ ] **Step 7: Implement shared helper and stream accounting**

In `src/resources/openai_chat.rs`, update imports:

```rust
use super::models::{
    model_response_too_large, ChatChunk, ChatMessage, ChatModel, ChatRequest, ChatStream,
    ModelCapability,
};
```

Update `StreamState`:

```rust
struct StreamState {
    bytes: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: SseDecoder,
    pending: VecDeque<ChatChunk>,
    upstream_bytes: usize,
    limits: OpenAiChatLimits,
}
```

When building the stream, fail early on known oversized `Content-Length` and initialize state:

```rust
if response
    .content_length()
    .is_some_and(|length| length > self.limits.max_upstream_bytes as u64)
{
    return Err(model_response_too_large());
}

let stream = stream::try_unfold(
    StreamState {
        bytes: Box::pin(response.bytes_stream()),
        decoder: SseDecoder::new(self.limits),
        pending: VecDeque::new(),
        upstream_bytes: 0,
        limits: self.limits,
    },
    |mut state| async move {
        loop {
            if let Some(chunk) = state.pending.pop_front() {
                return Ok(Some((chunk, state)));
            }
            match state.bytes.next().await {
                Some(Ok(bytes)) => {
                    if state
                        .upstream_bytes
                        .saturating_add(bytes.len())
                        > state.limits.max_upstream_bytes
                    {
                        return Err(model_response_too_large());
                    }
                    state.upstream_bytes += bytes.len();
                    state.pending.extend(state.decoder.push(&bytes)?);
                }
                Some(Err(error)) => {
                    return Err(RunError::new(
                        "UPSTREAM_STREAM",
                        format!(
                            "chat provider stream failed ({})",
                            classify_request_error(&error)
                        ),
                    ));
                }
                None => {
                    state.pending.extend(state.decoder.finish()?);
                    if let Some(chunk) = state.pending.pop_front() {
                        return Ok(Some((chunk, state)));
                    }
                    return Ok(None);
                }
            }
        }
    },
);
```

- [ ] **Step 8: Implement `SseDecoder` limit checks**

Replace the decoder with:

```rust
struct SseDecoder {
    buffer: Vec<u8>,
    limits: OpenAiChatLimits,
}

impl SseDecoder {
    fn new(limits: OpenAiChatLimits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = Vec::new();
        for segment in bytes.split_inclusive(|byte| *byte == b'\n') {
            let includes_lf = segment.last() == Some(&b'\n');
            let projected_line_len = self
                .buffer
                .len()
                .saturating_add(segment.len())
                .saturating_sub(usize::from(includes_lf));
            if projected_line_len > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            self.buffer.extend_from_slice(segment);
            chunks.extend(self.drain_complete_lines()?);
        }
        Ok(chunks)
    }

    fn finish(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = self.drain_complete_lines()?;
        if !self.buffer.is_empty() {
            if self.buffer.len() > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            let line = std::mem::take(&mut self.buffer);
            chunks.extend(parse_sse_line(trim_carriage_return(&line), self.limits)?);
        }
        Ok(chunks)
    }

    fn drain_complete_lines(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = Vec::new();
        while let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') {
            if index > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            let mut line = self.buffer.drain(..=index).collect::<Vec<_>>();
            line.pop();
            chunks.extend(parse_sse_line(trim_carriage_return(&line), self.limits)?);
        }
        Ok(chunks)
    }
}
```

Change `parse_sse_line` signature:

```rust
fn parse_sse_line(line: &[u8], limits: OpenAiChatLimits) -> Result<Vec<ChatChunk>, RunError>
```

Inside it, enforce payload/chunk/usage limits:

```rust
let Some(payload) = line.strip_prefix("data:") else {
    return Ok(Vec::new());
};
let payload = payload.trim_start();
if payload.as_bytes().len() > limits.max_event_payload_bytes {
    return Err(model_response_too_large());
}
if payload == "[DONE]" {
    return Ok(Vec::new());
}
```

Implement choice mapping as an explicit `for` loop so limit checks can return `Result` cleanly:

```rust
let mut chunks = Vec::new();
for (index, choice) in parsed.choices.into_iter().enumerate() {
    let text = choice.delta.content.unwrap_or_default();
    if text.len() > limits.max_chunk_text_bytes {
        return Err(model_response_too_large());
    }
    let usage = (index + 1 == choice_count)
        .then(|| parsed.usage.clone())
        .flatten();
    if let Some(usage) = &usage {
        let bytes = serde_json::to_vec(usage).map_err(|_| {
            RunError::new(
                "UPSTREAM_STREAM_INVALID",
                "invalid chat provider stream payload",
            )
        })?;
        if bytes.len() > limits.max_usage_json_bytes {
            return Err(model_response_too_large());
        }
    }
    if !text.is_empty() || choice.finish_reason.is_some() || usage.is_some() {
        chunks.push(ChatChunk {
            text,
            finish_reason: choice.finish_reason,
            usage,
        });
    }
}
if chunks.is_empty() {
    if let Some(usage) = parsed.usage {
        let bytes = serde_json::to_vec(&usage).map_err(|_| {
            RunError::new(
                "UPSTREAM_STREAM_INVALID",
                "invalid chat provider stream payload",
            )
        })?;
        if bytes.len() > limits.max_usage_json_bytes {
            return Err(model_response_too_large());
        }
        chunks.push(ChatChunk {
            text: String::new(),
            finish_reason: None,
            usage: Some(usage),
        });
    }
}
Ok(chunks)
```

- [ ] **Step 9: Run GREEN for OpenAI stream tests**

Run:

```bash
cargo test --test formal_resources -- --nocapture --test-threads=1
```

Expected: PASS; new limit tests and existing OpenAI/resource tests pass.

- [ ] **Step 10: Commit Task 2**

Run:

```bash
git add src/resources/openai_chat.rs tests/formal_resources.rs
git commit -m "feat: bound openai chat stream memory"
```

---

### Task 3: Enforce accumulated chat text in `ChatNode`

**Files:**
- Modify: `src/nodes/chat.rs`
- Modify: `tests/core_chat_action.rs`

**Interfaces:**
- Consumes: `ChatModel::max_accumulated_text_bytes()` and `model_response_too_large()`.
- Produces: accumulated text limit enforcement before append and before content emission.

- [ ] **Step 1: Add configurable test model for accumulated output**

In `tests/core_chat_action.rs`, add below `RecordingModel`:

```rust
#[derive(Clone)]
struct LimitModel {
    chunks: Vec<ChatChunk>,
    max_accumulated_text_bytes: usize,
}

impl fmt::Debug for LimitModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("LimitModel").finish()
    }
}

#[async_trait]
impl ChatModel for LimitModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    fn max_accumulated_text_bytes(&self) -> usize {
        self.max_accumulated_text_bytes
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        assert_eq!(request.messages.len(), 1);
        Ok(Box::pin(stream::iter(self.chunks.clone().into_iter().map(Ok))))
    }
}
```

- [ ] **Step 2: Add exact-boundary `ChatNode` regression**

Add this test:

```rust
#[tokio::test]
async fn chat_allows_accumulated_text_at_exact_model_limit() {
    let mut models = ModelRegistry::default();
    models
        .register(
            "limited",
            LimitModel {
                chunks: vec![
                    ChatChunk {
                        text: "ab".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: "c".to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 3,
            },
        )
        .unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"limited",
                "messages":[{"role":"user", "content":"Hi"}]
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.chat", EmitPolicy::Content, compilation);
    let (control, emitted) = capturing_control();

    let outcome = ChatNode
        .execute(&node, &context(json!({})), &control)
        .await
        .unwrap();

    assert_eq!(outcome.output["text"], "abc");
    assert_eq!(*emitted.lock().unwrap(), vec!["ab".to_string(), "c".to_string()]);
}
```

Run:

```bash
cargo test --test core_chat_action chat_allows_accumulated_text_at_exact_model_limit -- --nocapture
```

Expected before Step 4: this may already PASS because the old implementation is unbounded. Keep it as off-by-one acceptance coverage; the RED over-limit test in Step 3 drives the production change.

- [ ] **Step 3: Write RED over-limit `ChatNode` test**

Add this test:

```rust
#[tokio::test]
async fn chat_rejects_accumulated_text_before_appending_or_emitting_over_limit_chunk() {
    const OVER_LIMIT_SECRET: &str = "accumulated-text-secret";
    let mut models = ModelRegistry::default();
    models
        .register(
            "limited",
            LimitModel {
                chunks: vec![
                    ChatChunk {
                        text: "ok".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: OVER_LIMIT_SECRET.to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 2,
            },
        )
        .unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"limited",
                "messages":[{"role":"user", "content":"Hi"}]
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.chat", EmitPolicy::Content, compilation);
    let (control, emitted) = capturing_control();

    let error = ChatNode
        .execute(&node, &context(json!({})), &control)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "MODEL_RESPONSE_TOO_LARGE");
    assert_eq!(
        error.message(),
        "chat provider response exceeded the configured size limit"
    );
    assert!(!format!("{error:?} {error}").contains(OVER_LIMIT_SECRET));
    assert_eq!(*emitted.lock().unwrap(), vec!["ok".to_string()]);
}
```

Run:

```bash
cargo test --test core_chat_action chat_rejects_accumulated_text_before_appending_or_emitting_over_limit_chunk -- --nocapture
```

Expected: FAIL because current `ChatNode` appends and emits the over-limit chunk.

- [ ] **Step 4: Implement accumulated text enforcement**

In `src/nodes/chat.rs`, update imports:

```rust
resources::models::{
    model_response_too_large, ChatContent, ChatContentPart, ChatMessage, ChatModel, ChatRequest,
    ChatRole, ImageUrl, ModelCapability,
},
```

Inside `execute`, after `let mut usage = None;`, add:

```rust
let max_accumulated_text_bytes = body.model.max_accumulated_text_bytes();
```

Replace the text append block with:

```rust
if !chunk.text.is_empty() {
    if text.len().saturating_add(chunk.text.len()) > max_accumulated_text_bytes {
        return Err(model_response_too_large());
    }
    text.push_str(&chunk.text);
}
```

- [ ] **Step 5: Run GREEN for chat node tests**

Run:

```bash
cargo test --test core_chat_action -- --nocapture --test-threads=1
```

Expected: PASS; existing Chat, Action, cancellation, streaming behavior remains green.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add src/nodes/chat.rs tests/core_chat_action.rs
git commit -m "feat: bound accumulated chat output"
```

---

### Task 4: Add production runtime regression for capacity release after bounded failure

**Files:**
- Create: `tests/chat_memory_bounds.rs`

**Interfaces:**
- Consumes: `ChatNode` accumulated text limit enforcement.
- Consumes: production `AgentCompiler`, default node registries, `RunService`, `EventHub`, `SqliteRunRepository`, and Formal runtime history behavior.
- Produces: end-to-end proof that a bounded chat failure terminalizes and releases capacity so a later Run completes.

- [ ] **Step 1: Create failing integration test file**

Create `tests/chat_memory_bounds.rs`:

```rust
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::hub::{EventHub, EventHubConfig},
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::default_node_registries,
    resources::{
        actions::ActionRegistry,
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::{CompiledAgentRegistry, RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};
use tempfile::tempdir;

#[derive(Clone)]
struct ScenarioModel {
    chunks: Vec<ChatChunk>,
    max_accumulated_text_bytes: usize,
}

impl std::fmt::Debug for ScenarioModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ScenarioModel").finish()
    }
}

#[async_trait]
impl ChatModel for ScenarioModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    fn max_accumulated_text_bytes(&self) -> usize {
        self.max_accumulated_text_bytes
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter(self.chunks.clone().into_iter().map(Ok))))
    }
}

fn write_agent(root: &Path, id: &str, model: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"version: 1
id: {id}
name: {id}
input:
  schema:
    type: object
    additionalProperties: false
entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: {model}
      messages:
        - role: user
          content: Hi
  result:
    type: core.output
    config:
      data: {{ok: true}}
"#
        ),
    )
    .unwrap();
}

struct Fixture {
    _root: tempfile::TempDir,
    service: RunService,
}

async fn fixture() -> Fixture {
    let root = tempdir().unwrap();
    write_agent(root.path(), "too-large", "too_large_model");
    write_agent(root.path(), "success", "success_model");

    let mut models = ModelRegistry::default();
    models
        .register(
            "too_large_model",
            ScenarioModel {
                chunks: vec![
                    ChatChunk {
                        text: "ok".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: "capacity-release-secret".to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 2,
            },
        )
        .unwrap();
    models
        .register(
            "success_model",
            ScenarioModel {
                chunks: vec![ChatChunk {
                    text: "done".to_string(),
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                }],
                max_accumulated_text_bytes: 16,
            },
        )
        .unwrap();

    let (node_types, executors) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(5),
        CompileLimits {
            max_fork_branches: 8,
        },
    );
    let agents = ["too-large", "success"]
        .into_iter()
        .map(|id| Arc::new(compiler.compile_dir(&root.path().join(id)).unwrap()))
        .collect();
    let agents = CompiledAgentRegistry::new(agents).unwrap();

    let database_path = root.path().join("history.sqlite3");
    let repository = Arc::new(
        SqliteRunRepository::connect_path(&database_path)
            .await
            .unwrap(),
    );
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 1,
            max_parallel_branches_per_run: 1,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    Fixture {
        _root: root,
        service,
    }
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) {
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service.get_run(run_id).await.unwrap();
            if record.status == expected {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    result.unwrap();
}

#[tokio::test]
async fn bounded_chat_failure_releases_capacity_for_later_runs() {
    let fixture = fixture().await;
    let service = &fixture.service;

    let failed = service
        .create_detached("too-large", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(service, &failed.run_id, RunStatus::Failed).await;
    let failed_record = service.get_run(&failed.run_id).await.unwrap();
    assert_eq!(
        failed_record.error_code.as_deref(),
        Some("MODEL_RESPONSE_TOO_LARGE")
    );
    assert_eq!(
        failed_record.error_message.as_deref(),
        Some("chat provider response exceeded the configured size limit")
    );
    assert!(
        !format!("{failed_record:?}").contains("capacity-release-secret"),
        "bounded failure leaked oversized content: {failed_record:?}"
    );

    let success = service
        .create_detached("success", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(service, &success.run_id, RunStatus::Completed).await;
    let success_record = service.get_run(&success.run_id).await.unwrap();
    assert_eq!(success_record.status, RunStatus::Completed);

    fixture.service.shutdown(Duration::from_secs(1)).await.unwrap();
}
```

Run:

```bash
cargo test --test chat_memory_bounds -- --nocapture
```

Expected: PASS only after Task 3 is implemented; if run before Task 3, FAIL because the too-large Run completes instead of failing.

- [ ] **Step 2: Run focused runtime regression**

Run:

```bash
cargo test --test chat_memory_bounds -- --nocapture
```

Expected: PASS; the first Run fails with the stable too-large error and the second Run completes under `max_concurrent_runs: 1`.

- [ ] **Step 3: Commit Task 4**

Run:

```bash
git add tests/chat_memory_bounds.rs
git commit -m "test: verify chat memory bound capacity release"
```

---

### Task 5: Document A1 and run release gates

**Files:**
- Modify: `README.md`
- Read: `docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md`
- Read: all Task 1-4 changed files

**Interfaces:**
- Consumes: implemented A1 config and runtime behavior.
- Produces: branch ready for review and merge decision.

- [ ] **Step 1: Document optional model limits in README**

In `README.md`, extend the model YAML example near the `open_ai_chat` block with:

```yaml
    limits:
      max_upstream_bytes: 8388608
      max_buffered_line_bytes: 1048576
      max_event_payload_bytes: 1048576
      max_chunk_text_bytes: 262144
      max_usage_json_bytes: 65536
      max_accumulated_text_bytes: 1048576
```

Below the example, add:

```markdown
`limits` 可省略；省略时使用上述默认字节上限。上游响应体、SSE 行、单个 `data:` payload、单个文本 delta、usage JSON 和最终累计文本都会在写入内存前检查。超限 Run 使用稳定错误 `MODEL_RESPONSE_TOO_LARGE`，错误消息不会包含 provider body、prompt、API key 或响应片段。
```

- [ ] **Step 2: Run documentation and scope checks**

Run:

```bash
rg -n 'MODEL_RESPONSE_TOO_LARGE|limits:|max_upstream_bytes|max_accumulated_text_bytes' README.md \
  docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md \
  src tests
git diff --check
git diff --name-only main...HEAD
```

Expected: A1 references exist; diff check is clean; changed paths are limited to:

```text
README.md
src/resources/openai_chat.rs
src/resources/config.rs
src/resources/models.rs
src/nodes/chat.rs
tests/model_resources_v1.rs
tests/formal_resources.rs
tests/core_chat_action.rs
tests/chat_memory_bounds.rs
```

If this plan is executed from a branch whose base already includes the A1 design and plan commits, those docs may also appear in the branch diff:

```text
docs/superpowers/specs/2026-07-11-provider-memory-bounds-design.md
docs/superpowers/plans/2026-07-11-provider-memory-bounds.md
```

- [ ] **Step 3: Run focused A1 test suite**

Run:

```bash
cargo test --test model_resources_v1 --test formal_resources --test core_chat_action --test chat_memory_bounds -- --nocapture --test-threads=1
```

Expected: PASS; config, provider stream, direct ChatNode, and runtime capacity release tests all pass.

- [ ] **Step 4: Run formatting and strict lint**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both exit 0 with zero warnings.

- [ ] **Step 5: Run full Rust and dependency gates**

Run:

```bash
cargo test --all-targets
cargo audit
cargo deny check
```

Expected:

- `cargo test --all-targets`: all tests pass.
- `cargo audit`: exit 0; retains only the accepted `paste` unmaintained allowed warning.
- `cargo deny check`: exit 0; retains only documented duplicate/unmatched-license warnings.

- [ ] **Step 6: Run real PostgreSQL contract gate**

Run:

```bash
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

Expected: PASS with 1 test and no environment-missing skip path.

- [ ] **Step 7: Verify no out-of-scope changes**

Run:

```bash
git diff --name-only main...HEAD -- Cargo.toml Cargo.lock deny.toml migrations .sqlx agents src/api src/history src/runtime src/dsl
git status --short
git log --oneline --decorate -8
```

Expected:

- No output for Cargo manifests, lockfile, dependency policy, migrations, SQLx metadata, Agent YAML, API, history, runtime, or DSL paths.
- Working tree clean after committed changes.
- Commit log shows focused A1 commits only after the branch base.

- [ ] **Step 8: Commit docs if needed**

Run:

```bash
git add README.md
git commit -m "docs: document chat provider memory limits"
```

If README was already committed in an earlier task, skip this commit and record why in the task report.

- [ ] **Step 9: Request independent code review**

The reviewer must check:

```text
1. Existing model configs without limits still load and use defaults.
2. Zero limits fail startup with MODEL_CONFIG_INVALID.
3. OpenAI stream limits are enforced incrementally and use inclusive boundaries.
4. ChatNode checks accumulated text before append and before content emission.
5. Every limit violation returns only MODEL_RESPONSE_TOO_LARGE / chat provider response exceeded the configured size limit.
6. Limit errors cannot expose provider body, chunk text, usage JSON, URL token, API key, prompts, response headers, or numeric configured limits.
7. Stream limit failures drop upstream bodies and later Runs can proceed.
8. Public HTTP/SSE/event/Run/repository shapes, migrations, dependency graph, and A2-A8 remain unchanged.
```

Fix every Critical or Important issue, rerun the relevant focused tests, rerun Steps 2-7, and request re-review before presenting integration options.
