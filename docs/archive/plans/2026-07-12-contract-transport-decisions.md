# A8 Contract and Transport Decisions Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close A8 by making OpenAI-compatible plaintext HTTP explicit, narrowing public Agent timeout grammar, and aligning SSE/contract documentation.

**Architecture:** Keep safe defaults at the provider constructor boundary: existing `OpenAiChatModel::new(...)` and `new_with_limits(...)` stay HTTPS-only, while a new explicit transport-policy constructor is required for loopback or trusted-private HTTP. Keep model YAML transport policy in `src/resources/config.rs`, keep Agent timeout grammar in `src/dsl/raw.rs`, and keep docs-only SSE cleanup separate from runtime behavior.

**Tech Stack:** Rust, Serde YAML, `reqwest::Url`, Tokio integration tests, existing model/resource config tests, existing DSL parser tests.

## Global Constraints

- A8 does not add custom CA roots, certificate pinning, disabled certificate validation, DNS resolution, private-network probing, metrics, exporters, API routes, event replay, repository schema changes, or migration files.
- `OpenAiChatModel::new(...)` and `OpenAiChatModel::new_with_limits(...)` must reject every `http://` URL by default.
- Plaintext HTTP is allowed only through an explicit transport policy: `disabled`, `loopback`, or `trusted_private`; the default is `disabled`.
- `loopback` HTTP is restricted to exact `127.0.0.1`, `[::1]`, and `localhost`.
- `trusted_private` HTTP allows non-loopback HTTP only as explicit operator risk acceptance and must not attempt DNS/IP private-network classification.
- URL username/password userinfo fails for HTTPS, loopback HTTP, and trusted-private HTTP.
- Transport errors and `Debug` output must not expose API keys, bearer tokens, full URLs with query, headers, prompts, or model bodies.
- Agent timeout parsing accepts only the Formal V1 grammar: one positive base-10 integer immediately followed by `ms`, `s`, or `m`.
- Agent duration parse failures keep the public outer code `DSL_YAML_INVALID`.
- Duration serialization emits only the Formal V1 grammar.
- README and `docs/formal-v1-breaking-changes.md` must document live-only SSE and the A8 migration requirements.

---

## File Structure

- Modify `src/resources/openai_chat.rs`
  - Owns `OpenAiTransportPolicy`, default HTTPS-only validation, explicit loopback/trusted-private HTTP policy, and userinfo rejection.
- Modify `src/resources/config.rs`
  - Parses optional `transport.plaintext_http` for `open_ai_chat` model resources and passes the explicit policy into the provider.
- Modify `src/dsl/raw.rs`
  - Replaces `humantime` parsing for Agent node `timeout` values with the Formal V1 parser and canonical serializer.
- Modify `tests/formal_resources.rs`
  - Adds provider constructor policy tests and updates loopback streaming helpers to opt into loopback HTTP explicitly.
- Modify `tests/model_resources_v1.rs`
  - Adds model YAML transport-policy tests.
- Modify `tests/dsl_raw.rs`
  - Adds accepted/rejected timeout grammar and serialization tests.
- Modify `README.md`
  - Documents model transport policy and confirms live-only SSE wording.
- Modify `docs/formal-v1-breaking-changes.md`
  - Documents A8 breaking changes and migration requirements.

---

### Task 1: Add explicit OpenAI transport policy at the provider boundary

**Files:**
- Modify: `src/resources/openai_chat.rs`
- Modify: `tests/formal_resources.rs`

**Interfaces:**
- Produces:
  - `pub enum OpenAiTransportPolicy { HttpsOnly, AllowLoopbackHttp, AllowTrustedPrivateHttp }`
  - `OpenAiChatModel::new_with_limits_and_transport_policy(...) -> Result<Self, CompileError>`
- Preserves:
  - `OpenAiChatModel::new(...)` and `OpenAiChatModel::new_with_limits(...)` as HTTPS-only constructors.
- Consumed by Task 2 from `src/resources/config.rs`.

- [ ] **Step 1: Write failing provider transport-policy tests**

Update the import in `tests/formal_resources.rs`:

```rust
openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
```

Add these helper functions near the existing `model` and `model_with_limits` helpers:

```rust
fn loopback_model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    loopback_model_with_limits(base_url, api_key, OpenAiChatLimits::default())
}

fn loopback_model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits_and_transport_policy(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        limits,
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap()
}

fn trusted_private_model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits_and_transport_policy(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowTrustedPrivateHttp,
    )
    .unwrap()
}
```

Append this test:

```rust
#[test]
fn openai_transport_policy_rejects_plaintext_http_by_default() {
    for constructor in ["new", "new_with_limits"] {
        let error = if constructor == "new" {
            OpenAiChatModel::new(
                Some("api-key-secret".to_string()),
                "http://model-service.internal/v1?token=url-secret".to_string(),
                "fallback-model".to_string(),
                BTreeSet::new(),
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
            .unwrap_err()
        } else {
            OpenAiChatModel::new_with_limits(
                Some("api-key-secret".to_string()),
                "http://model-service.internal/v1?token=url-secret".to_string(),
                "fallback-model".to_string(),
                BTreeSet::new(),
                Duration::from_secs(1),
                Duration::from_secs(2),
                OpenAiChatLimits::default(),
            )
            .unwrap_err()
        };
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("api-key-secret"));
        assert!(!rendered.contains("url-secret"));
        assert!(!rendered.contains("model-service.internal"));
    }
}
```

Append this test:

```rust
#[test]
fn openai_transport_policy_allows_only_explicit_plaintext_scopes() {
    for base_url in [
        "http://127.0.0.1:8080/v1",
        "http://localhost:8080/v1",
        "http://[::1]:8080/v1",
    ] {
        loopback_model(base_url.to_string(), None);
    }

    let non_loopback = OpenAiChatModel::new_with_limits_and_transport_policy(
        None,
        "http://10.0.0.10:8080/v1".to_string(),
        "fallback-model".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap_err();
    assert_eq!(non_loopback.code(), "MODEL_CONFIG_INVALID");

    trusted_private_model("http://10.0.0.10:8080/v1".to_string(), None);
    trusted_private_model(
        "http://model.default.svc.cluster.local:8080/v1".to_string(),
        None,
    );
}
```

Append this test:

```rust
#[test]
fn openai_transport_policy_rejects_url_userinfo_for_every_policy() {
    for (base_url, policy) in [
        (
            "https://user:pass@models.example.test/v1",
            OpenAiTransportPolicy::HttpsOnly,
        ),
        (
            "http://user:pass@127.0.0.1:8080/v1",
            OpenAiTransportPolicy::AllowLoopbackHttp,
        ),
        (
            "http://user:pass@model.internal:8080/v1",
            OpenAiTransportPolicy::AllowTrustedPrivateHttp,
        ),
    ] {
        let error = OpenAiChatModel::new_with_limits_and_transport_policy(
            Some("api-key-secret".to_string()),
            base_url.to_string(),
            "fallback-model".to_string(),
            BTreeSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            OpenAiChatLimits::default(),
            policy,
        )
        .unwrap_err();
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("user:pass"));
        assert!(!rendered.contains("api-key-secret"));
    }
}
```

- [ ] **Step 2: Run the provider transport tests and verify RED**

Run:

```bash
cargo test --test formal_resources openai_transport_policy -- --nocapture
```

Expected: FAIL to compile because `OpenAiTransportPolicy` and `new_with_limits_and_transport_policy` do not exist yet, or fail at runtime because HTTP is still accepted by default.

- [ ] **Step 3: Implement `OpenAiTransportPolicy` and provider URL validation**

In `src/resources/openai_chat.rs`, add after `OpenAiChatLimits`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiTransportPolicy {
    HttpsOnly,
    AllowLoopbackHttp,
    AllowTrustedPrivateHttp,
}
```

Change `new_with_limits` to call the explicit constructor:

```rust
Self::new_with_limits_and_transport_policy(
    api_key,
    base_url,
    model,
    capabilities,
    connect_timeout,
    request_timeout,
    limits,
    OpenAiTransportPolicy::HttpsOnly,
)
```

Add the explicit constructor and move the current constructor body into it:

```rust
pub fn new_with_limits_and_transport_policy(
    api_key: Option<String>,
    base_url: String,
    model: String,
    capabilities: BTreeSet<ModelCapability>,
    connect_timeout: Duration,
    request_timeout: Duration,
    limits: OpenAiChatLimits,
    transport_policy: OpenAiTransportPolicy,
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
    validate_endpoint_transport(&endpoint, transport_policy)?;
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

Add these private helpers near `endpoint_origin`:

```rust
fn validate_endpoint_transport(
    endpoint: &Url,
    policy: OpenAiTransportPolicy,
) -> Result<(), CompileError> {
    if endpoint.host_str().is_none() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must include a host",
        ));
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must not include username or password",
        ));
    }
    match endpoint.scheme() {
        "https" => Ok(()),
        "http" => validate_plaintext_http(endpoint, policy),
        _ => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must use HTTP or HTTPS and include a host",
        )),
    }
}

fn validate_plaintext_http(
    endpoint: &Url,
    policy: OpenAiTransportPolicy,
) -> Result<(), CompileError> {
    match policy {
        OpenAiTransportPolicy::HttpsOnly => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must use HTTPS unless plaintext HTTP is explicitly allowed",
        )),
        OpenAiTransportPolicy::AllowLoopbackHttp if is_exact_loopback_host(endpoint) => Ok(()),
        OpenAiTransportPolicy::AllowLoopbackHttp => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI loopback HTTP is restricted to localhost, 127.0.0.1, or ::1",
        )),
        OpenAiTransportPolicy::AllowTrustedPrivateHttp => Ok(()),
    }
}

fn is_exact_loopback_host(endpoint: &Url) -> bool {
    matches!(
        endpoint.host_str(),
        Some("localhost" | "127.0.0.1" | "::1")
    )
}
```

- [ ] **Step 4: Update loopback provider tests to use explicit loopback policy**

In `tests/formal_resources.rs`, update existing helpers:

```rust
fn model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    loopback_model(base_url, api_key)
}

fn model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    loopback_model_with_limits(base_url, api_key, limits)
}
```

This keeps the existing loopback TCP fixtures explicit without rewriting every streaming test.

- [ ] **Step 5: Verify provider tests pass**

Run:

```bash
cargo test --test formal_resources openai_transport_policy -- --nocapture
cargo test --test formal_resources openai_ -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add src/resources/openai_chat.rs tests/formal_resources.rs
git commit -m "feat: require explicit OpenAI plaintext transport policy"
```

---

### Task 2: Add model YAML transport policy

**Files:**
- Modify: `src/resources/config.rs`
- Modify: `tests/model_resources_v1.rs`

**Interfaces:**
- Consumes `OpenAiTransportPolicy` and `OpenAiChatModel::new_with_limits_and_transport_policy` from Task 1.
- Produces model YAML:
  - default `transport.plaintext_http = disabled`
  - explicit values `disabled`, `loopback`, `trusted_private`

- [ ] **Step 1: Write failing model config transport tests**

Append to `tests/model_resources_v1.rs`:

```rust
#[test]
fn model_resources_reject_plaintext_http_by_default_without_leaking_secrets() {
    let yaml = model_yaml("").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://models.example.test/v1?token=url-secret",
    );
    let (_directory, path) = write_config(&yaml);
    let error = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap_err();

    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("api-key-secret"));
    assert!(!rendered.contains("url-secret"));
}
```

Append:

```rust
#[test]
fn model_resources_allow_explicit_loopback_and_trusted_private_http() {
    let loopback = model_yaml("    transport:\n      plaintext_http: loopback\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://localhost:11434/v1",
    );
    let (_directory, path) = write_config(&loopback);
    load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap();

    let trusted_private =
        model_yaml("    transport:\n      plaintext_http: trusted_private\n").replace(
            "base_url: https://models.example.test/v1",
            "base_url: http://model-service.internal:8080/v1",
        );
    let (_directory, path) = write_config(&trusted_private);
    load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap();
}
```

Append:

```rust
#[test]
fn model_resources_reject_non_loopback_http_with_loopback_policy() {
    let yaml = model_yaml("    transport:\n      plaintext_http: loopback\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://10.0.0.10:8080/v1",
    );
    let (_directory, path) = write_config(&yaml);
    let error = load_model_registry_with_env(&path, |name| {
        (name == "MODEL_API_KEY").then(|| "api-key-secret".to_string())
    })
    .unwrap_err();

    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
}
```

Append:

```rust
#[test]
fn model_resources_reject_unknown_transport_policy_and_url_userinfo() {
    let unknown = model_yaml("    transport:\n      plaintext_http: internet\n");
    let (_directory, path) = write_config(&unknown);
    let error = load_model_registry_with_env(&path, |_| Some("secret".to_string())).unwrap_err();
    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");

    let userinfo = model_yaml("    transport:\n      plaintext_http: trusted_private\n").replace(
        "base_url: https://models.example.test/v1",
        "base_url: http://user:pass@model-service.internal:8080/v1",
    );
    let (_directory, path) = write_config(&userinfo);
    let error = load_model_registry_with_env(&path, |_| Some("api-key-secret".to_string()))
        .unwrap_err();
    assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("user:pass"));
    assert!(!rendered.contains("api-key-secret"));
}
```

- [ ] **Step 2: Run model config tests and verify RED**

Run:

```bash
cargo test --test model_resources_v1 model_resources_ -- --nocapture
```

Expected: FAIL because `transport` is an unknown field and default HTTP behavior has not been wired through configuration.

- [ ] **Step 3: Implement YAML transport parsing**

In `src/resources/config.rs`, update imports:

```rust
openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
```

Add `transport` to `ModelYaml::OpenAiChat`:

```rust
transport: Option<OpenAiTransportYaml>,
```

In the `ModelYaml::OpenAiChat` match arm, bind `transport` and resolve it:

```rust
let transport_policy = transport.unwrap_or_default().plaintext_http.into();
let model = OpenAiChatModel::new_with_limits_and_transport_policy(
    api_key,
    base_url,
    model,
    capabilities,
    positive_duration(&connect_timeout, "connect_timeout")?,
    positive_duration(&request_timeout, "request_timeout")?,
    limits,
    transport_policy,
)
```

Add after `OpenAiChatLimitsYaml`:

```rust
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiTransportYaml {
    #[serde(default)]
    plaintext_http: PlaintextHttpYaml,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlaintextHttpYaml {
    #[default]
    Disabled,
    Loopback,
    TrustedPrivate,
}

impl From<PlaintextHttpYaml> for OpenAiTransportPolicy {
    fn from(value: PlaintextHttpYaml) -> Self {
        match value {
            PlaintextHttpYaml::Disabled => Self::HttpsOnly,
            PlaintextHttpYaml::Loopback => Self::AllowLoopbackHttp,
            PlaintextHttpYaml::TrustedPrivate => Self::AllowTrustedPrivateHttp,
        }
    }
}
```

- [ ] **Step 4: Verify model config tests pass**

Run:

```bash
cargo test --test model_resources_v1 -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/resources/config.rs tests/model_resources_v1.rs
git commit -m "feat: add model plaintext transport config"
```

---

### Task 3: Narrow Agent duration grammar to Formal V1

**Files:**
- Modify: `src/dsl/raw.rs`
- Modify: `tests/dsl_raw.rs`

**Interfaces:**
- Produces strict timeout grammar for public Agent YAML: `[1-9][0-9]*(ms|s|m)`.
- Preserves `DurationSpec::get(self) -> Duration`.

- [ ] **Step 1: Write failing duration grammar tests**

In `tests/dsl_raw.rs`, update the import:

```rust
use insight_agent_platform::dsl::{parse_raw_agent, DurationSpec, EmitPolicy};
```

Append:

```rust
fn parse_timeout(value: &str) -> Result<DurationSpec, String> {
    let yaml = FORMAL_V1.replace("timeout: 5s", &format!("timeout: {value}"));
    parse_raw_agent(&yaml)
        .map(|agent| agent.nodes["answer"].timeout.unwrap())
        .map_err(|error| {
            assert_eq!(error.code(), "DSL_YAML_INVALID");
            error.to_string()
        })
}
```

Append:

```rust
#[test]
fn accepts_only_formal_v1_positive_integer_duration_units() {
    assert_eq!(parse_timeout("1ms").unwrap().get().as_millis(), 1);
    assert_eq!(parse_timeout("250ms").unwrap().get().as_millis(), 250);
    assert_eq!(parse_timeout("5s").unwrap().get().as_secs(), 5);
    assert_eq!(parse_timeout("2m").unwrap().get().as_secs(), 120);
}
```

Append:

```rust
#[test]
fn rejects_out_of_contract_duration_spellings() {
    for value in [
        "0s",
        "01s",
        "+5s",
        "1.5s",
        "1 sec",
        "1s 500ms",
        "1h",
        "1d",
        "5S",
        "5 s",
        "soon",
    ] {
        let error = parse_timeout(value).expect_err("duration spelling must be rejected");
        assert!(
            error.contains("duration must match"),
            "unexpected error for {value}: {error}"
        );
    }
}
```

Append:

```rust
#[test]
fn rejects_duration_overflow() {
    let error = parse_timeout("18446744073709551615m")
        .expect_err("overflowing duration must be rejected");
    assert!(error.contains("duration is too large"), "{error}");
}
```

Append:

```rust
#[test]
fn serializes_duration_using_formal_v1_canonical_grammar() {
    for (input, expected) in [
        ("120000ms", "2m"),
        ("2000ms", "2s"),
        ("1500ms", "1500ms"),
    ] {
        let timeout = parse_timeout(input).unwrap();
        let value = serde_yaml::to_value(timeout).unwrap();
        assert_eq!(value.as_str(), Some(expected));
    }
}
```

- [ ] **Step 2: Run duration tests and verify RED**

Run:

```bash
cargo test --test dsl_raw duration -- --nocapture
```

Expected: FAIL because `humantime` currently accepts at least some out-of-contract values and serializes using humantime's formatter.

- [ ] **Step 3: Implement strict duration parser and serializer**

In `src/dsl/raw.rs`, remove `de` from the serde import:

```rust
use serde::{Deserialize, Deserializer, Serialize, Serializer};
```

Replace `DurationSpec::deserialize` with:

```rust
let value = String::deserialize(deserializer)?;
let duration = parse_formal_duration(&value).map_err(serde::de::Error::custom)?;
Ok(Self(duration))
```

Replace `Serialize` with:

```rust
serializer.serialize_str(&format_formal_duration(self.0))
```

Replace `Display` with:

```rust
formatter.write_str(&format_formal_duration(self.0))
```

Add helper functions below the `Display` impl:

```rust
fn parse_formal_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_u64)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000_u64)
    } else {
        return Err(
            "duration must match a positive integer followed by ms, s, or m".to_string(),
        );
    };
    if number.is_empty()
        || number.starts_with('0')
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(
            "duration must match a positive integer followed by ms, s, or m".to_string(),
        );
    }
    let amount = number
        .parse::<u64>()
        .map_err(|_| "duration is too large".to_string())?;
    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration is too large".to_string())?;
    Ok(Duration::from_millis(millis))
}

fn format_formal_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis % 60_000 == 0 {
        format!("{}m", millis / 60_000)
    } else if millis % 1_000 == 0 {
        format!("{}s", millis / 1_000)
    } else {
        format!("{millis}ms")
    }
}
```

- [ ] **Step 4: Verify duration tests pass**

Run:

```bash
cargo test --test dsl_raw -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/dsl/raw.rs tests/dsl_raw.rs
git commit -m "feat: narrow agent duration grammar"
```

---

### Task 4: Document A8 transport, duration, and live-only SSE contracts

**Files:**
- Modify: `README.md`
- Modify: `docs/formal-v1-breaking-changes.md`

**Interfaces:**
- Consumes implementation decisions from Tasks 1-3.
- Produces migration text for operators.

- [ ] **Step 1: Update README model transport docs**

In `README.md`, after the `open_ai_chat` model YAML example, add:

```markdown
`open_ai_chat.base_url` 默认必须使用 HTTPS。明文 HTTP 只能显式声明：

```yaml
transport:
  plaintext_http: loopback        # 仅 127.0.0.1 / localhost / [::1]
```

或：

```yaml
transport:
  plaintext_http: trusted_private # 部署方明确接受的私有网络模型服务链路
```

`trusted_private` 不会自动证明目标地址在内网，也不会做 DNS/IP 私网判定；它表示部署方确认该 HTTP 链路处于可信私有边界内。公网或其他不可信模型服务必须使用 HTTPS。模型 URL 不允许携带 username/password；密钥只通过 `api_key_env` 指向的环境变量注入。
```

In the Agent YAML timeout documentation area, add:

```markdown
节点 `timeout` 使用正式 V1 窄语法：正整数紧跟 `ms`、`s` 或 `m`，例如 `250ms`、`5s`、`2m`。不接受空格、复合值、别名、分数、`h/d` 等更大单位或前导零。
```

- [ ] **Step 2: Update breaking-change docs**

In `docs/formal-v1-breaking-changes.md`, under `## 配置与安全迁移`, append bullets:

```markdown
- A8 后 `open_ai_chat.base_url` 默认只接受 HTTPS。既有 HTTP 模型服务必须改为 HTTPS，或显式声明 `transport.plaintext_http: loopback` / `trusted_private`。`trusted_private` 是部署方对私有网络明文链路的风险接受，不是运行时自动内网判定。
- A8 后 Agent 节点 `timeout` 只接受正整数紧跟 `ms`、`s` 或 `m`。把 `1 sec`、`90 seconds`、`1h`、`1s 500ms` 等写法改为 `1s`、`90s`、`60m` 或等价毫秒值。
```

- [ ] **Step 3: Verify docs no longer imply reconnectable public SSE**

Run:

```bash
rg -n 'reconnect|replay|cursor|after_seq|Last-Event-ID|恢复|补发|重连' README.md docs/formal-v1-breaking-changes.md
```

Expected: every match explicitly says unsupported public SSE recovery, internal audit/recovery behavior, or migration rationale. If README opening still suggests reconnectable SSE, replace it with live-only wording:

```markdown
一个面向平台自有 Agent 的通用 Rust 运行时基线。它把严格 DSL 编译为不可变执行图，通过可扩展的节点、模型和 Action 注册表运行，并提供 live-only Attached SSE、Detached 轮询、显式取消以及 SQLite/PostgreSQL 事件历史。
```

- [ ] **Step 4: Verify docs diff**

Run:

```bash
git diff -- README.md docs/formal-v1-breaking-changes.md
```

Expected: docs mention `transport.plaintext_http`, `loopback`, `trusted_private`, Formal V1 timeout grammar, and live-only SSE.

- [ ] **Step 5: Commit**

Run:

```bash
git add README.md docs/formal-v1-breaking-changes.md
git commit -m "docs: document A8 contract migrations"
```

---

### Task 5: Run final A8 verification and review handoff

**Files:**
- No production files unless formatting requires it.

**Interfaces:**
- Consumes Tasks 1-4.
- Produces branch ready for code review and merge choice.

- [ ] **Step 1: Run focused verification**

Run:

```bash
cargo test --test formal_resources openai_ -- --nocapture
cargo test --test model_resources_v1 -- --nocapture
cargo test --test dsl_raw -- --nocapture
```

Expected: all commands PASS.

- [ ] **Step 2: Run full verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
```

Expected: all commands PASS.

- [ ] **Step 3: Inspect final diff scope**

Run:

```bash
git status --short --branch
git diff --stat main...HEAD
git diff --name-only main...HEAD
git diff -- Cargo.toml Cargo.lock
```

Expected:

- changed files are limited to A8 spec/plan docs, provider transport policy, model config, DSL duration parser, tests, README, and breaking-change docs;
- no API response shape, event envelope, repository schema, or migration files changed;
- no dependency graph change unless `humantime` becomes unused and is intentionally removed in a separate reviewed change.

- [ ] **Step 4: Commit plan or final verification docs if needed**

If only implementation commits exist and the working tree is clean, do not create an empty commit. If the implementation plan file is still uncommitted, run:

```bash
git add docs/superpowers/plans/2026-07-12-contract-transport-decisions.md
git commit -m "docs: plan contract transport decisions"
```

- [ ] **Step 5: Request final code review**

Use `superpowers:requesting-code-review` against the branch range from `git merge-base main HEAD` to `HEAD`. Resolve every Critical/Important finding, rerun Steps 1-3, then use `superpowers:finishing-a-development-branch` for merge/PR/cleanup.

---

## Final Verification Before Merge

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
git status --short --branch
```

Expected:

- formatting passes;
- clippy passes with `-D warnings`;
- all tests pass;
- branch is clean after commits;
- default OpenAI constructors reject HTTP;
- explicit `loopback` and `trusted_private` policies work as documented;
- Agent timeout grammar is Formal V1 only;
- README and breaking-change docs explain the A8 interface changes.
