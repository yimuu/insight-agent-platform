# Dependency Governance Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the approved dependency-governance remediation scope `R0 + R1 + R3 + R4` in one delivery wave while keeping each dependency-policy change independently reviewable.

**Architecture:** Add narrow project-owned boundaries before changing risky dependencies: a YAML adapter hides `yaml_serde`, a PostgreSQL URL policy hides SQLx TLS details, and the condition node hides the upstream CEL runtime. Compatible lockfile refresh stays separate from semantic replacement work so hidden dependency churn is easy to audit.

**Tech Stack:** Rust 1.94.1, Cargo, SQLx 0.9.0 with `tls-rustls-ring-webpki`, `cel` 0.14.0, `cel-parser` 0.10.1, `yaml_serde` 0.10.4, Tokio, Axum 0.7, serde/serde_json.

## Global Constraints

- Scope is exactly `R0 + R1 + R3 + R4` from `docs/reviews/2026-07-11-dependency-governance-review.md`.
- Do not upgrade JSON Schema to 0.47 in this branch.
- Do not upgrade Axum to 0.8 in this branch.
- Do not upgrade Reqwest to 0.13 in this branch.
- Do not align direct SHA-2 or thiserror versions in this branch.
- Do not upgrade SQLx itself in this branch.
- Do not add cargo-audit ignores or cargo-deny warning suppressions as a substitute for remediation.
- Do not change database schema, migrations, HTTP response shapes, SSE envelope shapes, event repository shapes, or Run status semantics.
- Remote TCP PostgreSQL must use `sslmode=verify-full`; exact local-development loopback and Unix socket URLs may remain plaintext.
- Replace `cel-interpreter`, prove `paste` is absent, and keep existing condition compile/runtime error codes stable.
- Replace direct `serde_yaml` usage with a project-owned adapter backed by `yaml_serde = "0.10.4"`.
- Refresh only `bytes 1.12.0 -> 1.12.1` and `regex 1.12.4 -> 1.13.0` for R4.

---

## File Structure

- `Cargo.toml`: remove `cel-interpreter` and `serde_yaml`; add `cel`, `yaml_serde`; add SQLx TLS feature.
- `Cargo.lock`: lock `yaml_serde`, `cel`, SQLx TLS dependencies, and compatible `bytes`/`regex` updates.
- `src/lib.rs`: export the crate-private YAML adapter module.
- `src/yaml.rs`: central YAML adapter with `from_str` and `to_value`.
- `src/dsl/raw.rs`: parse Agent YAML through `crate::yaml`.
- `src/config.rs`: parse platform YAML through `crate::yaml`; enforce PostgreSQL transport policy.
- `src/resources/config.rs`: parse model YAML through `crate::yaml`.
- `src/nodes/condition.rs`: replace `cel_interpreter` with `cel` behind local conversion/evaluation helpers.
- `tests/dsl_raw.rs`: YAML parser surface tests and removal of direct `serde_yaml`.
- `tests/platform_config_v1.rs`: PostgreSQL TLS policy tests.
- `tests/core_template_condition.rs`: CEL compatibility corpus.
- `README.md`: document local and remote PostgreSQL URL requirements.
- `docs/formal-v1-breaking-changes.md`: document the PostgreSQL TLS breaking contract.

## Task 1: R4 Compatible Lock Refresh

**Files:**
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: existing dependency graph.
- Produces: lockfile containing `bytes 1.12.1` and `regex 1.13.0`, with no unrelated version churn.

- [ ] **Step 1: Record current locked versions**

Run:

```bash
cargo tree -i bytes --locked
cargo tree -i regex --locked
```

Expected: `bytes v1.12.0` and `regex v1.12.4` appear in the locked graph.

- [ ] **Step 2: Apply only the approved compatible updates**

Run:

```bash
cargo update -p bytes --precise 1.12.1
cargo update -p regex --precise 1.13.0
```

- [ ] **Step 3: Verify the lockfile diff is limited**

Run:

```bash
git diff -- Cargo.lock
cargo tree -i bytes --locked
cargo tree -i regex --locked
```

Expected:

- `bytes` resolves to `1.12.1`.
- `regex` resolves to `1.13.0`.
- No Axum, Reqwest, JSON Schema, SQLx, SHA-2, thiserror, CEL, or YAML parser migration appears in this task's diff.

- [ ] **Step 4: Run focused verification**

Run:

```bash
cargo test --all-targets --quiet
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add Cargo.lock
git commit -m "chore: refresh compatible dependency locks"
```

## Task 2: R3 YAML Adapter and `yaml_serde` Migration

**Files:**
- Create: `src/yaml.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/lib.rs`
- Modify: `src/dsl/raw.rs`
- Modify: `src/config.rs`
- Modify: `src/resources/config.rs`
- Modify: `tests/dsl_raw.rs`

**Interfaces:**
- Consumes: `yaml_serde::{from_str, to_value, Value}`.
- Produces:
  - `crate::yaml::from_str<T>(source: &str, surface: YamlSurface) -> Result<T, String>`
  - `crate::yaml::to_value<T>(value: T) -> Result<yaml_serde::Value, String>`
  - `crate::yaml::YamlSurface::{Agent, Platform, ModelResources}`

- [ ] **Step 1: Write parser-surface tests before changing production parsing**

Append these tests to `tests/dsl_raw.rs`:

```rust
#[test]
fn agent_yaml_rejects_multi_document_streams() {
    let yaml = format!("{FORMAL_V1}\n---\nversion: 1\n");

    let error = parse_raw_agent(&yaml).unwrap_err();

    assert_eq!(error.code(), "DSL_YAML_INVALID");
}

#[test]
fn duration_spec_serializes_without_yaml_parser_dependency() {
    let agent = parse_raw_agent(FORMAL_V1).unwrap();
    let timeout = agent.nodes["start"].timeout.unwrap();

    let value = serde_json::to_value(timeout).unwrap();

    assert_eq!(value.as_str(), Some("1500ms"));
}
```

If the existing duration serialization test already exists with `serde_yaml::to_value`, replace only that conversion line with `serde_json::to_value(timeout).unwrap()`.

- [ ] **Step 2: Run tests and confirm the new surface is covered**

Run:

```bash
cargo test --test dsl_raw -- --nocapture
```

Expected: tests pass under the old parser, proving the desired compatibility behavior before the dependency swap.

- [ ] **Step 3: Replace the dependency declaration**

In `Cargo.toml`, replace:

```toml
serde_yaml = "0.9"
```

with:

```toml
yaml_serde = "0.10.4"
```

Do not use Cargo package rename to keep the import name `serde_yaml`.

- [ ] **Step 4: Create the YAML adapter**

Create `src/yaml.rs`:

```rust
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, Clone, Copy)]
pub(crate) enum YamlSurface {
    Agent,
    Platform,
    ModelResources,
}

impl YamlSurface {
    fn label(self) -> &'static str {
        match self {
            Self::Agent => "agent YAML",
            Self::Platform => "platform YAML",
            Self::ModelResources => "model resources YAML",
        }
    }
}

pub(crate) fn from_str<T>(source: &str, surface: YamlSurface) -> Result<T, String>
where
    T: DeserializeOwned,
{
    yaml_serde::from_str(source)
        .map_err(|error| format!("{} parse failed: {error}", surface.label()))
}

pub(crate) fn to_value<T>(value: T) -> Result<yaml_serde::Value, String>
where
    T: Serialize,
{
    yaml_serde::to_value(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{from_str, YamlSurface};
    use serde::Deserialize;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct SingleDocument {
        value: u32,
    }

    #[test]
    fn rejects_multi_document_streams() {
        let error = from_str::<SingleDocument>("value: 1\n---\nvalue: 2\n", YamlSurface::Agent)
            .unwrap_err();

        assert!(error.contains("agent YAML"));
        assert!(error.contains("more than one document"));
    }

    #[test]
    fn maps_errors_through_surface_labels() {
        let error = from_str::<SingleDocument>("value: [", YamlSurface::Platform).unwrap_err();

        assert!(error.contains("platform YAML"));
    }
}
```

- [ ] **Step 5: Export and route all production YAML parsing through the adapter**

In `src/lib.rs`, add:

```rust
pub(crate) mod yaml;
```

In `src/dsl/raw.rs`, replace the parser call with:

```rust
let agent: RawAgent =
    crate::yaml::from_str(yaml, crate::yaml::YamlSurface::Agent)
        .map_err(CompileError::yaml)?;
```

In `src/config.rs`, replace the parser call with:

```rust
let raw: PlatformYaml =
    crate::yaml::from_str(&yaml, crate::yaml::YamlSurface::Platform).map_err(|error| {
        PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            format!("invalid platform config: {error}"),
        )
    })?;
```

In `src/resources/config.rs`, replace the parser call with:

```rust
let raw: ModelResourcesYaml =
    crate::yaml::from_str(&yaml, crate::yaml::YamlSurface::ModelResources).map_err(|error| {
        ResourceConfigError::new(
            "MODEL_CONFIG_INVALID",
            format!("invalid model config: {error}"),
        )
    })?;
```

- [ ] **Step 6: Verify no direct `serde_yaml` production/test dependency remains**

Run:

```bash
rg -n "serde_yaml::|serde_yaml =" Cargo.toml src tests
rg -n "yaml_serde::" src tests
```

Expected:

- First command has no matches.
- Second command matches only `src/yaml.rs`.

- [ ] **Step 7: Run focused verification**

Run:

```bash
cargo test --test dsl_raw --test platform_config_v1 --test repository_agents_v1 --quiet
cargo test --lib yaml --quiet
```

Expected: PASS.

- [ ] **Step 8: Commit**

Run:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/yaml.rs src/dsl/raw.rs src/config.rs src/resources/config.rs tests/dsl_raw.rs
git commit -m "fix: route yaml parsing through maintained adapter"
```

## Task 3: R0 PostgreSQL TLS Contract

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/config.rs`
- Modify: `tests/platform_config_v1.rs`
- Modify: `README.md`
- Modify: `docs/formal-v1-breaking-changes.md`

**Interfaces:**
- Consumes: `sqlx::postgres::{PgConnectOptions, PgSslMode}`.
- Produces: `resolve_history` accepts plaintext only for exact local targets or Unix sockets; remote TCP URLs require `sslmode=verify-full`.

- [ ] **Step 1: Write failing PostgreSQL policy tests**

In `tests/platform_config_v1.rs`, update `postgres_history_secret_is_resolved_and_redacted` so `secret` is:

```rust
let secret = "postgres://user:password@database/private?sslmode=verify-full";
```

Append these tests near the existing PostgreSQL config test:

```rust
#[test]
fn postgres_history_requires_verify_full_for_remote_tcp() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for sslmode in [None, Some("prefer"), Some("allow"), Some("disable"), Some("require"), Some("verify-ca")] {
        let suffix = sslmode
            .map(|mode| format!("?sslmode={mode}"))
            .unwrap_or_default();
        let secret = format!("postgres://user:password@database/private{suffix}");
        let error = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.clone())]),
        )
        .unwrap_err();

        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
        assert!(error.to_string().contains("sslmode=verify-full"));
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains(&secret));
    }
}

#[test]
fn postgres_history_allows_exact_local_development_plaintext() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for secret in [
        "postgres://user:password@localhost/private",
        "postgres://user:password@127.0.0.1/private",
        "postgres://user:password@[::1]/private",
    ] {
        let config = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
        )
        .unwrap();

        assert_eq!(config.history.database_url(), Some(secret));
    }
}

#[test]
fn postgres_history_rejects_loopback_aliases_without_verify_full() {
    let yaml = base_yaml("  mode: disabled").replace(
        "history:\n  provider: sqlite\n  path: ../data/history.sqlite3",
        "history:\n  provider: postgres\n  database_url_env: HISTORY_URL",
    );
    let (_directory, path) = write_config(&yaml);

    for secret in [
        "postgres://user:password@127.1/private",
        "postgres://user:password@0:0:0:0:0:0:0:1/private",
    ] {
        let error = load(
            &path,
            BTreeMap::from([("HISTORY_URL".to_string(), secret.to_string())]),
        )
        .unwrap_err();

        assert_eq!(error.code(), "PLATFORM_CONFIG_INVALID");
    }
}
```

- [ ] **Step 2: Run tests and confirm the remote plaintext test fails**

Run:

```bash
cargo test --test platform_config_v1 postgres_history -- --nocapture
```

Expected: at least `postgres_history_requires_verify_full_for_remote_tcp` fails under the current prefix-only validation.

- [ ] **Step 3: Enable SQLx Rustls WebPKI TLS**

In `Cargo.toml`, add `tls-rustls-ring-webpki` to the existing SQLx feature list:

```toml
sqlx = { version = "0.9.0", features = ["runtime-tokio", "sqlite", "postgres", "chrono", "json", "migrate", "macros", "tls-rustls-ring-webpki"], default-features = false }
```

- [ ] **Step 4: Implement PostgreSQL URL validation**

In `src/config.rs`, extend imports:

```rust
use std::{
    collections::BTreeSet,
    env, fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use sqlx::postgres::{PgConnectOptions, PgSslMode};
```

In `resolve_history`, after `let database_url = required_secret(...)`, replace the string-prefix block with:

```rust
validate_postgres_history_url(database_url.expose())?;
```

Add these helpers near `resolve_history`:

```rust
fn validate_postgres_history_url(database_url: &str) -> Result<(), PlatformConfigError> {
    let options = PgConnectOptions::from_str(database_url).map_err(|_| {
        PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "PostgreSQL history URL must be a valid postgres:// or postgresql:// URL",
        )
    })?;

    if options.get_ssl_mode() == PgSslMode::VerifyFull {
        return Ok(());
    }
    if options.get_socket().is_some() || raw_postgres_host_is_exact_local(database_url) {
        return Ok(());
    }

    Err(PlatformConfigError::new(
        "PLATFORM_CONFIG_INVALID",
        "remote PostgreSQL history URL must set sslmode=verify-full; plaintext is allowed only for exact loopback or Unix socket development URLs",
    ))
}

fn raw_postgres_host_is_exact_local(database_url: &str) -> bool {
    raw_query_value(database_url, "host")
        .or_else(|| raw_query_value(database_url, "hostaddr"))
        .map(|host| exact_local_postgres_host(&host))
        .unwrap_or_else(|| {
            raw_authority_host(database_url)
                .as_deref()
                .map(exact_local_postgres_host)
                .unwrap_or(false)
        })
}

fn exact_local_postgres_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

fn raw_query_value(database_url: &str, name: &str) -> Option<String> {
    let query = database_url.split_once('?')?.1.split_once('#').map_or(
        database_url.split_once('?')?.1,
        |(query, _)| query,
    );
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

fn raw_authority_host(database_url: &str) -> Option<String> {
    let (_, rest) = database_url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        return Some(host_port[..=end].to_string());
    }
    Some(host_port.split(':').next().unwrap_or(host_port).to_string())
}
```

Keep diagnostics generic. Do not include `database_url`.

- [ ] **Step 5: Run focused verification**

Run:

```bash
cargo test --test platform_config_v1 postgres_history -- --nocapture
cargo test --test platform_config_v1 --quiet
```

Expected: PASS.

- [ ] **Step 6: Document the breaking transport contract**

In `README.md`, update the PostgreSQL section to show:

```yaml
history:
  provider: postgres
  database_url_env: RUN_HISTORY_DATABASE_URL
```

and add:

```text
Remote PostgreSQL URLs must include `sslmode=verify-full`. Plaintext PostgreSQL is accepted only for exact local development targets (`localhost`, `127.0.0.1`, `[::1]`) or Unix sockets.
```

Keep the existing local test command on `127.0.0.1`.

In `docs/formal-v1-breaking-changes.md`, add a short subsection under the platform/config area:

````markdown
## Dependency governance: PostgreSQL TLS transport

Remote PostgreSQL history URLs now require `sslmode=verify-full`. This intentionally breaks remote URLs that relied on SQLx's default `prefer` behavior because that mode can fall back to plaintext. Local development may keep exact loopback or Unix socket URLs.

Migration example:

```text
postgres://user:password@database/private
postgres://user:password@database/private?sslmode=verify-full
```
````

- [ ] **Step 7: Commit**

Run:

```bash
git add Cargo.toml Cargo.lock src/config.rs tests/platform_config_v1.rs README.md docs/formal-v1-breaking-changes.md
git commit -m "fix: require verified tls for remote postgres history"
```

## Task 4: R1 CEL Runtime Replacement

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/nodes/condition.rs`
- Modify: `tests/core_template_condition.rs`

**Interfaces:**
- Consumes: `cel::{Context, Program, Value, to_value}` and existing `cel_parser::Parser`.
- Produces: condition behavior equivalent for the frozen corpus, with `cel-interpreter` removed and `paste` absent.

- [ ] **Step 1: Add CEL behavior corpus tests before the dependency swap**

Append these tests near the existing condition tests in `tests/core_template_condition.rs`:

```rust
#[tokio::test]
async fn condition_preserves_json_value_corpus() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{
                    "when": "input.enabled && input.name == \"alpha\" && input.count == 3 && input.score > 1.5 && input.tags[1] == \"two\" && input.meta.region == \"apac\" && input.nullable == null",
                    "next": "done"
                }],
                "default": "fallback"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let outcome = ConditionNode
        .execute(
            &node,
            &run_context(json!({
                "enabled": true,
                "name": "alpha",
                "count": 3,
                "score": 2.25,
                "tags": ["one", "two"],
                "meta": {"region": "apac"},
                "nullable": null
            })),
            &control,
        )
        .await
        .unwrap();

    assert_eq!(outcome.transition, NodeTransition::Goto("done".to_string()));
}

#[tokio::test]
async fn condition_rejects_non_bool_results_at_runtime() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{"when": "input.kind", "next": "done"}],
                "default": "fallback"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let error = ConditionNode
        .execute(&node, &run_context(json!({"kind": "alpha"})), &control)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "CONDITION_RESULT_NOT_BOOL");
}
```

- [ ] **Step 2: Run condition tests before changing dependencies**

Run:

```bash
cargo test --test core_template_condition condition_ -- --nocapture
```

Expected: PASS under the old CEL runtime. If a corpus expression exposes a current unsupported behavior, simplify the expression to documented Formal V1 behavior before proceeding.

- [ ] **Step 3: Replace dependency declarations**

In `Cargo.toml`, replace:

```toml
cel-interpreter = "0.10.0"
cel-parser = "0.10.1"
```

with:

```toml
cel = "0.14.0"
cel-parser = "0.10.1"
```

Keep `cel-parser` because `src/dsl/references.rs` still uses its AST for compile-time reference extraction.

- [ ] **Step 4: Swap condition runtime imports and context conversion**

In `src/nodes/condition.rs`, replace:

```rust
use cel_interpreter::{Context as CelContext, Program as CelProgram, Value as CelValue};
```

with:

```rust
use cel::{Context as CelContext, Program as CelProgram, Value as CelValue};
```

Replace the variable-loading loop with:

```rust
for (name, value) in variables {
    let cel_value = cel::to_value(value.clone()).map_err(|error| {
        RunError::new(
            "CONDITION_CONTEXT_INVALID",
            format!("failed to prepare condition variable '{name}': {error}"),
        )
    })?;
    cel_context.add_variable_from_value(name, cel_value);
}
```

Keep the existing `Program::compile`, `program.execute`, and `CelValue::Bool` branches unless the compiler requires small signature adjustments.

- [ ] **Step 5: Verify focused CEL behavior and absence of `paste`**

Run:

```bash
cargo test --test core_template_condition condition_ -- --nocapture
cargo tree -i paste --locked
```

Expected:

- condition tests pass;
- `cargo tree -i paste --locked` exits nonzero with a message equivalent to "package ID specification `paste` did not match any packages".

- [ ] **Step 6: Run broader compile/runtime checks**

Run:

```bash
cargo test --test dsl_compiler --test dsl_parallel --test core_template_condition --quiet
cargo test --all-targets --quiet
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add Cargo.toml Cargo.lock src/nodes/condition.rs tests/core_template_condition.rs
git commit -m "fix: replace condition cel runtime"
```

## Task 5: Whole-Branch Dependency and Policy Verification

**Files:**
- Modify if needed: `README.md`
- Modify if needed: `docs/formal-v1-breaking-changes.md`
- Modify if needed: `docs/superpowers/specs/2026-07-12-dependency-governance-remediation-design.md`

**Interfaces:**
- Consumes: all previous task commits.
- Produces: final evidence that the selected dependency governance findings are remediated without hidden roadmap work.

- [ ] **Step 1: Check dependency graph remediation evidence**

Run:

```bash
rg -n "cel-interpreter|serde_yaml::|serde_yaml =" Cargo.toml Cargo.lock src tests
cargo tree -i paste --locked
cargo tree -i cel-interpreter --locked
cargo tree -i serde_yaml --locked
cargo tree -i yaml_serde --locked
cargo tree -i cel --locked
```

Expected:

- `rg` has no matches.
- `paste`, `cel-interpreter`, and `serde_yaml` tree commands fail because they are absent.
- `yaml_serde` and `cel` tree commands show the new dependency paths.

- [ ] **Step 2: Check R4 did not hide unrelated major upgrades**

Run:

```bash
cargo tree -i bytes --locked
cargo tree -i regex --locked
cargo tree -i axum --locked
cargo tree -i reqwest --locked
cargo tree -i jsonschema --locked
cargo tree -i sqlx --locked
```

Expected:

- `bytes` is `1.12.1`.
- `regex` is `1.13.0`.
- `axum` remains `0.7`.
- `reqwest` remains `0.12`.
- `jsonschema` remains `0.18`.
- `sqlx` remains `0.9.0`.

- [ ] **Step 3: Run formatting, linting, tests, audit, and deny**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
cargo audit
cargo deny check
```

Expected:

- fmt, clippy, tests, and audit pass.
- deny passes with no new policy failures. Existing duplicate/license warnings are acceptable only if they are the same residual warnings seen before this branch.

- [ ] **Step 4: Run optional real PostgreSQL gate when local environment is available**

Run when PostgreSQL is available:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

Expected: PASS. The local loopback URL remains valid without TLS by design.

- [ ] **Step 5: Review documentation and breaking-change rationale**

Run:

```bash
rg -n "sslmode=verify-full|yaml_serde|cel-interpreter|serde_yaml|paste" README.md docs/formal-v1-breaking-changes.md docs/superpowers/specs/2026-07-12-dependency-governance-remediation-design.md docs/reviews/2026-07-11-dependency-governance-review.md
```

Expected:

- README and breaking-change docs mention the PostgreSQL TLS contract.
- The design mentions `yaml_serde` as the selected YAML parser.
- Historical review docs may still mention old dependency findings; production docs must not instruct users to rely on `cel-interpreter`, `serde_yaml`, or `paste`.

- [ ] **Step 6: Commit any final documentation or cleanup**

If Step 5 requires documentation edits, commit them:

```bash
git add README.md docs/formal-v1-breaking-changes.md docs/superpowers/specs/2026-07-12-dependency-governance-remediation-design.md
git commit -m "docs: document dependency governance remediation"
```

If no edits are required, do not create an empty commit.

## Final Acceptance Checklist

- [ ] `Cargo.toml` no longer depends on `cel-interpreter`.
- [ ] `Cargo.toml` no longer depends on `serde_yaml`.
- [ ] `src` and `tests` do not call `serde_yaml::`.
- [ ] `src` and `tests` do not call `yaml_serde::` outside `src/yaml.rs`.
- [ ] SQLx remains `0.9.0` and includes `tls-rustls-ring-webpki`.
- [ ] Remote PostgreSQL URLs without `sslmode=verify-full` fail config validation.
- [ ] Exact local PostgreSQL development URLs still pass config validation.
- [ ] `cargo tree -i paste --locked` proves `paste` is absent.
- [ ] `bytes` is `1.12.1` and `regex` is `1.13.0`.
- [ ] No JSON Schema, Axum, Reqwest, SHA-2, thiserror, or SQLx upgrade is hidden in the branch.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [ ] `cargo test --all-targets --quiet` passes.
- [ ] `cargo audit` passes.
- [ ] `cargo deny check` passes.
