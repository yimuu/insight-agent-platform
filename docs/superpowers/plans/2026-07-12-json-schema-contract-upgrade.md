# JSON Schema Contract Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade JSON Schema validation to `jsonschema` 0.47 behind a stable project adapter with explicit Draft 7 and no external reference retrieval.

**Architecture:** Add `src/schema.rs` as the only production boundary over upstream `jsonschema`. Runtime, compiler, resources, and test fixtures consume `JsonSchemaValidator` instead of upstream `jsonschema::JSONSchema`, so future upstream API/default-feature changes do not leak into platform code. Disable upstream default features to avoid HTTP/file/CLI/TLS surfaces that the platform does not use.

**Tech Stack:** Rust 1.94.1, `jsonschema = { version = "0.47", default-features = false }`, serde_json, existing Agent compiler/runtime/resource tests, cargo-audit, cargo-deny.

## Global Constraints

- Scope is only JSON Schema contract and upgrade; do not upgrade Axum, Reqwest, SQLx, SHA-2, thiserror, CEL, YAML, Tokio, or history storage.
- Use `jsonschema = { version = "0.47", default-features = false }`.
- Do not enable `resolve-http`, `resolve-file`, `cli`, `tls-*`, `macros`, or async reference resolution features.
- The platform default JSON Schema draft is Draft 7.
- Schemas without `$schema` compile as Draft 7.
- Schemas with `$schema` equal to `http://json-schema.org/draft-07/schema#` or `https://json-schema.org/draft-07/schema#` compile as Draft 7.
- Schemas with any non-Draft-7 `$schema` value fail schema compilation.
- `$ref` values must be internal document references beginning with `#`; external, relative, HTTP(S), and file references fail schema compilation.
- Business code must not expose upstream `jsonschema` types, validation errors, registries, structured output, schema paths, or instance paths.
- Public runtime/API error codes and messages remain stable.
- Instance validation errors must not include rejected input, Action output, model parameters, or other instance payloads.
- The Rust API change is intentional: `CompiledAgent.input_schema`, `RegisteredAction` internals, and `OpenAiChatModel` internals use `JsonSchemaValidator` instead of upstream `jsonschema::JSONSchema`.

---

## File Structure

- Create `src/schema.rs`: project-owned JSON Schema adapter and unit tests.
- Modify `src/lib.rs`: expose `pub mod schema;` because `CompiledAgent` is public and stores `JsonSchemaValidator`.
- Modify `Cargo.toml` and `Cargo.lock`: upgrade `jsonschema` and disable default features.
- Modify `src/dsl/compiler.rs`: compile Agent input schema through the adapter.
- Modify `src/dsl/compiled.rs`: store `Arc<JsonSchemaValidator>`.
- Modify `src/resources/actions.rs`: compile and validate Action input/output schemas through the adapter.
- Modify `src/resources/openai_chat.rs`: compile and validate OpenAI parameter schema through the adapter.
- Modify `src/runtime/service.rs` and `src/runtime/coordinator.rs`: no behavior change; update internal test fixture construction if direct upstream imports remain.
- Modify integration test fixtures in `tests/api.rs`, `tests/run_scheduler.rs`, `tests/run_service.rs`, and `tests/run_coordinator.rs`: construct validators through `compile_schema`.
- Modify `tests/dsl_compiler.rs`: add schema contract tests for non-Draft-7 `$schema` and external `$ref`.
- Modify `tests/resource_registries.rs`: add Action registry schema policy tests.
- Modify `docs/formal-v1-breaking-changes.md`: document the internal Rust API/dependency contract change and reason.

---

## Task 1: Add Schema Adapter and Upgrade Dependency

**Files:**
- Create: `src/schema.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Test: `src/schema.rs` unit tests

**Interfaces:**
- Produces: `pub struct JsonSchemaValidator`
- Produces: `pub fn compile_schema(schema: &serde_json::Value) -> Result<JsonSchemaValidator, String>`
- Produces: `impl JsonSchemaValidator { pub fn is_valid(&self, value: &serde_json::Value) -> bool }`

- [ ] **Step 1: Export the module and write the failing adapter tests**

In `src/lib.rs`, add this line near the other top-level modules:

```rust
pub mod schema;
```

Create `src/schema.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::compile_schema;

    #[test]
    fn validates_basic_object_schema() {
        let validator = compile_schema(&json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1}
            },
            "additionalProperties": false
        }))
        .unwrap();

        assert!(validator.is_valid(&json!({"name": "demo"})));
        assert!(!validator.is_valid(&json!({})));
        assert!(!validator.is_valid(&json!({"name": "demo", "extra": true})));
    }

    #[test]
    fn missing_schema_uses_draft7_tuple_items_behavior() {
        let validator = compile_schema(&json!({
            "type": "array",
            "items": [{"type": "string"}],
            "additionalItems": false
        }))
        .unwrap();

        assert!(validator.is_valid(&json!(["ok"])));
        assert!(!validator.is_valid(&json!(["ok", "extra"])));
    }

    #[test]
    fn accepts_explicit_draft7_schema_uris() {
        for uri in [
            "http://json-schema.org/draft-07/schema#",
            "https://json-schema.org/draft-07/schema#",
        ] {
            let validator = compile_schema(&json!({
                "$schema": uri,
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}}
            }))
            .unwrap();

            assert!(validator.is_valid(&json!({"id": "agent"})));
            assert!(!validator.is_valid(&json!({"id": 1})));
        }
    }

    #[test]
    fn rejects_non_draft7_schema_uri() {
        let error = compile_schema(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object"
        }))
        .unwrap_err();

        assert!(error.contains("unsupported JSON Schema draft"));
    }

    #[test]
    fn allows_internal_refs() {
        let validator = compile_schema(&json!({
            "type": "object",
            "definitions": {
                "name": {"type": "string", "minLength": 1}
            },
            "required": ["name"],
            "properties": {
                "name": {"$ref": "#/definitions/name"}
            }
        }))
        .unwrap();

        assert!(validator.is_valid(&json!({"name": "alice"})));
        assert!(!validator.is_valid(&json!({"name": ""})));
    }

    #[test]
    fn rejects_external_refs_before_upstream_resolution() {
        for reference in [
            "https://example.invalid/schema.json",
            "http://example.invalid/schema.json",
            "file:///tmp/schema.json",
            "schemas/shared.json#/defs/name",
        ] {
            let error = compile_schema(&json!({"$ref": reference})).unwrap_err();
            assert!(error.contains("external JSON Schema references are not supported"));
        }
    }
}
```

- [ ] **Step 2: Run the focused test and confirm it fails before implementation**

Run:

```bash
cargo test schema:: -- --nocapture
```

Expected: compile failure because `compile_schema` is not implemented.

- [ ] **Step 3: Change the dependency line**

In `Cargo.toml`, replace:

```toml
jsonschema = "0.18"
```

With:

```toml
jsonschema = { version = "0.47", default-features = false }
```

Run:

```bash
cargo update -p jsonschema --precise 0.47.0
```

Expected: `Cargo.lock` moves `jsonschema` to `0.47.0` and removes the old `jsonschema -> reqwest` default-feature path.

- [ ] **Step 4: Implement the adapter**

Replace `src/schema.rs` with this complete file, keeping the tests from Step 1 at the bottom:

```rust
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct JsonSchemaValidator {
    inner: jsonschema::Validator,
}

pub fn compile_schema(schema: &Value) -> Result<JsonSchemaValidator, String> {
    validate_schema_policy(schema)?;
    let inner = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft7)
        .build(schema)
        .map_err(|error| error.to_string())?;
    Ok(JsonSchemaValidator { inner })
}

impl JsonSchemaValidator {
    pub fn is_valid(&self, value: &Value) -> bool {
        self.inner.is_valid(value)
    }
}

fn validate_schema_policy(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(schema_uri) = object.get("$schema").and_then(Value::as_str) {
                validate_schema_uri(schema_uri)?;
            }
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                validate_reference(reference)?;
            }
            for value in object.values() {
                validate_schema_policy(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_schema_policy(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn validate_schema_uri(uri: &str) -> Result<(), String> {
    match uri {
        "http://json-schema.org/draft-07/schema#"
        | "https://json-schema.org/draft-07/schema#" => Ok(()),
        _ => Err(format!(
            "unsupported JSON Schema draft '{uri}'; only Draft 7 is supported"
        )),
    }
}

fn validate_reference(reference: &str) -> Result<(), String> {
    if reference.starts_with('#') {
        Ok(())
    } else {
        Err("external JSON Schema references are not supported".to_string())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::compile_schema;

    #[test]
    fn validates_basic_object_schema() {
        let validator = compile_schema(&json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1}
            },
            "additionalProperties": false
        }))
        .unwrap();

        assert!(validator.is_valid(&json!({"name": "demo"})));
        assert!(!validator.is_valid(&json!({})));
        assert!(!validator.is_valid(&json!({"name": "demo", "extra": true})));
    }

    #[test]
    fn missing_schema_uses_draft7_tuple_items_behavior() {
        let validator = compile_schema(&json!({
            "type": "array",
            "items": [{"type": "string"}],
            "additionalItems": false
        }))
        .unwrap();

        assert!(validator.is_valid(&json!(["ok"])));
        assert!(!validator.is_valid(&json!(["ok", "extra"])));
    }

    #[test]
    fn accepts_explicit_draft7_schema_uris() {
        for uri in [
            "http://json-schema.org/draft-07/schema#",
            "https://json-schema.org/draft-07/schema#",
        ] {
            let validator = compile_schema(&json!({
                "$schema": uri,
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}}
            }))
            .unwrap();

            assert!(validator.is_valid(&json!({"id": "agent"})));
            assert!(!validator.is_valid(&json!({"id": 1})));
        }
    }

    #[test]
    fn rejects_non_draft7_schema_uri() {
        let error = compile_schema(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object"
        }))
        .unwrap_err();

        assert!(error.contains("unsupported JSON Schema draft"));
    }

    #[test]
    fn allows_internal_refs() {
        let validator = compile_schema(&json!({
            "type": "object",
            "definitions": {
                "name": {"type": "string", "minLength": 1}
            },
            "required": ["name"],
            "properties": {
                "name": {"$ref": "#/definitions/name"}
            }
        }))
        .unwrap();

        assert!(validator.is_valid(&json!({"name": "alice"})));
        assert!(!validator.is_valid(&json!({"name": ""})));
    }

    #[test]
    fn rejects_external_refs_before_upstream_resolution() {
        for reference in [
            "https://example.invalid/schema.json",
            "http://example.invalid/schema.json",
            "file:///tmp/schema.json",
            "schemas/shared.json#/defs/name",
        ] {
            let error = compile_schema(&json!({"$ref": reference})).unwrap_err();
            assert!(error.contains("external JSON Schema references are not supported"));
        }
    }
}
```

- [ ] **Step 5: Verify the adapter**

Run:

```bash
cargo test schema:: -- --nocapture
cargo tree -i jsonschema --locked
cargo tree -i reqwest --locked
```

Expected:

- `schema::` tests pass.
- `jsonschema v0.47.0` appears.
- `reqwest` still appears because the project directly uses it, but `jsonschema` is not in the inverted `reqwest` path.

- [ ] **Step 6: Commit Task 1**

Run:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/schema.rs
git commit -m "feat: add json schema validation adapter"
```

---

## Task 2: Route Production Code Through the Adapter

**Files:**
- Modify: `src/dsl/compiler.rs`
- Modify: `src/dsl/compiled.rs`
- Modify: `src/resources/actions.rs`
- Modify: `src/resources/openai_chat.rs`
- Test: existing compiler/action/model tests

**Interfaces:**
- Consumes: `crate::schema::{compile_schema, JsonSchemaValidator}`
- Produces: no new public runtime behavior
- Changes: `CompiledAgent.input_schema` type from `Arc<jsonschema::JSONSchema>` to `Arc<JsonSchemaValidator>`

- [ ] **Step 1: Update compiled Agent type**

In `src/dsl/compiled.rs`, replace:

```rust
use jsonschema::JSONSchema;
```

With:

```rust
use crate::schema::JsonSchemaValidator;
```

Replace:

```rust
pub input_schema: Arc<JSONSchema>,
```

With:

```rust
pub input_schema: Arc<JsonSchemaValidator>,
```

- [ ] **Step 2: Update Agent compiler schema compilation**

In `src/dsl/compiler.rs`, replace:

```rust
use jsonschema::JSONSchema;
```

With:

```rust
use crate::schema::compile_schema;
```

Replace:

```rust
let input_schema = Arc::new(JSONSchema::compile(&raw.input.schema).map_err(|error| {
    CompileError::new(
        "INPUT_SCHEMA_INVALID",
        format!("agent '{}' input schema is invalid: {error}", raw.id),
    )
})?);
```

With:

```rust
let input_schema = Arc::new(compile_schema(&raw.input.schema).map_err(|error| {
    CompileError::new(
        "INPUT_SCHEMA_INVALID",
        format!("agent '{}' input schema is invalid: {error}", raw.id),
    )
})?);
```

- [ ] **Step 3: Update Action schema compilation**

In `src/resources/actions.rs`, replace:

```rust
use jsonschema::JSONSchema;
```

With:

```rust
use crate::schema::{compile_schema, JsonSchemaValidator};
```

Replace the validator fields:

```rust
input_validator: JSONSchema,
output_validator: JSONSchema,
```

With:

```rust
input_validator: JsonSchemaValidator,
output_validator: JsonSchemaValidator,
```

Replace the `validate_json` signature:

```rust
fn validate_json(
    validator: &JSONSchema,
    value: &Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), RunError> {
```

With:

```rust
fn validate_json(
    validator: &JsonSchemaValidator,
    value: &Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), RunError> {
```

Replace schema compilation:

```rust
let input_validator = JSONSchema::compile(&descriptor.input_schema).map_err(|error| {
    CompileError::new(
        "ACTION_INPUT_SCHEMA_INVALID",
        format!("action '{name}' input schema is invalid: {error}"),
    )
})?;
let output_validator = JSONSchema::compile(&descriptor.output_schema).map_err(|error| {
    CompileError::new(
        "ACTION_OUTPUT_SCHEMA_INVALID",
        format!("action '{name}' output schema is invalid: {error}"),
    )
})?;
```

With:

```rust
let input_validator = compile_schema(&descriptor.input_schema).map_err(|error| {
    CompileError::new(
        "ACTION_INPUT_SCHEMA_INVALID",
        format!("action '{name}' input schema is invalid: {error}"),
    )
})?;
let output_validator = compile_schema(&descriptor.output_schema).map_err(|error| {
    CompileError::new(
        "ACTION_OUTPUT_SCHEMA_INVALID",
        format!("action '{name}' output schema is invalid: {error}"),
    )
})?;
```

- [ ] **Step 4: Update OpenAI parameter schema compilation**

In `src/resources/openai_chat.rs`, replace:

```rust
use jsonschema::JSONSchema;
```

With:

```rust
use crate::schema::{compile_schema, JsonSchemaValidator};
```

Replace:

```rust
parameter_validator: std::sync::Arc<JSONSchema>,
```

With:

```rust
parameter_validator: std::sync::Arc<JsonSchemaValidator>,
```

Replace:

```rust
let parameter_validator = JSONSchema::compile(&parameter_schema()).map_err(|_| {
    CompileError::new(
        "MODEL_CONFIG_INVALID",
        "failed to compile OpenAI parameter schema",
    )
})?;
```

With:

```rust
let parameter_validator = compile_schema(&parameter_schema()).map_err(|_| {
    CompileError::new(
        "MODEL_CONFIG_INVALID",
        "failed to compile OpenAI parameter schema",
    )
})?;
```

- [ ] **Step 5: Run focused production tests**

Run:

```bash
cargo test --test dsl_compiler --test core_chat_action --test formal_resources --quiet
```

Expected: all tests in those three integration test targets pass after direct production usage is migrated.

- [ ] **Step 6: Verify no production direct upstream usage remains outside the adapter**

Run:

```bash
rg -n "jsonschema::|JSONSchema" src
```

Expected: matches only in `src/schema.rs` before test modules, plus the string `JSON Schema` in docs/comments if any. No `jsonschema::JSONSchema` remains.

- [ ] **Step 7: Commit Task 2**

Run:

```bash
git add src/dsl/compiler.rs src/dsl/compiled.rs src/resources/actions.rs src/resources/openai_chat.rs
git commit -m "refactor: route schema validation through adapter"
```

---

## Task 3: Migrate Test Fixtures and Add Contract Coverage

**Files:**
- Modify: `tests/api.rs`
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/run_service.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `src/runtime/service.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/resource_registries.rs`
- Test: affected integration tests

**Interfaces:**
- Consumes: `insight_agent_platform::schema::compile_schema`
- Produces: tests that prove Draft 7 policy, external `$ref` rejection, and stable boundary errors

- [ ] **Step 1: Replace integration fixture imports**

In each integration test file that currently imports `jsonschema::JSONSchema`, replace that import with `compile_schema`.

Example replacement for `tests/api.rs`:

```rust
use insight_agent_platform::{
    api::{ApiAuth, ApiConfig, ApiState},
    dsl::{
        compiled::{CompiledAgent, CompiledNode, ExecutionPlan, NodeControl, RunOutput},
        EmitPolicy,
    },
    events::EventHub,
    history::{RunRepository, RunStatus, SqliteRunRepository},
    nodes::NodeExecutorRegistry,
    runtime::{
        ExecutionControl, RunAttachment, RunError, RunService, RunServiceConfig,
        ServiceError,
    },
    schema::compile_schema,
};
```

Then replace fixture construction like:

```rust
input_schema: Arc::new(
    JSONSchema::compile(&json!({
        "type": "object",
        "required": ["text"],
        "additionalProperties": false,
        "properties": {"text": {"type": "string"}}
    }))
    .unwrap(),
),
```

With:

```rust
input_schema: Arc::new(
    compile_schema(&json!({
        "type": "object",
        "required": ["text"],
        "additionalProperties": false,
        "properties": {"text": {"type": "string"}}
    }))
    .unwrap(),
),
```

Apply the same pattern in:

- `tests/run_scheduler.rs`
- `tests/run_service.rs`
- `tests/run_coordinator.rs`

- [ ] **Step 2: Replace internal unit test construction in `src/runtime/service.rs`**

In the unit test area of `src/runtime/service.rs`, replace:

```rust
input_schema: Arc::new(jsonschema::JSONSchema::compile(&json!({})).unwrap()),
```

With:

```rust
input_schema: Arc::new(crate::schema::compile_schema(&json!({})).unwrap()),
```

- [ ] **Step 3: Add Agent compiler schema policy tests**

Append these tests to `tests/dsl_compiler.rs`:

```rust
#[test]
fn compiler_rejects_non_draft7_input_schema_uri() {
    let yaml = r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema:
    $schema: https://json-schema.org/draft/2020-12/schema
    type: object
prompts: {}
entry: done
nodes:
  done:
    kind: test.terminal
    config: {}
"#;

    let (_temp, root) = write_agent(yaml, "");
    let error = compiler().compile_dir(&root).unwrap_err();

    assert_eq!(error.code(), "INPUT_SCHEMA_INVALID");
    assert!(error.to_string().contains("unsupported JSON Schema draft"));
}

#[test]
fn compiler_rejects_external_input_schema_ref() {
    let yaml = r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema:
    $ref: https://example.invalid/schema.json
prompts: {}
entry: done
nodes:
  done:
    kind: test.terminal
    config: {}
"#;

    let (_temp, root) = write_agent(yaml, "");
    let error = compiler().compile_dir(&root).unwrap_err();

    assert_eq!(error.code(), "INPUT_SCHEMA_INVALID");
    assert!(error
        .to_string()
        .contains("external JSON Schema references are not supported"));
}
```

- [ ] **Step 4: Add Action registry schema policy tests**

Append this helper and tests to `tests/resource_registries.rs`:

```rust
#[derive(Debug, Clone, Copy)]
struct SchemaPolicyAction {
    input_schema: fn() -> serde_json::Value,
    output_schema: fn() -> serde_json::Value,
}

#[async_trait::async_trait]
impl Action for SchemaPolicyAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "schema_policy",
            input_schema: (self.input_schema)(),
            output_schema: (self.output_schema)(),
            idempotent: true,
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(input)
    }
}

fn empty_object_schema() -> serde_json::Value {
    json!({"type": "object"})
}

fn draft202012_schema() -> serde_json::Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object"
    })
}

fn external_ref_schema() -> serde_json::Value {
    json!({"$ref": "file:///tmp/shared-schema.json"})
}

#[test]
fn action_registry_rejects_non_draft7_input_schema_uri() {
    let mut registry = ActionRegistry::default();
    let error = registry
        .register(SchemaPolicyAction {
            input_schema: draft202012_schema,
            output_schema: empty_object_schema,
        })
        .unwrap_err();

    assert_eq!(error.code(), "ACTION_INPUT_SCHEMA_INVALID");
    assert!(error.to_string().contains("unsupported JSON Schema draft"));
}

#[test]
fn action_registry_rejects_external_output_schema_ref() {
    let mut registry = ActionRegistry::default();
    let error = registry
        .register(SchemaPolicyAction {
            input_schema: empty_object_schema,
            output_schema: external_ref_schema,
        })
        .unwrap_err();

    assert_eq!(error.code(), "ACTION_OUTPUT_SCHEMA_INVALID");
    assert!(error
        .to_string()
        .contains("external JSON Schema references are not supported"));
}
```

If the file does not already import these names, add them to the existing imports:

```rust
use insight_agent_platform::resources::actions::{Action, ActionContext, ActionDescriptor, ActionRegistry};
use insight_agent_platform::runtime::RunError;
use serde_json::{json, Value};
```

- [ ] **Step 5: Run the affected tests**

Run:

```bash
cargo test --test api --test run_scheduler --test run_service --test run_coordinator --test dsl_compiler --test resource_registries --quiet
```

Expected: all affected tests pass, including new rejection tests.

- [ ] **Step 6: Verify all direct upstream usage is isolated**

Run:

```bash
rg -n "jsonschema::|JSONSchema" src tests
```

Expected: production direct usage is only in `src/schema.rs`. Tests should use `compile_schema`; if a remaining test imports `jsonschema::`, it must be removed unless it explicitly tests upstream behavior. This plan expects no direct `jsonschema::JSONSchema` in tests.

- [ ] **Step 7: Commit Task 3**

Run:

```bash
git add tests/api.rs tests/run_scheduler.rs tests/run_service.rs tests/run_coordinator.rs src/runtime/service.rs tests/dsl_compiler.rs tests/resource_registries.rs
git commit -m "test: cover json schema contract policy"
```

---

## Task 4: Document API Change and Run Final Dependency Gates

**Files:**
- Modify: `docs/formal-v1-breaking-changes.md`
- Verify: `Cargo.toml`, `Cargo.lock`, `src`, `tests`

**Interfaces:**
- Consumes: completed adapter and test migration
- Produces: documented reason for the Rust API/dependency boundary change

- [ ] **Step 1: Document the schema validator boundary change**

Append this section to `docs/formal-v1-breaking-changes.md`:

```markdown
## JSON Schema validator boundary

`CompiledAgent.input_schema` and resource internals now use the project-owned `JsonSchemaValidator` adapter instead of exposing the upstream `jsonschema::JSONSchema` type.

Reason: the platform needs a stable validation contract while upgrading `jsonschema` from 0.18 to 0.47. The adapter fixes the platform default at Draft 7, disables upstream HTTP/file/CLI default features, rejects non-Draft-7 `$schema` values, rejects external `$ref`, and keeps runtime validation errors redacted.

Runtime API behavior is unchanged: invalid run input, Action input/output, and OpenAI model parameters keep the existing public error codes and fixed messages.
```

- [ ] **Step 2: Run formatting and static analysis**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both commands exit 0.

- [ ] **Step 3: Run the full test suite**

Run:

```bash
cargo test --all-targets --quiet
```

Expected: all tests pass.

- [ ] **Step 4: Run dependency policy gates**

Run:

```bash
cargo audit
cargo deny check
```

Expected:

- `cargo audit` exits 0.
- `cargo deny check` exits 0. Existing duplicate/license warnings may remain, but this branch must not add advisory, license, source, or ban failures.

- [ ] **Step 5: Verify dependency graph and feature boundary**

Run:

```bash
rg -n 'jsonschema = ' Cargo.toml
rg -n "jsonschema::|JSONSchema" src tests
cargo tree -i jsonschema --locked
cargo tree -i reqwest --locked
cargo tree -i clap --locked
cargo tree -e features -i jsonschema --locked
```

Expected:

- `Cargo.toml` contains `jsonschema = { version = "0.47", default-features = false }`.
- `jsonschema::` production usage is isolated to `src/schema.rs`.
- `jsonschema v0.47.0` is present.
- `reqwest` is not pulled by `jsonschema`.
- `clap` is absent; `cargo tree -i clap --locked` exits nonzero with a package-not-found message unless another unrelated dependency introduces it later.
- `jsonschema` features do not include `resolve-http`, `resolve-file`, `cli`, `tls-aws-lc-rs`, `tls-ring`, `macros`, or `resolve-async`.

- [ ] **Step 6: Commit Task 4**

Run:

```bash
git add docs/formal-v1-breaking-changes.md
git commit -m "docs: document json schema adapter boundary"
```

---

## Final Review Gate

After Task 4, run a broad branch review against the branch base.

Reviewer requirements:

- Confirm only JSON Schema contract/upgrade files changed.
- Confirm `jsonschema = { version = "0.47", default-features = false }`.
- Confirm no Axum, Reqwest, SQLx, SHA-2, thiserror, CEL, YAML, or history storage upgrade/change was introduced.
- Confirm Draft 7, `$schema`, and `$ref` policies are implemented and tested.
- Confirm public runtime/API error codes and messages remain stable.
- Confirm direct upstream `jsonschema` production usage is isolated to `src/schema.rs`.
- Confirm final gates passed.

Do not merge until Critical and Important review findings are fixed and re-reviewed.
