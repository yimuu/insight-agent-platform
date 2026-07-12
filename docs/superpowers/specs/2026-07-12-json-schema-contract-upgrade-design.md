# JSON Schema Contract Upgrade Design

## Context

The platform uses JSON Schema at four trust boundaries:

- Agent input schema compilation in `src/dsl/compiler.rs`.
- Agent input validation before run preparation in `src/runtime/service.rs` and run recovery validation in `src/runtime/coordinator.rs`.
- Action input/output schema compilation and validation in `src/resources/actions.rs`.
- OpenAI-compatible model parameter validation in `src/resources/openai_chat.rs`.

The current direct dependency is `jsonschema = "0.18"`. Local `cargo info jsonschema --verbose` reports `0.18.3` with default features `resolve-http`, `resolve-file`, and `cli`, and reports latest `0.47.0`. Local `cargo info jsonschema@0.47.0 --verbose` reports default features `resolve-http`, `resolve-file`, and `tls-aws-lc-rs`, with optional `reqwest 0.13` retrieval support behind resolution features. Those defaults are broader than the platform's current use: this codebase only needs in-process boolean validation of schemas embedded in repository/platform data.

The upstream `jsonschema` project documents several breaking changes between these lines:

- The modern crate supports multiple JSON Schema drafts, external network/file reference fetching, structured output reports, meta-schema validation, and a CLI.
- The crate requires Rust 1.85.0 or later, which fits this repository's pinned Rust 1.94.1 toolchain.
- The default draft changed to Draft 2020-12 in the 0.25 line.
- The 0.46 line changed registry construction to an explicit prepare step.
- `jsonschema::options()` can configure draft behavior, and docs.rs warns that synchronous build must not run inside an async runtime when schemas require network-backed external references.

The repository currently has no checked-in `$schema`, `$ref`, `$defs`, or `definitions` usage in `src`, `tests`, `README.md`, or formal docs. This makes a narrow contract possible: upgrade the validator while preserving current platform semantics instead of adopting every new upstream feature.

## Goal

Upgrade the direct JSON Schema dependency to `jsonschema = "0.47"` behind a project-owned schema adapter, while making the platform's JSON Schema contract explicit, stable, and narrower than upstream defaults.

## Non-goals

- Do not upgrade Axum, Reqwest, SQLx, SHA-2, thiserror, CEL, or YAML in this branch.
- Do not add external network or file resolution for `$ref`.
- Do not introduce schema registry management for user-provided shared schemas.
- Do not expose structured JSON Schema output reports in public API/event payloads.
- Do not change the outer platform error codes/messages for input, action, or model parameter validation.
- Do not attempt to eliminate all `cargo deny` duplicate warnings in this branch.

## Selected approach

Use a project-owned adapter module, `src/schema.rs`, backed by `jsonschema = { version = "0.47", default-features = false }`.

The adapter owns:

- Upstream type names.
- Draft selection.
- Feature boundary.
- Error normalization.
- Validation entry points used by runtime/resource/compiler code.

Business code should depend on project types and functions, not directly on upstream `jsonschema` types.

## JSON Schema contract

### Draft policy

The platform default is Draft 7.

Reason: `jsonschema` 0.25 changed the upstream default to Draft 2020-12. Keeping the platform default at Draft 7 avoids silent behavior drift for existing schemas that omit `$schema`.

The adapter must compile schemas with an explicit draft rather than relying on upstream auto/default behavior.

Initial scope:

- Schemas without `$schema` compile as Draft 7.
- Schemas with `$schema` equal to `http://json-schema.org/draft-07/schema#` or `https://json-schema.org/draft-07/schema#` compile as Draft 7.
- Schemas with any non-Draft-7 `$schema` value are rejected with a schema compilation error.
- Custom/unknown meta-schemas are not supported.

### Reference policy

External `$ref` resolution is not supported in this phase.

The dependency must use `default-features = false` so `resolve-http`, `resolve-file`, and `cli` are not enabled by default. No code should perform network or filesystem retrieval during schema compilation or validation.

Allowed:

- Plain inline object schemas.
- Internal references within the same schema document when supported by Draft 7 and resolved entirely in memory.
- Internal composition keywords supported under Draft 7 when they do not require external retrieval.

Rejected / unsupported:

- HTTP(S) external `$ref`.
- `file:` external `$ref`.
- Custom meta-schema registries.
- Runtime schema retrieval from async contexts.

### Validation policy

The adapter exposes boolean validation only:

```rust
pub struct JsonSchemaValidator { /* private */ }

pub fn compile_schema(schema: &serde_json::Value) -> Result<JsonSchemaValidator, String>;

impl JsonSchemaValidator {
    pub fn is_valid(&self, value: &serde_json::Value) -> bool;
}
```

Names may be adjusted during implementation if they better match existing project conventions, but the adapter boundary must remain.

The adapter should not expose `ValidationError`, `ErrorIterator`, structured output, instance paths, schema paths, or upstream registry types to platform business code.

### Error policy

Existing outer error contracts remain stable:

- Invalid Agent input schema: `INPUT_SCHEMA_INVALID`.
- Invalid run input: `INPUT_INVALID` with message `input does not match the agent schema`.
- Invalid Action input schema: `ACTION_INPUT_SCHEMA_INVALID`.
- Invalid Action output schema: `ACTION_OUTPUT_SCHEMA_INVALID`.
- Invalid Action input: `ACTION_INPUT_INVALID` with message `action input validation failed`.
- Invalid Action output: `ACTION_OUTPUT_INVALID` with message `action output validation failed`.
- Invalid OpenAI parameter schema: `MODEL_CONFIG_INVALID` with message `failed to compile OpenAI parameter schema`.
- Invalid OpenAI parameters: `MODEL_PARAMETERS_INVALID` with message `OpenAI parameters do not match the allowed schema`.

Schema compilation errors may include schema-level diagnostics because schemas are developer/operator controlled. Instance validation errors must not include the rejected instance or Action/model output payload.

## Component design

### `src/schema.rs`

Create a focused adapter module with one responsibility: compile and validate JSON values against the platform's JSON Schema contract.

Responsibilities:

- Use `jsonschema` 0.47 APIs.
- Select Draft 7 explicitly.
- Hide upstream validator type names.
- Return sanitized compile errors as `String`.
- Provide `is_valid`.
- Contain all direct `jsonschema::` usage outside tests.

The module should include unit tests for:

- Basic object validation.
- Required properties.
- `additionalProperties: false`.
- Number and string constraints used by platform schemas.
- Explicit proof that missing `$schema` does not use Draft 2020-12-only semantics.
- Rejection or unsupported behavior for external HTTP/file `$ref`.

### Compiler/runtime/resource integration

Replace direct `jsonschema::JSONSchema` imports with the adapter type.

Target locations:

- `src/dsl/compiler.rs`
- `src/dsl/compiled.rs`
- `src/resources/actions.rs`
- `src/resources/openai_chat.rs`
- `src/runtime/service.rs`
- `src/runtime/coordinator.rs`
- test helpers that construct compiled agents directly

The resulting compiled structs should store `Arc<JsonSchemaValidator>` or `JsonSchemaValidator` depending on current ownership needs.

### Dependency configuration

Change:

```toml
jsonschema = "0.18"
```

To:

```toml
jsonschema = { version = "0.47", default-features = false }
```

Do not enable `resolve-http`, `resolve-file`, `cli`, `tls-*`, or macro features in this phase.

## Behavioral compatibility matrix

The implementation must preserve these behaviors:

- Valid Agent input passes.
- Invalid Agent input fails before run preparation with stable `INPUT_INVALID`.
- Invalid Agent input schema fails Agent compilation with stable `INPUT_SCHEMA_INVALID`.
- Static Action input validation at Agent compile time remains enforced.
- Runtime Action input/output validation remains enforced and redacted.
- OpenAI parameter validation continues to allow the existing supported parameter set and reject unknown/disallowed shapes.
- Existing checked-in Agent YAML and tests continue to compile and run.

The implementation must add tests for these upgrade-specific behaviors:

- No direct `jsonschema::` usage remains outside `src/schema.rs` and tests.
- `cargo tree -i jsonschema --locked` shows `jsonschema v0.47.x`.
- `cargo tree -i reqwest --locked` does not gain a path through `jsonschema`.
- `cargo tree -i clap --locked` does not gain a path through `jsonschema`.
- External HTTP/file reference behavior is covered and does not perform network/file retrieval.
- Draft default is explicit and does not silently become Draft 2020-12.

## Breaking change rationale

This is a controlled breaking dependency migration. The public platform API should remain stable, but schema documents that relied on upstream 0.18 quirks or implicit external reference resolution may need edits.

The breaking change is justified because:

1. `jsonschema` 0.18 is far behind the current upstream line.
2. The current default features enable unnecessary HTTP/file/CLI surfaces.
3. Upstream changed draft defaults, so staying implicit is unsafe for a stable baseline.
4. A project adapter gives the platform a small future replacement surface.

## Verification gates

Required before merge:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
cargo audit
cargo deny check
rg -n "jsonschema::" src tests
cargo tree -i jsonschema --locked
cargo tree -i reqwest --locked
cargo tree -i clap --locked
```

Expected:

- Format, Clippy, tests, audit, and deny pass.
- Direct `jsonschema::` in production code is isolated to `src/schema.rs`.
- Tests may import `jsonschema::` only if they are explicitly asserting upstream behavior; prefer adapter tests.
- `jsonschema` resolves to the 0.47 line.
- `reqwest` is still only present through the project's direct HTTP/model dependency, not through `jsonschema`.
- `clap` is not introduced by `jsonschema` CLI defaults.

## Rollback plan

Rollback is a single dependency/boundary revert:

- Restore `jsonschema = "0.18"`.
- Restore direct `JSONSchema` usage.
- Remove `src/schema.rs`.

The rollback should not affect CEL, YAML, PostgreSQL TLS, Axum, Reqwest, SQLx, or history storage changes.

## Acceptance criteria

- `jsonschema` is upgraded to the 0.47 line.
- `jsonschema` default features are disabled.
- The platform has an explicit schema adapter.
- Draft behavior is explicit and documented.
- External HTTP/file schema retrieval is not enabled.
- Existing schema validation behavior remains stable at platform boundaries.
- Public error codes/messages remain stable.
- Full verification gates pass.
