# Release Quickstart and Binary Smoke Design

> **Historical implementation record:** the quickstart outcome remains current, but the Agent ID and graph/node examples below were replaced by vNext `action_demo`. Use README and [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md) for current authored syntax; retained runtime semantics remain in [DSL vNext Region/SSA Design](./2026-07-16-dsl-vnext-region-ssa-design.md).

Date: 2026-07-12

Status: Design approved in conversation; awaiting written spec review.

## Context

The stable baseline and dependency-governance remediation work is implemented and pushed. CI already runs formatting, Clippy, all-target tests, cargo-audit, and cargo-deny. The remaining delivery gap is practical operability: a new maintainer should be able to start the platform locally without external model credentials, and CI should prove that the compiled binary can boot, serve HTTP, execute a minimal Run, persist history, and shut down.

Current repository evidence:

- `src/main.rs` starts the production binary from `PlatformConfig::from_env()`.
- `PLATFORM_CONFIG` overrides the default `config/platform.yaml` path.
- `config/platform.yaml` enables model-backed example agents and requires a real model secret through `config/models.yaml`.
- `agents/code_node_demo/agent.yaml` uses only the built-in `example.text_metrics` Action and does not call a model.
- `src/resources/config.rs` requires model config to define at least one model alias even if the enabled Agent does not use it.
- `tests/api.rs` covers the router and Run service in-process, but there is no integration test that launches `CARGO_BIN_EXE_insight-agent-platform`.

## Goals

1. Provide a local quickstart path that does not require `OPENAI_API_KEY` or any external model service.
2. Add a binary smoke test that launches the real `insight-agent-platform` executable.
3. Prove the production startup chain works end to end:
   - platform YAML loading;
   - model resource YAML loading;
   - enabled Agent compilation;
   - SQLite repository initialization;
   - HTTP listener binding;
   - `/health`;
   - `/v1/agents`;
   - detached Run creation;
   - Run lookup until `completed`;
   - graceful process shutdown.
4. Keep the smoke test deterministic, fast, and network-local.
5. Avoid changing Formal V1 HTTP, DSL, Run lifecycle, SSE, history, model, or Action interfaces.

## Non-goals

- Do not add real model-provider integration testing.
- Do not require external API keys in CI.
- Do not cover PostgreSQL in this smoke test; PostgreSQL remains covered by existing focused tests and CI service configuration.
- Do not re-enable SSE replay or event recovery routes.
- Do not test attached SSE behavior here; existing API tests already cover attached stream semantics in-process.
- Do not introduce new runtime dependencies or dev-dependencies unless implementation proves an existing dependency is insufficient.
- Do not change default production `config/platform.yaml` semantics.

## Chosen approach

Adopt a minimal no-external-service quickstart and binary smoke test.

This approach gives the repository a concrete release baseline without expanding scope into a full deployment matrix. The smoke path should use `code_node_demo` because it exercises a real Agent, real Action execution, template rendering, `core.output`, persistence, and HTTP lifecycle without requiring model credentials.

The model config still needs at least one alias. The smoke/quickstart model config should therefore include a syntactically valid dummy `open_ai_chat` alias with no `api_key_env`. Because `code_node_demo` does not reference that alias, the model is loaded but never called. This preserves current strict model-resource validation while avoiding fake external traffic.

## Quickstart design

Add a no-key quickstart configuration pair:

- `config/platform.quickstart.yaml`
- `config/models.quickstart.yaml`

`config/platform.quickstart.yaml` should:

- use `version: 1`;
- bind to `127.0.0.1:3000` for human local usage;
- set `auth.mode: disabled`;
- enable only `code_node_demo`;
- reference `models.quickstart.yaml`;
- enable only the built-in Action needed by the demo, `example.text_metrics`;
- use SQLite history at a local development path such as `../data/quickstart.sqlite3`;
- use conservative runtime capacities and timeouts consistent with the existing platform config shape.

`config/models.quickstart.yaml` should:

- use `version: 1`;
- define one dummy model alias;
- use HTTPS in `base_url` so no plaintext transport exception is needed;
- omit `api_key_env` so no secret is required;
- set valid positive connect/request timeouts.

The README should add a short no-key quickstart section that:

1. Runs the binary with `PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run`.
2. Calls `/health`.
3. Lists Agents.
4. Creates a detached Run against `code_node_demo`.
5. Polls `/v1/runs/{run_id}` until `completed`.

The existing model-key startup path should remain documented as the path for model-backed example Agents.

## Binary smoke test design

Add `tests/binary_smoke.rs`.

The test should:

1. Allocate a temporary directory.
2. Write a platform YAML into the temporary directory.
3. Write a model resources YAML into the same temporary directory.
4. Point the platform YAML to the repository's real `agents` directory.
5. Enable only `code_node_demo`.
6. Bind to a test port.
7. Start `CARGO_BIN_EXE_insight-agent-platform` with `PLATFORM_CONFIG=<temp platform yaml>`.
8. Poll `GET /health` until it returns `200` and body code `OK`.
9. Call `GET /v1/agents` and assert that only `code_node_demo` is exposed.
10. Call `POST /v1/agents/code_node_demo/runs` with `{"text":"hello rust world"}` and assert HTTP `202`.
11. Poll `GET /v1/runs/{run_id}` until status is `completed`.
12. Assert the Run output contains the expected action/template result shape, without asserting on volatile timestamps or IDs.
13. Terminate the child process and wait for exit.

The test must clean up the child process even on failure. If the test uses a fixed port, it must choose a high loopback port unlikely to collide. If the implementation can reliably reserve a free port before launching the child, it may use `127.0.0.1:0` discovery and inject the chosen concrete port into the temp YAML before spawning.

The preferred implementation is to reserve an ephemeral loopback port with `std::net::TcpListener::bind("127.0.0.1:0")`, read `local_addr()`, drop the listener, and immediately start the child with that port. This has a small time-of-check/time-of-use race but is acceptable for CI smoke. If that proves flaky, switch to a fixed high port plus retry or add a production-supported readiness/port reporting mechanism in a separate design.

## Error handling design

The smoke test should produce actionable failure messages:

- If the binary exits before readiness, include captured stderr/stdout.
- If readiness times out, kill the child and include recent process output.
- If HTTP calls fail, include method, URL, HTTP status, and response body.
- If Run status becomes `failed` or `cancelled`, include the returned Run record.

The test should not log secrets because the quickstart model config has no secret and the platform config uses disabled auth.

## Interface impact

No public interface changes are intended.

- No HTTP route changes.
- No request/response schema changes.
- No DSL syntax changes.
- No model resource schema changes.
- No platform config schema changes.
- No Run lifecycle changes.
- No SSE behavior changes.
- No database migration changes.

The only new files are supporting configuration, documentation, and test code. The reason for avoiding interface changes is that this milestone validates operability of the current stable baseline; adding or changing interfaces would make the smoke test validate a moving target instead of the baseline itself.

## CI impact

The existing CI `cargo test --all-targets` step should automatically run the new integration test.

No new workflow step is required unless implementation shows that the binary smoke test needs environment setup not provided by normal cargo integration tests. The test must avoid external network calls, API keys, and long sleeps so it remains suitable for default CI.

## Verification design

Focused verification:

- `cargo test --test binary_smoke -- --nocapture`
- `PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run` manual startup command if needed during implementation debugging.

Full verification before completion:

- `cargo fmt --check`
- `cargo test --all-targets --all-features -- --nocapture --test-threads=1`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo audit`
- `cargo deny check`
- `git diff --check`

If the implementation only changes docs/config/test code and focused smoke passes, failures in unrelated optional external-service tests should be triaged rather than papered over. Any verification limitation must be reported with exact command output.

## Acceptance criteria

The work is complete when:

1. A no-key quickstart config exists and starts the platform with only `code_node_demo` enabled.
2. README documents the no-key quickstart separately from model-backed examples.
3. A binary smoke test launches the real executable through `CARGO_BIN_EXE_insight-agent-platform`.
4. The smoke test verifies `/health`, `/v1/agents`, detached Run creation, and Run completion.
5. The smoke test does not require `OPENAI_API_KEY`, PostgreSQL, external model services, or external network access.
6. CI continues to cover the smoke test through the existing all-target test command.
7. No Formal V1 public interface changes are introduced.
