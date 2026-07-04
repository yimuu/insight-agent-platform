# Task 8 Report: Input Schema Validation and Sample Agent

## Scope

Implemented Task 8 in:

- `src/api/routes.rs`
- `tests/api.rs`
- `agents/researcher/agent.yaml`
- `agents/researcher/prompts/system.md`
- `agents/researcher/prompts/planner.md`
- `agents/researcher/prompts/final.md`

## RED/GREEN Evidence

### RED

Command:

```bash
cargo test --test api invalid_input_returns_400_before_sse
```

Observed failure before the route change:

```text
thread 'invalid_input_returns_400_before_sse' panicked
assertion `left == right` failed
  left: 200
 right: 400
```

Cause: `POST /v1/agents/:agent_id/runs/stream` accepted invalid request input and started the SSE response path instead of validating `agent.config.input.schema` first.

### GREEN

Commands:

```bash
cargo test --test api
cargo test --test agent_loader
```

Observed result after implementation:

```text
running 6 tests
... invalid_input_returns_400_before_sse ... ok
test result: ok. 6 passed; 0 failed

running 8 tests
test loads_agent_with_multiple_prompt_files ... ok
test result: ok. 8 passed; 0 failed
```

## What Changed

### API validation

In `src/api/routes.rs`:

- Compiled each agent's configured JSON Schema with `jsonschema::JSONSchema::compile(...)`.
- Validated `request.input` before constructing the SSE stream.
- Returned `AppError::Input(...)` on schema validation failure so the client gets HTTP 400 with the standard JSON error envelope.
- Returned `AppError::Config(...)` if an agent somehow contains an invalid input schema at runtime.

This keeps invalid requests out of the stream path entirely, so no `text/event-stream` response is started for bad input.

### API tests

In `tests/api.rs`:

- Tightened the in-test `test` agent schema to require `input.name: string`.
- Added `invalid_input_returns_400_before_sse`, proving invalid request payloads return HTTP 400 with `error.code == "input_error"`.
- Updated existing list/detail assertions to expect the stricter input schema now exposed by the API.

### Sample agent

Added a runnable sample agent under `agents/researcher/`:

- `agent.yaml`
- `prompts/system.md`
- `prompts/planner.md`
- `prompts/final.md`

Properties:

- No secrets or credentials.
- Prompt bodies live in Markdown files and are referenced from `agent.yaml`.
- Input schema requires a `question` string.
- Uses a plan-first flow with two LLM steps and one `current_time` tool step.

## Verification

Commands run:

```bash
cargo test --test api
cargo test --test agent_loader
cargo test
cargo check
```

Results:

- `cargo test --test api`: pass
- `cargo test --test agent_loader`: pass
- `cargo test`: pass
- `cargo check`: pass

## Mismatch From Brief

One codebase mismatch mattered:

- The brief referenced `agents/researcher/**`, but this worktree did not have an existing top-level `agents/` directory. I created `agents/researcher/` from scratch rather than editing an existing sample-agent tree.

No conflicting edits from other contributors were reverted.

## Self-Review

Findings: no blocking issues found.

Checks performed:

- Confirmed invalid input now returns HTTP 400 before SSE starts.
- Confirmed valid input still returns `text/event-stream` and existing stream tests remain green.
- Confirmed the sample agent prompt files are Markdown and referenced by relative paths from `agent.yaml`.
- Confirmed the sample agent contains no secrets.
