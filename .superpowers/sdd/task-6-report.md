# Task 6 Report

## Scope

Implemented Task 6 in `/Users/cc/projects/experiments/rust/insight-agent-platform/.worktrees/config-driven-agent-platform` for:

- `src/tools/current_time.rs`
- `src/tools/http_get.rs`
- `src/tools/registry.rs`
- `tests/runner.rs`

I did not modify `src/engine/runner.rs` because the current worktree already contains the Task 4 tool-step path with:

- `RunEventKind::ToolCallStarted`
- `RunEventKind::ToolCallCompleted`
- cancellation-aware `tokio::select!` around `tool.call(...)`

That is a brief/code mismatch, so I adapted the work to the existing contract instead of rewriting it.

## RED

Added failing tests first:

1. `tool_step_emits_tool_events_and_stores_output` in `tests/runner.rs`
2. `default_tool_registry_registers_built_in_tools` in `tests/runner.rs`

Initial failing command:

```bash
cargo test --test runner tool_step_emits_tool_events_and_stores_output
```

Observed failure:

- unresolved import `insight_agent_platform::tools::current_time::CurrentTimeTool`
- unresolved import `insight_agent_platform::tools::registry::default_tool_registry`

This confirmed the missing built-in tool implementation and registry wiring.

## GREEN

Implemented:

### `CurrentTimeTool`

- Registers as `current_time`
- Accepts optional `timezone`, defaulting to `UTC`
- Validates timezone via `chrono_tz::Tz`
- Returns JSON:
  - `timezone`
  - `iso8601`

### `HttpGetTool`

- Registers as `http_get`
- Requires string `url`
- Restricts requests to `https`
- Uses a reqwest client with a 10 second timeout
- Enforces a 256 KiB maximum response body while streaming
- Returns JSON:
  - `status`
  - `body`

### `default_tool_registry()`

- Registers:
  - `CurrentTimeTool`
  - `HttpGetTool::default()`

### Tests

Added:

- runner test for tool-step event emission and output persistence
- runner test for default registry contents
- unit test for `http_get` rejecting non-HTTPS URLs

## Verification

Targeted RED:

```bash
cargo test --test runner tool_step_emits_tool_events_and_stores_output
```

- failed before implementation

Targeted GREEN:

```bash
cargo test --test runner tool_step_emits_tool_events_and_stores_output
cargo test default_tool_registry_registers_built_in_tools
cargo test rejects_non_https_urls
```

- all passed after implementation

Required verification:

```bash
cargo test --test runner
cargo check
```

Results:

- `cargo test --test runner`: 8 passed, 0 failed
- `cargo check`: passed

Also ran:

```bash
cargo fmt
```

## Self-review

- Preserved existing runner streaming/cancellation behavior by not altering the Task 4 tool execution path.
- Kept `http_get` scheme handling strict: only `https`, no arbitrary schemes.
- Enforced response-size limits while reading the stream instead of after fully buffering.
- Left unrelated worktree changes in `src/model/openai.rs` untouched.

## Commit

Planned commit message:

```bash
feat: add built-in tools and default registry
```

## Follow-up: openai.rs formatting commit

Requested follow-up checked the remaining worktree diff in `src/model/openai.rs`.

### Diff inspection

Command:

```bash
git diff -- src/model/openai.rs
```

Result:

- formatting-only changes from `cargo fmt`
- line wrapping only
- no logic, literal, branch, or behavior changes

### Fresh verification before style commit

Commands:

```bash
cargo test --test runner
cargo check
```

Observed output summary:

- `cargo test --test runner`: 8 passed, 0 failed
- `cargo check`: passed

### Follow-up commit

Committed the formatting-only change separately as:

```bash
style: format openai client
```

## Review fixes

Addressed follow-up review findings in `src/tools/http_get.rs` and `tests/runner.rs`.

### Changes made

1. Added optional host allowlist support to `HttpGetTool`
   - `HttpGetTool::default()` still allows all HTTPS hosts
   - added `HttpGetTool::new_with_allowlist(timeout, max_bytes, allowlist)`
   - host validation runs on the parsed URL host before any request is sent

2. Sanitized `http_get` request failure errors
   - removed full URL echo from request-send failures
   - failures now return generic text like `http_get request failed (connection error)`
   - this prevents leaking query params, embedded credentials, paths, or internal hosts through run error events

3. Added focused regression tests
   - `allowlist_permits_allowed_host`
   - `allowlist_rejects_disallowed_host_before_request`
   - `request_failure_error_is_sanitized`
   - `tool_step_error_emits_error_event_and_stops_run`

### RED/GREEN evidence

RED:

```bash
cargo test allowlist_permits_allowed_host
cargo test request_failure_error_is_sanitized
```

Initial failure was due to missing `HttpGetTool::new_with_allowlist`, confirming the new allowlist behavior was not yet implemented.

The runner-level tool error regression already passed on the existing runner, confirming that behavior was already present and only needed to be preserved.

GREEN / verification:

```bash
cargo test tools::http_get::tests
cargo test --test runner
cargo check
```

Observed results:

- `cargo test tools::http_get::tests`: 4 passed, 0 failed
- `cargo test --test runner`: 9 passed, 0 failed
- `cargo check`: passed

### Review-fix commit

Committed as:

```bash
fix: restrict http_get hosts and sanitize errors
```
