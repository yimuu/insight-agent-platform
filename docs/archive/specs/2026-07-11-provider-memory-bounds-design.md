# Provider Memory Bounds Design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

**Status:** Approved in conversation on 2026-07-11; written-spec review pending.

## Context

The stable-baseline review identifies `BASE-P1-011`: OpenAI-compatible provider streams and accumulated chat output have no byte bound. Today `OpenAiChatModel` buffers upstream SSE bytes until LF or EOF, parses each `data:` payload, and `ChatNode` appends every decoded text chunk into one `String`. Timeouts and concurrency limits bound elapsed time and parallelism, but they do not bound bytes retained by one Run.

A0 has already contained Action validation errors. A1 is independent and should close only the provider/chat memory-bound finding while leaving transport security, clean-EOF semantics, dependency upgrades, and observability as later milestones.

## Goals

- Add deterministic byte limits to OpenAI-compatible provider response handling.
- Add a byte limit to final accumulated chat text.
- Keep existing `ChatModel`, `ChatStream`, `ChatNode` output, HTTP/event/Run shapes, and repository shapes unchanged.
- Keep existing model configurations valid by providing defaults.
- Allow per-model overrides in `models.yaml`.
- Fail excessive responses with one stable sanitized runtime error.
- Define byte-accounting semantics that A7 observability can later report without reinterpreting the counters.

## Non-goals

- Requiring HTTPS-only model transport; that belongs to A8.
- Changing OpenAI clean-EOF or `[DONE]` semantics; that remains Needs verification.
- Adding metrics, tracing fields, or a metrics backend; A7 owns body-free INFO observability.
- Changing provider parameter schema, required `ChatModel` methods, `ChatStream` item shape, node output JSON, migrations, or dependency versions.
- Limiting rendered prompt/request bytes. A1 bounds provider responses and accumulated model output; prompt-size policy remains separate.

## Selected approach

A1 uses default safe limits with optional per-model overrides under each `open_ai_chat` model resource. This keeps existing configs valid and lets operators tune large or small models without turning provider limits into global runtime settings.

Alternatives considered:

1. Fixed built-in limits only. This is simple but too rigid for different model providers and deployment profiles.
2. Default limits plus per-model overrides. This is selected because response size is a model/provider concern and the existing model YAML is already the provider-specific boundary.
3. Global runtime limits. This would reduce config surface but mixes provider-response memory policy with scheduler and journal capacity policy.

## Public configuration

`open_ai_chat` resources gain an optional `limits` object:

```yaml
version: 1
models:
  general_chat:
    type: open_ai_chat
    base_url: https://example-model-service.test/v1
    model: example-chat
    api_key_env: OPENAI_API_KEY
    capabilities: []
    connect_timeout: 5s
    request_timeout: 2m
    limits:
      max_upstream_bytes: 8388608
      max_buffered_line_bytes: 1048576
      max_event_payload_bytes: 1048576
      max_chunk_text_bytes: 262144
      max_usage_json_bytes: 65536
      max_accumulated_text_bytes: 1048576
```

The `limits` object is optional. Omitting it uses all defaults. When `limits` is present, each field inside it is independently optional and unspecified fields use defaults. Values are byte counts encoded as positive integers. Zero is invalid. Unknown fields remain invalid through strict YAML parsing.

Default values:

| Field | Default | Boundary |
|---|---:|---|
| `max_upstream_bytes` | 8,388,608 | Total provider response body bytes consumed from the upstream stream. |
| `max_buffered_line_bytes` | 1,048,576 | Maximum bytes retained while waiting for a line break. |
| `max_event_payload_bytes` | 1,048,576 | Maximum bytes in one parsed `data:` payload after trimming the `data:` prefix and leading spaces. |
| `max_chunk_text_bytes` | 262,144 | Maximum UTF-8 bytes in one decoded content delta. |
| `max_usage_json_bytes` | 65,536 | Maximum serialized JSON bytes in one decoded usage object. |
| `max_accumulated_text_bytes` | 1,048,576 | Maximum UTF-8 bytes accumulated by `ChatNode` across all text chunks. |

## Runtime error contract

Every A1 limit violation returns:

| Code | Message |
|---|---|
| `MODEL_RESPONSE_TOO_LARGE` | `chat provider response exceeded the configured size limit` |

The error must not include provider body text, chunk text, usage JSON, URL query values, API keys, request messages, response headers, or the numeric configured limit. Existing transport/status/invalid-stream errors keep their current codes and sanitized messages.

## Byte-accounting semantics

- Byte counts are UTF-8 bytes, not Unicode scalar counts.
- `max_upstream_bytes` is counted before bytes are appended to the SSE decoder buffer. A `Content-Length` header may fail early when it is present and exceeds the limit, but incremental counting is still required and authoritative.
- `max_buffered_line_bytes` is the maximum decoder buffer length between complete line drains. A no-LF stream cannot grow this buffer beyond the limit.
- `max_event_payload_bytes` applies to each non-empty `data:` payload before JSON parsing. `[DONE]` is still recognized after the same payload-size check.
- `max_chunk_text_bytes` applies to each decoded `delta.content` string before the chunk is yielded.
- `max_usage_json_bytes` applies to the serialized JSON representation of each decoded `usage` value before the chunk is yielded.
- `max_accumulated_text_bytes` applies in `ChatNode` before appending the next chunk to the final `text` output and before emitting that chunk as content.

The configured bounds are inclusive. A value exactly equal to a limit is accepted; the first byte above the limit fails.

## Component design

### `OpenAiChatLimits`

Add an `OpenAiChatLimits` value owned by `OpenAiChatModel`. It is `Copy` or cheap to clone, has `Default`, validates all fields as nonzero, and is included in model construction.

Keep `OpenAiChatModel::new(...)` as the default-limit constructor used by existing callers. Add `OpenAiChatModel::new_with_limits(...)` for configuration loading and limit-specific tests.

### Model YAML

`src/resources/config.rs` parses optional `limits` for `open_ai_chat`, merges it with `OpenAiChatLimits::default()`, validates nonzero fields, and passes the result to `OpenAiChatModel::new_with_limits(...)`. Existing checked-in `config/models.yaml` remains unchanged so operators are not forced to spell defaults. README must mention the optional limits block and defaults.

### Provider stream limits

`OpenAiChatModel::stream_chat` passes limits into `StreamState` and `SseDecoder`.

The decoder enforces:

- cumulative upstream bytes;
- buffered line length;
- per-event payload length;
- per-chunk text length;
- per-usage JSON length.

The stream returns `MODEL_RESPONSE_TOO_LARGE` as soon as a limit is exceeded. Returning the error drops the stream state, which drops the `reqwest` response body and closes the in-flight upstream body as the existing drop test already verifies.

### Chat accumulated output limit

`ChatNode` enforces `max_accumulated_text_bytes` while reading chunks. Because `ChatNode` only has access to `Arc<dyn ChatModel>`, the model interface needs a way to expose the chat output limit without changing stream shape or requiring every existing model implementation to change. Add a default method to `ChatModel`; the generic `models` module owns the default accumulated-text constant so the trait does not depend on the OpenAI-specific provider:

```rust
fn max_accumulated_text_bytes(&self) -> usize {
    DEFAULT_MAX_ACCUMULATED_TEXT_BYTES
}
```

`OpenAiChatModel` overrides it to return its configured value. Existing test/fake models inherit the default. Before `ChatNode` appends or emits a chunk, it checks `text.len().saturating_add(chunk.text.len()) > limit` and returns the stable A1 error when exceeded.

## Testing strategy

Tests must follow TDD: each new behavior gets a failing test before production code changes.

Required focused coverage:

- Model resource config accepts omitted limits and rejects zero limit values.
- OpenAI stream succeeds at exact `max_upstream_bytes`, `max_event_payload_bytes`, `max_chunk_text_bytes`, and `max_usage_json_bytes` boundaries.
- OpenAI stream fails one byte over total upstream bytes.
- OpenAI stream fails no-LF buffered line growth over `max_buffered_line_bytes`.
- OpenAI stream fails one oversized `data:` payload before parsing or exposing body text.
- OpenAI stream fails oversized content delta and oversized usage JSON.
- Exceeding a provider limit closes the in-flight HTTP body.
- `ChatNode` fails before appending/emitting a chunk that would exceed `max_accumulated_text_bytes`.
- After a bounded failure, a subsequent chat Run can still execute and release permits.
- Every failure asserts exact code/message and absence of fixture body text, API key, URL secret, chunk text, and usage text.

Release gate:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test model_resources_v1 --test formal_resources --test core_chat_action -- --nocapture --test-threads=1
cargo test --all-targets
cargo audit
cargo deny check
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

## Rollout and rollback

A1 is source-shape compatible at the HTTP/event/repository level and config-compatible for existing YAML because limits default when omitted. Runtime behavior changes only for excessive provider responses and excessive accumulated chat output. Rollback restores the previous unbounded behavior; no data migration or history reset is required.

## Acceptance criteria

1. Existing model configs without `limits` still load and use defaults.
2. Any configured zero limit fails startup with `MODEL_CONFIG_INVALID`.
3. OpenAI response streaming enforces total body, buffered-line, event-payload, chunk-text, and usage JSON limits incrementally.
4. `ChatNode` enforces accumulated text bytes before append and before content emission.
5. Every limit violation returns exactly `MODEL_RESPONSE_TOO_LARGE` / `chat provider response exceeded the configured size limit`.
6. Limit errors do not expose provider body, chunk text, usage JSON, URL tokens, API keys, prompts, or response headers.
7. Exceeding a stream limit drops the upstream body and permits later Runs to proceed.
8. Public HTTP/SSE/event/Run/repository shapes, migrations, dependency graph, and A2-A8 behavior remain unchanged.
