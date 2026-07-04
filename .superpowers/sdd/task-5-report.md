# Task 5 Report

## Scope handled

- Implemented `src/model/openai.rs` with:
  - `OpenAiModelClient::new(api_key, base_url, default_model)`
  - `ModelClient for OpenAiModelClient`
  - default-model fallback when `ChatRequest.model` is empty
  - OpenAI-compatible SSE parsing into streamed token deltas
  - sanitized upstream errors that do not expose API keys
- Added missing LLM runner regression coverage in `tests/runner.rs`

## Brief mismatch

- `src/engine/runner.rs` already contained the Task 4 LLM execution path, live `token_delta` emission, empty-string model forwarding, and cancellation-aware streaming.
- Because of that, the new runner regression test from the brief passed immediately instead of failing with the older `"llm step 'answer' is not available until the model task is completed"` behavior.
- I left `runner.rs` unchanged to avoid regressing the already-correct Task 4 implementation.

## RED/GREEN evidence

### RED

1. Added `tests/runner.rs::llm_step_streams_token_delta_events_and_final_output`
   - `cargo test --test runner llm_step_streams_token_delta_events_and_final_output`
   - Result: passed immediately because Task 4 behavior was already present
2. Added `src/model/openai.rs` unit tests for:
   - default-model fallback + SSE token streaming
   - sanitized upstream error handling
3. Ran:
   - `cargo test model::openai::tests::stream_chat_uses_default_model_for_empty_request_and_streams_content --lib`
   - Result: failed before implementation because `OpenAiModelClient` did not exist

### GREEN

After implementing `OpenAiModelClient` and SSE parsing:

- `cargo test model::openai::tests::stream_chat_uses_default_model_for_empty_request_and_streams_content --lib`
- `cargo test model::openai::tests::stream_chat_sanitizes_error_messages --lib`
- `cargo test --test runner llm_step_streams_token_delta_events_and_final_output`

All passed.

## Final verification

- `cargo test --test runner`
- `cargo check`

Both passed.

## Files changed

- `src/model/openai.rs`
- `tests/runner.rs`

## Self-review

- Confirmed no use of `model_config.provider` as a model fallback.
- Confirmed runner streaming and cancellation behavior remains unchanged.
- Confirmed error surfaces do not include the API key in explicit error text.
- Confirmed SSE parsing tolerates arbitrary transport chunk boundaries as long as OpenAI-compatible `data:` lines are newline-delimited.

## Review fix: SSE parser hardening

- Root cause: the initial parser converted each transport `Bytes` chunk with `String::from_utf8_lossy` before newline framing, which could corrupt multibyte UTF-8 when a code point was split across chunk boundaries.
- Fix: replaced string-based chunk accumulation with a byte-buffered `SseDecoder` that:
  - buffers raw bytes until a full newline-delimited SSE line is available
  - trims trailing `\r` at the byte level
  - decodes each complete line with strict UTF-8
  - parses `data:` payloads only after complete-line decoding
- Error handling change: malformed non-`[DONE]` `data:` payloads now return `AppError::Upstream("invalid model stream payload")` instead of being silently dropped.
- Sanitization: error messages remain generic and do not expose the API key.

## Review fix RED/GREEN evidence

### RED

1. Added `model::openai::tests::stream_chat_returns_error_for_malformed_data_json`
   - Before the fix, the malformed `data:` line was silently ignored and the test failed because the stream ended without returning an error.
2. Added `model::openai::tests::sse_decoder_handles_split_multibyte_utf8_across_chunks`
   - Initially failed to compile because `SseDecoder` did not exist, forcing the byte-buffered parser implementation.

### GREEN

After implementing the byte-safe decoder and strict payload handling:

- `cargo test model::openai::tests --lib`
- `cargo test --test runner`
- `cargo check`

All passed.
