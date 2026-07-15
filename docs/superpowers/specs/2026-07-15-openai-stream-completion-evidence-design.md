# OpenAI Stream Completion Evidence Design

**Date:** 2026-07-15

**Status:** Implemented and verified

**Scope:** OpenAI-compatible Chat Completions SSE completion evidence, clean EOF,
truncated payloads, transport failures, and body-free error reporting

## 1. Context

The OpenAI-compatible adapter currently discards `data: [DONE]` and treats every
clean HTTP-body EOF as success. A provider can therefore emit an empty response or
partial content and close cleanly without a terminal choice, and the Run cannot
distinguish that truncation from a complete response.

The adapter must distinguish three separate facts:

- a choice stopped generating tokens (`finish_reason`);
- the OpenAI application stream declared logical completion (`data: [DONE]`);
- the HTTP response body ended cleanly rather than with a transport failure.

The [OpenAI Chat Completions reference](https://developers.openai.com/api/reference/resources/chat)
defines `finish_reason` as the reason a choice stopped generating tokens and places
an optional usage-only chunk before `data: [DONE]`. Generation termination is
therefore not stream-framing termination. No checked-in platform contract requires
weaker EOF semantics.

## 2. Decision

A stream succeeds only when `data: [DONE]` is parsed. This is authoritative
OpenAI-application completion and does not wait for transport EOF. It is an
OpenAI-specific sentinel carried in SSE, not a generic SSE-standard marker.

A terminal finish reason is generation evidence, not an instruction to stop reading
and not a substitute for `[DONE]`. The adapter continues reading so it can deliver a
following usage-only chunk and observe malformed data, a transport failure, or the
required sentinel.

The following are not completion evidence:

- content chunks whose finish reason is null;
- an empty response body;
- comment, blank, or unknown SSE fields;
- a usage-only chunk with an empty choices array;
- a choice with a non-null `finish_reason`;
- transport EOF represented as an error rather than clean body termination.

Clean EOF without completion evidence yields:

- code: `UPSTREAM_STREAM_INCOMPLETE`;
- message: `chat provider stream ended without completion evidence`.

The code and message are fixed and contain no endpoint, credential, request body,
partial model output, provider payload, or transport detail.

## 3. Decoder and Stream State

`SseDecoder` records whether it parsed `[DONE]`. Parsed chat chunks remain queued in
their original order. If one received byte block contains both chunks and `[DONE]`,
the queued chunks are yielded first and the next poll completes the stream.

After `[DONE]`, the response body is dropped and no additional bytes are read. This
prevents a provider that keeps the connection open after its sentinel from hanging a
Run. Data after the sentinel is outside the completed logical stream and is ignored.
The existing line-oriented decoder also accepts a complete `data: [DONE]` line at
clean EOF without a trailing LF; the full sentinel bytes remain explicit completion
evidence.

`StreamState` separately records whether the body has reached clean EOF.

Clean EOF is consumed exactly once. The state first calls `decoder.finish()` so a
valid final line without LF is parsed and malformed trailing data retains its
specific error. It then drains any final decoded chunks. A later poll succeeds only
if `[DONE]` was parsed; otherwise it returns the incomplete error. The underlying body
stream is never polled again after it returned `None`.

## 4. Failure Precedence

Failure classification is ordered as follows:

1. Existing configured response-size violations remain
   `MODEL_RESPONSE_TOO_LARGE`.
2. Invalid UTF-8 or malformed JSON, including a malformed or truncated final payload,
   remains `UPSTREAM_STREAM_INVALID` with the existing sanitized message.
3. A `reqwest` body error remains `UPSTREAM_STREAM`, even after content or a terminal
   finish chunk was already yielded.
4. Only a clean EOF with no `[DONE]` becomes
   `UPSTREAM_STREAM_INCOMPLETE`.

Earlier partial output never converts one of these failures into success. A terminal
finish does not suppress later protocol or transport errors.

## 5. Observability and Compatibility

Successful response metadata is logged once after sentinel completion using only the
existing model identifier, byte/chunk/usage counts, and elapsed time. No response body
is added to logs.

This change does not alter the public `ChatChunk`, `ChatModel`, Agent, Run, HTTP, SSE,
repository, configuration, or migration shapes. There is no compatibility flag. If a
future provider cannot emit the documented sentinel, weakening completion evidence
requires an explicit per-model design rather than silently weakening every endpoint.

## 6. Verification

Deterministic fake-provider tests must prove:

1. `[DONE]` drops the response body before a subsequent consumer poll and completes
   without waiting for socket EOF.
2. clean EOF after a terminal finish reason still fails without `[DONE]`;
3. a usage-only chunk after the terminal finish is delivered before the exact
   incomplete failure;
4. content-only, finish-only, usage-only, empty, and comment-only clean EOF fail with
   the exact incomplete code/message;
5. malformed JSON and truncated UTF-8 at EOF retain their exact invalid-stream
   classifications and do not leak payload sentinels;
6. a body transport error after a terminal finish remains `UPSTREAM_STREAM`;
7. `[DONE]` without a trailing newline is accepted;
8. the existing canonical, size-limit, fragmented UTF-8, cancellation/drop, and
   redaction suites continue to pass.

## 7. Acceptance Criteria

1. No clean EOF without explicit completion evidence can silently end a Chat stream.
2. `[DONE]` terminates promptly without waiting for TCP close.
3. A terminal finish reason does not authorize EOF or hide later errors.
4. Usage-only chunks are preserved but never treated as completion evidence.
5. Truncated data and transport failures retain stable, sanitized classifications.
6. No public compatibility surface, schema, or dependency changes.
7. Focused and complete repository gates pass before remediation item 14 is marked
   `Addressed`.
