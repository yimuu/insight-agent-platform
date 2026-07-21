# A7 — Body-free INFO observability design

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical implementation record:** the body-free logging principle remains current; node-era names and event examples are superseded. Current LLM/Action authoring and privacy contracts are defined by [DSL Authoring Surface Redesign](./2026-07-17-dsl-authoring-surface-redesign.md), with retained runtime semantics in [DSL vNext Region/SSA Design](./2026-07-16-dsl-vnext-region-ssa-design.md).

Date: 2026-07-12

## Status

Design approved for A7 implementation planning.

## Context

`BASE-P2-013` identifies a gap between the formal V1 observability contract and the current implementation. The approved runtime design says INFO logs contain identifiers, timings, counts, and output sizes without prompt, input, model output, action input, or action output bodies. Current production `tracing::` sites provide selected startup/error/request records but do not systematically report Run and node elapsed time or output sizes.

A7 closes only that structured logging gap. It does not add a metrics backend, tracing exporter, OpenTelemetry integration, sampling policy, or dashboard.

## Decision

A7 will add body-free structured `tracing::info!` lifecycle records at the existing execution boundaries:

1. Run lifecycle in `RunCoordinator`.
2. Node lifecycle in `runtime::execution`.
3. Chat model request/response metadata in `core.chat` and the OpenAI-compatible provider.

The implementation will prefer local helper functions for size calculation and logging only where they prevent duplicated field mistakes. It will not introduce a general observability framework; repeated byte/timing logic can be extracted into small helpers.

## Log contract

All A7 logs must be body-free. The logs may include identifiers, enum/status values, elapsed durations, counts, and serialized byte lengths. They must not include:

- raw request input values;
- rendered prompt/message text;
- model response text;
- action input or output bodies;
- event payload bodies;
- full URLs, query strings, headers, or credentials;
- API keys, bearer tokens, or configured secrets.

Byte fields use UTF-8/JSON serialization bytes:

- String output size is `str::len()`.
- JSON output size is `serde_json::to_vec(value).len()` when serialization succeeds.
- Run output size is the serialized `RunOutput`.
- Usage size is serialized usage JSON bytes.
- When serialization unexpectedly fails, log `*_bytes = 0` and continue; serialization failure should not change runtime behavior.

Elapsed fields use monotonic `Instant` timing and are logged as integer milliseconds with saturating conversion.

## Run lifecycle records

Add Run-level INFO records in `RunCoordinator`:

1. `run.started`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `attachment`.
   - Emitted after the run is marked running and before scheduler execution.

2. `run.finished`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `attachment`, `status`, `elapsed_ms`, `output_bytes`, `error_code`.
   - Emitted exactly once for the durable terminal outcome observed by the coordinator.
   - `output_bytes` is nonzero only for completed runs with a terminal `RunOutput`.
   - `error_code` is present on every record and is the empty string for success.

Infrastructure recovery remains an error path. Existing `tracing::error!` records stay, but the recovered terminal result still produces the final INFO record when the coordinator obtains a durable terminal state in that path.

## Node lifecycle records

Add node-level INFO records in `runtime::execution`:

1. `node.completed`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `node_id`, `kind`, `elapsed_ms`, `output_bytes`.
   - Emitted after `put_node_output` and `node.completed` event publication succeed.

2. `node.failed`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `node_id`, `kind`, `elapsed_ms`, `error_code`, `error_kind`.
   - Emitted after `node.failed` event publication succeeds for node/stop errors.
   - Infrastructure errors that prevent event publication keep their existing error classification; do not add best-effort INFO logs that could imply durable publication happened.

Node output bodies are never logged. Node-level `output_bytes` is the serialized size of `NodeOutcome::output`, not the value itself.

## Chat/model records

Chat observability is split at two levels:

1. `chat.request` from `core.chat`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `node_id`, `messages_count`, `image_parts_count`, `parameters_keys_count`.
   - Emitted after templates render and before calling `ChatModel::stream_chat`.
   - Does not include rendered text, image URLs, parameter values, or prompt references.

2. `chat.response` from `core.chat`
   - Fields: `run_id`, `request_id`, `agent_id`, `agent_version`, `node_id`, `chunks_count`, `text_bytes`, `usage_bytes`, `finish_reason_present`, `elapsed_ms`.
   - Emitted after the stream completes successfully and before returning `NodeOutcome`.
   - Does not include generated text, finish reason value, or usage JSON body.

3. Provider metadata from `OpenAiChatModel`
   - Keep the existing outbound request INFO record, but do not log full endpoint URLs or request/response bodies.
   - Add provider response INFO fields where safe: `model`, `upstream_bytes`, `chunks_count`, `usage_bytes`, `elapsed_ms`.
   - Provider fields are implementation-detail metadata and should not replace node-level chat records.

A1 already defined provider byte-accounting limits. A7 uses the same byte semantics when reporting observed provider bytes. It does not change the memory-bound behavior.

## Action records

A7 does not add action-specific body logs. `core.action` is covered by the node-level lifecycle records:

- action input body is not logged;
- action output body is not logged;
- action output size is reported through node `output_bytes`;
- action failure code is reported through node `error_code`.

This keeps the public Action extension contract simple and avoids duplicating sensitive body handling in every action.

## Testing strategy

Add recording-subscriber tests that exercise representative production paths and inspect structured log output. The tests verify:

1. Linear success:
   - one Run start and one Run finish INFO record;
   - node completion INFO records for each node;
   - output byte fields are present and nonzero where expected.

2. Parallel success:
   - branch nodes emit node completion records once each;
   - the final Run finish record is emitted once.

3. Chat/model path:
   - chat request/response records include counts and byte sizes;
   - prompt text, generated text, usage JSON body, and image URL bodies are absent.

4. Action success/failure:
   - action node output/error is represented by node lifecycle records;
   - action input/output fixture secrets are absent.

5. Failure path:
   - failed nodes include error code/kind;
   - failed Run finish record includes terminal status and error code;
   - no body data appears in logs.

Use stable field assertions where possible. If the existing `tracing_subscriber::fmt` text output is too brittle for structured field assertions, add a small test-only subscriber/layer in the test module rather than changing production code for test convenience.

## Documentation impact

Update README or formal V1 notes only if implementation introduces a visible observability field contract worth documenting. The minimum acceptable documentation is a short body-free INFO logging statement:

- logs include identifiers, elapsed timings, counts, and byte sizes;
- logs exclude input, prompts, model output, action bodies, URLs with query strings, headers, and credentials;
- metrics/exporters are outside V1 baseline.

## Rejected alternatives

### Add a metrics backend

Metrics are useful but broader than the review finding. They require naming, aggregation, export, deployment, and retention decisions. A7 is structured logging only.

### Log action/model bodies with redaction

Redaction is error-prone and unnecessary for the stated operator questions. Counts and byte sizes answer latency/growth questions without copying sensitive bodies.

### Central observability framework first

A framework may become useful later, but the current need is small and well-scoped. A7 starts with clear local records and only extracts helpers for repeated byte/timing logic.

## Success criteria

A7 succeeds when INFO logs consistently show Run/node/chat lifecycle identifiers, elapsed timings, safe counts, and output/serialized byte sizes, while tests prove representative fixture bodies and secrets are absent. The implementation must remain additive and must not introduce a metrics backend or change public runtime behavior.
