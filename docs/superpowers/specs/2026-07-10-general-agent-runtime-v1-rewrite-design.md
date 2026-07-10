# General Agent Runtime V1 Rewrite Design

Date: 2026-07-10
Status: Approved for implementation

## Goal

Replace the pre-release runtime with a stable, extensible development baseline for platform-owned agents. The rewrite defines the first formal V1 contracts for the agent DSL, node extension system, run lifecycle, event protocol, history store, and HTTP API.

The medical report interpreter remains an example of a multimodal vertical agent. It does not define the platform architecture or add medical-specific compliance requirements to the core runtime.

## Versioning Decision

The repository's existing `/v1` API and implicit agent YAML format are pre-release prototype contracts. They are removed rather than supported alongside the rewrite.

The rewritten contracts use:

- HTTP prefix `/v1`.
- Agent DSL `version: 1`.
- Event envelope `schema_version: 1`.
- Fresh V1 baseline database migrations.

This avoids shipping a V2 without a supported V1 and avoids a second mechanical rename after the rewrite. The breaking-change guide must call the removed implementation “prototype V1” and the replacement “formal V1”.

## Success Criteria

- Enabled agents compile into immutable execution plans before the HTTP server starts.
- Invalid node configuration, templates, expressions, resources, graph edges, and output topology fail compilation.
- A new statically linked node type can be registered without changing the DSL parser, graph compiler, scheduler, event system, or HTTP layer.
- Every run reaches exactly one terminal state: `completed`, `failed`, `cancelled`, or `interrupted`.
- Attached runs cancel after their final subscriber has remained disconnected for the configured grace period.
- Detached runs continue without subscribers and stop only through completion, failure, timeout, shutdown interruption, or explicit cancellation.
- Event subscribers can reconnect with `after_seq` and receive ordered missing events.
- Slow subscribers cannot create unbounded memory growth.
- SQLite and PostgreSQL repositories exhibit the same observable behavior.
- Formatting, strict Clippy, unit/integration tests, and real PostgreSQL tests run in CI.

## Non-Goals

- Backward compatibility with the prototype HTTP, SSE, YAML, or database contracts.
- Migration of existing run-history data.
- Cycles, loops, parallel branches, joins, map/reduce, or subflows.
- Dynamic libraries, WASM nodes, or runtime plugin downloads.
- Distributed workers, durable task recovery, or cross-process event delivery.
- Automatic retry or compensation policies.
- Agent hot reload after process startup.
- Complete multi-tenant authorization, encryption, retention management, or industry-specific compliance.
- Medical-specific model-output validation.

## Architecture

```text
Agent YAML + prompt files
        |
        v
AgentCompiler ---- NodeTypeRegistry ---- ModelRegistry / ActionRegistry
        |
        v
CompiledAgent (immutable DAG + content hash)
        |
        v
RunService ---- RunRepository
        |
        v
RunCoordinator (single lifecycle state machine)
        |
        +---- NodeExecutorRegistry
        |
        +---- EventHub ---- bounded live subscribers
                    |
                    +---- EventJournal ---- SQLite/PostgreSQL
```

The compiler owns configuration correctness. The coordinator owns lifecycle correctness. The event hub owns sequencing and delivery. HTTP is an adapter over `RunService` and `EventHub`; it does not own task lifetime.

## Source Layout

The rewrite uses these responsibilities:

```text
src/
  dsl/
    raw.rs          # Versioned YAML envelopes only
    compiler.rs     # Resource resolution and static validation
    graph.rs        # DAG validation and topological metadata
    compiled.rs     # Immutable CompiledAgent and CompiledNode

  nodes/
    registry.rs     # NodeTypeRegistry and extension contracts
    template.rs     # core.template
    chat.rs         # core.chat
    action.rs       # core.action
    condition.rs    # core.condition
    output.rs       # core.output

  resources/
    models.rs       # Named chat-model resources and providers
    actions.rs      # Unified ActionRegistry and schemas

  runtime/
    service.rs      # Create, inspect, subscribe, and cancel runs
    coordinator.rs  # Sequential DAG execution
    state.rs        # Run state machine and terminal transition guard
    context.rs      # input/run/nodes expression and template context
    attachment.rs   # Attached-run subscriber lease and grace timer

  events/
    protocol.rs     # Versioned event envelope and payloads
    hub.rs          # Sequence allocation, ring buffer, bounded subscribers
    journal.rs      # Ordered batch persistence and terminal flush barrier

  history/
    repository.rs   # Backend-independent run and event operations
    sqlite.rs
    postgres.rs

  api/
    routes.rs
    auth.rs
    sse.rs
    response.rs
```

The existing modules are removed after their replacement tests pass. The rewrite does not preserve compatibility shims.

## Formal V1 Agent DSL

### Complete Example

```yaml
version: 1
id: researcher
name: Research Assistant
description: Plans and answers a question.

input:
  schema:
    type: object
    required: [question]
    additionalProperties: false
    properties:
      question:
        type: string
        minLength: 1

prompts:
  planner: prompts/planner.md
  final: prompts/final.md

entry: plan

nodes:
  plan:
    type: core.chat
    next: answer
    emit: none
    timeout: 30s
    config:
      model: default_chat
      messages:
        - role: user
          content:
            template_ref: planner

  answer:
    type: core.chat
    next: result
    emit: content
    timeout: 30s
    config:
      model: default_chat
      messages:
        - role: user
          content:
            template_ref: final

  result:
    type: core.output
    config:
      content:
        template: "{{ nodes.answer.output.text }}"
      format: markdown
      data: null
```

### Raw Envelope

Top-level and common node envelopes reject unknown fields. Node-specific `config` remains raw JSON/YAML until the matching `NodeType` strictly deserializes it.

```rust
struct RawAgent {
    version: u32,
    id: String,
    name: String,
    description: String,
    input: RawInput,
    prompts: BTreeMap<String, String>,
    entry: String,
    nodes: BTreeMap<String, RawNode>,
}

struct RawNode {
    kind: String,
    next: Option<String>,
    emit: EmitPolicy,
    timeout: Option<DurationSpec>,
    config: serde_json::Value,
}
```

`type` deserializes into `kind`. `EmitPolicy` accepts only `none` and `content`. The default is `none`. Duration strings use an integer followed by `ms`, `s`, or `m`; zero durations are invalid.

### Compilation Pipeline

Compilation is deterministic and performs these stages:

1. Parse `version: 1` with strict top-level fields.
2. Resolve prompt paths relative to the agent directory and reject paths outside it.
3. Compile the input JSON Schema.
4. Resolve every node type through `NodeTypeRegistry`.
5. Strictly parse each node's `config` and compile its templates or CEL programs.
6. Resolve model and action aliases through their registries.
7. Validate all graph edges.
8. Reject cycles and nodes unreachable from `entry`.
9. Require every reachable path to terminate at `core.output`.
10. Reject `next` on `core.output` and require `next` on non-control nodes other than a terminal output.
11. Produce an immutable `CompiledAgent` and SHA-256 content hash over normalized YAML, resolved prompt contents, and referenced resource identifiers.

Only enabled agents must compile for server startup. Repository tests compile every checked-in agent. A deployment cannot silently start with a missing explicitly configured platform or agent path.

### Context and References

Templates and CEL expressions expose only:

```text
input
run.id
run.request_id
run.agent_id
run.agent_version
run.started_at
nodes.<node_id>.output
```

Node output is stored exactly as JSON returned by its executor. The runtime does not wrap strings in an implicit `{ "text": ... }` object. Each node contract defines its own output shape.

The compiler uses graph predecessors to reject references to nodes that cannot have completed before the referencing node. Templates use strict Handlebars rendering with HTML escaping disabled because templates produce prompts and JSON values, not HTML. Conditions use precompiled CEL programs.

## Node Extension Contract

The statically linked extension boundary is:

```rust
trait NodeType: Send + Sync {
    fn kind(&self) -> &'static str;

    fn compile(
        &self,
        node_id: &str,
        config: serde_json::Value,
        context: &CompileContext,
    ) -> Result<CompiledNodeBody, CompileError>;
}

#[async_trait]
trait NodeExecutor: Send + Sync {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError>;
}

struct NodeOutcome {
    output: serde_json::Value,
    transition: NodeTransition,
}

enum NodeTransition {
    Next,
    Goto(String),
    Complete(RunOutput),
}
```

`ExecutionControl` exposes cancellation state, the effective deadline, and `emit_content`. Content emission succeeds only for a node compiled with `emit: content`; calling it from an `emit: none` node is a typed runtime error. This gives streaming model and action implementations one bounded publication path without exposing the event hub itself.

`NodeTypeRegistry` rejects duplicate kind names. Names use namespaces such as `core.chat`, `builtin.http_request`, or `company.vector_search`. A contract test registers a synthetic node, compiles an agent using it, and executes it without changes to core compiler or coordinator code.

## Core Nodes

### `core.template`

Purpose: render a string, array, or object recursively.

Configuration:

```yaml
type: core.template
next: next_node
emit: none
config:
  value:
    query: "{{ input.question }}"
    plan: "{{ nodes.plan.output.text }}"
```

Every string leaf is a strict Handlebars template. Non-string leaves are copied unchanged. Output is the rendered JSON value. When `emit: content`, the output must be a string; other output types are a compile error for this policy.

### `core.chat`

Purpose: invoke one named chat-model resource with templated text or multimodal messages.

Configuration contains:

- `model`: required resource alias.
- `messages`: non-empty ordered role/content list.
- `parameters`: optional provider-validated JSON object.

Supported roles are `system`, `user`, and `assistant`. Content is either a text template or an ordered list of text and image-url template parts. The compiler verifies that a model used with images declares the `vision` capability.

Output shape:

```json
{
  "text": "complete generated text",
  "finish_reason": "stop",
  "usage": null
}
```

Providers may populate `finish_reason` and `usage`; absent values are `null`. Model transport is always consumed as a stream when the provider supports it. `emit: content` publishes text deltas, while `emit: none` keeps deltas private and stores only the final node output.

### `core.action`

Purpose: invoke a named capability through the unified `ActionRegistry`.

Configuration:

```yaml
type: core.action
next: render
config:
  action: current_time
  input:
    timezone: Asia/Shanghai
```

String leaves in `input` are strict templates. The compiler validates statically shaped input against the action input schema where possible; the executor validates the fully rendered input before invocation and validates returned output against the output schema.

An action descriptor declares name, input schema, output schema, and whether it is idempotent. V1 records idempotency metadata but performs no automatic retries.

The action execution context exposes cooperative cancellation, the effective deadline, and `emit_content`. Streaming actions may call `emit_content`; the compiler rejects `emit: content` when the selected action descriptor does not declare streaming-content support. The action's final JSON result remains the node output and is not inferred from emitted chunks.

### `core.condition`

Purpose: select one graph edge using ordered precompiled CEL expressions.

Configuration:

```yaml
type: core.condition
config:
  cases:
    - when: "nodes.classify.output.label == 'medical'"
      next: medical_answer
    - when: "nodes.classify.output.label == 'finance'"
      next: finance_answer
  default: general_answer
```

At least one case is required. `default` is required. The node does not use the common `next` field. Output records the selected case index and target for diagnostics. The transition is `Goto(target)`.

### `core.output`

Purpose: define the only successful graph terminal.

Configuration:

```yaml
type: core.output
config:
  content:
    template: "{{ nodes.answer.output.text }}"
  format: markdown
  data:
    answer: "{{ nodes.answer.output.text }}"
```

`content` is optional and renders to a string. `format` is required when content is present and accepts `text` or `markdown`. `data` is optional recursively rendered JSON. At least one of `content` or `data` is required. The node returns `Complete(RunOutput)` and cannot declare `next` or `emit` other than `none`.

## Model and Action Resources

Platform configuration defines named resources. Agent files reference aliases, not connection details or secret environment names.

```yaml
models:
  default_chat:
    provider: dashscope
    model: qwen3.6-flash
    capabilities: []

actions:
  enabled:
    - current_time
    - example.text_metrics
```

The formal V1 chat boundary is specific to chat models. Embedding, reranking, speech, or image generation require separate node types and provider traits rather than optional branches in `core.chat`.

The unified action boundary replaces prototype Tool and Code registries. Local Rust functions, restricted HTTP clients, and later MCP adapters are action implementations with the same JSON contract. An action descriptor declares whether it supports streaming content so the compiler can validate the node's `emit` policy.

## Run Lifecycle

### States

```text
created -> running -> completed
                   -> failed
                   -> cancelled
                   -> interrupted
```

Transitions are atomic and terminal states are immutable. A terminal transition records status and terminal metadata before the matching terminal event becomes visible.

`cancelled` means an explicit cancellation or attached-run lease expiry. `interrupted` means execution stopped because the process shut down or a stale `running` record was found at startup. Node or provider errors produce `failed`.

### Execution

The coordinator executes one node at a time. It checks cancellation before node start, while awaiting node work, and before committing output. A node output and its `node.completed` event are recorded before the successor starts.

Every executable node receives an effective timeout from its node envelope or the platform default. Timeout produces a typed node error and a failed run. V1 does not retry.

### Attached Runs

`POST /v1/agents/{agent_id}/runs/stream` creates an attached run and opens its first event subscription.

An attached run remains active while at least one event subscriber is connected. When the last subscriber disconnects, `RunService` starts the configured grace timer. A subscriber reconnecting through the events endpoint before expiry cancels the timer. Expiry requests cooperative cancellation and produces a `run.cancelled` terminal event.

The response includes `X-Run-Id`. Reconnection uses the authenticated runtime API, `run_id`, and `after_seq`; formal V1 does not introduce a second attachment secret.

### Detached Runs

`POST /v1/agents/{agent_id}/runs` creates a detached run and returns its initial representation. It continues without subscribers. It stops on completion, failure, effective run timeout, explicit cancellation, or shutdown interruption.

### Explicit Cancellation

`DELETE /v1/runs/{run_id}` is idempotent. Cancelling `created` or `running` returns the resulting cancelled run. Cancelling an already terminal run returns the unchanged terminal run. It never rewrites one terminal state into another.

### Shutdown and Startup Reconciliation

Graceful shutdown stops accepting new runs, requests cooperative cancellation of attached runs, marks active detached runs interrupted, flushes their terminal state, and then stops executors. On startup, any persisted `created` or `running` records are marked interrupted because formal V1 does not recover work.

## Event Protocol

### Envelope

```json
{
  "schema_version": 1,
  "type": "node.completed",
  "seq": 4,
  "request_id": "req_...",
  "run_id": "run_...",
  "agent_id": "researcher",
  "agent_version": "sha256:...",
  "node_id": "plan",
  "time": "2026-07-10T00:00:00Z",
  "code": "OK",
  "message": "ok",
  "data": {}
}
```

`node_id` is omitted for run-level events. Codes are stable strings rather than numeric ranges.

### Event Types

- `run.created`
- `run.started`
- `node.started`
- `content.delta`
- `node.completed`
- `node.failed`
- `run.completed`
- `run.failed`
- `run.cancelled`
- `run.interrupted`

`content.delta` contains `content` and originates only from a node with `emit: content`. `run.completed` contains the formal `RunOutput` from `core.output`; it does not concatenate all prior node deltas.

### Sequencing, Backpressure, and Replay

`EventHub` is the only sequence allocator. It keeps a bounded per-run ring buffer and gives each subscriber a bounded queue. A lagging subscriber receives a terminal transport error instructing it to reconnect with its last observed `seq`; the run is not blocked by a slow subscriber.

`EventJournal` consumes an ordered bounded queue and persists events in batches. Queue exhaustion is an infrastructure error and fails the run rather than dropping history silently. Before a terminal event is broadcast, the journal flushes prior events and atomically persists the terminal status and terminal event.

Active-run replay merges the durable journal with the in-memory ring buffer by sequence and removes duplicates. Completed-run replay uses the journal. Requests with an `after_seq` older than retained active memory still read durable events.

## History Schema

Fresh SQLite and PostgreSQL migrations define:

### `runs`

- `run_id` primary key.
- `request_id`, `agent_id`, and `agent_version`.
- `attachment` constrained to `attached` or `detached`.
- `status` constrained to formal lifecycle states.
- `started_at`, `ended_at`, and `updated_at` using native backend timestamp representation.
- `input_summary` JSON.
- `output` nullable JSON.
- `error_code` and sanitized `error_message`.

### `run_events`

- Foreign key to `runs` with cascade deletion.
- `(run_id, seq)` unique constraint.
- Event type, node ID, timestamp, code, message, and JSON data.

### `node_outputs`

- Foreign key to `runs` with cascade deletion.
- `(run_id, node_id)` primary key.
- JSON output and completion timestamp.

SQLite stores JSON as validated text where native JSON storage is unavailable. PostgreSQL uses `TIMESTAMPTZ` and `JSONB`. Repository tests, rather than shared SQL strings, enforce equivalent behavior.

Raw input is not persisted in formal V1. `input_summary` contains the top-level keys and serialized byte length without copying values. INFO logs contain identifiers, timings, counts, and output sizes but no prompt, input, model output, action input, or action output bodies.

## HTTP API

### Endpoints

```text
GET    /health
GET    /v1/agents
GET    /v1/agents/{agent_id}

POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs

GET    /v1/runs/{run_id}
GET    /v1/runs/{run_id}/events?after_seq=<u64>
DELETE /v1/runs/{run_id}
```

Run creation bodies are the agent input object directly. Attached creation returns SSE. Detached creation returns HTTP 202 with the created run. Event replay returns SSE and may subscribe to an active run after replaying missing events.

JSON responses use:

```json
{
  "code": "OK",
  "message": "ok",
  "data": {}
}
```

HTTP errors use stable codes:

- `INPUT_INVALID` -> 400.
- `UNAUTHORIZED` -> 401.
- `AGENT_NOT_FOUND` and `RUN_NOT_FOUND` -> 404.
- `RUN_CONFLICT` -> 409.
- `UPSTREAM_FAILURE` -> 502 before streaming starts, otherwise a failed run event.
- `INTERNAL` -> 500 with a sanitized message.

Compile errors prevent startup and are not exposed through runtime endpoints.

## Platform Configuration

The platform file is strictly parsed. If `PLATFORM_CONFIG` is set, the named file must exist. Relative paths resolve from the platform file's directory.

Authentication is explicit:

```yaml
auth:
  mode: disabled
```

or:

```yaml
auth:
  mode: bearer_env
  token_env: AGENT_RUNTIME_TOKEN
```

There is no `public` agent flag in formal V1. Enabled-agent selection and runtime authentication are separate platform concerns. The checked-in development configuration binds to loopback and explicitly disables auth.

Runtime configuration includes:

- Maximum concurrent runs.
- Default node and run timeouts.
- Attached reconnect grace duration.
- Per-subscriber event capacity.
- Per-run replay-ring capacity.
- Journal queue capacity and batch size.

Secret-bearing configuration types implement redacted `Debug` manually or do not implement `Debug`.

## Error Handling

Compile errors include agent ID, node ID when applicable, configuration path, and a sanitized reason. Runtime errors have a stable code, safe client message, internal source chain for logs, and optional node ID.

Provider and action errors never include credentials, authorization headers, full URLs with query strings, prompt bodies, or response bodies in client messages. Detailed source errors are logged only after applying component-specific redaction.

History write failure is not ignored. Because event replay and terminal consistency are formal V1 behavior, a journal failure transitions the run to an infrastructure failure when possible and prevents a false completed event.

## Breaking Changes and Reasons

| Prototype contract | Formal V1 contract | Reason |
|---|---|---|
| Ordered `steps` array | `entry` plus `nodes` DAG | Removes mixed implicit order, `goto`, and `end` control semantics |
| `prompt` and `text` nodes | `core.template` | Rendering and publication are separate concerns |
| Generic `llm` node | `core.chat` | Chat, embeddings, speech, and reranking require different contracts |
| Separate `tool` and `code` nodes | `core.action` | Both are typed JSON capability invocation |
| `condition` with runtime CEL compilation | `core.condition` with compiled CEL | Invalid platform-owned workflows must fail before serving traffic |
| Implicit final step | Required `core.output` | Final display and structured output become deterministic |
| `stream` flag | Common `emit` policy | Provider transport and client publication are different concerns |
| Agent `public` flag | Explicit platform auth and enabled-agent policy | Business workflow metadata must not define deployment security |
| Stream connection owns all runs | Attached and detached policies | Interactive cancellation and background work need distinct semantics |
| Transport-coupled history | Independent bounded event journal | Persistence must not depend on an SSE consumer |
| Numeric event/API codes | Stable string codes | String codes are self-describing and avoid undocumented numeric ranges |
| Prototype `/v1` | Replaced formal `/v1` | No compatibility requirement exists and the project is pre-release |

## Testing Strategy

### Compiler

- Strict unknown-field rejection at every envelope and built-in node config.
- Prompt path containment and missing prompt errors.
- Invalid JSON Schema, template, CEL, model, action, and node type errors.
- Duplicate registry names.
- Missing entry, missing edge, cycle, unreachable node, non-output terminal, output with next, and invalid predecessor reference errors.
- Stable content hash for identical normalized inputs and changed hash for changed prompt contents.

### Nodes

- Recursive template rendering for strings, arrays, and objects.
- Chat text, multimodal messages, provider parameters, deltas, and normalized output.
- Action input rendering plus input/output schema validation.
- Ordered condition cases and default branch.
- Output content-only, data-only, and combined results.
- Synthetic extension node registration, compilation, and execution.

### Runtime

- Legal and illegal lifecycle transitions.
- Success, node failure, timeout, explicit cancellation, and interruption.
- Attached disconnect grace, reconnect, and expiry cancellation.
- Detached execution without subscribers.
- Graceful shutdown and startup stale-run reconciliation.
- Exactly one terminal event and terminal state under cancellation/completion races.

### Events and History

- Monotonic sequence under concurrent action progress emission.
- Bounded slow-subscriber behavior.
- Replay from `after_seq` without gaps or duplicates.
- Ordered batch persistence.
- Terminal flush barrier.
- SQLite and PostgreSQL parity for runs, events, outputs, filters, cancellation, and replay.

### API

- Strict JSON input validation before attached SSE starts.
- Attached and detached creation.
- Run lookup, event replay, and idempotent cancellation.
- Auth enabled and explicitly disabled modes.
- Sanitized JSON and SSE failures.
- Agent listing without prompt contents or platform secrets.

## Quality Gates

The repository pins a Rust toolchain and CI runs:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

CI starts PostgreSQL and sets the integration-test URL; the PostgreSQL test must not silently pass by returning early in CI. Local execution may skip it with an explicit skipped-test message.

## Example Migration

The checked-in researcher, code-node demo, and medical-report interpreter are rewritten to formal V1. They demonstrate, respectively:

- Private intermediate chat output followed by public final chat output.
- A custom Rust action invoked through `core.action`.
- Multimodal `core.chat` nodes with an explicit `core.output` terminal.

README documentation leads with the generic compiler, node registry, action registry, lifecycle policies, and event reconnection. The medical example appears only as one vertical example.
