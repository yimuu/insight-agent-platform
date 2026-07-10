# Config-Driven Agent Platform Design

Date: 2026-07-04

## Goal

Build a Rust-based agent platform where agents are implemented inside the platform and defined by local configuration. External clients do not create or modify agents through API calls. They only discover available built-in agents and run them through a streaming HTTP interface.

The first version focuses on a usable service-side runtime:

- Load built-in agents from `agents/<agent_id>/agent.yaml`.
- Support multiple prompts per agent, with long prompts in Markdown files and short prompts inline in YAML.
- Execute fixed step types through an event-driven runner.
- Stream run progress and model tokens over Server-Sent Events.
- Call OpenAI-compatible chat completion APIs through environment-based provider configuration.
- Support a small built-in local tool registry.

## Non-Goals

The first version will not include:

- External API for creating, editing, or deleting agents.
- Web UI.
- Database persistence.
- User accounts, auth, or permissions.
- MCP or arbitrary external tool protocols.
- Long-running task recovery.
- Multi-agent collaboration.
- Looping, map/reduce, or complex workflow DSL features.

These can be added later without changing the core decision that agents are platform-owned and run through a stable event stream.

## Recommended Architecture

Use an event-driven runner with a fixed step DSL.

```text
Client
  -> POST /v1/agents/{agent_id}/runs/stream
    -> API Layer (axum)
      -> AgentRegistry
      -> RunEngine
        -> PromptRenderer
        -> ModelClient (OpenAI-compatible)
        -> ToolRegistry
      -> SSE Event Stream
```

The runner is the center of the platform. It interprets the configured steps, updates run context, and emits structured run events. The HTTP layer only adapts those events into SSE. This keeps future logging, async jobs, WebSocket support, or UI integration from depending on HTTP-specific execution logic.

## Project Layout

```text
agents/
  researcher/
    agent.yaml
    prompts/
      system.md
      planner.md
      final.md
    examples/
      basic.json

config/
  platform.yaml

src/
  main.rs
  config.rs
  error.rs

  api/
    mod.rs
    routes.rs
    sse.rs

  agent/
    mod.rs
    config.rs
    loader.rs
    registry.rs

  engine/
    mod.rs
    runner.rs
    context.rs
    event.rs
    step.rs

  model/
    mod.rs
    openai.rs
    types.rs

  prompt/
    mod.rs
    renderer.rs
    store.rs

  tools/
    mod.rs
    registry.rs
    current_time.rs
    http_get.rs
```

## Agent Configuration

Each agent is a directory. `agent.yaml` stores structured configuration, while longer prompts live in `prompts/*.md`.

```yaml
id: researcher
name: Research Assistant
description: Research and answer questions with a simple plan-first flow.

model:
  provider: openai_compatible
  model: qwen3.6-flash
  temperature: 0.3

input:
  schema:
    type: object
    required: [question]
    properties:
      question:
        type: string

prompts:
  system: prompts/system.md
  planner: prompts/planner.md
  final: prompts/final.md

steps:
  - id: plan
    type: llm
    system_prompt_ref: system
    prompt_ref: planner
    stream: true

  - id: now
    type: tool
    tool: current_time
    args:
      timezone: Asia/Shanghai

  - id: answer
    type: llm
    system_prompt_ref: system
    prompt_ref: final
    stream: true
```

Prompt references and inline prompts are both supported:

```yaml
prompt_ref: final
```

```yaml
prompt: |
  Please answer {{ input.question }} using this plan:
  {{ steps.plan.output }}
```

Rules:

- One agent can define multiple named prompts.
- Long prompts should use Markdown files.
- Short glue prompts can be inline YAML.
- `prompt_ref` and `prompt` are mutually exclusive.
- `system_prompt_ref` and `system_prompt` are mutually exclusive.
- Prompt file paths must resolve inside the agent directory.
- Missing prompt references are startup configuration errors.
- Template variables include `input.*`, `steps.<step_id>.output`, and `run.*`.

## Step Types

First version step types:

- `prompt`: render a template and store the rendered text as step output.
- `llm`: render prompts, call the model, stream token events, and store the full generated text as step output.
- `tool`: call a registered built-in local tool and store its JSON result as step output.

The DSL can reserve space for a future `branch` step, but branch execution should not be implemented in the first version. That keeps the initial runner easy to reason about while preserving the direction for conditional flows.

## Model Configuration

The platform reads provider defaults from environment variables:

```text
OPENAI_API_KEY
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
OPENAI_DEFAULT_MODEL=qwen3.6-flash
```

The API key must never be stored in agent YAML or committed to the repository. Agent config may override non-secret model settings such as:

- `model`
- `temperature`
- `max_tokens`
- provider-specific request options, if represented as structured JSON

The first model provider is OpenAI-compatible streaming chat completions. The Rust boundary should be a trait so tests can use a fake streaming model:

```rust
trait ModelClient {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream>;
}
```

## Run Context

Each run has a context object:

```text
run.id
run.agent_id
run.started_at
input
steps.<step_id>.output
```

Steps can read earlier step outputs but not future outputs. Step IDs must be unique inside one agent. The loader should validate duplicate step IDs at startup.

## Streaming Event Contract

The runner emits structured `RunEvent` values. The API layer encodes them as Server-Sent Events.

Core event names:

- `run.started`
- `step.started`
- `thinking.delta`
- `content.delta`
- `tool_call.started`
- `tool_call.completed`
- `step.completed`
- `step.failed`
- `run.completed`
- `run.failed`
- `run.cancelled`

SSE example:

```text
event: run.started
data: {"type":"run.started","seq":1,"request_id":"req_...","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:00Z","code":0,"message":"ok","data":{"status":"running"}}

event: step.started
data: {"type":"step.started","seq":2,"request_id":"req_...","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:00Z","code":0,"message":"ok","data":{"step_id":"plan","status":"running"}}

event: content.delta
data: {"type":"content.delta","seq":3,"request_id":"req_...","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:00Z","code":0,"message":"ok","data":{"step_id":"plan","content":"Rust"}}

event: run.completed
data: {"type":"run.completed","seq":8,"request_id":"req_...","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:01Z","code":0,"message":"ok","data":{"status":"completed","content":"Rust...","content_format":"markdown","output":null,"conversation":null}}
```

All events use the same top-level envelope:

- `type`
- `seq`, monotonically increasing within one run
- `request_id`
- `run_id`
- `agent_id`
- `time`
- `code`
- `message`
- event-specific `data`

The runner accumulates `content.delta.data.content` into display-ready final content. `run.completed.data.content` contains the complete Markdown response, and `run.completed.data.output` contains an optional structured object or array. Runtime failures emit `step.failed` when a step is active and then `run.failed`.

## API Surface

First version endpoints:

```http
GET  /health
GET  /v1/agents
GET  /v1/agents/{agent_id}
POST /v1/agents/{agent_id}/runs/stream
```

`GET /v1/agents` lists built-in agent metadata and input schemas. It must not return full prompt contents.

```json
[
  {
    "id": "researcher",
    "name": "Research Assistant",
    "description": "Research and answer questions with a simple plan-first flow.",
    "input_schema": {}
  }
]
```

Run request:

```http
POST /v1/agents/researcher/runs/stream
Accept: text/event-stream
Content-Type: application/json
```

```json
{
  "input": {
    "question": "Help me design a Rust agent platform"
  }
}
```

## Tool System

Tools are registered inside the platform. Agent config can only call tools already present in the registry.

```rust
trait Tool {
    fn name(&self) -> &'static str;
    fn input_schema(&self) -> JsonSchema;
    async fn call(&self, args: Value, ctx: ToolContext) -> Result<Value>;
}
```

First version built-in tools:

- `current_time`: returns the current time for a requested timezone.
- `http_get`: performs a restricted HTTP GET with timeout, response size limit, and optional allowlist.

Tool step example:

```yaml
- id: now
  type: tool
  tool: current_time
  args:
    timezone: Asia/Shanghai
```

Tool errors interrupt the run in the first version. Recovery controls such as `on_error: continue` and `fallback_output` should be deferred until there is a concrete need.

## Error Handling

Use three primary error classes:

- `ConfigError`: startup-time invalid agent or platform configuration.
- `InputError`: request input does not match the agent input schema.
- `RunError`: runtime failure during prompt rendering, model streaming, or tool execution.

Behavior:

- `ConfigError`: fail platform startup.
- `InputError`: return HTTP `400` before opening SSE.
- `RunError`: if SSE has started, emit `step.failed` when applicable, emit `run.failed`, and close the stream.
- Unknown errors should be sanitized. Never expose API keys, full upstream auth headers, or sensitive environment values.

## Validation and Testing

First version tests should cover:

- Agent directory loading.
- Prompt file resolution and path containment.
- Mutual exclusion of `prompt_ref` and `prompt`.
- Duplicate step ID rejection.
- Input schema validation.
- Prompt rendering with `input.*` and `steps.*`.
- Mock model streaming into `content.delta` events and final run content.
- Tool registry lookup and tool execution.
- SSE encoding for success and runtime error flows.

## Acceptance Criteria

The first version is complete when:

- The Rust service starts with one sample agent under `agents/`.
- `GET /v1/agents` returns the sample agent metadata.
- `POST /v1/agents/{agent_id}/runs/stream` streams structured SSE events.
- An `llm` step can call the OpenAI-compatible provider using environment variables.
- A `tool` step can call `current_time`.
- Prompt references can load multiple Markdown prompt files for one agent.
- Inline prompts work for short steps.
- Invalid agent configuration fails startup with a clear error.
- Invalid request input returns HTTP `400`.
- Runtime failures emit typed failure events without leaking secrets.
