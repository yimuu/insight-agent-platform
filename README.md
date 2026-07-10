# Insight Agent Platform

Rust service for platform-owned, config-driven agents.

## Configuration

Set environment variables:

```text
OPENAI_API_KEY=...
PLATFORM_CONFIG=config/platform.yaml
```

The service loads `.env` automatically when started with `cargo run`.

Do not commit real API keys.

Platform runtime settings are configured in `config/platform.yaml`. Agent exposure controls which agent directories are loaded and exposed by the service.

```yaml
bind_addr: 127.0.0.1:3000
model_providers_config: config/models.yaml

agents:
  directory: agents
  default_enabled: false
  default_public: false
  exposure:
    medical_report_interpreter:
      enabled: true
      public: true
    researcher:
      enabled: true
      public: true
    code_node_demo:
      enabled: false
      public: false

history:
  provider: sqlite
  database_url: sqlite://data/run_history.sqlite3
```

To protect runtime APIs behind a backend or gateway, configure an internal bearer token environment variable. `/health` remains public; `/v1/...` requires `Authorization: Bearer <token>` when this is set.

```yaml
auth:
  internal_token_env: AGENT_RUNTIME_TOKEN
```

The runtime also accepts caller context headers for logs:

```text
X-Request-Id
X-Caller-Service
X-Tenant-Id
X-User-Id
```

Model providers are configured in `config/models.yaml`. Provider entries define connection details, authentication environment variables, model type groups, per-type defaults, and LLM features such as `vision`.

```yaml
default_provider: dashscope
providers:
  dashscope:
    kind: openai_compatible
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: OPENAI_API_KEY
    defaults:
      llm: qwen3.6-flash
    models:
      llm:
        qwen3.6-flash: {}
        qwen-vl-plus:
          features: [vision]
      text_embedding:
        text-embedding-v4: {}
      speech2text:
        paraformer-realtime-v2: {}
```

Agents reference provider IDs:

```yaml
model:
  provider: dashscope
  type: llm
  model: qwen-vl-plus
  temperature: 0.2
```

Condition steps use CEL expressions in `cases[].when`. The expression context exposes `input`, `run`, and `steps`; text outputs are available as `steps.<step_id>.output.text`.

```yaml
steps:
  - id: branch
    type: condition
    cases:
      - when: "steps.classify.output.text == 'medical' && input.age >= 18"
        goto: medical_adult
    default: reject
```

Code steps call native Rust handlers registered in the host application. YAML references the handler and maps templated inputs; the handler returns JSON, which is saved as `steps.<step_id>.output`. Handlers may also emit text while running, and that text is streamed to clients as normal `content.delta` events.

At startup, the platform only registers code handlers referenced by enabled agents. If an agent is disabled in `config/platform.yaml`, its code-step handlers are not added to the runtime registry.

```yaml
steps:
  - id: normalize_report
    type: code
    handler: medical.normalize_report
    inputs:
      report_text: "{{ input.report_text }}"
```

The repository includes a code-node example: `agents/code_node_demo`. It is disabled in `config/platform.yaml` by default; set `agents.exposure.code_node_demo.enabled` to `true` before calling it.

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs/stream
```

## Run

```bash
cargo run
```

## List Agents

```bash
curl http://127.0.0.1:3000/v1/agents
```

## Stream a Run

Run endpoints accept the agent input object directly as the JSON request body. The `input.schema` in the agent YAML validates this body; clients do not wrap it in an extra `input` field.

Clients may pass `X-Request-Id` for request tracing. When omitted, the service generates one. Stream responses include the same `X-Request-Id` header, and every SSE event includes both `request_id` and `run_id`.

```bash
curl -N \
  -H 'x-request-id: req_demo_001' \
  -H 'authorization: Bearer <internal-token>' \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"question":"用中文解释这个平台的架构"}' \
  http://127.0.0.1:3000/v1/agents/researcher/runs/stream
```

Each SSE `event` name matches the JSON `type`. Events use one envelope with monotonically increasing `seq` values within a run:

```text
event: content.delta
data: {"type":"content.delta","seq":3,"request_id":"req_demo_001","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:00Z","code":0,"message":"ok","data":{"step_id":"answer","content":"Rust"}}

event: run.completed
data: {"type":"run.completed","seq":8,"request_id":"req_demo_001","run_id":"run_...","agent_id":"researcher","time":"2026-07-10T00:00:01Z","code":0,"message":"ok","data":{"status":"completed","content":"Rust...","content_format":"markdown","output":null,"conversation":null}}
```

`content.delta.data.content` is display-ready incremental content. `run.completed.data.content` is the complete display content, while `run.completed.data.output` contains an optional structured object or array. Runtime failures emit `step.failed` when a step is active, followed by `run.failed`.

## Run History

Runs are recorded through sqlx. `history` in `config/platform.yaml` selects the backend and defaults to SQLite at `sqlite://data/run_history.sqlite3` when no platform config exists.

Run-history migrations currently target fresh development databases. After a migration schema changes, delete `data/run_history.sqlite3` or recreate the PostgreSQL development volume instead of upgrading existing history data.

```yaml
history:
  provider: sqlite
  database_url: sqlite://data/run_history.sqlite3
```

PostgreSQL can be enabled by using a PostgreSQL URL directly or by reading it from an environment variable:

```yaml
history:
  provider: postgres
  database_url_env: RUN_HISTORY_DATABASE_URL
```

The old `history.db` and `RUN_HISTORY_DB` SQLite path settings are still accepted for compatibility.
Run records include request and caller context fields when provided: `request_id`, `caller_service`, `tenant_id`, and `user_id`.
Run list endpoints return paginated data and support filters. Supported query parameters are `agent_id`, `request_id`, `caller_service`, `tenant_id`, `user_id`, `status`, `started_after`, `started_before`, `after`, and `limit`.

List responses use this shape:

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "items": [],
    "next_cursor": null
  }
}
```

Use `data.next_cursor` as the next request's `after` parameter.

```bash
curl http://127.0.0.1:3000/v1/agents/medical_report_interpreter/runs
curl 'http://127.0.0.1:3000/v1/agents/medical_report_interpreter/runs?user_id=user_456'
curl 'http://127.0.0.1:3000/v1/runs?agent_id=medical_report_interpreter&status=completed&limit=20'
curl 'http://127.0.0.1:3000/v1/runs?request_id=req_demo_001&after=<next_cursor>'
curl http://127.0.0.1:3000/v1/runs/run_xxx
```

To run PostgreSQL history integration tests locally:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test postgres_history_store_records_and_filters_runs_when_configured
```

## Stream a Medical Report Interpretation

`medical_report_interpreter` supports report text, image URLs or base64 data URLs, and caller-provided conversation history.

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{
    "report_text": "血红蛋白 105 g/L，参考范围 130-175 g/L",
    "images": ["https://example.com/report.png"],
    "messages": [
      {"role": "user", "content": "这是我的体检报告，请先整体看看。"},
      {"role": "assistant", "content": "我会结合报告内容解释异常指标和后续建议。"}
    ],
    "question": "这个结果严重吗？"
  }' \
  http://127.0.0.1:3000/v1/agents/medical_report_interpreter/runs/stream
```
