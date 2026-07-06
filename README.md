# Insight Agent Platform

Rust service for platform-owned, config-driven agents.

## Configuration

Set environment variables:

```text
OPENAI_API_KEY=...
MODEL_PROVIDERS_CONFIG=config/models.yaml
AGENTS_DIR=agents
RUN_HISTORY_DB=data/run_history.sqlite3
BIND_ADDR=127.0.0.1:3000
```

The service loads `.env` automatically when started with `cargo run`.

Do not commit real API keys.

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

Code steps call native Rust handlers registered in the host application. YAML references the handler and maps templated inputs; the handler returns JSON, which is saved as `steps.<step_id>.output`. Handlers may also emit text while running, and that text is streamed to clients as normal `token_delta` events.

```yaml
steps:
  - id: normalize_report
    type: code
    handler: medical.normalize_report
    inputs:
      report_text: "{{ input.report_text }}"
```

The repository includes a runnable code-node example: `agents/code_node_demo`.

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"input":{"text":"hello rust world"}}' \
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

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{"input":{"question":"用中文解释这个平台的架构"}}' \
  http://127.0.0.1:3000/v1/agents/researcher/runs/stream
```

## Run History

Runs are recorded to SQLite. `RUN_HISTORY_DB` controls the database path and defaults to `data/run_history.sqlite3`.

```bash
curl http://127.0.0.1:3000/v1/agents/medical_report_interpreter/runs
curl http://127.0.0.1:3000/v1/runs/run_xxx
```

## Stream a Medical Report Interpretation

`medical_report_interpreter` supports report text, image URLs or base64 data URLs, and caller-provided conversation history.

```bash
curl -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -d '{
    "input": {
      "report_text": "血红蛋白 105 g/L，参考范围 130-175 g/L",
      "images": ["https://example.com/report.png"],
      "messages": [
        {"role": "user", "content": "这是我的体检报告，请先整体看看。"},
        {"role": "assistant", "content": "我会结合报告内容解释异常指标和后续建议。"}
      ],
      "question": "这个结果严重吗？"
    }
  }' \
  http://127.0.0.1:3000/v1/agents/medical_report_interpreter/runs/stream
```
