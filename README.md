# Insight Agent Platform

Rust service for platform-owned, config-driven agents.

## Configuration

Set environment variables:

```text
OPENAI_API_KEY=...
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
OPENAI_DEFAULT_MODEL=qwen3.6-flash
AGENTS_DIR=agents
BIND_ADDR=127.0.0.1:3000
```

Do not commit real API keys.

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
