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
