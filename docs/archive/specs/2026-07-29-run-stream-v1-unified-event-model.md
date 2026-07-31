# Run Stream v1 统一事件模型优化规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 变更类型 | Public SSE Protocol / Wire Schema / Terminal Snapshot / Clean Cutover |
| 日期 | 2026-07-29 |
| 目标版本 | `run-stream/v1`、`insight.agent/v1` |
| 替代协议 | `/runs/stream` 上的 `response-stream/v1` |
| 影响范围 | `insight-engine`、`insight-runtime`、`insight-durable`、`insight-storage`、`insight-api`、公开 schema、Agent discovery、示例、当前文档与 conformance tests |

> **2026-07-31 superseded 注记：**本文记录的 25-event 集合是 MCP interaction 接入前的
> `run-stream/v1` 历史基线。由于协议尚未发布且没有需要兼容的旧客户端，当前
> `run-stream/v1` 已 clean-cut 扩展为 27 个闭合事件，新增
> `run.interaction.required` 与 `run.interaction.closed`。当前合同以
> [HTTP 与 SSE API](../../current/api.md) 和公开 schema 为准；本文中的 25-event 数字仅保留作为
> 当时的决策记录。

## 1. 决策摘要

当前 `/v1/agents/{agent_id}/runs/stream` 同时发送 OpenAI Responses 风格的 `response.*` 和平台扩展
`workflow.*`。两组事件最初用于区分模型输出与真实工作流执行，但在一个面向 Run 的公开流中形成了
割裂的命名、重复的公共身份和不一致的终态表达：

```text
response.completed
response.failed

workflow.tool.started
workflow.tool.completed

workflow.response.cancelled
workflow.response.interrupted
```

本规范将该端点一次性切换为新的 `run-stream/v1` 平台协议族：

1. 所有公开事件统一使用 `run.*` 根命名空间；
2. 使用 `lifecycle`、`output`、`tool`、`retrieval` 和 `stream` 五个闭合事件族；
3. 保留“模型输出的工具调用意图”与“平台真实工具执行”之间的语义分离；
4. 保留当前 25 个事件所表达的能力，不在本轮增加 Agent 调度、审批、plan 或 reasoning 事件；
5. terminal frame 不再并列携带 `response` 与 `workflow`，而只携带一个状态特化、强类型、权威的
   `run` 快照；
6. 公共身份统一为 `run_id`，Attached HTTP 响应不再公开 `X-Response-ID`；
7. SSE 仍然是一条连接对应一个 Run 的 live-only 下行流，不改为 WebSocket，也不采用 OpenClaw
   的通用 `agent + stream + data` envelope；
8. OpenAI Responses 兼容性移出该平台协议。未来如有需要，应由同一内部事实模型投影独立且严格的
   `/v1/responses` adapter；
9. 仓库仍处于 `0.1.x` 快速迭代阶段，服务端、示例和受控客户端执行一次 clean cutover，不提供旧
   `response-stream/v1` 与新 `run-stream/v1` 的双写、别名或长期兼容层。

`run-stream/v1` 是新的协议族首版，不是 `response-stream/v2`。协议名称变化使旧客户端能在 discovery
阶段明确发现不兼容，而不是把一次根语义变化伪装成原协议的增量。

本次映射不是无条件字符串替换。当前 `response.in_progress` 表示 Response 生成状态，目标
`run.lifecycle.running` 必须来自真实 Run lifecycle authority；不得在 dispatcher 建立后立即伪造
running。

## 2. 规范效力与既有合同

本文已完成实施并通过验证，现作为归档决策记录保存。当前 schema、实现、conformance tests 和
[HTTP 与 SSE API](../../current/api.md) 是可执行合同。

本次实施替代以下已归档规范中关于 `/runs/stream` 公共 wire 命名和 terminal envelope 的
结论，但不推翻它们已经验证的执行、发布、隐私、重试和校准语义：

- [Response 实时流与 LLM 发布控制规范](2026-07-19-response-streaming-and-llm-publication-design.md)；
- [Response Stream v1 工具活动可视化优化规范](2026-07-29-response-stream-v1-tool-activity-visibility-optimization.md)。

以下既有合同保持不变：

- Attached SSE 是 live-only，不支持 `Last-Event-ID`、历史 replay 或连接 resume；
- 一个 Attached SSE 连接只对应一个 Run；
- `sequence_number` 是当前连接内从零开始的全局单调序号；
- provisional output、tool progress 和 retrieval observation 丢失不能改变 Run 执行结果；
- terminal snapshot 是客户端最终校准权威，发送后立即 EOF；
- Attached 连接在 Run 非终态时断开仍提交取消意图；
- full runtime 的 durable checkpoint、claim、lease、fence、retry 和 terminal commit 仍是执行权威；
- terminal-only runtime 不因本规范新增中间持久化写入；
- LLM `stream` 继续只控制 Provider 请求模式；
- LLM `publish` 与 Action/Retrieval public policy 继续共同控制临时公开内容；
- Provider 原始正文、Action 私有输入输出、凭据、内部错误和未授权 Artifact 不得进入公共事件；
- `call_id` 继续是模型工具调用意图和平台真实工具执行之间的唯一公开关联键。

实施、current 文档、schema、baseline 与测试已经同步，本文据此标记为
`Implemented / Verified` 并归档。

## 3. 背景与问题

### 3.1 一个端点承载两个协议身份

当前端点路径和执行单位都是 Run：

```text
/v1/agents/{agent_id}/runs/stream
```

但 discovery 把它描述为 `response-stream/v1`，事件又混合：

```text
response.*
workflow.*
```

该端点并不是 OpenAI `/v1/responses` 的 SDK drop-in replacement，却要求调用方理解哪些同名事件
遵循 OpenAI 语义、哪些事件属于平台扩展、哪些 terminal frame 又在标准形状上增加了 `workflow`
字段。OpenAI 风格带来的熟悉度已经小于双层协议给客户端、文档和实现造成的认知成本。

### 3.2 terminal envelope 重复公共身份

当前成功 terminal frame 同时包含：

```json
{
  "type": "response.completed",
  "response": {
    "id": "resp_run_123",
    "object": "response",
    "status": "completed",
    "output": [],
    "usage": null,
    "error": null
  },
  "workflow": {
    "run_id": "run_123",
    "result": {},
    "tool_results": [],
    "retrievals": [],
    "usage_status": "partial"
  }
}
```

客户端必须维护 `response.id` 与 `workflow.run_id` 两个身份，并把 output、result、usage、工具结果和
检索结果跨两个对象合并。取消和中断又使用 `workflow.response.*`，名称本身同时跨越 workflow 与
response 两层概念。

目标协议只公开一个 Run 身份和一个 Run terminal snapshot。

### 3.3 命名空间承担了本应由事件族承担的语义

当前使用 `response.*` 暗示模型/响应输出，使用 `workflow.*` 暗示执行事实。这一分离目的正确，但
根命名空间不是最佳表达方式：

- 模型产生的函数调用意图应属于 `run.output.function_call.*`；
- 平台真实工具执行应属于 `run.tool.*`；
- Run 终态应全部属于 `run.lifecycle.*`；
- live broker 缺口和公共流故障应属于 `run.stream.*`。

这样客户端无需理解两个协议身份，仍能从事件族直接判断事实来源。

### 3.4 未来活动无法自然扩展

未来可能增加多 Agent 调度、审批、计划、人工输入和后台任务。继续使用 `workflow.*` 会让所有平台
活动无限堆积在一个泛化命名空间；把它们放进 `response.*` 又会错误暗示 OpenAI 标准兼容。

统一的 `run.<family>.<event>` 可以在仍保持闭合 schema 的前提下扩展，但本规范不会提前把尚未实现
的事件加入 v1。

## 4. 外部参考与对齐边界

### 4.1 OpenClaw

OpenClaw Gateway 使用长期 WebSocket 连接承载多会话、多 Run、RPC、设备和系统事件。Agent 活动
通过统一 `agent` event 发送，并在 payload 内以 `stream` 区分 `lifecycle`、`assistant`、`tool`
等活动；工具流再使用 start、update 和 terminal phase。

参考：

- [OpenClaw Agent loop](https://docs.openclaw.ai/concepts/agent-loop)；
- [OpenClaw Gateway protocol](https://docs.openclaw.ai/gateway/protocol)；
- [OpenClaw Gateway client](https://docs.openclaw.ai/gateway/clients)。

本项目吸收以下思想：

- 生命周期、输出、工具等活动必须有稳定事件族；
- 工具开始、进度和终态必须能更新同一个 UI activity；
- 每个 Run 使用独立 sequence 进行去重、排序和缺口检测；
- reconnect 后必须以权威状态校准临时 UI。

本项目不复制以下 wire 设计：

- 不把所有 SSE frame 压缩成 `event: agent` 或 `event: run`；
- 不增加开放的 `data: Record<string, unknown>`；
- 不使用 camelCase；
- 不把成功与失败压缩成带 `isError` 的同一个 result 事件；
- 不在单 Run SSE 中重复加入用于多路复用的 `runId + stream + phase` envelope；
- 不宣称 OpenClaw wire compatibility。

OpenClaw 的 envelope 适合多路复用 WebSocket；当前端点是一条连接对应一个 Run 的 SSE。使用具体
SSE event name 可以直接路由、监控和测试，并与 Rust 闭合枚举一一对应。

### 4.2 OpenAI Responses

当前 `response.*` 名称来自 OpenAI Responses streaming。目标 `run-stream/v1` 不再宣称事件级
OpenAI 对齐，但可以继续复用以下不带协议兼容承诺的数据词汇：

- output item 的 `message`、`function_call`、`file_search_call`；
- content part 的 `output_text`；
- item status 的 `in_progress`、`completed`、`failed`、`incomplete`；
- token usage 的 `input_tokens`、`output_tokens`、`total_tokens` 及 details。

这些是目标平台 schema 冻结的数据枚举，不再通过在线 OpenAI 文档动态解释。现有 vendor snapshot
保留为历史和未来 adapter 的上游参考，但不再是 `/runs/stream` 的 canonical schema。

未来严格 OpenAI adapter 必须：

1. 使用独立路由或明确协议协商；
2. 只发送 adapter 支持的标准 `response.*` 事件；
3. 不向标准 Response 对象塞入 `run`、tool execution、retrieval 或其他平台字段；
4. 从内部事实模型投影，不反向改变 canonical Run 执行或 `run-stream/v1`。

### 4.3 A2A 与 Agent 调度

A2A 是远程 Agent 互操作协议，不是本规范的公共 Run UI 协议，也不是平台内部 durable scheduler。
未来 Agent 调度应先形成一等公民的内部 AgentInvocation/Child Run，再按需要由 A2A adapter 投影远程
Task。

本规范只预留 `run.agent_call.*` 事件族的命名方向，不把任何 Agent 调度事件加入当前 25 个事件。
加入新事件仍需新的活动规范、schema 更新和客户端状态机。

## 5. 目标与非目标

### 5.1 目标

1. 一个 `/runs/stream` 只暴露一个明确的协议身份：`run-stream/v1`；
2. 25 个公开事件全部位于 `run.*` 根命名空间；
3. 事件族直接表达 lifecycle、output、tool、retrieval 和 stream control；
4. 模型工具调用意图与真实工具执行仍能通过名称和 `call_id` 清晰区分；
5. terminal frame 只包含一个状态特化的 `run` 快照；
6. 公共 Run identity 只使用 `run_id`；
7. terminal snapshot 继续包含权威 output、typed result 或 safe error、tool results、retrievals 和
   usage completeness；
8. full 与 terminal-only runtime 对外发送完全相同的 wire 语义；
9. 继续使用 Rust 闭合枚举、`deny_unknown_fields` 和独立 JSON Schema；
10. 客户端可以从具体 SSE event name 直接路由，无需解析通用 envelope 后二次分发；
11. 保持当前工具进度、耗时、安全结果、gap 和 terminal calibration 能力；
12. 实施后代码、schema、current 文档和 baseline 不再把 `/runs/stream` 描述为 Response 协议。

### 5.2 非目标

- 不实现 WebSocket Gateway；
- 不实现多 Run 或多 Conversation 事件复用；
- 不实现 SSE replay、resume 或 `Last-Event-ID`；
- 不实现 `/v1/responses`；
- 不保证 OpenAI SDK 可直接连接；
- 不实现 Agent 调度、A2A、handoff、spawn、agent message 或 `run.agent_call.*`；
- 不增加 plan、reasoning、thinking、approval、question、human task 或 compaction 事件；
- 不改变 Tool/Retrieval public policy；
- 不公开 Provider raw event、原始 stdout、私有工具参数或私有结果；
- 不改变 Action 副作用、幂等、retry、fence、cancel 或 deadline 语义；
- 不把 live output、tool progress 或 retrieval observation 写入 terminal-only durable history；
- 不重命名内部 execution ledger 的历史业务事件，除非它们直接暴露为本协议公共类型；
- 不同时维护 `response-stream/v1` 和 `run-stream/v1` 两套公共 SSE emitter。

## 6. 协议身份、发现与传输

### 6.1 Discovery

Agent discovery 的 streaming contract 改为：

```yaml
streaming:
  protocol: run-stream/v1
  transport: sse
  live_only: true
  sources:
    - id: answer
      kind: llm
      mode: streaming
      format: text
```

目标 Rust 枚举只接受：

```rust
pub enum AgentStreamingProtocol {
    RunStreamV1,
}
```

不得保留 `ResponseStreamV1` serde alias。受控客户端必须以 discovery 中的精确协议值决定是否建立
stream。

### 6.2 HTTP 路由与 Header

路由保持不变：

```text
POST /v1/agents/{agent_id}/runs/stream
POST /v1/conversations/{conversation_id}/messages/stream
```

请求继续使用：

```text
Accept: text/event-stream
Content-Type: application/json
X-Request-ID: <stable-id>
```

成功响应继续返回：

```text
X-Run-ID: run_...
X-Request-ID: ...
```

目标协议删除公共 `X-Response-ID`。`run_id` 是 admission、查询、取消、artifact、trace、Conversation
归属和 stream terminal calibration 的唯一公共执行身份。

### 6.3 SSE frame

每个 frame 的 SSE `event` 必须与 JSON `type` 完全相等：

```text
event: run.output.text.delta
data: {"type":"run.output.text.delta","sequence_number":4,...}
```

不得：

- 发送 SSE `id`；
- 发送 `retry`；
- 接受或解释 `Last-Event-ID`；
- 使用通用 `event: run` 再依赖 `stream` 或 `phase` 二次路由；
- 在同一连接发送其他 Run 的 frame。

## 7. 闭合事件集合

`RunStreamEventType::ALL` 必须精确包含以下 25 个事件。

命名语法：

```text
run.<family>.<subject?>.<event>
```

规则：

- 根对象固定为 `run`；
- family 只允许本版定义的 `lifecycle`、`output`、`tool`、`retrieval`、`stream`；
- 多词 subject 使用 snake_case，例如 `content_part`、`function_call`；
- `done` 只用于 output item/content/argument 的组装完成；
- `completed` 用于 Run、Tool 或 Retrieval 的真实执行成功；
- `failed` 表示业务或执行终态失败；
- `error` 只用于无法继续满足公共 stream 协议；
- 不混用 `end`、`finished`、`result`、`success` 等同义 phase。

### 7.1 Run lifecycle

```text
run.lifecycle.created
run.lifecycle.running
run.lifecycle.completed
run.lifecycle.failed
run.lifecycle.timed_out
run.lifecycle.cancelled
run.lifecycle.interrupted
```

### 7.2 Run output

```text
run.output.item.added
run.output.content_part.added
run.output.text.delta
run.output.text.done
run.output.content_part.done
run.output.function_call.arguments.delta
run.output.function_call.arguments.done
run.output.item.done
run.output.file_search_call.in_progress
run.output.file_search_call.searching
run.output.file_search_call.completed
```

### 7.3 Tool execution

```text
run.tool.started
run.tool.progress
run.tool.completed
run.tool.failed
```

### 7.4 Retrieval

```text
run.retrieval.completed
```

### 7.5 Stream control

```text
run.stream.gap
run.stream.error
```

除上述事件外，任何其他 `run.*` 都必须被 `RunStreamEvent` 反序列化拒绝。客户端也不得使用
`run.*` 通配符推断未知事件可以安全忽略；是否允许 additive event 是协议版本决策。

## 8. 当前事件到目标事件的完整映射

| 当前 `response-stream/v1` | 目标 `run-stream/v1` |
|---|---|
| `response.created` | `run.lifecycle.created` |
| `response.in_progress` | `run.lifecycle.running` |
| `response.output_item.added` | `run.output.item.added` |
| `response.content_part.added` | `run.output.content_part.added` |
| `response.output_text.delta` | `run.output.text.delta` |
| `response.output_text.done` | `run.output.text.done` |
| `response.content_part.done` | `run.output.content_part.done` |
| `response.function_call_arguments.delta` | `run.output.function_call.arguments.delta` |
| `response.function_call_arguments.done` | `run.output.function_call.arguments.done` |
| `response.output_item.done` | `run.output.item.done` |
| `response.file_search_call.in_progress` | `run.output.file_search_call.in_progress` |
| `response.file_search_call.searching` | `run.output.file_search_call.searching` |
| `response.file_search_call.completed` | `run.output.file_search_call.completed` |
| `response.completed` | `run.lifecycle.completed` |
| `response.failed` | `run.lifecycle.failed` |
| `error` | `run.stream.error` |
| `workflow.tool.started` | `run.tool.started` |
| `workflow.tool.progress` | `run.tool.progress` |
| `workflow.tool.completed` | `run.tool.completed` |
| `workflow.tool.failed` | `run.tool.failed` |
| `workflow.retrieval.completed` | `run.retrieval.completed` |
| `workflow.stream.gap` | `run.stream.gap` |
| `workflow.response.timed_out` | `run.lifecycle.timed_out` |
| `workflow.response.cancelled` | `run.lifecycle.cancelled` |
| `workflow.response.interrupted` | `run.lifecycle.interrupted` |

映射是 clean replacement，不是双向 alias。目标 parser 不接受左列名称，目标 emitter 不发送左列
名称。

## 9. 公共 Run 快照

### 9.1 公共状态

目标闭合状态枚举：

```text
created
running
completed
failed
timed_out
cancelled
interrupted
```

状态与 terminal event 必须精确对应：

| Event | `run.status` | Run terminal |
|---|---|---:|
| `run.lifecycle.created` | `created` | 否 |
| `run.lifecycle.running` | `running` | 否 |
| `run.lifecycle.completed` | `completed` | 是 |
| `run.lifecycle.failed` | `failed` | 是 |
| `run.lifecycle.timed_out` | `timed_out` | 是 |
| `run.lifecycle.cancelled` | `cancelled` | 是 |
| `run.lifecycle.interrupted` | `interrupted` | 是 |

不得将 timeout 映射为 `failed`，不得将 interrupted 映射为 `incomplete`，不得用附加 reason 才能
确定真正 Run 状态。

### 9.2 Created 与 running

第一帧：

```json
{
  "type": "run.lifecycle.created",
  "sequence_number": 0,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "created",
    "output": [],
    "usage": null
  }
}
```

进入 running 后：

```json
{
  "type": "run.lifecycle.running",
  "sequence_number": 1,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "running",
    "output": [],
    "usage": null
  }
}
```

`run.lifecycle.created` 固定为首帧。`run.lifecycle.running` 不是固定第二帧，只能在 runtime 的
权威 lifecycle 已进入 running 后发送，且每个连接最多一次。正常 live Run 通常会发送 running；
terminal replay、admission 后直接终止或在开始执行前失败的 Run 可以从 created 直接进入 terminal。

这两个非终态快照不包含 `result`、`error`、`tool_results`、`retrievals` 或 `usage_status`。其
`output` 固定为空；后续临时 output 由增量事件发送，完整 output 只在 terminal snapshot 校准。

### 9.3 Completed

```json
{
  "type": "run.lifecycle.completed",
  "sequence_number": 16,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "completed",
    "output": [
      {
        "type": "message",
        "id": "item_message_1",
        "status": "completed",
        "role": "assistant",
        "content": [
          {
            "type": "output_text",
            "text": "任务完成。",
            "annotations": []
          }
        ]
      }
    ],
    "result": "任务完成。",
    "tool_results": [],
    "retrievals": [],
    "usage": null,
    "usage_status": "partial"
  }
}
```

`result` 是 Agent output contract 验证后的强类型值，不要求与某个 output text 相同。`output`
是用户可见的完整输出 item 快照。

### 9.4 Failed

```json
{
  "type": "run.lifecycle.failed",
  "sequence_number": 9,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "failed",
    "output": [],
    "error": {
      "code": "LLM_PROVIDER_RESPONSE_INVALID",
      "message": "model provider returned an invalid response"
    },
    "tool_results": [],
    "retrievals": [],
    "usage": null,
    "usage_status": "partial"
  }
}
```

`error` 只包含稳定 code 和安全 message。目标公共 Run error 不包含始终为 `null` 的 OpenAI
`param`，也不重复另一份 workflow error。

### 9.5 Timed out

```json
{
  "type": "run.lifecycle.timed_out",
  "sequence_number": 9,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "timed_out",
    "output": [],
    "error": {
      "code": "RUN_TIMEOUT",
      "message": "run timed out"
    },
    "tool_results": [],
    "retrievals": [],
    "usage": null,
    "usage_status": "partial"
  }
}
```

Timeout 是独立 Run terminal 状态，不伪装为模型失败。

### 9.6 Cancelled 与 interrupted

取消：

```json
{
  "type": "run.lifecycle.cancelled",
  "sequence_number": 9,
  "run": {
    "id": "run_123",
    "object": "run",
    "status": "cancelled",
    "output": [],
    "tool_results": [],
    "retrievals": [],
    "usage": null,
    "usage_status": "partial"
  }
}
```

中断使用相同字段集合，但 event 和 status 为：

```text
run.lifecycle.interrupted
interrupted
```

状态本身已经完整表达 stop reason，不增加与 status 重复的 `reason` 字段。

### 9.7 状态特化的闭合 union

实现不得用一个拥有大量无约束 `Option<T>` 的公共 struct 表达全部快照。目标 Rust/Schema 合同必须
使用状态特化的闭合 union，至少保证：

- completed 必须有 `result`，不得有 `error`；
- failed 与 timed_out 必须有 `error`，不得有 `result`；
- cancelled 与 interrupted 不得有 `result` 或 `error`；
- created 与 running 不得有 terminal-only 字段；
- 所有变体必须拒绝未知字段；
- `run.status` 必须与外层 event variant 一致。

## 10. Output 事件合同

Output 表达模型或其他公开输出源形成的用户可见 item，不证明任何平台 Action 已执行。

所有 output item、content part、item status、role、usage 和 index 字段继续使用当前已冻结的闭合数据
形状，仅事件名和 Rust 公共类型归属切换到 Run。

### 10.1 Item added

```json
{
  "type": "run.output.item.added",
  "sequence_number": 2,
  "output_index": 0,
  "item": {
    "type": "function_call",
    "id": "item_fc_1",
    "status": "in_progress",
    "call_id": "call_1",
    "name": "progress_counter",
    "arguments": ""
  }
}
```

### 10.2 Tool-call argument intent

增量：

```json
{
  "type": "run.output.function_call.arguments.delta",
  "sequence_number": 3,
  "item_id": "item_fc_1",
  "output_index": 0,
  "delta": "{\"total\":10}"
}
```

完成：

```json
{
  "type": "run.output.function_call.arguments.done",
  "sequence_number": 4,
  "item_id": "item_fc_1",
  "output_index": 0,
  "name": "progress_counter",
  "arguments": "{\"total\":10}"
}
```

这些事件只表示模型形成了调用意图。只有 `run.tool.started` 才证明 runtime 已进入真实 Action
execution boundary。

### 10.3 Text delta

```json
{
  "type": "run.output.text.delta",
  "sequence_number": 12,
  "item_id": "item_message_1",
  "output_index": 1,
  "content_index": 0,
  "delta": "任务"
}
```

`run.output.text.done` 携带相同 identity/index 字段和完整 `text`。`run.output.content_part.added`、
`run.output.content_part.done` 与 `run.output.item.done` 继续携带当前对应的完整闭合 part/item。

### 10.4 File search call

```text
run.output.file_search_call.in_progress
run.output.file_search_call.searching
run.output.file_search_call.completed
```

三个事件继续携带：

```text
sequence_number
item_id
output_index
```

它们属于 output item 的 managed-call 状态，不替代平台 `run.retrieval.completed`。

## 11. Tool execution 事件合同

### 11.1 Started

```json
{
  "type": "run.tool.started",
  "sequence_number": 6,
  "call_id": "call_1",
  "tool_name": "progress_counter",
  "arguments": {
    "total": 10
  }
}
```

`arguments` 继续按冻结 public policy 选择性出现。`started` 表示 runtime 已获得当前执行权并将进入
Action boundary，不表示模型仅计划调用。

### 11.2 Progress

```json
{
  "type": "run.tool.progress",
  "sequence_number": 7,
  "call_id": "call_1",
  "tool_name": "progress_counter",
  "content": [
    {
      "type": "output_json",
      "json": {
        "completed": 5,
        "total": 10
      }
    }
  ]
}
```

progress 继续是 live-only、best-effort、安全 Schema 验证后的观察。content 只允许当前闭合的
`output_text` 或 `output_json`，不得携带临时 Artifact。

### 11.3 Completed

```json
{
  "type": "run.tool.completed",
  "sequence_number": 8,
  "call_id": "call_1",
  "tool_name": "progress_counter",
  "duration_ms": 12,
  "content": [
    {
      "type": "output_json",
      "json": {
        "completed": 10,
        "total": 10
      }
    }
  ]
}
```

`content: []` 继续表示“执行成功，但结果正文未获授权公开”，不表示返回了 `null`。

### 11.4 Failed

```json
{
  "type": "run.tool.failed",
  "sequence_number": 8,
  "call_id": "call_1",
  "tool_name": "progress_counter",
  "duration_ms": 12,
  "error": {
    "code": "MODEL_TOOL_ACTION_FAILED",
    "message": "The tool request was rejected."
  }
}
```

只有 logical tool call 已进入不可重试 terminal failure 时才发送。可重试 Attempt failure 不产生公共
`run.tool.failed`。

### 11.5 关联和并行

- 所有工具 activity 必须按 `call_id` 关联；
- 不得按 `tool_name`、output index、数组顺序或事件相邻性关联；
- 多个 logical call 的 started/progress/completed/failed 可以任意交错；
- 同一个 `call_id` 的 terminal tool event 最多一个；
- late progress 在 terminal tool event 或 Run terminal 后必须丢弃；
- Run 取消、timeout 或中断不要求伪造不存在的 tool failure。

## 12. Retrieval 事件合同

```json
{
  "type": "run.retrieval.completed",
  "sequence_number": 5,
  "retrieval_id": "retrieval_1",
  "query": "example query",
  "results": []
}
```

字段、结果上限、metadata、score、snippet、URI 和 Artifact 安全合同保持当前行为。`query` 继续按
public policy 可选出现。

`run.retrieval.completed` 表示平台 Retrieval 执行和公共投影完成，不等同于 output item 的
file-search managed call 状态。

## 13. Stream control 事件合同

### 13.1 Gap

```json
{
  "type": "run.stream.gap",
  "sequence_number": 13,
  "item_id": "item_message_1",
  "attempt_no": 1,
  "missing_from": 3,
  "missing_to": null,
  "unknown_tail": true,
  "action": "discard_provisional_item"
}
```

gap 继续只表达某个 provisional output item 的投递缺口，不表达：

- durable execution event 缺口；
- tool progress 丢失；
- retrieval history replay；
- 跨连接 sequence；
- Run terminal authority 丢失。

客户端收到 `discard_provisional_item` 后必须丢弃该 item 的临时聚合，并等待 terminal snapshot
校准。

### 13.2 Stream error

```json
{
  "type": "run.stream.error",
  "sequence_number": 14,
  "code": "RUN_STREAM_SNAPSHOT_INVALID",
  "message": "terminal run snapshot is invalid"
}
```

`run.stream.error` 表示公共流无法继续满足协议，不等同于 Run 执行失败。它必须是最后一帧并紧随
EOF；客户端不能据此把 Run 标记为 failed，必须通过 `GET /v1/runs/{run_id}` 查询权威状态。

目标类型必须分别提供：

```text
is_run_terminal()
ends_stream()
```

语义：

- lifecycle completed/failed/timed_out/cancelled/interrupted：
  `is_run_terminal = true`、`ends_stream = true`；
- `run.stream.error`：
  `is_run_terminal = false`、`ends_stream = true`；
- 其他事件两个值都为 false。

公共 stream error code 使用 `RUN_STREAM_*` 前缀。现有 `RESPONSE_*` stream/projection error code
必须一并迁移，不能只改 event name。

## 14. Terminal authority 与持久化

### 14.1 单一 durable Run payload

当前 durable terminal snapshot 分别保存 `response_payload` 与 `workflow_payload`，API 在发送前再
解码并组合。本规范要求新 terminal transaction 直接形成 canonical `run_payload`：

```text
run_id
protocol = run-stream/v1
terminal_kind
run_payload
public_item_manifest
snapshot_hash
created_at
```

不得在投递时临时把两个独立公共对象拼接成 target terminal frame。`run_payload` 必须：

- 在权威 Run terminal transaction 内生成；
- 经过状态特化类型和 public value limits 验证；
- 使用 canonical JSON 计算 snapshot hash；
- 与 terminal kind、Run lifecycle 和 stored output/result 一致；
- 同时供 Attached SSE terminal frame 和断言/恢复路径使用。

### 14.2 存储迁移

PostgreSQL 与 SQLite 必须使用同一逻辑迁移策略：

1. 新增或迁移到 protocol-aware 的 Run stream snapshot 存储；
2. 新写入只生成 `run-stream/v1` canonical payload；
3. 旧 `response_payload + workflow_payload` 行不得直接通过新 emitter 透传；
4. 如需保留旧历史，必须使用一次性的内部 legacy decoder 验证并转换为 canonical Run payload；
5. legacy decoder 不得公开、不得接受网络输入、不得成为长期双协议 emitter；
6. snapshot hash 必须在转换后重新以新协议域分离计算；
7. 无法验证的旧 snapshot 必须 fail closed，不能猜测字段或静默丢失 terminal result；
8. migration 和 rollback 边界必须在 PostgreSQL、SQLite 与 terminal store contract tests 中证明。

最终实现选择“明确不支持旧 snapshot 原地迁移”的边界：durable Schema 使用新的 opaque contract
ID，旧 `response_snapshots` 合同在 PostgreSQL 和 SQLite 启动校验中 fail closed；需要保留的旧历史
只能通过独立、离线且经过校验的导出/导入流程迁移。

具体表名可以由实现阶段选择，但 current 文档和公共 API 不再把存储对象称为 Response authority。
内部遗留列若为安全 migration 暂时保留，必须有删除计划，不能继续承载新写入。

### 14.3 Terminal-only

terminal-only 仍只持久化 admission 和 terminal result，不持久化：

- output delta；
- output item lifecycle；
- tool started/progress/completed live observation；
- retrieval live observation；
- stream gap；
- per-token usage observation。

目标 terminal Run payload 作为既有 terminal result 的公共投影，不得增加随 token、tool progress 或
事件数量增长的数据库写入。

## 15. 顺序与客户端状态机

### 15.1 Connection ordering

每条成功建立的 Attached SSE：

1. `run.lifecycle.created` 固定为 sequence 0；
2. `run.lifecycle.running` 只在真实 lifecycle 进入 running 后发送，每个连接最多一次；
3. 正常 live output/tool/retrieval activity 不得先于 running；terminal replay 可以没有 running；
4. created 后的每个 frame 每次严格加一；
5. output、tool、retrieval 和 gap 共用一个 connection sequence；
6. 最后一帧必须是一个 Run terminal lifecycle event 或 `run.stream.error`；
7. 最后一帧后立即 EOF。

sequence 使用饱和或 fail-closed overflow 处理，不得回绕。

### 15.2 Output item

客户端以 `item_id` 为稳定 UI identity：

```text
item.added
  -> content_part.added?
  -> text.delta*
  -> text.done?
  -> content_part.done?
  -> item.done
```

发生 retry 时新 Attempt 可以产生新的 item identity。客户端不得把不同 item 仅按 output index 合并。

### 15.3 Tool card

客户端以 `call_id` 为稳定 identity：

```text
output.function_call.arguments.done   模型调用意图完成
run.tool.started                  真实执行开始
run.tool.progress*                可丢失进度
run.tool.completed | failed       真实执行终态
```

如果 progress 或 completed 在 started 丢失后到达，客户端可以合成对应 running/completed activity；
如果重复 started 到达，必须按 `call_id` 幂等更新而不是创建第二张卡片。

### 15.4 Terminal calibration

收到 Run terminal snapshot 后，客户端必须：

1. 用 `run.output` 替换 provisional output 聚合；
2. 用 `run.tool_results` 校准仍处于 running 但已成功公开的工具；
3. 用 `run.retrievals` 校准公开 retrieval；
4. 关闭所有属于当前 Run 的临时 activity；
5. 使用 `run.status` 作为唯一 Run terminal 状态；
6. 使用 `run.usage + usage_status` 展示最终 token 完整度；
7. 不保留已被 `run.stream.gap` 标记需要丢弃的 provisional item。

工具 progress 仍不进入 terminal snapshot。没有公开 result 的成功工具继续以 `content: []` 出现在
`run.tool_results`。

### 15.5 典型工具调用时序

```text
run.lifecycle.created
run.lifecycle.running

run.output.item.added(function_call)
run.output.function_call.arguments.delta*
run.output.function_call.arguments.done
run.output.item.done(function_call)

run.tool.started
run.tool.progress*
run.tool.completed | run.tool.failed

run.output.item.added(message)
run.output.content_part.added
run.output.text.delta*
run.output.text.done
run.output.content_part.done
run.output.item.done(message)

run.lifecycle.completed | failed | timed_out | cancelled | interrupted
```

未授权、未执行或失败路径可以省略不适用的 optional activity，但不得改变以下因果关系：

- function-call arguments 未完成验证前不能执行工具；
- tool terminal 前不能把该工具结果交给模型 continuation；
- Run terminal 前必须完成 terminal barrier 与必要的 unknown-tail gap；
- Run terminal 或 stream error 后不得再发送任何 frame。

## 16. 发布、隐私与边界

本次命名切换不得扩大任何可见性：

- `publish: false` 的 LLM 不产生临时 output、tool 或 retrieval 公共 activity；
- Tool 名称、参数、progress 和 result 继续分别受冻结 public policy 控制；
- Retrieval query 与 results 继续分别受 public policy 控制；
- terminal output/result/tool_results/retrievals 必须使用与 live path 相同或更严格的安全投影；
- unknown fields、超大 payload、过深 JSON、非有限 score、非法 URI、未归属 Artifact 全部 fail
  closed；
- stream error 只发送稳定 code 和安全固定 message；
- 默认日志只记录 run_id、event type、大小、计数、duration 和安全 code，不记录正文、参数、结果或
  凭据。

公共类型从 `WorkflowTool*`/`WorkflowRetrieval*` 改名为 `RunTool*`/`RunRetrieval*` 不得改变验证
器、大小上限或授权含义。

## 17. Rust 公共模型与模块边界

目标核心类型：

```text
RunStreamEventType
RunStreamEvent
RunObjectKind
RunStatus
RunInitialSnapshot
RunCompletedSnapshot
RunFailedSnapshot
RunStoppedSnapshot
RunOutputItem
RunOutputItemStatus
RunOutputContentPart
RunOutputRole
RunUsage
RunUsageStatus
RunPublicError
RunToolContent
RunToolProgressContent
RunToolResult
RunRetrieval
RunRetrievalResult
RunStreamGapAction
```

推荐将公共 wire 从 `insight_engine::response` 移至明确的 `insight_engine::run_stream` 模块。runtime
facade、API、durable projector 和 tests 使用同一 owner module，不继续通过 `response` 模块导出目标
协议。

以下旧公共类型不提供 type alias：

```text
ResponseStreamEventType
ResponseStreamEvent
PublicResponse
ResponseStatus
ResponseOutputItem
WorkflowCompleted
WorkflowFailure
WorkflowStopped
WorkflowTool*
WorkflowRetrieval*
WorkflowStreamGapAction
```

内部 live broker 如果仍只服务该协议，也应同步使用 `LiveRunStream*` 命名，避免新代码继续产生
`Response` 与 `Workflow` 两套概念。纯内部 migration adapter 可以保留 legacy 名称，但不得重新
export。

## 18. API、Schema 与基线切换

必须新增 canonical：

```text
schemas/run-stream-v1.json
```

该 schema 必须：

- `oneOf` 精确覆盖 25 个 event；
- 所有对象默认 `additionalProperties: false`；
- type const 与 `RunStreamEventType::as_str()` 一致；
- 状态特化 terminal run snapshot；
- 复用闭合 output/tool/retrieval/artifact/usage definitions；
- 明确公共字符串、数组、JSON、artifact 和 metadata 上限；
- 不引用在线 schema；
- 不把 OpenAI vendor snapshot 作为 target protocol binding。

现有：

```text
schemas/vendor/openai-responses-streaming-2026-07-19.snapshot.json
```

可以保留作为上游历史参考，但必须移除它对当前 Agent streaming discovery 和 target baseline 的
权威地位。不得仅在该 vendor 文件中把事件字符串批量替换后声称获得 run-stream schema。

以下内容必须同步：

- `tests/baselines/phase0-wire.json`；
- public API baseline；
- Agent discovery fixtures；
- Graph product fixtures；
- HTTP SSE fixtures；
- Conversation streaming fixtures；
- SQLite、PostgreSQL、full 和 terminal-only E2E；
- `docs/current/api.md`；
- `docs/current/dsl.md`；
- `docs/current/architecture.md`；
- `CHANGELOG.md`；
- 示例 curl 与多工具 Agent 输出。

## 19. Future multi-Agent 与 Gateway 边界

### 19.1 单 Run SSE

未来父 Run 调用子 Agent 时，父 Run 可以新增独立事件族：

```text
run.agent_call.started
run.agent_call.progress
run.agent_call.completed
run.agent_call.failed
run.agent_call.cancelled
```

建议字段包括：

```text
invocation_id
target_agent_id
child_run_id?
remote_task_id?
mode
duration_ms?
safe result/error
```

这些名称只是预留方向，不属于当前 `RunStreamEventType::ALL`。正式加入时必须先定义 durable
AgentInvocation、父子取消、预算、权限、A2A 映射和 terminal calibration。

父 Run 只发送子 Agent 调用的安全 lifecycle/summary；子 Run 的详细 output/tool/retrieval 属于子
Run 自己的 stream，不复制进父 Run。

### 19.2 多 Run WebSocket Gateway

当 Control UI 需要同时观察多个 Conversation、root Run 和 child Run 时，可以增加独立 Gateway
projection：

```json
{
  "type": "event",
  "event": "run",
  "payload": {
    "root_run_id": "run_root",
    "run_id": "run_child",
    "parent_run_id": "run_parent",
    "seq": 18,
    "stream": "tool",
    "data": {}
  }
}
```

该 Gateway 可以参考 OpenClaw 的多路复用 envelope，但不能反向要求当前单 Run SSE 使用同一 wire。
两者应从内部 canonical activity facts 独立投影。

## 20. 实施阶段

所有 Phase 在一个受控 release 中完成；中间 commit 可以不具备对外兼容性，但主分支最终状态不得
同时暴露两套协议。

### Phase 1：核心类型与 schema

1. 新增 `run_stream` owner module；
2. 定义 `RunStreamEventType::ALL` 25 个闭合事件；
3. 定义状态特化 Run snapshot；
4. 定义 canonical `schemas/run-stream-v1.json`；
5. 为全部 event 建立 serde round-trip、unknown-field rejection 和 schema conformance；
6. 删除目标公共类型对旧 `response.*`/`workflow.*` 名称的 alias。

### Phase 2：Terminal authority

1. 构建 canonical durable `run_payload`；
2. PostgreSQL 与 SQLite migration；
3. snapshot hash protocol domain separation；
4. full recovery terminal projector；
5. terminal-only result projector；
6. legacy snapshot 的一次性内部迁移或明确不支持边界；
7. 删除投递时拼接 `response + workflow` 的逻辑。

### Phase 3：Live projection

1. output publication 映射到 `run.output.*`；
2. tool activity 映射到 `run.tool.*`；
3. retrieval 映射到 `run.retrieval.completed`；
4. broker gap 映射到 `run.stream.gap`；
5. fatal public protocol failure 映射到 `run.stream.error`；
6. 保持 broker 容量、优先级、late publisher 和 terminal barrier 语义。

### Phase 4：HTTP 与 discovery cutover

1. discovery 切换 `run-stream/v1`；
2. Attached SSE 初始、terminal 和 error frame 切换；
3. 删除 `X-Response-ID`，保留 `X-Run-ID`；
4. Conversation streaming 同步切换；
5. 示例与 current 文档切换；
6. 服务端和受控客户端一次性发布。

### Phase 5：验证与归档

1. 更新 wire/public API baseline；
2. 运行 full、terminal-only、SQLite、PostgreSQL 和 Conversation E2E；
3. 运行 workspace test、Clippy、fmt 和文档链接检查；
4. 更新 CHANGELOG；
5. 将本文状态改为 Implemented / Verified；
6. 移入 `docs/archive/specs` 并更新 `docs/specs/README.md`。

## 21. 测试与验收矩阵

### 21.1 Event set

- `RunStreamEventType::ALL.len() == 25`；
- enum 顺序与 canonical schema/baseline 完全一致；
- 每个 event `event_type().as_str()` 与 serialized `type` 相等；
- SSE `event` 与 JSON `type` 相等；
- parser 拒绝全部旧 `response.*`、`workflow.*` 和裸 `error`；
- parser 拒绝未知 `run.*`；
- 所有 variant 拒绝未知字段。

### 21.2 Lifecycle

- created sequence 固定 0；
- running 只来自权威 Run lifecycle，每个连接最多一次；
- 正常 live activity 不先于 running，terminal replay 可以从 created 直接到 terminal；
- 每种 Run terminal lifecycle 精确映射 event 与 `run.status`；
- terminal lifecycle 是最后一帧并紧随 EOF；
- `run.stream.error` 结束流但不被判断为 Run terminal；
- stream error 后客户端可用 `X-Run-ID` 查询 Run；
- output/result/error/reason 的状态特化不变量由 serde 和 schema 同时证明。

### 21.3 Output

- streaming 与 buffered publication 都产生目标 output event；
- publish=false 不产生临时 output；
- delta 累积与 terminal output 一致或被 gap 明确废弃；
- function-call arguments 只在现有授权条件满足时公开；
- output function-call intent 与 tool execution 通过相同 `call_id` 关联；
- retry item 不与旧 Attempt item 混合。

### 21.4 Tool

- started/progress/completed/failed wire 全部切换为 `run.tool.*`；
- progress Schema、频率、大小和 best-effort 行为不回退；
- completed/failed 保留 `duration_ms`；
- status-only completed 仍有 `content: []`；
- terminal tool_results 校准 live completion；
- retryable Attempt failure 不产生 public failed；
- parallel calls 只按 `call_id` 更新。

### 21.5 Retrieval

- live event 为 `run.retrieval.completed`；
- query/results 授权不回退；
- metadata、URI、score、snippet、Artifact 上限不回退；
- terminal retrievals 校准 live observation；
- output file-search call 与 platform retrieval 不混淆。

### 21.6 Storage

- 新 snapshot 原子存储 canonical run payload；
- hash 覆盖协议版本和全部 terminal public payload；
- SQLite/PostgreSQL hash 与 canonical bytes 一致；
- crash point 不产生半个 terminal payload；
- legacy snapshot 明确不支持的边界和 fail-closed 路径均有 PostgreSQL/SQLite 测试；
- terminal-only 写入次数不随 output delta/tool progress 数量增长；
- full recovery 后 terminal frame 与未重启执行相同。

### 21.7 HTTP 与 discovery

- discovery 只返回 `run-stream/v1`；
- Attached response 有 `X-Run-ID`，没有 `X-Response-ID`；
- 路由和 request idempotency 不变；
- non-terminal disconnect 取消行为不变；
- current docs 中所有示例可由 E2E fixture 产生；
- Conversation attached turn 使用相同 target protocol。

### 21.8 Repository hygiene

实施完成后，以下 public-contract 范围不得再出现旧 wire 字符串：

```text
crates/engine public run-stream model
crates/runtime public facade
crates/api SSE emitter
schemas/run-stream-v1.json
tests/baselines/phase0-wire.json
docs/current
active examples
```

允许出现旧字符串的范围仅限：

- `docs/archive`；
- vendor OpenAI reference；
- 一次性内部 migration fixture；
- CHANGELOG 的迁移说明；
- 明确的 future OpenAI adapter test。

## 22. Rollout 与兼容性

### 22.1 Clean cutover

本次不提供：

- 同一请求同时发送两套事件；
- query/header 选择旧 emitter；
- serde alias；
- `ResponseStreamEvent` type alias；
- 服务端内容协商降级；
- 在 terminal frame 同时发送 `run` 与 `response/workflow`。

客户端升级步骤：

1. 读取 discovery 并要求 `run-stream/v1`；
2. 替换事件名；
3. terminal parser 改为读取单一 `run`；
4. 删除 response_id 与 workflow merge；
5. 以 `run.status` 判断终态；
6. 以 `run.stream.error` 触发 GET Run 校准；
7. 更新工具卡片的 event filter。

### 22.2 混合版本部署

同一负载均衡池不得长期混跑分别发布 `response-stream/v1` 和 `run-stream/v1` 的 API 实例。发布必须
使用以下任一方式：

- maintenance/blue-green 切换整个 API cohort；
- 按环境或 host 分离旧新客户端；
- 在服务端和受控客户端同时切换的短窗口内暂停 Attached admission。

不得依赖随机命中同版本实例。readiness 和部署 smoke test 必须检查 discovery protocol 与实际首帧
一致。

### 22.3 Rollback

数据库 migration 必须在实施前定义可验证 rollback 边界。若新写入已经产生 canonical
`run-stream/v1` snapshot，回滚旧 binary 不得静默读取或覆盖新 payload。可接受方式：

- rollout 前 schema 具备双读存储能力但公共 emitter 仍单协议；
- rollback binary 明确拒绝新 protocol rows；
- blue-green 回滚同时恢复兼容的数据库 snapshot。

公共 wire 不因 rollback 需求而双发。

## 23. 风险与控制

| 风险 | 控制 |
|---|---|
| 旧客户端完全失效 | discovery 新协议名、受控 clean cutover、无静默 alias |
| 名称统一后误把模型意图当执行成功 | `run.output.function_call.*` 与 `run.tool.*` 分离 |
| terminal 合并时丢字段 | 状态特化 Run snapshot、canonical schema、旧新 fixture 对照 |
| response_id 删除破坏关联 | public 统一 run_id，内部 barrier/migration 独立处理 |
| 存储迁移破坏历史 Run | protocol-aware payload、hash、legacy fail-closed test |
| generic `run.*` 未来无限扩张 | 25 个闭合集合，新增事件必须形成规范和 schema 变更 |
| OpenAI 调用方误认为仍兼容 | protocol 改名、文档移除 aligned 声明、未来独立 adapter |
| 直接照搬 OpenClaw 导致开放 payload | 保持具体 SSE event 和 Rust closed union |
| terminal-only 写放大 | live activity 仍不持久化，只改变既有 terminal payload |
| mixed fleet 发送不同首帧 | homogeneous cohort/blue-green 和 discovery smoke test |

## 24. 被拒绝的方案

### 24.1 保持 `response.* + workflow.*`

拒绝。它保留了当前割裂、重复身份和 `workflow.response.*` 跨层命名，也会让未来 Agent activity
继续堆入泛化 workflow namespace。

### 24.2 全部改为 `response.*`

拒绝。真实 Action、Retrieval、Run cancel/timeout/interruption 不是 OpenAI Responses 标准事件。
继续使用 `response.*` 会强化不存在的 SDK compatibility 承诺。

### 24.3 通用 `event: run + stream + phase + data`

本轮拒绝。该方案适合 OpenClaw 一类多路复用 WebSocket Gateway，但当前 SSE 已由 URL 和连接绑定
唯一 Run。通用 envelope 会失去 SSE 原生事件过滤、增加二次 discriminator，并容易退化成开放 data。

### 24.4 使用 `agent.*` 作为根命名空间

拒绝。一个 Agent definition 可以产生多个 Run，一个 Run 也可能在未来调度多个 child Agent。当前
stream 的身份、sequence 和 terminal authority 都属于 Run，根命名空间应为 `run`。

### 24.5 仅重命名 event，保留 `response + workflow` terminal

拒绝。客户端仍需维护两个身份和两套 terminal object，核心割裂没有解决。

### 24.6 升级为 `response-stream/v2`

拒绝。目标不再是 Response 协议的兼容演进，而是新的平台 Run 协议。使用 `run-stream/v1` 能让协议
身份与 endpoint/authority 一致。

### 24.7 双发旧新事件

拒绝。双发会让 tool activity、delta 和 terminal 被处理两次，增加顺序与容量压力，也无法定义两个
terminal 哪一个权威。

### 24.8 在本轮同时实现 WebSocket、Agent 调度或 A2A

拒绝。它们需要多 Run subscription、双向 RPC、AgentInvocation、父子取消、权限和恢复等独立合同，
会掩盖本轮可验证的公共 wire clean cutover。

## 25. 完成定义

只有全部满足以下条件，本规范才算完成：

1. discovery 只公开 `run-stream/v1`；
2. `RunStreamEventType::ALL` 精确包含第 7 节的 25 个事件；
3. `/runs/stream` 不再发送 `response.*`、`workflow.*` 或裸 `error`；
4. 所有 SSE `event` 与 JSON `type` 相等；
5. terminal frame 只包含一个状态特化的 `run` snapshot；
6. public terminal identity 只使用 run_id；
7. Attached HTTP 不再公开 `X-Response-ID`；
8. canonical durable terminal authority 存储单一 `run_payload`；
9. full 与 terminal-only wire parity 由测试证明；
10. output intent 与真实 tool execution 继续按 `call_id` 分离并关联；
11. tool progress、duration、status-only completion 与 terminal calibration 不回退；
12. retrieval、privacy、Artifact 和大小限制不回退；
13. gap、terminal barrier、stream error 与 EOF 行为由 E2E 证明；
14. PostgreSQL、SQLite 和 legacy snapshot 明确不支持的 fail-closed 边界通过 contract tests；
15. terminal-only 没有新增随事件数量增长的持久化写入；
16. canonical JSON Schema、wire baseline 和 public API baseline 已更新；
17. `docs/current`、示例、CHANGELOG 和 Agent discovery 已同步；
18. workspace tests、Clippy、fmt、schema conformance 和文档链接检查全部通过；
19. discovery/实际首帧一致性和数据库 rollback 边界有明确 smoke/contract test 证据；
20. 本文状态改为 Implemented / Verified，并按规范流程归档。

## 26. 实施与验证记录

实施于 2026-07-29 完成，最终选择与证据如下：

- 公共协议 clean-cut 为 `run-stream/v1`，`RunStreamEventType::ALL` 和 canonical schema 均精确包含
  25 个闭合事件；旧 `response.*`、`workflow.*` 和未知 `run.*` 由 parser rejection tests 覆盖；
- durable authority 使用 `run_stream_snapshots(protocol, terminal_kind, run_payload,
  public_item_manifest, snapshot_hash)`；hash 域包含 `run-stream/v1`，terminal 投递不再组合
  `response + workflow`；
- `public_sse_protocol_covers_all_stream_publish_combinations` 在同一个 API fixture 中同时验证
  discovery 声明 `run-stream/v1`、实际首帧为 `run.lifecycle.created`，并覆盖 streaming/buffered
  与 publish on/off；
- rollout 只支持 homogeneous cohort/blue-green。rollback 不允许旧 binary 直接打开当前数据库；
  必须恢复与 binary 匹配的数据库快照。新的 durable contract ID、PostgreSQL/SQLite 旧 contract
  拒绝测试和无在线 migration/repair 路径共同证明该边界会 fail closed；
- SQLite 与真实 PostgreSQL 16 storage 全目标测试、workspace 全目标全特性测试、doc tests、
  Clippy `-D warnings`、fmt、schema/wire/public API baseline、crate boundary、cutover residual、
  bench harness 和当前文档链接检查全部通过。
