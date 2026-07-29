# Response Stream v1 工具活动可视化优化规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 变更类型 | Public SSE Protocol / Action SPI / Runtime Observation / In-place v1 Cutover |
| 日期 | 2026-07-29 |
| 目标版本 | `response-stream/v1`、`insight.agent/v1` |
| 影响范围 | `insight-engine`、`insight-resources`、`insight-runtime`、`insight-durable`、`insight-api`、公开 schema、示例 Agent、当前文档与 conformance tests |

## 1. 决策摘要

本规范在不改为 `response-stream/v2` 的前提下，增强当前 `response-stream/v1` 的工具活动可见性。
仓库仍处于 `0.1.x` 快速迭代阶段，服务端、示例和调用方按一次 clean cutover 同步升级；当前阶段不为
旧版 v1 客户端提供跨版本兼容承诺。

本轮吸收 OpenClaw Control UI 的工具执行体验，但不复制 OpenClaw Gateway 的事件封装、命名和开放
字段模型。平台继续保留：

- OpenAI Responses 风格的 `response.*` 事件；
- 平台自有的 `workflow.*` 扩展命名；
- snake_case 字段；
- Rust 闭合枚举和 `deny_unknown_fields`；
- Action 与 Agent 双重发布授权；
- Schema 验证后才允许公开参数、进度和结果；
- terminal snapshot 作为最终校准依据。

必须实施以下变更：

1. 新增 `workflow.tool.progress`，表达工具运行中的安全、结构化、best-effort 进度；
2. 为 `workflow.tool.completed` 和 `workflow.tool.failed` 增加 `duration_ms`；
3. 扩展 `ToolPublicPolicy`，增加可选的闭合 `progress` Schema；
4. 为 Action 提供不会改变执行结果的 best-effort progress publisher；
5. terminal `workflow.tool_results` 必须包含所有已授权公开调用元数据且执行成功的工具，即使其结果
   内容保持私有，此时 `content` 为空；
6. 为安全的内置示例工具公开受控结果，使 `tool_assistant` 能真实展示多个工具结果；
7. 明确客户端按 `call_id` 合并生命周期、处理并行交错、容忍进度丢失并在 terminal frame 校准。

本轮不引入 OpenClaw 的通用 `agent + stream + data` envelope，不增加 `item`、`plan`、`thinking`、
`approval`、`terminal.data` 等与当前 Run SSE 范围无关的事件。

## 2. 规范效力与既有合同

本文是
[Response 实时流与 LLM 发布控制规范](2026-07-19-response-streaming-and-llm-publication-design.md)
的增量规范。实施完成前，当前 schema、实现、测试和
[HTTP 与 SSE API](../../current/api.md) 仍是可执行合同；本文不能用于推断尚未交付的事件。

实施完成后必须同步：

- `ResponseStreamEventType::ALL`；
- `ResponseStreamEvent` 闭合 wire enum；
- `schemas/vendor/openai-responses-streaming-*.snapshot.json`；
- `tests/baselines/phase0-wire.json`；
- HTTP SSE、SQLite、PostgreSQL、full 和 terminal-only 测试；
- `docs/current/api.md` 与 `docs/current/dsl.md`；
- `CHANGELOG.md`；
- 本文状态和 `docs/specs/README.md`。

实现和当前文档全部成为事实后，本文移入 `docs/archive/specs`。

以下既有合同不变：

- Attached SSE 是 live-only，不支持 `Last-Event-ID`、replay 或 resume；
- Attached 连接在非终态断开时继续提交取消意图；
- provisional 事件丢失不能改变 Action、LLM continuation 或 Run 的执行结果；
- full runtime 的 durable checkpoint、claim、lease、fence 和 retry 仍是执行权威；
- terminal-only runtime 不因本规范重新持久化中间事件；
- `stream` 继续只控制 Provider 请求模式；
- LLM `publish` 继续控制该节点的 provisional 用户可见性；
- terminal snapshot 必须在权威 Run result 提交后发送，并在发送后 EOF；
- Provider 原始正文、Action 私有输入/输出、凭据和内部错误不得进入公共事件或默认日志。

## 3. 背景与当前缺口

当前实现已经提供：

```text
response.function_call_arguments.delta
response.function_call_arguments.done

workflow.tool.started
workflow.tool.completed
workflow.tool.failed
```

两组事件表达不同事实：

- `response.function_call_arguments.*` 表示模型正在形成或已经形成工具调用意图；
- `workflow.tool.*` 表示平台已经开始、完成或终止真实 Action 执行。

这个分层是正确的，但当前用户体验仍有四个明显缺口。

### 3.1 示例工具只公开状态

`current_time`、`text_metrics`、`integer_calculator` 和 `text_replace` 当前都使用：

```text
call: true
arguments: private
result: none
```

因此客户端能看到工具名和成功/失败，却看不到任何成功结果：

```json
{
  "type": "workflow.tool.completed",
  "call_id": "call_123",
  "tool_name": "integer_calculator",
  "content": []
}
```

terminal `workflow.tool_results` 还会完全省略没有公开 result Schema 的成功调用，客户端无法在
provisional completion 丢失后确认这类工具已经成功。

### 3.2 没有执行中进度

Action SPI 只有一次性的 `call(input, context) -> output`。长命令、HTTP、浏览器、文件处理或未来的
外部任务无法公开“仍在执行”以及有界进度。客户端只能从 `started` 一直等待到
`completed/failed`。

### 3.3 没有工具耗时

`workflow.tool.completed/failed` 不包含耗时。客户端可以用本地接收时间估算，但在重试、排队、
网络延迟或丢失 `started` 时不准确，也不能稳定用于性能展示。

### 3.4 协议语义没有形成客户端状态机

当前文档只要求以 `call_id` 关联调用，没有明确：

- 并行工具事件可以怎样交错；
- 重试时重复 `started` 如何处理；
- `progress` 在 `started` 丢失时如何处理；
- terminal frame 到达时怎样关闭残留 running 卡片；
- 哪些字段是模型意图、哪些字段是执行事实；
- 结果为空是“执行没有结果”还是“结果未授权公开”。

## 4. 外部参考与对齐边界

OpenClaw 当前通过 Gateway WebSocket 的 `agent` 事件转发 Agent 活动，payload 以 `stream` 区分
`lifecycle`、`assistant`、`tool` 等活动：

```text
event: agent
payload.stream: lifecycle | assistant | tool | ...
```

其中工具流表达 start、update 和 terminal activity；当前 OpenClaw adapter 使用以下字段关联并归并
工具卡片：

```text
phase: start | update | result
toolCallId
name
args | partialResult | result
isError
```

Control UI 再把这些事件渲染为工具卡片、命令输出、diff、耗时和聚合摘要。参考：

- [OpenClaw Agent loop](https://docs.openclaw.ai/concepts/agent-loop)
- [OpenClaw Control UI](https://docs.openclaw.ai/web/control-ui)
- [OpenClaw Gateway protocol](https://docs.openclaw.ai/gateway/protocol)
- [OpenClaw ACP translator](https://github.com/openclaw/openclaw/blob/main/src/acp/translator.ts)

本项目只对齐以下语义：

| 语义 | OpenClaw | 本项目目标 |
|---|---|---|
| 工具开始 | `tool/start` | `workflow.tool.started` |
| 工具进度 | `tool/update` | `workflow.tool.progress` |
| 工具成功 | `tool/result, isError=false` | `workflow.tool.completed` |
| 工具失败 | `tool/result, isError=true` | `workflow.tool.failed` |
| 调用关联 | `toolCallId` | `call_id` |
| 工具耗时 | item/command timing | `duration_ms` |
| 最终恢复 | history/session projection | terminal workflow snapshot |

以下内容明确不对齐：

- 不改成 WebSocket Gateway envelope；
- 不把事件收敛为 `event: agent`；
- 不使用开放的 `data: Record<string, unknown>`；
- 不改用 camelCase；
- 不把成功和失败压缩为一个带 `isError` 的 result 事件；
- 不复制会话、设备、语音、terminal、approval、cron 和 plugin 广播事件；
- 不宣称 OpenClaw wire compatibility。

如果未来需要让 OpenClaw 客户端直接连接本平台，必须实现独立协议 adapter，把内部活动事实投影成
OpenClaw Gateway 事件；不得为表面兼容而改变核心 SSE 合同。

## 5. 目标与非目标

### 5.1 目标

1. 客户端可以可靠展示工具的 running、progress、completed 和 failed 状态；
2. 多个并行工具通过稳定 `call_id` 独立更新，不依赖事件相邻；
3. 工具可以公开有界、Schema 验证后的进度；
4. 工具完成和失败事件包含服务端计算的 `duration_ms`；
5. status-only 成功工具可以通过 terminal snapshot 校准；
6. 示例 Agent 至少展示三个带公开结果的不同工具；
7. 参数、进度和结果继续分别授权，任一层未授权都必须 fail closed；
8. progress broker 丢失、客户端断开或慢消费者不得阻塞 Action；
9. full 与 terminal-only runtime 对外产生相同 wire 语义；
10. 保持 OpenAI Responses function-call 事件的现有字段和顺序语义。

### 5.2 非目标

- 不构建仓库外的 Web 或桌面 Control UI；
- 不实现通用 `workflow.item.*` UI activity feed；
- 不公开模型 reasoning 或 thinking；
- 不在本轮增加 plan、approval、question 或 compaction 事件；
- 不把 progress 持久化为 Conversation message、execution event 或 terminal result；
- 不允许对原始 stdout、Provider chunk 或任意 partial JSON 做字符串级“脱敏后公开”；
- 不让模型看到 progress；模型 continuation 仍只消费最终 Action result；
- 不承诺所有 Action 都有 progress 或公开 result；
- 不增加跨 Run 多路复用；一个 Attached SSE 仍对应一个 Run；
- 不提供旧 v1 客户端和新 v1 服务端混用的长期兼容层；
- 不改变工具副作用、幂等、重试、取消和 fence 语义。

## 6. 术语与状态机

### 6.1 术语

| 术语 | 定义 |
|---|---|
| model tool intent | 模型输出的工具名、`call_id` 和参数；尚未证明 Action 已执行 |
| logical tool call | 一个模型生成的稳定 `call_id` 对应的完整工具调用，可能经历多个执行 Attempt |
| tool attempt | runtime 对 logical tool call 的一次 fenced Action 执行尝试 |
| tool progress | Action 运行期间主动发布的安全、非权威、可丢失观测 |
| public call metadata | 已授权公开的 `call_id` 和 `tool_name` |
| public arguments | 按 `private`、字段列表或 `all` 投影的模型可见参数 |
| public result | 完整 Action output 通过冻结 result Schema 后形成的 caller-visible content |
| terminal calibration | 客户端用最终 `response.*` 中的 workflow snapshot 修正 provisional UI |

### 6.2 客户端状态机

客户端必须把同一 `call_id` 合并为一个逻辑工具卡片：

```text
unknown
   │
   ├── started ───────────────▶ running
   │                              │
   └── progress ───────────────▶ running
                                  │
                                  ├── progress ─▶ running
                                  ├── completed ─▶ completed
                                  └── failed ────▶ failed
```

约束：

- `progress` 可以在 `started` 丢失后到达；客户端必须惰性创建 running 卡片；
- durable retry 可能让同一 `call_id` 再次出现 `started`；客户端必须幂等更新，不得创建重复卡片；
- 不同 `call_id` 的事件可以任意交错；
- 同一连接上的 `sequence_number` 决定消费顺序；
- terminal event 到达后，后续同一 `call_id` 的 provisional 事件必须忽略；
- `response.completed` 到达时，仍为 running 且能在 `workflow.tool_results` 找到的调用改为 completed；
- `response.failed`、`workflow.response.timed_out/cancelled/interrupted` 到达时，仍未终态的卡片随
  Run 终态关闭；客户端不得伪造具体工具错误；
- `content: []` 表示调用和成功状态允许公开，但结果正文没有获得公开授权，不表示 Action 返回空值。

## 7. v1 原地演进与切换策略

### 7.1 为什么不升级 v2

仓库和全部 workspace crate 当前版本为 `0.1.0`，仍处于 pre-1.0 快速迭代阶段。当前没有已经发布
并要求长期兼容的独立客户端矩阵。本轮增加一个事件并扩充工具终态字段时，继续使用
`response-stream/v1`，避免为尚未稳定的协议维护双栈。

这是一项有意的 in-place v1 cutover，不代表 1.0 后仍可用同样方式修改 v1。

### 7.2 切换规则

- 不双发旧/新工具终态事件；
- 不增加配置开关选择旧 v1；
- server、公开 schema、示例、测试和受控客户端必须同一变更集更新；
- 新客户端必须忽略它不理解的未来 SSE event name，但本轮服务端不依赖该容错提供兼容承诺；
- 实施窗口不得保留旧 binary 与新 schema 的混合滚动部署；
- full runtime 升级前必须让旧进程停止领取新工作；
- 如果实现需要改变持久化 model-tool timing 数据，按现有 pre-1.0 schema provisioning 流程执行
  clean cutover，不在请求路径动态迁移。

### 7.3 1.0 后规则

在 public 1.0 compatibility boundary 建立后：

- 给闭合事件新增必需字段；
- 删除或重命名事件；
- 修改字段含义；
- 收紧已接受的枚举值；

都必须升级协议版本或提供明确的兼容期。本文只授权当前这一次 pre-1.0 v1 原地优化。

## 8. 目标事件集合

实施后 `ResponseStreamEventType::ALL` 从 24 个增加到 25 个。

### 8.1 保留的 Responses 事件

```text
response.created
response.in_progress
response.output_item.added
response.content_part.added
response.output_text.delta
response.output_text.done
response.content_part.done
response.function_call_arguments.delta
response.function_call_arguments.done
response.output_item.done
response.file_search_call.in_progress
response.file_search_call.searching
response.file_search_call.completed
response.completed
response.failed
error
```

### 8.2 目标 workflow 扩展事件

```text
workflow.tool.started
workflow.tool.progress
workflow.tool.completed
workflow.tool.failed
workflow.retrieval.completed
workflow.stream.gap
workflow.response.timed_out
workflow.response.cancelled
workflow.response.interrupted
```

除新增 `workflow.tool.progress` 和第 9 节明确扩充的字段外，其他事件 wire shape 不变。

## 9. 工具事件 wire 合同

### 9.1 `workflow.tool.started`

结构保持不变：

```json
{
  "type": "workflow.tool.started",
  "sequence_number": 7,
  "call_id": "call_123",
  "tool_name": "integer_calculator",
  "arguments": {
    "operation": "add",
    "left": 20,
    "right": 22
  }
}
```

`arguments` 继续是可选字段：

- `private`：字段缺省；
- 字段列表：只包含完整参数对象中授权字段；
- `all`：包含完整参数，并允许发送标准 function-call item/argument 事件。

`started` 表示 runtime 已取得当前执行权并即将进入 Action boundary，不表示模型只是计划调用工具。

### 9.2 `workflow.tool.progress`

新增结构：

```json
{
  "type": "workflow.tool.progress",
  "sequence_number": 8,
  "call_id": "call_123",
  "tool_name": "example.long_running",
  "content": [
    {
      "type": "output_json",
      "json": {
        "stage": "processing",
        "completed": 40,
        "total": 100
      }
    }
  ]
}
```

字段：

| 字段 | 类型 | 合同 |
|---|---|---|
| `sequence_number` | integer | 当前 SSE 连接全局单调递增 |
| `call_id` | string | 模型生成并经 runtime 验证的稳定调用 ID |
| `tool_name` | string | 冻结的 Action 名称 |
| `content` | array | 1 到上限个安全 progress content part |

progress content 使用闭合 union：

```text
output_text
output_json
```

本轮不允许 progress 携带 `output_image`、`output_file` 或 `output_audio`。Artifact 必须先完成持久化、
完整性校验和 public reference 绑定，再通过最终 result 或独立 Artifact 合同公开；临时 progress 不得
创建无法由 terminal snapshot 引用的孤立 Artifact。

每个 progress payload 必须：

1. 来自 Action 主动调用 progress publisher；
2. 通过冻结的 public progress Schema；
3. 通过公共 JSON 深度、节点数、字符串长度和总字节限制；
4. 通过 LLM `publish: true` 与 Action `public.call/progress` 双重授权；
5. 在进入 live broker 前转换为闭合 public content。

### 9.3 `workflow.tool.completed`

目标结构：

```json
{
  "type": "workflow.tool.completed",
  "sequence_number": 9,
  "call_id": "call_123",
  "tool_name": "integer_calculator",
  "duration_ms": 18,
  "content": [
    {
      "type": "output_json",
      "json": {
        "value": 42
      }
    }
  ]
}
```

`duration_ms` 为非负整数，表示 logical tool call 从第一次进入 Action execution boundary 到成功
terminal commit 的服务端 wall-clock 时长，包含 retry/backoff。它不是 CPU time、Provider latency
或客户端感知耗时。

成功调用在 `public.call: true` 时总是发送 `completed`：

- result 已授权：`content` 包含验证后的公开结果；
- result 未授权：`content` 是空数组；
- public result 声明存在但实际结果不符合冻结 Schema：不得降级为空数组，必须按既有
  `MODEL_TOOL_PUBLIC_RESULT_INVALID` 不变量失败。

### 9.4 `workflow.tool.failed`

目标结构：

```json
{
  "type": "workflow.tool.failed",
  "sequence_number": 9,
  "call_id": "call_123",
  "tool_name": "example.long_running",
  "duration_ms": 1804,
  "error": {
    "code": "MODEL_TOOL_ACTION_FAILED",
    "message": "The tool request was rejected."
  }
}
```

只有 logical tool call 已经进入不可重试 terminal failure 时才发送 `failed`。单次 Attempt 的可重试
失败不能发送公共 terminal failure。

Run 级取消、超时或中断可以直接以：

```text
workflow.response.cancelled
workflow.response.timed_out
workflow.response.interrupted
```

关闭调用方 UI；平台不得为了补齐卡片而伪造一个并不存在的 Action 业务错误。

`error` 继续只允许稳定 code 和固定安全 message，不包含 Action 原始错误正文、参数、结果或凭据。

### 9.5 duration authority

full runtime 必须从 durable logical call timing fact 计算 `duration_ms`。如果当前模型工具表没有可用的
第一次 execution start 时间，实现必须增加对应持久化字段或等价事实；不得只依赖某个 worker
进程的 `Instant`，否则重试或重启会改变含义。

terminal-only runtime 可以在进程内记录相同边界。所有计算必须：

- 使用服务端时间；
- 对负值、时钟回拨和整数溢出 fail closed 或饱和为 `u64::MAX`；
- 不把 duration 当作执行正确性、deadline 或 billing authority；
- 不在默认日志中记录参数或结果来解释 duration。

## 10. terminal snapshot 校准

### 10.1 成功工具

当前 terminal projector 只返回拥有 public result Schema 的工具。本规范改为：

- `public.call: false`：整个工具调用保持私有，`tool_results` 省略；
- `public.call: true, result: none`：包含一个 `content: []` 的 `WorkflowToolResult`；
- `public.call: true, result: schema`：包含验证后的公开 `content`。

示例：

```json
{
  "workflow": {
    "run_id": "run_123",
    "result": "done",
    "tool_results": [
      {
        "call_id": "call_status_only",
        "tool_name": "text_replace",
        "content": []
      },
      {
        "call_id": "call_public_result",
        "tool_name": "integer_calculator",
        "content": [
          {
            "type": "output_json",
            "json": {
              "value": 42
            }
          }
        ]
      }
    ],
    "retrievals": [],
    "usage_status": "complete"
  }
}
```

`tool_results` 顺序继续使用 durable invocation order，而不是 live event 到达顺序。

### 10.2 失败与停止

本轮不新增 terminal `tool_failures`。客户端在 Run 失败、超时、取消或中断时：

- 保留已经收到的具体 `workflow.tool.failed`；
- 对仍为 running 的工具只标记为随 Run 停止；
- 不显示伪造的工具 error code；
- 以顶层 workflow error/stop reason 解释 Run 终态。

未来如果产品需要在断线后精确恢复每个失败工具的 terminal outcome，应单独设计统一
`tool_outcomes`，不能把失败项塞进名为 `tool_results` 的成功结果集合。

### 10.3 progress 不进入终包

progress 不进入：

- `workflow.tool_results`；
- Conversation message；
- final assistant message；
- durable execution history；
- detached GET Run 响应。

terminal snapshot 只校准调用是否成功及其公开最终结果，不重放中间进度。

## 11. Action 公共发布策略

### 11.1 目标结构

`ToolPublicPolicy` 增加可选 `progress`：

```yaml
public:
  call: true
  arguments: private
  progress:
    type: object
    required: [stage, completed, total]
    properties:
      stage:
        type: string
      completed:
        type: integer
        minimum: 0
      total:
        type: integer
        minimum: 1
    additionalProperties: false
  result:
    type: object
    required: [value]
    properties:
      value:
        type: integer
    additionalProperties: false
```

Rust 字段建议：

```rust
pub struct ToolPublicPolicy {
    pub call: bool,
    pub arguments: ToolPublicArguments,
    pub progress_schema: Option<Value>,
    pub result_schema: Option<Value>,
}
```

wire 名分别为 `progress` 和 `result`。

`progress_schema` 必须使用 `default + skip_serializing_if = None` 的规范化方式，使没有 progress 的
既有冻结 policy 在解码和重新规范化后保持相同 canonical bytes，不因新增空字段改变 descriptor
identity。

### 11.2 不变量

- `call: false` 时，`arguments` 必须为 private，`progress` 和 `result` 必须缺省；
- `progress` 和 `result` 必须是自包含、闭合的 Draft 2020-12 JSON Schema；
- 禁止开放对象、远程 `$ref`、动态引用和无界字符串；
- progress Schema 描述一次公开 progress payload，不描述内部累计状态；
- result Schema 描述完整 Action output 的公开合同，现有行为保持不变；
- `arguments: all` 只授权原始 Provider argument delta，不自动授权 progress 或 result；
- `publish: false` 时，即使 Action policy 允许，仍不得发送任何 workflow tool 事件；
- policy 必须冻结到 Deployment Revision，运行时不能查询当前 registry policy 替代冻结事实。

### 11.3 无公开 progress 时的处理

Action 可以拥有内部进度，但只有冻结 progress Schema 存在时才安装 public publisher。没有公开
progress 合同时：

- runtime 不序列化、不验证、不排队该值；
- 不产生公共事件；
- 不把该值写日志；
- 不影响 Action result。

## 12. Action Progress SPI

### 12.1 接口形态

`ActionContext` 增加只用于当前调用的 progress publisher。具体 Rust API 可以调整，但语义必须等价于：

```rust
pub enum ActionProgressDisposition {
    Published,
    Suppressed,
    Dropped,
}

impl ActionContext {
    pub fn publish_progress(
        &self,
        value: Value,
    ) -> Result<ActionProgressDisposition, ActionProgressError>;
}
```

### 12.2 发布语义

- `Published`：payload 已通过冻结 Schema 和公共大小限制，并进入 best-effort broker；
- `Suppressed`：调用方未 `publish`、Action 没有公开 progress 合同或公共 call 不允许；
- `Dropped`：broker 已满、关闭、订阅者丢失或事件超出临时队列预算；
- `Err`：Action 提交了不符合其冻结公开 progress Schema 的值。

`Dropped` 和 `Suppressed` 不得让 Action 失败。非法 public progress 返回 `Err`，Action 作者可以选择
传播为稳定业务失败，也可以停止发送进度并继续；无论如何，非法 payload 都不得进入 broker、日志或
terminal snapshot。

### 12.3 执行隔离

progress publisher：

- 必须绑定当前 `run_id`、logical `call_id`、tool name 和 execution fence；
- 过期 lease/fence 的 publisher 必须静默失效或返回 `Dropped`；
- Action 返回后 publisher 必须关闭，晚到 progress 不得越过 completed/failed；
- clone 只能在当前 Action 调用生命周期内使用；
- 不能提供访问其他 Run、其他 call 或公共 broker 的通用句柄；
- 不能让慢消费者对 Action 形成 backpressure；
- 不能成为 Action 是否成功、retry 或 commit 的输入。

## 13. 顺序、并行、重试与丢失

### 13.1 单次模型调用顺序

当 `arguments: all` 且所有事件均未丢失时，一个工具调用的典型顺序是：

```text
response.output_item.added(function_call)
response.function_call_arguments.delta*
response.function_call_arguments.done
response.output_item.done(function_call)
workflow.tool.started
workflow.tool.progress*
workflow.tool.completed | workflow.tool.failed
```

如果 arguments 不是 `all`，标准 function-call 事件可以全部缺省，但 workflow lifecycle 仍按独立
授权发送。

### 13.2 并行工具

并行调用只保证：

- 每个 `call_id` 的 producer-local 顺序；
- SSE `sequence_number` 的最终传输顺序；
- 每个 logical call 至多有一个权威 terminal outcome。

不同调用允许：

```text
A.started
B.started
B.progress
A.progress
B.completed
A.completed
```

客户端禁止根据数组位置、工具名或事件相邻性关联调用。

### 13.3 retry

- retry 不能创建新的模型 `call_id`；
- retryable Attempt failure 不发送 `workflow.tool.failed`；
- 重复 `started` 必须由客户端视为同一逻辑卡片的重新执行信号；
- `duration_ms` 覆盖从第一次执行开始到最终 terminal commit 的完整逻辑时段；
- progress 可以在 retry 后回退或重新开始，除非 Action 自己的 progress Schema 声明单调语义；
- runtime 不为 progress 自动增加百分比或 attempt 字段。

### 13.4 丢失

progress 是可丢失观测，单个 progress 丢失不产生 `workflow.stream.gap`。理由是：

- progress 不参与输出 item seal；
- 没有可 replay 的 durable progress authority；
- 下一个 progress 或 terminal event 足以继续 UI；
- 为不可恢复的每个 progress gap 发事件只会制造新的噪声。

如果 `started` 丢失，客户端从 progress/completed/failed 惰性创建卡片。如果 terminal tool event
丢失，客户端按第 10 节使用 terminal snapshot 和顶层 Run 终态校准。

现有 `workflow.stream.gap` 继续只表达需要丢弃 provisional response item 的输出流缺口，不扩展为
通用观测丢失通知。

## 14. 内置示例与默认公开策略

### 14.1 安全默认

所有第三方和新注册 Action 继续默认完全私有：

```text
call: false
arguments: private
progress: none
result: none
```

本规范不把“Control UI 可以展示结果”解释为“所有工具默认公开结果”。

### 14.2 `tool_assistant`

`tool_assistant` 保持四个工具：

```text
current_time
text_metrics
integer_calculator
text_replace
```

目标公开策略：

| 工具 | call | arguments | progress | result |
|---|---|---|---|---|
| `current_time` | true | private | none | 完整、闭合时间结果 |
| `text_metrics` | true | private | none | `characters/words/lines` |
| `integer_calculator` | true | private | none | `value` |
| `text_replace` | true | private | none | 保持私有 |

`text_replace` 结果包含调用者文本，不能为了演示而默认公开。未来如果 Action SPI 增加显式 public result
projection，可以只公开 `replacements`；本规范不把原始输出 Schema 当作字段裁剪器。

示例必须能展示：

- 至少三个非空 `workflow.tool.completed.content`；
- 一个 status-only `content: []`；
- 并行调用按 `call_id` 合并；
- terminal `tool_results` 同时包含公开结果和 status-only 成功调用。

### 14.3 progress 测试 Action

增加仅用于集成测试或明确标记为 example 的有界 progress Action。它必须发布至少两个
Schema-valid progress payload，再返回最终结果，用于验证：

- full runtime；
- terminal-only runtime；
- SSE 序列化；
- broker 丢失；
- invalid progress；
- publish/private policy；
- terminal result 不包含 progress。

不得用真实 sleep 构造长时间、易抖动测试；测试通过同步 barrier、短受控 channel 或虚拟时钟控制。

## 15. 客户端展示合同

本仓库不实现完整 Control UI，但公开 API 文档必须给出最低消费规则。

推荐展示：

| 事件 | UI |
|---|---|
| `started` | 创建工具卡片，显示工具名和可选安全参数 |
| `progress` | 更新同一 `call_id` 的进度区 |
| `completed` | 标记成功，展示 `content` 或“结果未公开” |
| `failed` | 标记失败，展示安全 code/message |
| Run terminal | 校准仍未闭合的卡片 |

客户端可以根据已知 `tool_name` 选择专用 renderer，但必须有通用 fallback：

- `output_text`：有界文本；
- `output_json`：折叠 JSON；
- `output_image/file/audio`：只从最终 content 解析 ArtifactRef；
- 空 `content`：明确显示“完成，结果未公开”，不能显示为 `null` 或“无返回值”。

本轮不在 wire 中加入 `title`、`display_kind` 或任意 HTML。展示标题应由客户端根据稳定 tool name 和
本地资源表确定。服务端未来若要提供 display hint，必须使用闭合枚举和独立规范。

## 16. 隐私与安全

### 16.1 三个独立公开面

参数、进度和结果必须独立授权：

```text
arguments policy ──▶ started.arguments / standard function-call arguments
progress schema ───▶ workflow.tool.progress.content
result schema ─────▶ completed.content / terminal tool_results
```

任何授权不能推导另一个授权。

### 16.2 禁止字符串级脱敏

以下方案禁止：

```text
raw stdout
  -> regex 删除 token
  -> workflow.tool.progress
```

平台不能证明任意字符串已经安全。公开 progress 必须由 Action 构造符合闭合 Schema 的新值，或者由
受信任 adapter 生成明确的安全结构。

### 16.3 大小与频率

实现必须定义并测试：

- 单个 progress text 最大字节数；
- 单个 progress JSON 最大字节数、深度和节点数；
- 单事件 content part 上限；
- 单 call progress 发送频率上限；
- 单 Run 临时 progress queue 上限；
- 超限后的 `Dropped` 行为。

建议复用现有 `WorkflowToolContent` 公共大小常量，并为 progress 采用不高于最终 result 的限制。
频率限制必须在 runtime publisher 侧执行，不能只依赖 UI 节流。

### 16.4 日志

默认 INFO/WARN 日志只允许：

```text
run_id
call_id
tool_name
progress_disposition
duration_ms
payload_bytes
failure_code
```

不得记录 arguments、progress content、result content、Artifact body 或原始 Action error。

## 17. 实现边界

### 17.1 `insight-engine`

- 增加 `WorkflowToolProgressContent` 或等价闭合类型；
- 增加 `ResponseStreamEventType::WorkflowToolProgress`；
- 扩充 completed/failed 的 `duration_ms`；
- 扩充 `ToolPublicPolicy.progress_schema`；
- 编译和执行冻结 progress Schema；
- 修改 terminal public projection，使 status-only 成功调用产生空 content result；
- 保持所有 public constructor 的大小和 label 校验。

### 17.2 `insight-resources`

- 扩充 Action registry 的 progress Schema normalization/validation；
- 在 `ActionContext` 提供 scoped progress publisher；
- 默认 publisher 为 suppressed/private；
- 为三个安全内置工具声明 public result Schema；
- 保持 `text_replace` result 私有；
- descriptor hash 只在实际声明 progress 时包含该字段。

### 17.3 `insight-runtime`

- full 与 terminal-only 都安装相同语义的 Action progress publisher；
- 把 progress 投影为 best-effort live observation；
- 以 logical call 为单位计算 duration；
- 在 Action terminal 后拒绝 late progress；
- retry 不产生中间 failed；
- broker failure 不改变 worker result。

### 17.4 `insight-durable`

- 如有必要，持久化 logical call 第一次 execution start timing；
- terminal projector 包含 status-only 成功调用；
- 保持 invocation order 和 frozen policy revalidation；
- 不持久化 progress payload；
- 不让 timing migration 改变 effect evidence、claim 或 fence 语义。

### 17.5 `insight-api`

- SSE 序列化新增事件和字段；
- schema discovery 与 snapshot 更新；
- Attached Run 和 Conversation Attached turn 使用同一 wire；
- terminal barrier、EOF、错误映射和 content type 不变。

## 18. 测试与验收

### 18.1 类型和 schema

- `ResponseStreamEventType::ALL` 精确包含 25 个事件；
- 新事件和扩充字段均拒绝 unknown fields；
- progress content 只接受 `output_text/output_json`；
- 空、超长、超深和超节点 JSON fail closed；
- public policy 缺省 progress 时保持既有 canonical identity；
- `call: false + progress` 在注册或 deployment link 阶段拒绝。

### 18.2 发布矩阵

至少覆盖：

| LLM publish | Action call | progress | result | 预期 |
|---|---|---|---|---|
| false | true | schema | schema | 无 provisional tool 事件，final result 不受影响 |
| true | false | none | none | 无 tool 事件 |
| true | true | none | none | started/completed，content 为空 |
| true | true | schema | none | started/progress/completed，terminal content 为空 |
| true | true | none | schema | started/completed，结果非空 |
| true | true | schema | schema | 完整生命周期和结果 |

还必须覆盖 arguments 的 `private`、字段列表和 `all`。

### 18.3 执行行为

- 顺序调用；
- parallel tool calls 交错；
- progress 在 started 后、terminal 前；
- progress 先于 started 到达的客户端 reducer fixture；
- retryable failure 后成功；
- retry exhausted；
- Action timeout；
- Run cancel、timeout、interrupt；
- late progress；
- stale lease/fence；
- broker full、closed、lagged；
- Subscriber disconnect；
- progress callback panic/错误隔离；
- duration 跨 retry 和 worker 重启保持定义一致。

### 18.4 terminal 校准

- public result 成功项进入 `tool_results`；
- status-only 成功项以空 content 进入 `tool_results`；
- private call 不进入 `tool_results`；
- invalid public result 不能降级为空 content；
- completed、failed、timed_out、cancelled、interrupted terminal frame 使用相同稳定成功
  `tool_results`；
- progress 永不进入 terminal snapshot；
- terminal 后 EOF；
- terminal snapshot 与 GET Run authority 一致。

### 18.5 端到端

必须在以下组合通过：

```text
SQLite + full
PostgreSQL + full
SQLite + terminal_only
PostgreSQL + terminal_only
Attached Run
Conversation Attached turn
Provider stream=true
Provider stream=false
```

真实 HTTP SSE 测试必须验证 JSON frame，而不只验证内部 Rust enum。

### 18.6 文档示例

`docs/current/api.md` 必须增加完整 curl/SSE 示例，至少包含：

```text
function_call arguments
tool started
tool progress
tool completed with content
assistant continuation
response completed
```

`docs/current/dsl.md` 必须解释：

- 模型意图事件与真实执行事件的区别；
- Action public policy；
- 空 content 的含义；
- progress 的 live-only 语义；
- 示例工具的公开结果差异。

## 19. 实施阶段

### Phase 0：合同先行

1. 更新 engine wire types 和 schema snapshot；
2. 先写失败测试固定 25 个事件和新字段；
3. 固定 public policy normalization 与 privacy matrix；
4. 固定 terminal status-only result 合同。

### Phase 1：Action progress SPI

1. 增加 progress Schema 和 validator；
2. 增加 scoped publisher；
3. 实现大小、频率和 queue 限制；
4. 覆盖 suppressed/dropped/invalid 行为。

### Phase 2：Runtime 接线

1. full model-tool worker；
2. terminal-only model-tool executor；
3. duration authority；
4. retry、late event 和 fence；
5. live broker 与 sequence ordering。

### Phase 3：示例结果

1. `current_time`；
2. `text_metrics`；
3. `integer_calculator`；
4. `text_replace` 保持 status-only；
5. progress fixture/example。

### Phase 4：HTTP 与文档切换

1. SSE E2E；
2. Conversation E2E；
3. baselines；
4. current docs；
5. CHANGELOG；
6. 一次性 v1 cutover 说明。

各 Phase 必须独立通过 `cargo test --workspace` 相关子集；最终合并前运行全 workspace 测试和公开 API
baseline 检查。

## 20. 风险与控制

| 风险 | 控制 |
|---|---|
| progress 泄漏私有输出 | 只接受 Action 主动构造、冻结 Schema 验证后的新值 |
| 高频 progress 拖慢 Action | 非阻塞 publisher、频率限制、有界 queue、Dropped |
| client 把模型意图当执行成功 | 保留 `response.*` 与 `workflow.*` 分层并在文档中明确 |
| parallel call UI 串线 | 只按 `call_id` 关联 |
| retry 产生重复卡片 | started 幂等、logical call duration、terminal 单一 |
| old v1 client 不认识 progress | pre-1.0 同步 cutover，不维护混合版本 |
| status-only 结果被误认为空输出 | 文档和客户端 fallback 明确“结果未公开” |
| timing 跨重启漂移 | full runtime 使用 durable logical timing fact |
| progress 丢失导致错误状态 | progress 非权威，terminal snapshot 校准 |
| Action 作者忽略 invalid progress | payload fail closed；测试和日志仅记录固定 failure code |

## 21. 被拒绝的方案

### 21.1 完全复制 OpenClaw 事件名称和字段

拒绝。OpenClaw 是多会话 Gateway WebSocket，本项目是单 Run SSE。复制
`event: agent + stream + data` 会失去闭合类型和 Responses 兼容性，却不能获得真实 Gateway
compatibility。

### 21.2 立即升级 `response-stream/v2`

本轮拒绝。当前仍是 `0.1.x` 快速迭代，调用方受控，维护双版本成本高于收益。1.0 后不得沿用这一
理由原地破坏 v1。

### 21.3 只依赖 `response.function_call_arguments.done`

拒绝。该事件只能证明模型参数生成完成，不能证明 Action 已开始、成功或失败。

### 21.4 发送原始 partial result

拒绝。任意 partial JSON、stdout 或 Provider chunk 无法在未完成时证明符合最终 result Schema，也
不能靠字符串脱敏建立安全边界。

### 21.5 为每种工具新增专用事件

拒绝在本轮实现 `workflow.command.*`、`workflow.patch.*`、`workflow.search.*`。先用通用
tool progress/content 合同验证需求；专用事件必须有不能由闭合 content 表达的稳定语义后再立项。

### 21.6 把 progress 写入 durable history

拒绝。它会恢复 token/chunk 级写放大，与 terminal-only 目标冲突，也会让临时 UI 观测变成执行
authority。

### 21.7 默认公开所有工具结果

拒绝。公开能力必须由 Action 注册方显式授权，并由 LLM `publish` 再授权。

## 22. 完成定义

只有全部满足以下条件，规范才算完成：

1. `response-stream/v1` 的闭合事件集合为 25 个；
2. `workflow.tool.progress` 在 full 和 terminal-only 路径可用；
3. progress 使用独立冻结 Schema，未经验证的值不可见；
4. progress 丢失和慢消费者不改变 Action 结果；
5. completed/failed 含定义明确的 `duration_ms`；
6. retry、并行和 late progress 行为有测试证明；
7. terminal `tool_results` 包含 status-only 成功工具的空 content 项；
8. `current_time`、`text_metrics`、`integer_calculator` 返回非空公开结果；
9. `text_replace` 结果继续保持私有；
10. 示例能展示至少三个结果和一个 status-only 工具；
11. SQLite/PostgreSQL、full/terminal-only、Run/Conversation E2E 全部通过；
12. public schema、baseline、current docs 和 CHANGELOG 同步；
13. 没有参数、progress、结果、Artifact body 或原始错误进入默认日志；
14. 没有引入 OpenClaw wire compatibility 的错误声明；
15. 实现证据补入本文，状态更新为 Implemented / Verified 后归档。

## 23. 实施证据

本规范已于 2026-07-29 完成一次性 v1 cutover，证据如下：

- `insight-engine` 将闭合事件集合扩充为 25 个，增加
  `workflow.tool.progress`、闭合 progress content、`duration_ms` 与独立冻结 progress Schema；
- `insight-resources` 增加 scoped、非阻塞的 Action progress publisher，并为
  `current_time`、`text_metrics`、`integer_calculator` 声明闭合公开结果；
  `text_replace` 保持 status-only；
- full runtime 使用 durable model-tool 第一次执行时间和 terminal commit 时间计算 logical call
  duration；retryable Attempt failure 不发布终态失败，progress 使用有界 queue、频率和总量限制；
- terminal-only runtime 使用相同公开投影、progress 限制和 logical-call 生命周期，持久化最终
  `tool_results`，并补齐多轮工具调用的标准 function-call tail 事件；
- SQLite 与 PostgreSQL schema、schema contract id、terminal store 和公开事件 broker 已同步；
  progress 仍为 live-only，不写入 durable history 或 terminal snapshot；
- 新增 `progress_counter` 与 `progress_tool_assistant`，真实 HTTP/SSE 测试覆盖 full retry 和
  terminal-only 两条路径，验证 arguments、progress、duration、公开结果、assistant continuation、
  terminal calibration 与 EOF；
- `schemas/vendor`、Phase 0 wire baseline、root public API baseline、当前 API/DSL 文档及
  `CHANGELOG.md` 已同步。

最终验证：

```text
cargo check --workspace --all-targets
cargo test --workspace --quiet
bash scripts/check-public-api-baseline.sh
bash scripts/check-crate-boundaries.sh
bash scripts/check-cutover-residuals.sh
```

以上命令全部通过。workspace 测试包含共享真实 PostgreSQL 16 gate、SQLite、full、terminal-only、
Attached Run、Conversation、Provider streaming/complete adapter 与 JSON SSE frame 验证。
