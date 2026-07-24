# Response 实时流与 LLM 发布控制规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 变更类型 | Breaking / Public SSE Protocol / LLM Runtime |
| 日期 | 2026-07-19 |
| 目标版本 | `insight.agent/v1` |

## 1. 范围与规范效力

本文记录并规范以下目标合同：

- Attached Run 的用户响应必须实时传输 LLM 输出，而不是只发送生命周期事件；
- 公共响应流的核心事件与 OpenAI Responses streaming 保持一致；
- LLM 节点使用独立的 `stream` 与 `publish` 字段控制模型请求方式和用户可见性；
- 工具调用、工具结果、RAG 检索结果、最终强类型结果和 token usage 的传输方式；
- 实时增量与 durable workflow result 之间的顺序、重试、隐私和恢复边界。

本文是 [DSL v1 持久化图执行架构规范](./2026-07-18-dsl-v1-durable-graph-execution-design.md)
第 6.7 节的窄增量。它保留以下既有合同：

- Attached SSE 仍然 live-only，不提供 replay、resume 或 `Last-Event-ID` 恢复；
- Attached 连接在 Run 非终态时断开，仍提交取消意图；
- 实时 delta 是有界、best-effort、非持久化的观测数据；
- 最终验证后的 workflow result 才是 durable authority；
- durable terminal snapshot 发送后立即 EOF；
- Provider 入站流的完成证据继续由
  [OpenAI Stream Completion Evidence](./2026-07-15-openai-stream-completion-evidence-design.md)
  负责。

本规范已经实现、验证并完成切换，替代了“LLM 只在内部聚合，公共 SSE 不提供内容”的旧结论。

当前实现已经落地 LLM `stream`/`publish`、Provider 双请求路径、闭合 response-stream/v1、单进程与
PostgreSQL shared live broker、持久化 item/model-call/usage/terminal snapshot authority、Agent
discovery、SSE 终态校准、医疗 Agent 实时输出，以及按 Run 和公开终包引用校验的有界 Artifact 读取
API。模型 `tool_calls` 的 completion 与完整 batch 会形成 durable checkpoint；工具任务 activation、
barrier、continuation 恢复、工具/RAG 公共事件和终包持久化均已接入生产执行路径并有 SQLite、
PostgreSQL 与 HTTP SSE 证据。

当前数据模型仍无 tenant/owner identity，因此 Artifact API 只声明 Run、公开终包引用和 retention
隔离，不声明尚不存在的 tenant 隔离。框架已经支持注册、链接和执行 first-class retrieval，并持久化
其公开事件与终包结果；默认二进制当前只创建空的 `RetrievalRegistry`，没有内置具体 retrieval
provider，部署方必须注册实现后才能执行对应节点。

## 2. 立项时缺口（实现基线）

本规范立项时，运行时有 Provider 流，但没有用户答案流：

1. `ChatModel` 只有 `stream_chat`，OpenAI adapter 固定发送流式请求；
2. `LlmTaskExecutor` 会聚合 Provider chunk，并可选发送进程内 `LlmTokenObservation`；
3. production worker registry 没有安装该 observer；
4. observer 只有文本，没有工具调用、usage、节点实例内序号和类型化结果；
5. `/v1/agents/{agent_id}/runs/stream` 只发送 `run.*` 与 `operation.*`；
6. `run.completed` 不携带最终 workflow result；
7. `operation.completed.output_bytes` 对用户没有业务意义。

因此，立项时的 SSE 只能证明 Run 在运行，不能实时展示回答，也不能在终包中完整交付工具结果、RAG
结果和强类型业务结果；第 15、16 节记录了该基线的切换结果与验收证据范围。

## 3. 核心决策

### 3.1 两个独立控制维度

LLM 节点必须把模型传输和公共发布拆开：

```yaml
- type: llm
  id: answer
  model: vision_chat
  stream: true
  publish: true
  parameters:
    temperature: 0.2
  response: string
```

- `stream` 控制发给模型 Provider 的请求是否流式；
- `publish` 控制作者显式授权的 provisional Provider 内容是否进入调用者的实时响应流；
- `parameters` 只包含影响模型生成的参数；
- `response` 只声明节点最终业务输出类型。

`stream` 与 `publish` 都是 LLM 节点顶层字段，不得写入 `parameters`。

默认值固定为：

```yaml
stream: true
publish: false
```

`publish: false` 是安全默认值，避免内部分析节点、并行视角、原始结构化 JSON 或敏感工具调用被
意外公开。面向用户的节点必须显式声明 `publish: true`。

`publish` 只控制节点级实时事件，不控制 Run 级 `response.created` 和最终 terminal snapshot。
即使所有节点都为 `publish: false`，Attached 调用仍会收到最终强类型结果；Detached Run 不保存或
重放实时 delta，只通过 GET Run 返回最终结果。

### 3.2 四种组合

| `stream` | `publish` | 合同 |
|---|---|---|
| `true` | `true` | Provider 实时流式返回，安全公开事件立即进入响应流 |
| `true` | `false` | Provider 流式执行，但节点增量不向用户发布 |
| `false` | `true` | Provider 完整返回后，以一个完整 delta 发布，不人为切片 |
| `false` | `false` | Provider 非流式执行，只产生最终节点输出 |

无论组合如何，节点都必须生成同一种强类型最终 output，并经过相同的本地 Schema 验证。

### 3.3 `publish` 与 `return` 不同

- `publish` 决定运行过程中用户能实时看到什么；
- `return` 决定工作流结束时的最终强类型结果。

节点可以实时发布文本但不直接成为最终结果，也可以不发布任何增量但参与最终 `return`。

### 3.4 Agent discovery

调用者和 Graph UI 必须能在运行前发现实时能力，不需要读取私有 Plan：

```yaml
streaming:
  protocol: response-stream/v1
  transport: sse
  live_only: true
  sources:
    - id: answer
      kind: llm
      mode: streaming
      format: text

output_schema: {...}
```

`sources` 列出所有可能执行且 `publish: true` 的作者 step；`id` 使用稳定 step ID，`mode` 为
`streaming` 或 `buffered`，分别对应 `stream: true/false`。运行时动态 occurrence 仍通过 opaque
`item_id` 区分。没有公开节点时 `sources` 为空，但 final snapshot 仍存在。

Model/Deployment discovery 还必须暴露 adapter 支持的 `streaming_request` 与 `complete_request`
能力，发布阶段由 deployment linker 验证。编译器从节点推导 Agent streaming discovery，不增加
Agent 级重复开关。本增量实现后，明确扩展现有 Public Agent Contract，公开 `streaming` 与
`output_schema`。

## 4. Provider 执行合同

### 4.1 真实的双请求路径

运行时不得通过“调用流式接口并在服务端攒完”来伪装非流式模型请求。Provider adapter 必须支持
两条真实路径：

```text
stream: true
  -> Provider 请求使用 stream=true
  -> adapter 解析类型化增量事件

stream: false
  -> Provider 请求使用 stream=false
  -> adapter 解析完整响应对象
```

模型边界应提供等价于以下能力：

```rust
async fn chat(request: ChatRequest) -> ChatResponse;
async fn stream_chat(request: ChatRequest) -> ChatEventStream;
```

具体 Rust 接口可以使用两个方法或一个带返回枚举的方法，但不能丢失这两个执行语义。

`stream` 作用于由该 LLM 节点发起的每次模型调用，包括工具结果返回后的 continuation。工具执行本身
不得成为 LLM executor 内不可恢复的内存循环；模型工具调用必须物化为受 durable Activation、Attempt、
effect evidence、幂等键、retry 和恢复合同约束的 durable Tool/Action activation，再调度后续模型调用。

一次模型调用以 `tool_calls` 结束时，scheduler 必须用不可歧义的 durable 状态机完成以下 handoff：

1. checkpoint 归一化 finish reason、完整且已验证的 calls、稳定 `call_id`、参数和 continuation state；
2. 为每个 call 创建确定性 child Tool/Action activation 或等价的 durable scheduling fact；
3. 将父 LLM activation 置为等待该 tool batch 的状态。

允许直接在一个事务中完成以上三项，也允许使用下面的两阶段 materialization；两者的安全语义必须
等价：

1. 第一事务原子提交模型 completion、完整 batch 和全部 call 行，状态为 `checkpointed`。这些行只是
   immutable scheduling intent，不得带有可领取的 child execution authority；
2. 第二事务仅从 checkpoint 和父 LLM 冻结的 deployment binding 推导确定性 child identity，必须同时
   物化全部 child authority 并把 batch/父 continuation 切换为 `active/waiting_tools`；不得存在只有部分
   child 可执行，或 child 已可执行但父节点尚未等待的持久状态；
3. 两事务之间崩溃时，父任务恢复必须得到 `ActivateCheckpointed`（或等价闭合状态），直接重做第二
   事务，禁止重新请求上一轮 Provider。checkpointed child 不可被 tool worker 领取；取消和 deadline
   仍可从该状态安全收敛。

因此这里的“原子 handoff”指执行权的原子转移，而不是强制把可能很大的 Provider completion checkpoint
与 child materialization 写进同一个数据库事务。公共 SSE 不是任何阶段的恢复或执行权依据。

如果进程在 Provider 已返回、但第一事务的 checkpoint 尚未提交时崩溃，平台没有 durable completion
evidence，不能同时承诺“绝不再次请求 Provider”和“不丢失这次返回”；恢复只能复用同一个 durable
model-call identity，并按冻结的 retry/effect policy 决定是否重试。该 crash cut 必须证明 checkpoint、
batch 和 calls 要么全部提交、要么全部不存在，不能留下半批。只有 checkpoint 已提交之后，恢复才必须
严格禁止重放该轮 Provider，并直接完成或重做 materialization。

同一模型轮次的多个 call 可以并行执行，但 continuation 必须等待该 batch 的所有 call 达到 durable
terminal。任何未被工具 retry/error policy 恢复的失败都会使父 LLM 失败。全部成功后，scheduler 用
CAS 只创建一次下一 `model_call_no`；恢复时从 checkpoint 重建，不能重新请求上一轮模型，也不能依赖
公共 SSE 判断调用是否存在。工具 effect 成功与 result checkpoint 之间的崩溃继续由既有 effect
evidence 和幂等键收敛。

这同样不是平台单方面的 exactly-once executor invocation 承诺：幂等工具在外部 effect 已发生、但
result 尚未提交时，可以用相同 `effect_id` 重入，工具端必须据此去重并返回同一结果；非幂等工具在
effect outcome unknown 时不得自动重放，而应按冻结 policy 收敛为失败。result 已提交后的恢复不得再
进入 executor，且父 barrier 必须从 durable result 唯一唤醒。

### 4.2 非流式请求映射到公共 SSE

当 `stream: false, publish: true` 时，外层 HTTP 仍然是 SSE。模型完整返回后：

- 文本作为一个完整的 `response.output_text.delta` 发送；
- 经第 6.3 节 `arguments: all` 授权的函数参数作为一个完整的
  `response.function_call_arguments.delta` 发送；
- 随后发送相应的 `done` 和 `output_item.done`；
- 不得把完整内容人为切成多个伪增量。

这不会改善首字延迟，但可让调用者使用同一套 Responses 事件消费代码。

### 4.3 Provider 能力验证

模型注册信息必须能区分流式与非流式请求能力。配置要求 Provider 不支持的模式时，Agent 发布或
Run admission 必须 fail closed，不得静默改成另一种模式。

### 4.4 完成原因

Provider adapter 必须归一化完成原因；SSE `[DONE]` 或 HTTP EOF 只证明传输结束，不证明模型业务
输出成功：

| 归一化原因 | 节点语义 |
|---|---|
| `stop` | 继续执行本地解析、Schema 验证和节点提交 |
| `tool_calls` | 不完成 LLM 业务输出；提交 durable Tool/Action activation 和 continuation |
| `length` | 节点失败，稳定 code 为 `MODEL_OUTPUT_TRUNCATED` |
| `content_filter` | 节点失败，稳定 code 为 `MODEL_OUTPUT_FILTERED` |
| 缺失或未知 | 节点失败，稳定 code 为 `MODEL_FINISH_REASON_INVALID` |

`length` 返回的非空字符串不得因为通过最小长度校验而被误判为完整回答。失败 attempt 可以保留已
公开 item 的 `incomplete` 展示状态，但不能产生 `output_text.done` 或成功节点 output。

## 5. 双平面事件模型

### 5.1 Durable execution plane

Run、Activation、Attempt、checkpoint、最终 output、terminal state 和 terminal delivery 继续是
持久化执行事实。现有数据库 ledger、projection 与 outbox 不保存正文 delta。

`run.*`、`operation.*`、耗时、attempt、worker 和 payload size 属于执行 Trace/Admin 视图，不再是
用户答案流的主要协议。

`output_bytes` 可以继续作为内部大小限制、Artifact 决策或 Trace 指标，但不得出现在公共用户响应中。

Attached API 可以继续在内部消费 durable RunSubscription，用它检测 terminal、维持订阅 lease 和执行
断连取消；“移出用户响应流”只表示不再把 `run.*`/`operation.*` 编码给调用者，不删除内部终态信号。

### 5.2 Live response plane

文本 delta、函数参数 delta、工具执行进度等是临时响应事件。目标合同将主规范第 6.7 节第 7 条拆成：

- durable lifecycle、terminal snapshot 与 terminal delivery 继续来自 projection/outbox；
- transient response delta 来自独立 Live Response Broker，不写 projection/outbox。

临时响应事件满足：

- 有界；
- live-only；
- best-effort；
- 不写 execution ledger、Public Event outbox、trace 或默认日志；
- 丢失不能改变 Worker 的执行结果；
- 不能用于恢复或重放 Run。

最终 terminal event 携带由 durable Run 状态生成的完整快照，是客户端的最终校准依据；精确映射见
第 9 节。

## 6. OpenAI Responses 对齐边界

### 6.1 核心事件保持一致

`response-stream/v1` 初始版本固定支持以下 OpenAI Responses 标准事件；它不是无约束的 Provider
事件透传：

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

同名事件不得承载平台自创的不同语义。OpenAI 未定义的工作流事件必须使用 `workflow.*` 命名空间，
不得伪装成新的 `response.*` 事件。

本平台的 Agent Run 包含任意工作流、多个 LLM、自动工具执行、RAG 和强类型 `return`，因此切换后的
`/runs/stream` 是 OpenAI-aligned 的平台协议，不宣称是 `/v1/responses` 的 SDK drop-in replacement。
将来如需零改动使用 OpenAI SDK，应从同一内部事件模型投影独立的严格兼容端点。

`response-stream/v1` 的平台扩展事件也固定为以下集合：

```text
workflow.tool.started
workflow.tool.completed
workflow.tool.failed
workflow.retrieval.completed
workflow.stream.gap
workflow.response.timed_out
workflow.response.cancelled
workflow.response.interrupted
```

`/runs/stream` 的目标用户响应事件集合只包含上面两组事件。新增标准事件、平台扩展事件或字段必须
提升 response protocol version，不能借助通配符静默扩展。现有 `run.*`、`operation.*` 以及
`output_bytes` 完全移出该端点，保留在独立 Trace/Admin 投影；不得在同一响应中重复发送生命周期
terminal。`response.created` 后必须发送一次 `response.in_progress`，再发送任何 output item。`error`
只是可选诊断事件；失败 Run 仍必须以唯一的 `response.failed` 或第 9 节规定的 `workflow.*` terminal
结束。

参考：

- [OpenAI Streaming API responses](https://developers.openai.com/api/docs/guides/streaming-responses)
- [OpenAI Function calling](https://developers.openai.com/api/docs/guides/function-calling)

### 6.2 SSE envelope

每个 SSE frame 使用事件类型作为 `event`，JSON 中仍必须包含 `type`：

```text
event: response.output_text.delta
data: {"type":"response.output_text.delta", ...}
```

所有公开 frame 只经过一个 connection dispatcher，由它分配 `sequence_number`：

- `response.created` 必须是第一帧，`sequence_number` 固定为 `0`；
- 后续 frame 严格加一；
- 覆盖 `response.*` 与 `workflow.*` 公开事件；
- terminal event 必须是最后一帧；
- 不等于数据库 Public Event sequence；
- 不作为 SSE replay cursor；
- 不承诺跨连接、重启或重试保持相同。

目标用户响应流删除现有 durable `seq` 到 SSE `id` 的映射并省略 SSE `id`。数据库 durable sequence
只用于内部 outbox、Trace 和终态相关性，不进入连接排序，也不支持 `Last-Event-ID`。

### 6.3 函数参数增量

当 LLM 节点 `publish: true`，且 Tool public policy 同时为 `call: true`、`arguments: all` 时，API 才能发送
`response.function_call_arguments.delta`。普通 UI 可以忽略，不应把残缺 JSON 直接展示给用户；
开发者客户端可以累计它。

原始 JSON 增量只有在 public policy 明确写成 `arguments: all` 时才能实时转发。若 policy 只允许部分
字段，runtime 必须先在内部缓冲到参数完成，解析、校验并投影允许字段，然后只把安全对象放入
`workflow.tool.started.arguments`；不得发送标准 function-call item、
`response.function_call_arguments.delta` 或 `response.function_call_arguments.done`，也不得对未完成
JSON 做字符串级脱敏。服务端注入参数不属于
model-visible Schema，永远不得进入 Provider 参数流或公共事件。`arguments` 未授权时不公开参数。

`arguments` 的外部 Responses 合同保持 JSON 字符串。工具执行由 Provider adapter 产生的内部参数
完成事实驱动，公共 `response.function_call_arguments.done` 只是该事实的安全投影；公共事件丢失、
延迟或无人订阅都不得影响执行。内部完成后必须执行以下步骤：

1. 拼接完整字符串；
2. 解析 JSON；
3. 按工具参数 Schema 校验；
4. 注入服务端受保护上下文；
5. 创建 durable Tool/Action activation，由 worker effect 合同执行工具。

不得执行未完成参数，也不得用自创的 `response.tool_call.ready` 替代标准公开 `done` 事件。

### 6.4 Wire Schema 固定

实现必须 vendor 一份经过 conformance test 的 OpenAI Responses streaming Schema snapshot，并以平台
协议版本 `response-stream/v1` 固定，不能在运行时跟随在线文档漂移。非 terminal 标准事件保持以下
闭合字段组：

| 事件 | 关键字段 |
|---|---|
| `response.created` / `response.in_progress` | `type`, `response`, `sequence_number` |
| `response.output_item.added` / `done` | `type`, `output_index`, `item`, `sequence_number` |
| `response.content_part.added` / `done` | `type`, `item_id`, `output_index`, `content_index`, `part`, `sequence_number` |
| `response.output_text.delta` | `type`, `item_id`, `output_index`, `content_index`, `delta`, `sequence_number` |
| `response.output_text.done` | `type`, `item_id`, `output_index`, `content_index`, `text`, `sequence_number` |
| `response.function_call_arguments.delta` | `type`, `item_id`, `output_index`, `delta`, `sequence_number` |
| `response.function_call_arguments.done` | `type`, `item_id`, `output_index`, `name`, `arguments`, `sequence_number` |
| `response.file_search_call.in_progress` / `searching` / `completed` | `type`, `item_id`, `output_index`, `sequence_number` |
| `error` | `type`, `code`, `message`, `param`, `sequence_number` |

非 terminal 标准事件不得附加 `node_id`、`channel` 或平台私有字段；关联信息使用 opaque `item_id`、
Deployment discovery 或独立 `workflow.*` 事件。为满足“最后一包同时返回强类型工作流结果”的合同，
terminal 的平台 envelope 是唯一例外：它由标准的 `type`、`sequence_number`、`response` 加一个顶层
`workflow` 组成，具体形状由第 9 节固定。`response` 子对象仍服从 vendor Schema；`workflow` 不得塞入
标准 Response 子对象。这个明确扩展也是本端点只宣称 OpenAI-aligned、而非 SDK drop-in 的原因。

所有 `workflow.*` payload 和 terminal envelope 也必须是版本化、拒绝未知字段的封闭 Schema。新增
事件或字段需要提升平台 response protocol version。

## 7. 工具调用与异构结果

### 7.1 模型调用与平台执行分离

需要调用工具的 LLM 使用作者可见的工具列表；名称由 deployment linker 解析到冻结的 Tool descriptor：

```yaml
- type: llm
  id: answer
  tools: [interpret_indicator, retrieve_medical_knowledge]
  tool_choice: auto
  tool_limits:
    max_rounds: 8
    max_calls: 32
```

`tool_choice` 初始支持 `auto`、`required` 或一个工具名，默认 `auto`。未列入 `tools` 的资源不得被
模型调用；工具参数 Schema 来自已解析 descriptor，不允许作者在 prompt 中伪造工具定义。
`tool_limits` 是节点执行合同，不属于 Provider `parameters`：`max_rounds` 限制 continuation 轮数，
`max_calls` 限制一个 LLM activation 创建的工具调用总数。默认值固定为上例的 `8` 和 `32`，Canonical
Plan 必须显式写入，且不能超过 PlatformConfig 的硬上限；越界分别以
`LLM_TOOL_ROUND_LIMIT`、`LLM_TOOL_CALL_LIMIT` 失败。
Run deadline、取消和 Attempt fencing 覆盖整个模型—工具—continuation 链。

模型生成的函数调用使用标准 `response.*` 事件。平台自动执行函数属于工作流能力，使用扩展事件：

```text
workflow.tool.started
workflow.tool.completed
workflow.tool.failed
```

每次工具调用必须使用稳定 `call_id`，并将它纳入 durable activation/effect identity。工具结果通过
`call_id` 与模型函数调用关联；稳定 `call_id` 不能替代 Attempt、幂等键或 effect evidence。

公开工具信息需要双重授权：

1. Agent 的 LLM 节点声明 `publish: true`；
2. Tool descriptor 声明允许公开的安全投影。

Tool descriptor 的 canonical public policy 至少包含：

```yaml
public:
  call: true
  arguments: all
  result: IndicatorToolPublicResult
```

`call`、`arguments` 和 `result` 默认全部私有。`arguments` 可取 `all` 或允许公开的 model-visible
参数字段列表；服务端注入字段永远不可公开。`all` 才授权第 6.3 节的标准原始参数事件；字段列表只
授权完成后的 `workflow.tool.started.arguments` 安全对象，即使该列表恰好覆盖当前 Schema 也不改变
协议分支。`result` 引用一个封闭的公共结果类型。只要 `arguments` 或 `result` 不是私有值，`call`
就必须为 `true`；否则 Linker 拒绝 descriptor，不能让参数或结果绕过调用元数据授权。Linker 必须验证
双重授权，并把解析后的 policy 冻结进 Deployment Revision，运行时不得重新解释作者 YAML。

标准 function-call output item 只有在 `call: true` 且 `arguments: all` 时才公开；其 terminal snapshot
必须保存相同的安全参数投影，不得重新放回 Provider 原始参数。只授权调用元数据而不授权参数时，
平台只能发送不含参数的 `workflow.tool.started`；字段列表授权时只能在该事件中增加完成后的安全
`arguments` 对象。两种情况都不能生成一个语义不完整或被裁剪的标准 function-call item。

三个工具扩展事件的 v1 公共字段固定为：

| 事件 | 公共字段 |
|---|---|
| `workflow.tool.started` | `type`, `sequence_number`, `call_id`, `tool_name`, optional `arguments` |
| `workflow.tool.completed` | `type`, `sequence_number`, `call_id`, `tool_name`, `content` |
| `workflow.tool.failed` | `type`, `sequence_number`, `call_id`, `tool_name`, `error` |

其中 `error` 只包含稳定、脱敏的 `code` 与面向调用者的 `message`；不得包含 worker、Provider 或工具
原始错误体。

### 7.2 工具结果内容类型

工具结果不是只能返回文本。`workflow.tool.completed` 的公共安全投影支持以下封闭 content union：

```yaml
- type: output_text
  text: "白细胞计数偏高"

- type: output_json
  json:
    indicator: WBC
    status: abnormal

- type: output_image
  artifact: {artifact_id: art_image, content_hash: "..."}

- type: output_file
  artifact: {artifact_id: art_file, content_hash: "..."}

- type: output_audio
  artifact: {artifact_id: art_audio, content_hash: "..."}
```

大值和二进制值必须通过 `ArtifactRef` 公开，不得内联进无限制 SSE frame。

这些 `output_json`、`output_image`、`output_file`、`output_audio` 是 `workflow.*` namespaced payload，
不是 OpenAI `response.output` 的标准 content type，不得塞入标准 output item。

公共 payload 使用 opaque artifact handle，而不是内部 store 路径。当前身份模型下，平台按 Run、
公开终包中的 artifact reference 和 retention 授权读取，例如
`GET /v1/runs/{run_id}/artifacts/{artifact_id}`；响应需要正确 media type 与大小限制。终包不得持久化
会过期的签名 URL，可以在授权读取时临时生成。tenant/owner identity 及其隔离规则不由本规范实现，
也不得从现有 Run 级校验中推断出来。

工具 descriptor 必须定义安全的公共结果投影。原始 executor payload、Authorization、API key、数据库
凭据、租户内部字段和未脱敏第三方响应不得进入公共事件。`publish: true` 不能覆盖工具自身的私有策略。

## 8. RAG 与最终检索结果

通用平台 RAG 在 v1 只公开 `workflow.retrieval.completed`；Provider 原生 file search 只使用第 6.1 节
列出的 `response.file_search_call.in_progress`、`response.file_search_call.searching` 和
`response.file_search_call.completed`，不允许透传其他未固定事件。

检索同样需要双重授权：作为 LLM tool 调用时，由所属 LLM 的 `publish: true` 提供 Agent 授权；
first-class retrieval 节点自行声明 `publish: true`。Retrieval descriptor 再用封闭 public result
Schema 指定允许公开的字段；query、snippet、URI 和 metadata 默认全部私有。Linker 将该 policy
冻结进 Deployment Revision。框架运行时只执行已经注册的 retrieval；默认二进制的注册表为空，
因此并不隐含任何内置检索 provider。

`workflow.retrieval.completed` 固定包含 `type`、`sequence_number`、`retrieval_id` 和经过 public Schema
验证的 `results`；`query` 只有在 descriptor 明确授权时才存在。以下示例只表示已经通过安全投影允许
的字段：

```yaml
type: workflow.retrieval.completed
sequence_number: 24
retrieval_id: ret_1
query: 白细胞升高意味着什么
results:
  - id: doc_1
    title: 白细胞计数临床解释
    uri: https://example.test/document/1
    score: 0.92
    snippet: ...
    metadata: {}
```

被声明为公开的完整检索结果必须出现在最终终包中。正文、超大 metadata 或附件使用 ArtifactRef；
最终结果不得只保留一个“检索成功”状态而丢失可展示的来源列表。

## 9. 最终终包

成功 Run 的最后一个 SSE frame 是平台固定 envelope 的 `response.completed`，发送后立即 EOF。它的
`response` 子对象遵循 vendor 的标准 Response Schema，顶层 `workflow` 承载平台强类型结果；不得在
它后面再发送 body-free `run.completed`。

v1 成功终包 envelope 固定为：

```json
{
  "type": "response.completed",
  "sequence_number": 37,
  "response": {
    "id": "resp_run_xxx",
    "object": "response",
    "status": "completed",
    "output": [
      {
        "id": "msg_1",
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [
          {"type": "output_text", "text": "...", "annotations": []}
        ]
      }
    ],
    "usage": {
      "input_tokens": 1200,
      "input_tokens_details": {"cached_tokens": 300},
      "output_tokens": 420,
      "output_tokens_details": {"reasoning_tokens": 80},
      "total_tokens": 1620
    }
  },
  "workflow": {
    "run_id": "run_xxx",
    "result": {
      "mode": "initial",
      "answer": "...",
      "abnormal_indicators": []
    },
    "tool_results": [],
    "retrievals": [],
    "usage_status": "complete"
  }
}
```

`response.id` 在 Run admission 时分配并持久化，同一个 Run 的 GET 与 terminal snapshot 必须使用同一
身份。失败、超时、取消和中断 envelope 使用相同的顶层 `response`、`workflow` 结构，只按下表改变
事件类型、状态、结果或脱敏错误字段。

字段职责：

- `response.output`：调用者在实时过程中看到的 message、function call 等展示项；
- `workflow.result`：DSL `return` 产生并通过 `output` 类型校验的强类型业务结果；
- `workflow.tool_results`：公开的类型化工具结果；
- `workflow.retrievals`：公开的完整 RAG 检索结果；
- `response.usage`：本次 Agent Run 的 token 使用汇总。

`workflow` 是按 lifecycle 判别的闭合 union：成功分支要求 `run_id`、`result`、`tool_results`、
`retrievals`、`usage_status`；失败和超时分支用脱敏 `error` 替代 `result`；取消和中断分支用稳定
`reason` 替代 `result`。已经完成且获准公开的 tool/retrieval 项即使 Run 最终失败也保留；未获授权的
项不得因为进入终包而改变可见性。`usage_status` 只允许 `complete`、`partial`、`unavailable`。

durable Run 状态只允许以下终态映射：

| Run lifecycle | terminal event | `response.status` | 终态信息 |
|---|---|---|---|
| `succeeded` | `response.completed` | `completed` | `workflow.result` |
| `failed` | `response.failed` | `failed` | 标准 `response.error` 与脱敏 workflow code |
| `timed_out` | `workflow.response.timed_out` | `failed` | `workflow.error.code = RUN_TIMEOUT` |
| `cancelled` | `workflow.response.cancelled` | `cancelled` | `workflow.reason` |
| `interrupted` | `workflow.response.interrupted` | `incomplete` | `workflow.reason` |

取消、workflow timeout 和 runtime interruption 不得伪装成 OpenAI `response.incomplete`；无法保持
OpenAI 同名语义时使用 `workflow.*`。每个 Run 只能发送上表中的一个 terminal event，它必须是最后
一个 SSE frame，并紧随 EOF。

terminal snapshot 或其闭合投影必须 durable；大内容必须表示为 durable ArtifactRef。Run terminal、
final response snapshot/reference 与 terminal outbox 必须在同一个权威事务中提交。客户端不得依赖
此前所有 best-effort delta 都完整到达。该事务还必须提交 snapshot Artifact 的引用和 retention；
在 Run/response 保留期结束前，GC 不得释放终包所引用的 Artifact。

持久化内容是 `response`、`workflow`、usage、public item manifest 及 ArtifactRef；SSE `type` 和
连接级 `sequence_number` 由 dispatcher 在交付时包裹，不属于 durable snapshot。终包中的
`workflow.result` 必须与 GET Run 返回的规范化业务 output 完全一致。

transient chunk 和 SSE frame 不逐条持久化。completed item 只在本地解析、Schema 校验和 public policy
投影完成后，将最终完整内容写入 snapshot；failed/incomplete item 只保存 `item_id`、`output_index`、
`status` 和必要安全元数据，不保存 provisional partial text。因此 durable `response.output` 可以重建
最终展示和索引，但不能用于重放实时 delta。

需要外置 Artifact 的 snapshot 必须先写入并校验内容，再在 terminal 权威事务中提交引用。准备或
序列化 snapshot 失败时不得先提交 `succeeded`；应在 terminal CAS 前收敛为稳定基础设施失败。一旦
terminal 已提交，后续 SSE 编码/投递失败只能产生 transport error，不能改写 Run outcome。

## 10. Usage 合同

公开 token 字段使用 OpenAI Responses 命名：

```yaml
usage:
  input_tokens: 1200
  input_tokens_details:
    cached_tokens: 300
  output_tokens: 420
  output_tokens_details:
    reasoning_tokens: 80
  total_tokens: 1620
```

规则：

1. Chat Completions Provider 的 `prompt_tokens` 映射为 `input_tokens`；
2. `completion_tokens` 映射为 `output_tokens`；
3. 汇总包含本次 Run 中所有 Provider 报告 usage 的模型调用；
4. 工具 continuation 和已经发起的 retry 使用不同 model-call identity，并分别计入；
5. 工具调用次数、检索次数或外部费用不得混入 token usage；
6. Provider 未报告完整 usage 时不得伪造零值；`usage` 为 `null`，并将
   `workflow.usage_status` 标为 `partial` 或 `unavailable`；
7. `output_bytes` 不属于 usage，也不得作为用户响应字段返回；
8. usage 是 Provider-reported telemetry，不是平台账单或结算 authority。

每次模型调用在发出请求前必须获得 durable identity：

```text
run_id + activation_id + attempt_no + model_call_no
```

Provider 返回的归一化 usage 以该 identity 幂等提交；同一调用重复投递不得重复计数。usage 必须随
仍持有 authority 的 Attempt checkpoint 提交，或者使用同样受 lease fence、epoch、Run terminal cutoff
约束的 telemetry append 合同。成功、失败、取消和 tool continuation 只要在 authority 有效时拿到
Provider usage 都保留记录；失去 authority 的 abandoned/stale worker 和 terminal 后迟到写入必须被
拒绝，不能为了统计绕过 fencing。terminal snapshot 在权威事务中从已接受记录聚合；调用结果不确定、
Provider 未报告、崩溃或迟到拒绝导致记录缺失时，完整度不得标为 `complete`。OpenAI-compatible
streaming adapter 应由自身注入获取 usage 所需的 Provider 参数，Agent 作者不得在 `parameters` 中
手工拼装该传输选项。

## 11. 顺序、并行与重试

### 11.1 输出身份

一个公开输出实例至少由以下内部身份唯一确定：

```text
run_id + activation_id + attempt_no + item_id
```

`node_id` 只是 UI 标签，不能区分 Map、Loop、并行分支或同一节点的多个 occurrence。

同一 item 的 delta 必须有严格的局部顺序。并行公开节点可以在连接级事件流中交错，但调用者必须按
`item_id`/`output_index` 重建，不能把所有文本 delta 无条件拼成一个字符串。

Producer 必须在进入有界队列前，为每个 `(activation_id, attempt_no, item_id)` 分配从 `0` 递增的
item-local index。该索引是 broker 保序、gap 检测和 terminal barrier 的内部 authority，不替代公开
`sequence_number`。

在发送 `response.output_item.added` 前，runtime 必须以 append-only 方式持久分配 opaque `item_id`
和下一个 `output_index`。该分配冻结后不得重排或复用，最终 `response.output[output_index]` 必须对应
同一 item。并行到达顺序可以决定首次分配顺序，但一经提交就成为该 Response 的 durable 顺序。

如果产品只需要一段连续回答，Agent 应只设置一个最终 LLM 节点 `publish: true`。

### 11.2 重试

已公开的失败 attempt 不得与 retry 的内容静默拼接：

- 在尚未分配/发布 output item 前，可以按既有 WorkerEffectPolicy 透明 retry；
- 已发布内容后 retry 必须使用新的 item identity；
- 在线时旧 item 必须发送 `response.output_item.done`，其中 `item.status = incomplete`；如果该事件
  丢失，最终 `response.output[output_index].status` 仍必须为 `incomplete`；
- retry item 获得新的、追加的 `output_index`；
- 最终 `response.output` 保留所有已经公开分配的 item，索引不变；只有 completed item 参与展示成功值，
  `workflow.result` 仍是唯一业务 authority；
- 客户端以最终快照校准 provisional 内容。

本文不提高现有 LLM 最大 attempt 数；只有显式 retry/effect policy 已允许 retry 时才应用上述合同。

### 11.3 结构化响应

Provider delta 在本地 JSON 解析和 Schema 校验完成前都是 provisional。`publish: true` 可以公开这些
原始 JSON 字符串作为标准 `output_text` 增量，但不能把它们声明为强类型结果。验证后的对象/数组只
进入 `workflow.result` 或其他 `workflow.*` 安全投影；平台不得发明标准 `response.output` 的
`output_json` content type。只有验证成功的节点 output 和最终 `workflow.result` 才是业务合同。

## 12. 背压、丢失与终态屏障

实时响应通道必须按 Run 隔离并有界，避免一个高流量 Run 挤占其他 Run 或造成跨 Run 明文泄漏。

当临时内容 subscriber、broker 或 connection dispatcher 发现 item-local index 缺口时：

- 不得阻塞或失败 Worker；
- 不得仅因 delta 丢失而取消 Run；
- 必须发送 `workflow.stream.gap`，并包含受影响的 `item_id`、attempt identity 与缺失 index 区间；
- 客户端应丢弃受影响的 provisional 拼接结果，并等待最终快照；
- 只要 HTTP 连接仍可写，dispatcher 必须继续交付 durable terminal。

`workflow.stream.gap` 与 terminal 使用独立、有界的优先控制队列，不能重新塞入已经饱和的正文队列。
如果真正落后的是不读取、已关闭或不可写的 HTTP 客户端，则无法承诺 gap 或 terminal 到达：写入在
有界期限后关闭连接，并按现有 Attached 合同提交取消意图。此类传输失败不得改写已经赢得的 durable
Run terminal；客户端仍可用 `run_id` 查询最终状态。

Gap payload 是封闭结构：

```json
{
  "type": "workflow.stream.gap",
  "sequence_number": 18,
  "item_id": "msg_1",
  "attempt_no": 1,
  "missing_from": 12,
  "missing_to": 15,
  "unknown_tail": false,
  "action": "discard_provisional_item"
}
```

Gap 是两个分支组成的闭合 union：已知缺口使用整数 `missing_to` 和 `unknown_tail: false`；seal 前崩溃
造成的未知尾部使用 `missing_to: null` 和 `unknown_tail: true`，`missing_from` 是 dispatcher 已连续
收到的最后 index 加一。

正常完成的公开 item/attempt 必须产生内部 seal，记录最后一个 item-local index 和完成状态。若
producer 在 seal 前崩溃，scheduler 必须把 durable item manifest 收敛为 `incomplete_unsealed`；不能
假定 crashable producer 总能发送 seal。final response snapshot 保存 publication manifest，列出所有
公开 item 的身份、状态和可用的 seal watermark，但不保存 transient chunk 正文。

终态不能只以“本地队列已经为空”作为发送条件。Response dispatcher 收到 durable terminal 后，必须
对 manifest 中每个 item 满足以下条件之一：

1. 已接收并发送到 seal watermark；或
2. 已确认缺失区间并先发送强制 `workflow.stream.gap`；对于 `incomplete_unsealed` item，使用
   `unknown_tail: true` 关闭未知尾部。

随后才能发送 terminal snapshot。等待 broker 中在途 frame 必须有界；超时按第二种情况收敛，不能
永久阻塞 durable terminal，也不能静默截断。

### 12.1 多 runtime

进程内 observer 不能跨 runtime 送达 Attached 连接。生产多 runtime 部署若要部署或接纳包含公开
streaming source 的 Agent，必须提供共享、非持久化、按 item-local index 保序并传递 seal 的 Live
Response Broker。单 runtime 开发模式可以使用同一接口的进程内实现。

没有共享 broker 的多 runtime 环境可以只运行不公开实时节点的 Agent；production publication/start
gate 必须拒绝把包含 `publish: true` source 的 Agent 部署到该环境，admission 再做 fail-closed 防线，
不能静默运行后只返回生命周期事件。worker affinity 若要成为生产替代方案，必须另行定义覆盖 claim、retry、lease
expiry、恢复、shutdown 和 owner loss 的完整 fencing 合同；本文不将一个未定义的 affinity 名称视为
等价实现。Live Response Broker 不是执行 journal，也不得提供 replay 承诺。

## 13. 安全与传输

1. `publish: true` 是作者对 provisional `output_text` 的显式公开授权，不是自动脱敏或内容安全证明；
2. 内部 Prompt、Message、reasoning、凭据和未脱敏 Provider body 不进入 SSE；
3. 凭据由 executor 在参数校验后注入，模型不得生成秘密参数；
4. 临时 observation 类型不得派生会意外打印正文的 `Debug`/`Serialize`；
5. 日志和 Trace 只记录 ID、类型、计数、耗时及安全错误码；
6. SSE 强制设置 `Cache-Control: no-store, no-transform` 和 `X-Accel-Buffering: no`，避免代理缓存、
   改写或聚合正文；
7. frame、per-item 累计值、per-run queue 和最终快照都有独立大小上限；
8. 大结果通过 ArtifactRef 返回；
9. Attached 断连取消、keepalive 和无 replay 合同保持不变。

本节所称不持久化的 Prompt/Message，特指一次 Run 在执行期把模板、输入和 continuation 物化后形成的
完整 Provider request envelope；平台不得为了观测、重试或调试再保存这份物化副本。Agent author
source、Prompt 模板、canonical `message_program` 是 Deployment Revision 的既有持久化 authority，
规范化 Run input/output 也是既有业务 authority，它们不属于 transient Provider request。即使这些
authority 的字段参与渲染，也不能据此把完整物化请求、secret 或 Provider 原始响应体写入 ledger、
snapshot、outbox、Trace 或日志。

平台不得把 reasoning、refusal、工具原始参数或 Provider error body重新标成普通 `output_text`。
Secret 不得进入模型 messages；因为模型可能复述输入，平台不能承诺用事后字符串替换可靠清除已经
进入 Prompt 的秘密。

## 14. 医疗报告示例

当前 checked-in 医疗 Agent 没有单独的 `final_answer` LLM；initial 路径的 `answer` 是三个 LLM
字符串在 `return` 中拼接而成。真实节点已经显式完成 `stream`/`publish` 切换，而不是只增加平台字段：

以下是控制字段片段，沿用现有节点中的 `model`、`messages` 和 `parameters`，不是可独立复制的完整
Agent YAML：

```yaml
# follow_up 路径
- type: llm
  id: answer
  stream: true
  publish: true
  response: NonEmptyText

# initial 路径
- type: llm
  id: abnormal_indicators
  stream: true
  publish: true
  response: NonEmptyText

- type: llm
  id: comprehensive_interpretation
  stream: true
  publish: true
  response: NonEmptyText

- type: llm
  id: health_advice
  stream: true
  publish: true
  response: NonEmptyText

- return:
    mode: initial
    answer: |-
      {{ abnormal_indicators }}

      {{ comprehensive_interpretation }}

      {{ health_advice }}
    abnormal_indicators: $abnormal_indicators
    comprehensive_interpretation: $comprehensive_interpretation
    health_advice: $health_advice
```

三个 initial 节点按当前顺序分别成为公开 output item，用户可以逐段看到结果；最终
`workflow.result.answer` 仍是 durable 拼接结果。客户端不能假设多 item delta 本身就是最终字符串。

若业务合同需要异常指标列表，`MedicalResult.abnormal_indicators` 应迁移为真实列表，而不是把列表
序列化进字符串。为了兼容当前只支持 object-root structured output 的 `vision_chat`，节点使用对象
wrapper，最终业务字段仍是真实列表：

```yaml
types:
  AbnormalIndicator:
    fields:
      name: string
      value: string
      interpretation: string

  AbnormalIndicatorsResult:
    fields:
      indicators: AbnormalIndicator[]

  MedicalResult:
    fields:
      mode: string
      answer: string
      abnormal_indicators: AbnormalIndicator[]
      comprehensive_interpretation: string
```

该类型调整是单独的 breaking Agent output/prompt 迁移，但本规范记录其终包要求。结构化节点的实时
展示必须明确 presentation；不能把未完成 JSON 当成已验证的异常指标列表。

## 15. 实现与切换清单

以下代码、规范和 checked-in evidence 项均已完成；对应可执行验收范围见第 16 节。标为“发布时执行”
的条目是每个实际部署环境的操作要求，仓库状态不声称已经替外部环境完成发布：

1. [x] 为 LLM parser/AST 增加 `stream`、`publish`、`tools`、`tool_choice`、`tool_limits`，拒绝
   `parameters.stream` 和未知字段；
2. [x] compiler 把默认 `stream: true`、`publish: false` 显式写入 Canonical Plan，并覆盖 reducer、
   Graph Author round-trip、Plan verifier 和 deployment hash；
3. [x] 为 Model capability/config 增加 `streaming_request` 与 `complete_request`，在 deployment link 阶段
   fail closed，并更新 checked-in model evidence；
4. [x] 为 `ChatModel` 增加真实非流式调用，将文本、函数调用、finish reason 和 usage 从 `ChatChunk`
   升级为类型化 Provider event；
5. [x] 实现工具白名单、`tool_choice`、参数 Schema、原子 tool-batch handoff、durable Tool/Action
   activation、continuation barrier、预算、cancel、retry、稳定 `call_id` 与秘密注入；
6. [x] 为 Tool/Retrieval descriptor 增加封闭 public policy，并在 Deployment Revision 中冻结双重授权；
7. [x] 实现共享 Live Response Broker、单 runtime adapter、per-item index、seal、gap 和优先控制通道，
   并增加 PlatformConfig 的 frame/item/run/queue/timeout 限制；
8. [x] 在发送 `output_item.added` 前持久分配 item/output index，持久化成功/incomplete item manifest；
9. [x] 按 model-call identity 持久化归一化 usage，并实现幂等聚合与完整度；
10. [x] 在 terminal 事务中原子提交 response snapshot/reference、Artifact retention 和 terminal outbox；
11. [x] 在执行开始前完成 Attached 订阅，让 SSE dispatcher multiplex live response 与 durable terminal，
    同时保留 RunSubscription 的 lease/断连取消职责；
12. [x] 将 `run.*`、`operation.*`、`output_bytes` 移出用户响应，提供独立 Trace/Admin 投影；
13. [x] 为公开 Artifact handle 增加按 Run 与公开终包引用授权的读取 API、大小限制和 retention 校验；
14. [x] 强制 SSE `Cache-Control: no-store, no-transform`、禁用代理缓冲，并验证 outbound bounds；
15. [x] 扩展 Agent discovery 的 streaming sources 与 `output_schema`；
16. [发布时执行] LLM descriptor/worker/adapter binding version 已提升，旧 Plan 会 fail closed；每个部署
    环境采用维护窗口 clean cutover，先 drain active Run，再重新编译所有 Deployment Revision，不允许
    旧 Plan 用新默认值静默恢复；
17. [x] 更新旧的“拒绝内容增量”协议断言、当前设计说明和 API 文档；
18. [x] 为 checked-in 医疗 Agent 的 follow-up 与三个 initial LLM 显式增加 `stream`/`publish`，并增加
    真实 HTTP SSE E2E，证明首个 delta 早于节点完成且终包结果与 GET Run 一致。

两项范围边界不属于未完成切换项：本规范没有引入 tenant/owner identity；默认二进制没有内置
retrieval provider，只会执行部署方已注册进 `RetrievalRegistry` 的实现。

## 16. 验收标准

1. `stream` 和 `publish` 均为 LLM 顶层字段，默认分别为 `true`、`false`；`parameters.stream` 编译失败。
2. `stream: false` 确实向 Provider 发送非流式请求；四种 `stream`/`publish` 组合都通过 adapter 测试。
3. `publish: false` 不泄漏节点正文、函数参数或工具结果；`publish: true` 的流式文本在节点完成前到达。
4. 只有 `call: true`、`arguments: all` 时标准 arguments delta 才可累计；字段列表授权时不产生标准 function-call
   item/event，只能收到 `workflow.tool.started.arguments` 的完成后安全投影；公共事件丢失不影响执行。
   `call: false` 与任何非私有 `arguments`/`result` 组合必须在 Linker 阶段失败。
5. 工具只能在完整参数解析、Schema 校验和秘密注入后执行；模型 checkpoint 必须形成不可执行且可恢复
   的完整 batch intent，全部 child activations 与 `waiting_tools` 必须在同一执行权事务中物化；
   checkpoint 事务失败的 crash-point 测试证明零半批，checkpoint 提交后以及 materialization 前后的
   crash-point 测试证明不重放 Provider、也不产生半批工具。Provider 已返回但 checkpoint 尚未提交的
   窗口遵守第 4.1 节的同一 model-call identity 与冻结 retry/effect policy，不宣称不可实现的 exactly-once。
6. 工具 effect 完成后崩溃、worker stale、retry、多个并行 call、取消和 deadline 都遵守既有 fencing、
   幂等键和 barrier；`max_rounds`/`max_calls` 硬上限可证明终止无限工具循环。
7. 文本、JSON、图片、文件和音频工具结果都有类型化安全投影；RAG 公开结果出现在最终终包。
8. Provider `stop`、`tool_calls`、`length`、`content_filter`、缺失和未知 finish reason 全部通过矩阵测试；
   截断文本不得产生成功 output、`output_text.done` 或 `response.completed`。
9. usage 按 model-call identity 受 fence 幂等提交；重复投递、retry、continuation、失败/取消、Provider
   缺失、重启聚合和 terminal 后迟到写入都通过测试，且缺失时不伪造零值。
10. `response.completed` 携带最终强类型 `workflow.result` 和完整或明确不可用的 usage；`output_bytes`
    不出现在公共响应。
11. 并行节点、循环 occurrence 和 retry 不混合不同 attempt 的 delta；已发布 retry 使用新 item/index，
    旧 item 在实时事件和终包中均为 `incomplete`。
12. broker/subscriber lag 不使 Worker 失败；当 HTTP 在写入期限内仍可写时，dispatcher 先发送确定范围
    或 `unknown_tail` gap，再交付 durable terminal snapshot。
13. 不读、关闭或不可写的 HTTP 客户端在有界期限内关闭；非终态 Attached Run 提交取消意图，已经
    terminal 的 Run 不被改写，并可通过 GET 恢复最终状态。
14. terminal 只在所有 item 达到 seal watermark 或已发送 gap 后出现，且发送后立即 EOF。
15. Detached Run 不依赖实时订阅；Attached 的 lease、断连取消和无 replay 合同保持有效。
16. transient chunk/frame、每次 Run 物化出的完整 Provider Prompt/Message request、secret 和 raw
    Provider body 不被另行写入 ledger、snapshot、outbox、Trace 或日志；author source、canonical
    模板/message program 及声明的 Run input/output 仍按各自 authority 持久化。completed item 只持久化
    验证后的完整安全内容，incomplete item 不持久化 partial text。
17. GET Run 的最终 result 与 terminal SSE 中的 `workflow.result` 一致，snapshot、terminal、outbox 和
    Artifact retention 的 crash-point 测试证明原子提交。
18. 多 runtime 环境缺少 shared live broker 时，production publication/start/admission gate 拒绝含
    `publish: true` source 的 Deployment 或 Run；有 broker 时通过 item-local index、seal、unsealed
    recovery 和 gap 测试。
19. Agent discovery 只列 `publish: true` 的可能 source，`streaming`/`buffered` 与节点一致，
    `output_schema` 与 Run validator 同源，且不泄漏 prompt、message、model 私参或私有 Tool policy。
20. Provider 不支持所需 request mode 时 deployment link fail closed；旧 Plan 不会用新默认值恢复。
21. `succeeded`、`failed`、`timed_out`、`cancelled`、`interrupted` 分别通过事件名、`response.status`、
    脱敏字段、唯一 terminal 和立即 EOF 测试。
22. 医疗 Agent 的 `messages: []` 路径依次产生三个 initial item，非空 `messages` 路径产生 follow-up
    `answer` item；两条路径首个正文都早于对应节点完成和 Run terminal，终包结果均与 GET 一致。
