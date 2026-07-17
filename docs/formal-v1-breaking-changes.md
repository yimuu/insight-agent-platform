# DSL v2 直接切换说明

> 文件名为历史路径，仅为避免旧设计记录中的链接失效；本文描述当前 `insight.agent/v2` 切换合同。当前作者语法以 [DSL 作者语法精简规范](./superpowers/specs/2026-07-17-dsl-authoring-syntax-simplification.md) 为准；旧作者层重设计规范中的冲突示例不再具有 canonical 权威。

仓库中的旧 Agent YAML 和图执行器属于未发布原型，不是兼容目标。迁移时必须原子部署新 binary 与全部 Agent YAML；旧文档明确编译失败，不提供 alias、双 parser 或双 scheduler。

HTTP 的 `/v1` 是服务 API 版本，与 Agent DSL 的 `insight.agent/v2` 是两个独立版本空间；本次 DSL 切换不改 HTTP 路径。

## 变更总表

| 旧作者语法 | 当前 v2 语法 |
|---|---|
| 顶层 `schema_dialect`、`$defs`、`input.schema`、`output.data_schema` | `types`、`inputs`、`output: Type` |
| step `kind: llm/action/parallel/switch` | step `type: llm/action/parallel/switch` |
| `{from: ...}` | `$name` 或 `$name.field` |
| `literal/object/array/template/bindings` wrapper | 自然 YAML scalar、mapping、sequence；文本插值使用 `{{ name }}` |
| LLM authored `inputs` | 从 messages、Prompt slot 和运行时引用自动捕获 |
| `content: ...` 的私有 union、`parts`、`spread`、`concat` | `content` 列表中的单键 `{text: ...}` / `{image_url: ...}`；动态列表直接写 `- $messages` |
| `response: {format, schema}` | `response: string` 或 `response: ResultType` |
| branch/Switch `output_schema` | `output: Type` |
| root `content/format/data` wrapper | root `result.return` 直接返回顶层 `output` 值 |
| `entry + nodes + next`、fork/join/end 类节点 | 顺序 `workflow.steps` 与结构化 `parallel/switch/result` |

旧 generic operation 仍是明确的负向语法：

<!-- dsl-example: negative; entry: step; code: VNEXT_AGENT_PARSE_FAILED -->
```yaml
- kind: operation
  id: answer
  uses: ai.chat
  with: {}
  config: {}
```

## 最小 Agent 迁移

<!-- dsl-example: canonical; entry: agent -->
```yaml
api_version: insight.agent/v2
kind: agent

metadata:
  id: demo
  name: Demo
  description: Minimal v2 Agent.

types:
  Answer:
    fields:
      answer: string

prompts:
  system:
    inline: You are concise.

inputs:
  question:
    type: string
    min_length: 1
  messages:
    type: Message[]
    default: []
  image_url:
    type: string
    optional: true

output: Answer

workflow:
  steps:
    - type: llm
      id: answer
      model: vision_chat
      messages:
        - role: system
          content:
            - text: system
        - $messages
        - role: user
          content:
            - text: "Question: {{ question }}"
            - image_url: $image_url
      response: Answer

  result:
    return: $answer
```

自定义类型使用 PascalCase，输入、字段和 step id 使用 snake_case，数组写成 `Type[]`。平台内建 `Message`；Agent 不得重新声明 Message Schema。作者不书写 JSON Schema，编译器仍会生成并执行运行时输入、输出和 Provider 响应校验。

## 值、引用与消息迁移

自然 YAML 直接表达数据；`$` 只用于完整运行时值，`{{ }}` 只用于文本插值：

<!-- dsl-example: canonical; entry: value -->
```yaml
mode: strict
question: $question
labels: [technical, risk]
summary: "Question: {{ question }}"
```

顶层输入、当前 block 捕获以及当前 body 中更早 step 的输出都使用词法短名。比如 `$question` 引用当前可见的 question，`$answer.text` 引用较早 step 的直接业务输出。动态 key/index、前向引用、跨 branch/arm 引用和 child-local escape 会在编译期失败。

精确匹配旧 `{from: ...}`、`{literal: ...}`、`{object: ...}`、`{array: ...}` 或 `{template: ...}` 的对象会直接拒绝，不会作为兼容 alias 或普通业务对象接受。

LLM `messages` 是原生有序列表。每条 authored message 只有 `role` 和 `content`；每个 content part 必须恰好有一个键：

<!-- dsl-example: canonical; entry: step -->
```yaml
- type: llm
  id: analyze
  model: vision_chat
  messages:
    - $messages
    - role: user
      content:
        - text: "Analyze: {{ question }}"
        - text: $question
        - image_url: $image_url
  parameters: {temperature: 0.2}
  response: Perspective
```

已声明 Prompt ID 可以直接作为 `text` 的值；普通 inline 文本不需要先声明 Prompt。运行时 `text: $question` 不做 Prompt 查找或二次模板渲染。`image_url` 接受 string 或 string 引用，可选 null 图片会省略。`- $messages` 展开真实的 `Message[]`，不是 JSON 字符串。

`response: string` 以及最终解析为 string 的命名别名绑定文本；对象、数组及其他非 string 类型生成结构化响应合同。step 输出就是该业务值，后续直接使用 `$analyze` 或 `$analyze.field`，不再使用 `.output.data`。

## Action 与结构化控制流迁移

Action 直接声明静态 `call` 和自然 `inputs`：

<!-- dsl-example: canonical; entry: step -->
```yaml
- type: action
  id: now
  call: current_time
  inputs:
    timezone: Asia/Shanghai
    request_id: $run.request_id
```

Parallel 捕获父作用域值，为每个 branch 建立词法子作用域，并同时承担 spawn 与 barrier。Switch 按顺序执行第一个匹配 case，且 default 必需：

<!-- dsl-example: canonical; entry: step -->
```yaml
- type: parallel
  id: analyses
  inputs:
    question: $question
  settle: all_settled
  max_concurrency: 2
  branches:
    technical:
      output: string
      steps:
        - type: llm
          id: analyze
          model: general_chat
          messages:
            - role: user
              content:
                - text: "Technical feasibility: {{ question }}"
          response: string
      result:
        return: $analyze

    risk:
      output: string
      steps:
        - type: llm
          id: analyze
          model: general_chat
          messages:
            - role: user
              content:
                - text: "Risk and compliance: {{ question }}"
          response: string
      result:
        return: $analyze

- type: switch
  id: policy
  inputs:
    analyses: $analyses
  output: string
  cases:
    - id: complete
      when:
        cel: >-
          scope.analyses.technical.status == 'ok' &&
          scope.analyses.risk.status == 'ok'
      result:
        return: complete
  default:
    id: partial
    result:
      return: partial
```

`all` 要求全部 branch 成功；`all_settled` 把可收集结果转换为严格的 success/error 联合值。停止、中断、ownership/persistence 丢失和其他基础设施失败不会伪装成 branch 业务数据。

Child `result.return` 降低为内部 RegionYield，只完成当前作用域。Root `result.return` 才完成 Run，并且值必须满足顶层 `output` 类型。作者定义的失败仍需先在顶层 `errors` 声明，再由可见 block 执行 `raise`：

```yaml
errors:
  rejected:
    category: workflow
    code: WORKFLOW_POLICY_REJECTED
    public_message: The workflow policy rejected this run.
```

## 平台配置迁移

旧图运行时并发字段由作用域/Operation 字段替换：

```yaml
runtime:
  max_concurrent_runs: 32
  max_concurrent_operations: 32
  max_concurrent_operations_per_run: 8
  operation_timeout: 60s
  operation_cancel_grace_period: 5s
  max_template_output_bytes: 262144
  run_timeout: 5m
```

- `max_concurrent_operations`：进程范围 Operation semaphore；
- `max_concurrent_operations_per_run`：单 Run Operation semaphore；
- `parallel.max_concurrency`：单个 Parallel 的 branch admission 上限；
- `operation_timeout`：单次 Operation attempt deadline；
- `operation_cancel_grace_period`：任意 attempt stop 后的独立有界协作清理窗口；
- `run_timeout`：RunService 在执行开始时计算一次的绝对 execution deadline；attempt 使用它与 operation deadline 的较早者；
- `max_template_output_bytes`：单次显式 template 结果上限。

所有数值与 timeout 必须大于零。duration 使用正整数紧跟 `ms`、`s` 或 `m`。

## HTTP 与事件

对外端点保持：

```text
GET    /health
GET    /health/live
GET    /health/ready
GET    /v1/agents
GET    /v1/agents/{agent_id}
POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs
GET    /v1/runs/{run_id}
DELETE /v1/runs/{run_id}
```

Attached SSE 是 live-only，断开会取消未终止 Run；Detached 返回 202 并通过 GET Run 轮询。平台不提供公开事件 replay，`seq` 和 SSE `id` 不是恢复 cursor。

v2 叶子调用只发布 `operation.started`、`operation.completed` 和 `operation.failed` 生命周期元数据。Operation 不提供公共内容增量，输出值也不进入公共事件或 journal。模型流只在运行时内部聚合并验证。完成事件只含身份、类型、attempt、elapsed 与 output byte count；失败事件使用固定公共 message，不携带自由诊断。Execution deadline 到达后的协作 cleanup 与终态持久化可以继续占用额外有界时间。

## 部署、历史与安全

1. 在发布前用新 compiler 编译全部 enabled Agent；任何旧文档都必须失败。
2. 停止所有连接目标 history store 的旧 runtime。
3. 原子部署新 binary、Agent YAML 和平台配置；不要滚动混用两套 authored/runtime 合同。
4. disposable 开发 store 直接切到 `migrations/formal_v2` 和新的 `data/formal_v2.sqlite3`；不要在旧 store 上编写 DSL/runtime 兼容路径。
5. 启动后先检查 `/health/ready`、Agent metadata、detached action-only smoke，再恢复流量。

历史 Run 的终态 envelope 可以按持久化结构读取，但旧 Agent YAML 不会被重新编译或恢复执行。平台不存储原始 Run 输入，只保存输入摘要；中间 Operation 值不进入公共事件。

远程 PostgreSQL history URL 必须使用 `sslmode=verify-full`。每个 PostgreSQL store 只允许一个 active runtime，并要求 session-affine advisory lock；PgBouncer transaction/statement pooling 不适用。

`open_ai_chat.base_url` 默认只接受 HTTPS。HTTP 仅允许显式 `loopback` 或部署方接受风险的 `trusted_private`。`trusted_private` 不代表运行时完成 DNS/IP 私网证明。

Action input/output Schema 校验失败使用固定公共错误，不拼接 validator instance：

```text
ACTION_INPUT_INVALID  / action input validation failed
ACTION_OUTPUT_INVALID / action output validation failed
```

OpenAI-compatible 成功流必须包含完整 `data: [DONE]`。clean EOF、`finish_reason` 和 usage-only chunk 都不是完成证据。

## 验证切换

```bash
rg -n 'api_version: insight.agent/v2' agents/*/agent.yaml
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

可执行的 v2 数据流、Parallel/Switch 与 partial/zero-success 策略以 `agents/parallel_researcher/agent.yaml` 为准。
