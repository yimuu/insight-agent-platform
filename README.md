# Insight Agent Platform

一个面向平台自有 Agent 的通用 Rust 运行时。平台在启动时把严格的结构化 DSL 编译为不可变、类型化的 Region/SSA 执行计划，再以作用域任务树运行；对外提供 live-only Attached SSE、Detached 轮询、显式取消以及 SQLite/PostgreSQL 事件历史。

医学报告解读只是仓库中的一个多模态示例，不是平台的领域边界。

## 架构

启动和执行链路是确定的：

```text
严格平台配置 + 命名模型/Action 资源
                    ↓
WorkflowCompiler（语法、Schema、类型、作用域、支配关系）
                    ↓
不可变 CompiledWorkflow + 已验证 typed CallPlan / Region/SSA IR
                    ↓
RunService → RunCoordinator → ScopeScheduler
                    ↓
LLM/Action executor + 作用域任务树 → EventHub/Journal → RunRepository
                    ↓
                  /v1 JSON + SSE
```

Agent 作者只使用四种结构化 step：

| `type` | 作用 |
|---|---|
| `llm` | 调用一个命名模型，使用有序 `messages`，并绑定经过验证的响应 |
| `action` | 调用一个版本化、Schema 驱动的 Action，并绑定类型化输出 |
| `parallel` | 创建词法隔离的子作用域，按 `all` 或 `all_settled` 策略等待所有已接纳任务清理完成 |
| `switch` | 按顺序选择第一个匹配分支，并把唯一分支结果绑定为直接输出 |

控制流是语言结构，不是可注册能力。编译器把顺序、并发和选择降低为内部 `Call`、`Parallel`、`Branch/Phi`、`RegionYield`、`WorkflowReturn` 和 `Raise`；这些 IR 指令不能出现在 Agent YAML 中。

`ai.chat`、`action.call`、Operation registry 和 provider 私有 content part 只存在于 compiler/runtime 内部。作者不能写 `kind: operation`、`uses` 或任意 `config` bag；新业务能力应实现为有稳定 ID、SemVer、输入/输出 Schema、effect、幂等性与取消合同的 Action，再由 `type: action` 调用。当前进程不加载外部动态库、WASM、远程插件或下载代码。

## 配置与启动

无密钥本地 quickstart：

```bash
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

该配置只启用 `action_demo`，使用 SQLite 本地历史和内置 `example.text_metrics` Action。它不会调用外部模型服务，因此不需要 `OPENAI_API_KEY`。

另开一个终端验证 readiness 和 Agent 列表：

```bash
curl --silent http://127.0.0.1:3000/health/ready
curl --silent http://127.0.0.1:3000/v1/agents
```

创建一个 detached Run：

```bash
curl --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/action_demo/runs
```

复制响应里的 `data.run_id`，循环查询 Run；detached Run 是异步执行，短时间内可能仍为 `created` 或 `running`：

```bash
RUN_ID=<paste-run-id>
while true; do
  BODY=$(curl --silent "http://127.0.0.1:3000/v1/runs/${RUN_ID}")
  printf '%s\n' "$BODY"
  STATUS=$(printf '%s' "$BODY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["status"])')
  [ "$STATUS" = "completed" ] && break
  sleep 0.2
done
```

模型示例需要配置真实密钥：

```bash
cp .env.example .env
# 编辑 .env，设置 OPENAI_API_KEY
cargo run
```

平台配置示例：

```yaml
version: 1
bind_addr: 127.0.0.1:3000
auth:
  mode: bearer_env
  token_env: AGENT_RUNTIME_TOKEN
agents:
  directory: ../agents
  enabled: [action_demo, medical_report_interpreter, researcher]
models:
  config: models.yaml
actions:
  enabled: [current_time, example.text_metrics]
history:
  provider: sqlite
  path: ../data/formal_v2.sqlite3
runtime:
  max_concurrent_runs: 32
  max_concurrent_operations: 32
  max_concurrent_operations_per_run: 8
  operation_timeout: 60s
  operation_cancel_grace_period: 5s
  max_template_output_bytes: 262144
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 128
  journal_capacity: 1024
  journal_batch_size: 32
  journal_operation_timeout: 30s
  readiness_probe_timeout: 2s
  shutdown_grace_period: 30s
  shutdown_hard_deadline: 35s
```

`max_concurrent_operations` 是进程范围的 leaf-operation 并发上限，`max_concurrent_operations_per_run` 是单 Run 并发上限，`operation_timeout` 是单次调用上限，`operation_cancel_grace_period` 是任意 stop 请求后的独立协作清理窗口。`run_timeout` 由 RunService 在开始执行时计算为一次性的绝对 execution deadline；每个 Operation attempt 的有效 deadline 是它与 operation deadline 的较早者。`parallel.max_concurrency` 还能进一步收紧一个 Parallel 内的并发。

Execution deadline 到达后，运行时先向叶子能力发送 typed stop，再允许其在 `operation_cancel_grace_period` 内协作清理。因此 `run_timeout` 不是整个 HTTP 请求或终态持久化的墙钟上限：cleanup、终态事件写入和 repository commit 可以在 execution deadline 之后继续占用有界时间。

`agents.enabled` 默认是空集合，不会意外暴露目录中的 Agent。相对路径从平台配置文件所在目录解析。未知字段、零容量、零超时、缺失文件和缺失或空密钥都会阻止启动；`shutdown_hard_deadline` 必须严格大于 `shutdown_grace_period`。

默认只监听 `127.0.0.1:3000` 并关闭鉴权。`/health/live`、`/health/ready` 和 `/health` 始终公开且返回 `Cache-Control: no-store`；`/health` 是 readiness 的兼容别名。未 ready 时返回 `503/RUNTIME_UNHEALTHY`，且不暴露数据库诊断。

### 模型传输

Agent 只引用模型别名，不感知供应商分组：

```yaml
version: 1
models:
  general_chat:
    type: open_ai_chat
    base_url: https://example-model-service.test/v1
    model: example-chat
    api_key_env: OPENAI_API_KEY
    capabilities: [json_schema_output]
    connect_timeout: 5s
    request_timeout: 2m
    limits:
      max_upstream_bytes: 8388608
      max_buffered_line_bytes: 1048576
      max_event_payload_bytes: 1048576
      max_chunk_text_bytes: 262144
      max_usage_json_bytes: 65536
      max_accumulated_text_bytes: 1048576
```

`open_ai_chat.base_url` 默认必须使用 HTTPS。明文 HTTP 只能显式声明 `transport.plaintext_http: loopback`（精确本机目标）或 `trusted_private`（部署方明确接受风险的可信私网链路）。`trusted_private` 不是自动的 DNS/IP 私网证明。公网或其他不可信服务必须使用 HTTPS；URL 不允许携带 username/password，密钥只通过 `api_key_env` 注入。

OpenAI-compatible 流只有收到完整的 `data: [DONE]` 才算成功。HTTP clean EOF、`finish_reason` 或 usage-only chunk 都不能替代完成标记。缺少标记固定以 `UPSTREAM_STREAM_INCOMPLETE` 失败，错误不会回显 provider payload、部分输出、密钥或 endpoint query。

结构化 `response` 需要显式声明供应商的真实能力：`json_schema_output` 直接发送严格 JSON Schema；`json_object_output` 仅适用于顶层对象，适配器会注入平台生成的 JSON/Schema 指令，并在返回后按 Schema 本地校验。数组、数字和布尔等非对象根只能使用 `json_schema_output`；两种能力同时存在时固定优先前者。带图片的消息另需 `vision`，文本 `response: string` 不要求结构化输出能力。

## Agent DSL

当前作者语法以 [DSL 作者语法精简规范](docs/superpowers/specs/2026-07-17-dsl-authoring-syntax-simplification.md) 为准。作者只声明业务类型和结构化步骤；JSON Schema、消息平台类型与内部 Region/SSA 计划均由编译器生成。

### 最小完整文档

`types`、`inputs`、`output` 使用简洁类型表达式；自定义类型使用 PascalCase，数组写成 `Type[]`。输入名、字段名和 step id 使用 snake_case。

<!-- dsl-example: canonical; entry: agent -->
```yaml
api_version: insight.agent/v2
kind: agent

metadata:
  id: concise_answer
  name: Concise Answer
  description: Answer one question with optional history and image.

types:
  Answer:
    fields:
      answer: string
      confidence: number

prompts:
  system:
    inline: You are a concise assistant.

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
      parameters: {temperature: 0.2}
      response: Answer

  result:
    return: $answer
```

作者不再在类型与响应合同位置书写 `schema_dialect`、`$defs`、`$ref`、`input.schema`、`output.data_schema` 或 `output_schema`。需要默认值、可选性或长度约束时，在字段的完整声明中添加 `default`、`optional`、`min_length`、`max_length`、`min_items`、`max_items`、`pattern` 或 `enum`。Action 业务 payload 中同名的普通 mapping key 不会被误判为类型声明。

`Message` 是平台内建类型。动态历史必须声明为 `Message[]`，通常使用默认值 `[]`；Agent 不重复定义 Message、role 或 content part Schema。

### 自然值与引用

YAML mapping、sequence 和 scalar 直接表示对象、数组和常量。完整运行时值以 `$name` 或 `$name.field` 引用，文本中的 `{{ name }}` 是受限插值：

<!-- dsl-example: canonical; entry: value -->
```yaml
mode: strict
question: $question
labels: [technical, risk]
summary: "Question: {{ question }}"
metadata:
  request_id: $run.request_id
```

顶层可见输入直接写成 `$question`；当前 body 中较早 step 的直接业务输出写成 `$answer` 或 `$answer.field`。Parallel/Switch 子作用域只看得到该结构通过 `inputs` 捕获的名字和本地较早 step。前向引用、动态 key/index、跨 branch/arm 读取和 child-local escape 都会在编译期失败。

旧 `{from: ...}`、`{literal: ...}`、`{object: ...}`、`{array: ...}`、`{template: ...}` 形状会被明确拒绝，不作为兼容别名或普通业务对象解释。输入 `default` 是纯静态数据：不会执行引用或模板，完整匹配 `$name`/`$name.field` 的字符串会被拒绝，避免把看似运行时引用的值静默物化成字面量。

### LLM 消息与响应

`messages` 本身就是有序列表。Authored message 只有 `role` 和 `content`；`content` 是有序列表，每个 part 必须是严格的单键 `{text: ...}` 或 `{image_url: ...}`：

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

声明过的 Prompt ID 可以直接作为 `text` 的值；普通文本无需预先声明 Prompt。以 `$` 开头的 `text` 是运行时 string，不会进行 Prompt 查找或二次模板渲染。`image_url` 的值是 URL 字符串或 string 引用；可选值为 null 时省略该图片。`- $messages` 在当前位置展开真实消息列表，不需要 `spread`、`concat` 或 JSON 字符串拼接。

`response` 直接写 `string`、`ResultType` 或 `ResultType[]`。文本输出和结构化输出都直接绑定为 step 的业务值，不存在作者可见的 `.output.data` envelope。

### Action、Parallel 与 Switch

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

Parallel 同时拥有 spawn 和 barrier 语义。每个 branch 是词法子作用域，有自己的 `output`、`steps` 与 `result`；Switch 按顺序执行第一个匹配 case，且必须声明 default：

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
                - text: "Analyze technical feasibility: {{ question }}"
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
                - text: "Analyze risk and compliance: {{ question }}"
          response: string
      result:
        return: $analyze

- type: switch
  id: synthesis_mode
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
        return: full
  default:
    id: partial
    result:
      return: partial
```

`settle: all` 要求所有 branch 成功；可收集失败会取消同级并完整 drain。`all_settled` 把每个 branch 变成严格的 success/error 联合值。停止、进程中断、ownership/persistence 丢失和 task panic 等基础设施失败不会伪装成业务 branch 数据。

Child `result.return` 只完成当前子作用域；workflow root 的 `result.return` 才完成 Run，并且其值必须满足顶层 `output` 类型。平台负责公共 RunOutput envelope，作者不再声明 `content/format/data` 包装。任意可见 block 仍可通过顶层 `errors` catalog 中的 ID 执行 `raise`。

### Region/SSA 与运行时作用域树

结构化 YAML 是唯一作者合同，Region/SSA 是唯一运行时计划。编译器拒绝重复身份、use-before-definition、跨 Region value escape、未声明 capture 和类型不匹配。运行时父作用域拥有所有子任务：关闭 admission 后请求协作取消并完整 drain，不会留下脱离父作用域的工作流任务。

## HTTP、事件与 Run 生命周期

JSON 响应统一使用字符串码：

```json
{"code":"OK","message":"ok","data":{}}
```

正式端点：

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

Agent 发现接口返回 `id`、`name`、`description`、内容寻址 `version` 和公开的 authored `input_schema`。Run 创建先按这份合同接收输入并物化 default/optional，再按编译出的 normalized contract 验证完整输入；API 不公开 prompt、structured workflow、模型或 Action config。

Attached POST 会先原子订阅实时事件再启动 Run。终态事件发送后 SSE 立即关闭；非终态连接断开会取消 Run。Detached POST 返回 HTTP 202，连接断开不影响执行，客户端通过 GET Run 轮询或 DELETE 幂等取消。平台不提供公开事件重放；`seq` 与 SSE `id` 只用于单 Run 排序和审计关联，不是恢复游标。

v2 leaf operation 只公开生命周期元数据事件：

- `operation.started`；
- `operation.completed`；
- `operation.failed`。

Operation 不提供公共内容增量；输出值也不会进入公共事件或 journal。完成事件只公开限定 `operation_id`、类型、attempt、耗时和输出字节数；失败事件使用固定公共消息，不携带内部诊断。模型 provider 仍可使用内部流式传输，但 `ai.chat` 会在运行时内聚合并完成响应校验后才产生叶子结果。中间值只存在于运行期数据流，只有显式 `$` 引用能把它交给后续 Operation，只有 root return 能把结果写入公共 Run 终态。

默认结构化日志保持 body-free：只记录身份、状态、稳定错误 code/kind、耗时、计数和序列化字节数，不记录自由错误 message、Run 输入、prompt、模型或 Action 输入/输出、事件 payload、带 query 的完整 URL、header 或凭据。

收到 SIGINT/SIGTERM 后，进程先关闭新 Run admission；liveness 仍为 200，readiness 为 503。运行时在 grace period 内终态化并持久化活动 Run，再关闭 HTTP。panic、journal 失败或 hard deadline 会触发 fail-stop 和非零退出，由 supervisor 启动干净 replacement。

## 历史后端与单运行时所有权

SQLite 是本地默认后端。PostgreSQL 使用环境变量保存连接密钥：

```yaml
history:
  provider: postgres
  database_url_env: RUN_HISTORY_DATABASE_URL
```

远程 PostgreSQL URL 必须包含 `sslmode=verify-full`。明文只允许精确 loopback 开发目标或 Unix socket。

每个 PostgreSQL history store（同一数据库和当前 schema）只允许一个 active runtime。runtime 在 migration、启动 reconciliation 和 HTTP bind 前取得 session advisory lock，并以 ownership generation fence 保护写入。竞争者启动时直接失败，不等待成为 standby；运行期 ownership 丢失会关闭 readiness 和 admission，尝试 drain，然后固定非零退出。平台不会自动重新取得 ownership。

该合同要求 session affinity：必须直连 PostgreSQL，或使用 session-pooling 代理。PgBouncer transaction/statement pooling 不支持该所有权合同。升级 disposable store 时应先停止所有旧 runtime，再重建 schema；应用不会静默解释或升级不兼容历史。

本地运行 PostgreSQL 合同测试：

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

## 仓库示例

- `agents/researcher`：私有规划、时间 Action 与公开回答的顺序工作流；
- `agents/action_demo`：`example.text_metrics` 原生 Action，不调用模型，适合确定性冒烟；
- `agents/medical_report_interpreter`：使用 Switch 区分首轮与追问的多模态示例；
- `agents/parallel_researcher`：技术可行性与风险合规两个差异化 Parallel branch，支持完整、降级与零成功策略；
- `agents/workflow_failure_demo`：不需要密钥的 authored workflow raise 示例。

## 验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

旧 Agent 文档不会被静默解释为 v2。当前作者语法见 [DSL 作者语法精简规范](docs/superpowers/specs/2026-07-17-dsl-authoring-syntax-simplification.md)；旧作者层重设计文档仅保留未被精简规范覆盖的设计背景。Region/SSA、结构化控制流与作用域运行时的仍有效部分见 [DSL vNext Region/SSA Design](docs/superpowers/specs/2026-07-16-dsl-vnext-region-ssa-design.md)，权威性规则见 [Design-document authority](docs/superpowers/README.md)，切换原则和数据处理见 [DSL v2 直接切换说明](docs/formal-v1-breaking-changes.md)。
