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

| `kind` | 作用 |
|---|---|
| `llm` | 调用一个命名模型，使用有序 `messages`，并绑定经过验证的响应 |
| `action` | 调用一个版本化、Schema 驱动的 Action，并绑定类型化输出 |
| `parallel` | 创建词法隔离的子作用域，按 `all` 或 `all_settled` 策略等待所有已接纳任务清理完成 |
| `switch` | 按顺序选择第一个匹配分支，并把唯一分支结果绑定为直接输出 |

控制流是语言结构，不是可注册能力。编译器把顺序、并发和选择降低为内部 `Call`、`Parallel`、`Branch/Phi`、`RegionYield`、`WorkflowReturn` 和 `Raise`；这些 IR 指令不能出现在 Agent YAML 中。

`ai.chat`、`action.call`、Operation registry 和 provider 私有 content part 只存在于 compiler/runtime 内部。作者不能写 `kind: operation`、`uses` 或任意 `config` bag；新业务能力应实现为有稳定 ID、SemVer、输入/输出 Schema、effect、幂等性与取消合同的 Action，再由 `kind: action` 调用。当前进程不加载外部动态库、WASM、远程插件或下载代码。

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
    capabilities: []
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

## Agent DSL

### 文档外形

每个 Agent 目录包含一个严格的 `agent.yaml`：

<!-- dsl-example: canonical; entry: agent -->
```yaml
api_version: insight.agent/v2
kind: agent

metadata:
  id: text_metrics
  name: Text Metrics
  description: Compute deterministic text metrics.

schema_dialect: https://json-schema.org/draft/2020-12/schema

$defs: {}
prompts: {}
errors: {}

input:
  schema:
    type: object
    required: [text]
    properties:
      text: {type: string}
    additionalProperties: false

output:
  data_schema:
    type: object
    required: [characters, words, lines]
    properties:
      characters: {type: integer, minimum: 0}
      words: {type: integer, minimum: 0}
      lines: {type: integer, minimum: 0}
    additionalProperties: false

workflow:
  steps:
    - kind: action
      id: analyze_text
      call: example.text_metrics
      inputs:
        text: {from: input.text}

  result:
    return:
      content:
        template:
          text: "characters={{ characters }}, words={{ words }}, lines={{ lines }}"
          bindings:
            characters: {from: steps.analyze_text.output.characters}
            words: {from: steps.analyze_text.output.words}
            lines: {from: steps.analyze_text.output.lines}
      format: text
      data: {from: steps.analyze_text.output}
```

`api_version` 和 `kind` 是精确判别符，`schema_dialect` 必填且只能是 canonical `https://json-schema.org/draft/2020-12/schema`。每层都拒绝未知字段。标识符匹配 `[A-Za-z_][A-Za-z0-9_]*`。顶层 `$defs` 会注入每一份 authored contract，因此 `#/$defs/Name` 在 input、output、Parallel branch、Switch 和 LLM structured response 中含义一致。

### 类型化 JSON Schema profile

运行时合同使用 Draft 2020-12 验证器，但编译器只接受一套能够保守映射为静态类型的 profile：布尔 schema；`type`（含类型数组）；标量 `const`/`enum`；带显式 `items` 和可选 `minItems` 的同构数组；`properties`、`required`、`additionalProperties` 对象；以及没有 shape-changing sibling 的 `oneOf`/`anyOf`。`#/$defs/<Identifier>` 会先完整展开，再进入静态类型编译。

`minLength`、`maximum`、`format`、`maxItems` 等不改变可达字段类型的约束仍由运行时验证器执行，但不会赋予静态路径或窄化能力。会改变结构而当前静态类型没有建模的关键字会在启动编译期明确失败，包括 `allOf`、`not`、`if`/`then`/`else`、`dependentSchemas`、`dependentRequired`、`patternProperties`、`propertyNames`、`unevaluatedProperties`、`minProperties`/`maxProperties`、`prefixItems`、`unevaluatedItems` 和动态引用。这样不会出现运行时 schema 与编译器所声称的字段合同不一致。

### ValueExpr 与数据流

所有运行期派生值都必须使用一个显式、递归的 `ValueExpr`。下面一个 object 同时展示 `literal`、`from`、`array` 与 `template`：

<!-- dsl-example: canonical; entry: value -->
```yaml
object:
  limit: {literal: 10}
  question: {from: input.question}
  perspectives:
    array: [{literal: technical}, {literal: risk}]
  label:
    template:
      text: "Question: {{ question }}"
      bindings:
        question: {from: input.question}
```

- `literal` 保留完整 JSON 类型，不做字符串化；
- `from` 保留源值类型；
- `object`、`array` 递归组合类型化值；
- `template` 只看到显式 `bindings`，不会获得全局上下文；
- 普通字符串始终是普通字符串，不会被解释成模板或引用；
- 一个 ValueExpr 对象只能包含一个表达式键。

Prompt 不再是通用 ValueExpr。它只在 LLM `content` 中以已声明 Prompt ID 出现，例如 `content: system`。空 `{array: []}` 使用 bottom/Never element type，不会退化成绕过检查的 `Any`。

初始引用根只有：

- `input`：Run 输入；
- `run`：闭合的安全 Run 元数据对象，只含字符串字段 `id`、`request_id`、`agent_id`、`agent_version`、`started_at`；
- `scope`：结构化 block 通过 `inputs` 显式捕获的不可变值；
- `steps.<id>.output`：当前 body 中更早成功完成的 step 输出。

标识符形状的对象键可使用点路径；任意 JSON key 和固定数组索引使用 RFC 6901 JSON Pointer 后缀，例如 `input#/items/0/display-name` 和 `steps.lookup.output#/data/a~1b`。动态 key、动态索引、前向引用、跨分支引用和未被所有路径支配的引用都会在启动编译期失败。

结构化 block 先在父作用域计算 `inputs`，再只把这些值作为子作用域的 `scope` 暴露。子作用域内部值不会泄露到父作用域或兄弟作用域；唯一向外的数据通道是该 block 的 `result`。

### LLM 的 instruction/data 边界

LLM 作者层直接使用有序 `messages`。顶层 Prompt catalog 可以把 `technical_system` 声明为 `file: prompts/technical_system.md` 或 `inline: ...`；`.md` 文件在启动时受 containment、regular-file、UTF-8、大小和受限模板检查。Prompt 内的 `{{ question }}` 等 slot 按同名连接当前 LLM `inputs`，不会看到全局 workflow 上下文。

`content: technical_system` 是 Prompt ID；`{text: ...}` 是 inline template；`{from: inputs.question}` 是不做二次模板解释的运行时字符串：

<!-- dsl-example: canonical; entry: step -->
```yaml
- kind: llm
  id: analyze
  model: general_chat
  inputs:
    question: {from: input.question}
  messages:
    - role: system
      content: technical_system
    - role: user
      content:
        - text: Analyze the following untrusted question.
        - from: inputs.question
  parameters: {temperature: 0.2}
  response:
    format: json
    schema: {$ref: "#/$defs/Perspective"}
```

system/assistant 消息只接受零运行时 slot 的 authored Prompt 或 inline text。user Prompt/inline template 可以消费当前节点的同名 `inputs` slot；运行时文本和 `{image: {from: inputs.image_url}}` 也只允许进入 user content，nullable image 为 `null` 时自动省略。所有 LLM `inputs` binding 必须被 message source、content 或 template slot 消费，不能存在隐式额外上下文。

动态历史使用真实列表引用：在 `inputs.history` 的静态 Schema 可证明满足 closed `DynamicMessage[]` profile 后，`messages` 中的 `- {from: inputs.history}` 会在当前位置自动展开；它不是 JSON 字符串，也不需要 `spread`、`concat` 或 `parts`。动态消息只能使用 `user`/`assistant`，不会创建 system prompt，也不会触发模板或 Prompt ID 解析。

`response.format: text` 返回字符串；`response.format: json` 要求 `schema`，模型完成结果必须通过 Draft 2020-12 校验后才会绑定。稳定输出为 `{data, finish_reason, usage}`。请求预算统计最终 RuntimeMessage 与 parameters 的 provider-neutral 序列化大小，超限时不会调用 provider。

### Parallel 与 Switch

Parallel 同时拥有 spawn 和 barrier 语义。每个 branch 是一个完整的词法子作用域，有自己的 `output_schema`、`steps` 与 `result`：

<!-- dsl-example: canonical; entry: step -->
```yaml
- kind: parallel
  id: analyses
  inputs:
    question: {from: input.question}
  settle: all_settled
  max_concurrency: 2
  branches:
    technical:
      output_schema: {$ref: "#/$defs/Perspective"}
      steps:
        - kind: llm
          id: analyze
          model: general_chat
          inputs:
            question: {from: scope.question}
          messages:
            - role: user
              content:
                - {text: Analyze technical feasibility.}
                - {from: inputs.question}
          parameters: {}
          response: {format: text}
      result:
        return: {from: steps.analyze.output.data}

    risk:
      output_schema: {$ref: "#/$defs/Perspective"}
      steps:
        - kind: llm
          id: analyze
          model: general_chat
          inputs:
            question: {from: scope.question}
          messages:
            - role: user
              content:
                - {text: Analyze risk and compliance.}
                - {from: inputs.question}
          parameters: {}
          response: {format: text}
      result:
        return: {from: steps.analyze.output.data}
```

完整的差异化 prompt 和后续综合策略见 `agents/parallel_researcher/agent.yaml`。

`settle: all` 要求全部 branch 成功；一个可收集失败会停止继续接纳、取消同级并等待已接纳任务全部清理。`settle: all_settled` 把可收集结果变成以下严格联合类型：

```json
{"status":"ok","value":{}}
{"status":"error","error":{"category":"workflow","code":"WORKFLOW_...","retryable":false,"origin":"/workflow/..."}}
```

停止、进程中断、ownership/persistence 丢失、task panic 等基础设施失败不会被伪装成业务数据。它们向外传播并触发作用域取消与 drain。

Switch 是有序 first-match 选择；`default` 是必需的。每个正常完成的 arm 必须返回同一份完整 `output_schema`，或显式 raise：

<!-- dsl-example: canonical; entry: step -->
```yaml
- kind: switch
  id: synthesis_mode
  inputs:
    technical: {from: steps.analyses.output.technical}
    risk: {from: steps.analyses.output.risk}
  output_schema:
    type: string
    enum: [full, technical_only, risk_only]
  cases:
    - id: complete
      when:
        cel: >-
          scope.technical.status == 'ok' &&
          scope.risk.status == 'ok'
      result:
        return: {literal: full}
    - id: technical_only
      when:
        cel: scope.technical.status == 'ok'
      result:
        return: {literal: technical_only}
  default:
    id: risk_only
    result:
      return: {literal: risk_only}
```

只有被选择的 arm 执行；编译器在内部使用 Branch/Phi 合并唯一结果。未选择 arm 不产生占位值，也没有额外 authored 聚合步骤。

`when.cel` 使用刻意收窄的 typed predicate profile：唯一根是 `scope`；每个字段必须静态可读；只接受类型正确的布尔逻辑、相等/顺序比较和 `size()`；最终类型必须是 boolean。`scope.result.status == 'ok'` 这类合取中的标量判别会只在当前 case 内把联合类型收窄，因此该 case 可以安全读取 `scope.result.value`。`default` 不会自动继承前序条件的否定窄化。

### Root return、child yield 与 raise

每个 Parallel branch 和 Switch arm 都有一个 `result`：

```yaml
result:
  return: {from: steps.analyze.output}
```

编译器把 child return 降低为内部 `RegionYield`。它只完成当前子作用域，不创建公共 `RunOutput`。

只有 workflow root 的 return 能成功完成 Run，并拥有平台的公共 `{content?, format?, data}` envelope：

```yaml
workflow:
  steps: []
  result:
    return:
      content: {literal: done}
      format: text
      data: {literal: null}
```

`data` 必须满足 `output.data_schema`。`content` 存在时必须是字符串，并同时声明 `format: text|markdown`。Run 只有在 root return 已形成、所有后代完成或取消并 drain、输出合同与大小限制通过、权威终态事务提交后才算成功。

Authored failure 先在顶层声明，再由任意可见 block `raise`：

```yaml
errors:
  all_failed:
    category: workflow
    code: WORKFLOW_ALL_BRANCHES_FAILED
    public_message: No analysis perspective was available.

workflow:
  result:
    raise: all_failed
```

Authored error 只能使用 `workflow` category，不能伪造 operation timeout、stop、ownership 或 infrastructure failure。

### Region/SSA 与运行时作用域树

结构化 YAML 是唯一作者合同，Region/SSA 是唯一运行时计划。编译器为作用域、Operation 和值生成稳定的限定身份，例如：

```text
/workflow/analyses/branches/technical/analyze
/workflow/synthesis_mode/cases/complete
```

IR verifier 在运行前拒绝重复身份、错误 terminator、use-before-definition、跨 Region value escape、未声明 capture、Schema/类型不匹配及错误 Branch/Phi 结构。

ScopeScheduler 递归执行 RunScope、OperationScope、ParallelScope、BranchScope 和选中 arm。父作用域拥有所有子 future：关闭 admission 后先请求协作取消，再完整 drain；不会留下脱离父作用域的工作流任务。

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

Agent 发现接口返回 `id`、`name`、`description`、内容寻址 `version` 和 `input_schema`。`input_schema` 是编译和 Run 创建时使用的同一份 Draft 2020-12 合同；API 不公开 prompt、structured workflow、模型或 Action config。

Attached POST 会先原子订阅实时事件再启动 Run。终态事件发送后 SSE 立即关闭；非终态连接断开会取消 Run。Detached POST 返回 HTTP 202，连接断开不影响执行，客户端通过 GET Run 轮询或 DELETE 幂等取消。平台不提供公开事件重放；`seq` 与 SSE `id` 只用于单 Run 排序和审计关联，不是恢复游标。

v2 leaf operation 只公开生命周期元数据事件：

- `operation.started`；
- `operation.completed`；
- `operation.failed`。

Operation 不提供公共内容增量；输出值也不会进入公共事件或 journal。完成事件只公开限定 `operation_id`、类型、attempt、耗时和输出字节数；失败事件使用固定公共消息，不携带内部诊断。模型 provider 仍可使用内部流式传输，但 `ai.chat` 会在运行时内聚合并完成响应校验后才产生叶子结果。中间值只存在于运行期数据流，只有显式 ValueExpr 投影能把它交给后续 Operation，只有 root return 能把结果写入公共 Run 终态。

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

旧 Agent 文档不会被静默解释为 v2。当前作者层合同见 [DSL 作者层 LLM、Action 与消息模型重设计规范](docs/superpowers/specs/2026-07-17-dsl-authoring-surface-redesign.md)，Region/SSA、结构化控制流与作用域运行时的仍有效部分见 [DSL vNext Region/SSA Design](docs/superpowers/specs/2026-07-16-dsl-vnext-region-ssa-design.md)，权威性规则见 [Design-document authority](docs/superpowers/README.md)，切换原则和数据处理见 [DSL v2 直接切换说明](docs/formal-v1-breaking-changes.md)。
