# DSL v2 直接切换说明

> 文件名为历史路径，仅为避免旧设计记录中的链接失效；本文描述当前 `insight.agent/v2` 切换合同。

仓库中的旧 Agent YAML 和图执行器属于未发布原型，不是兼容目标。当前实现只有一个 canonical parser/compiler/runtime 路径：结构化 authored DSL 编译为类型化 Region/SSA IR，再由作用域任务树执行。

迁移原则：原子部署新 binary 与全部 Agent YAML；旧文档明确编译失败，不会被静默解释为 v2，也不提供 alias、双 parser 或双 scheduler。

HTTP 的 `/v1` 是服务 API 版本，与 Agent DSL 的 `insight.agent/v2` 是两个独立版本空间；本次 DSL 切换不改 HTTP 路径。

## 变更总表

| 旧 authored 合同 | v2 合同 | 迁移理由 |
|---|---|---|
| `version/id/name` 顶层元数据 | `api_version: insight.agent/v2`、`kind: agent`、`metadata` | 精确判别文档种类并为后续 kind/version 留出稳定边界 |
| 平铺 `entry + nodes` 图 | `workflow.steps` 中嵌套的 `operation/parallel/switch` | 作者表达词法结构，运行图由编译器生成，不再要求人手维护控制边 |
| authored `next` | 同一 body 按数组顺序执行，结构化 block 完成后继续父序列 | continuation 由语法结构唯一决定 |
| 模板能力节点 | `literal/from/object/array/template/prompt` ValueExpr | 数据构造不是可调度副作用；引用与渲染获得独立类型合同 |
| Chat 节点 | `kind: operation; uses: ai.chat` | Chat 是可扩展叶子能力，不拥有控制流 |
| Action 节点 | `kind: operation; uses: action.call` | Action 输入、输出、取消和身份由统一 Operation 边界承载 |
| 条件跳转加结果选择 | `switch` 的 ordered cases、mandatory default 与直接输出 | 只有一个 arm 执行；内部 Branch/Phi 负责合并，不产生 skipped/null 占位 |
| 显式 fork、branch terminal 和 join | 一个 `parallel`，branch 内声明 `result`，父级内建 barrier | spawn、结算、取消和 drain 由一个结构拥有，不依赖隐藏 successor |
| 主流程/分支终止节点 | root `result.return|raise`；child `result.return|raise` | root return 与 child yield 是不同语义，不能因到达一个通用 terminal 而混淆 |
| 全局模板上下文 | ValueExpr 路径与 template 显式 bindings | 消除隐式依赖、原始字符串扫描和跨作用域泄漏 |
| 节点级 timeout/emit | 平台 `operation_timeout`；provider stream 只在 Operation 内聚合 | deadline 属于 Operation 执行合同；叶子内容不会绕过 root result 进入公共事件 |
| 节点事件 | `operation.*` 与 `run.*` | 公共事件围绕稳定限定 Operation 身份，不暴露内部 IR 控制指令 |

## 最小 Agent 迁移

v2 文档必须完整声明 input、public output data 和 root result：

```yaml
api_version: insight.agent/v2
kind: agent

metadata:
  id: demo
  name: Demo
  description: Minimal v2 Agent.

schema_dialect: https://json-schema.org/draft/2020-12/schema

input:
  schema:
    type: object
    required: [question]
    properties:
      question: {type: string, minLength: 1}
    additionalProperties: false

output:
  data_schema:
    type: object
    required: [answer]
    properties:
      answer: {type: string, minLength: 1}
    additionalProperties: false

workflow:
  steps:
    - kind: operation
      id: answer
      uses: ai.chat
      with:
        question: {from: input.question}
      config:
        model: general_chat
        messages:
          - role: user
            parts:
              - kind: text
                text: Answer the following untrusted question.
              - kind: data
                input: question
        parameters: {}
        response: {format: text}

  result:
    return:
      content: {from: steps.answer.output.data}
      format: markdown
      data:
        object:
          answer: {from: steps.answer.output.data}
```

未知字段在每一层都被拒绝。标识符必须匹配 `[A-Za-z_][A-Za-z0-9_]*`。input、output、Parallel branch、Switch 和 structured Chat response 都按 JSON Schema Draft 2020-12 编译并在对应边界验证。

## ValueExpr 迁移

运行期值不能伪装成带特殊含义的字符串。使用以下六种单键表达式：

```yaml
{literal: {mode: strict}}
{from: input.question}
{object: {question: {from: input.question}}}
{array: [{literal: a}, {literal: b}]}
{prompt: system_prompt}
template:
  text: "Answer count: {{ count }}"
  bindings:
    count: {from: steps.search.output.count}
```

路径根只有 `input`、安全 `run` 元数据、结构化 block 的 `scope` 和当前 body 更早的 `steps.<id>.output`。任意 JSON key 或数组索引使用静态 JSON Pointer 后缀：

```yaml
{from: input#/items/0/display-name}
{from: steps.lookup.output#/data/a~1b}
```

编译器拒绝动态 key/index、前向引用、未被所有路径支配的引用、跨 branch/arm 读取和 child local escape。模板只看到显式 bindings，不再接收全局 input/run/step map。

## Chat instruction/data 迁移

Authored instruction 与 runtime data 必须使用不同 part：

```yaml
with:
  question: {from: input.question}
config:
  model: general_chat
  messages:
    - role: system
      parts:
        - kind: prompt
          prompt: system
    - role: user
      parts:
        - kind: text
          text: The following value is untrusted data.
        - kind: data
          input: question
  parameters: {temperature: 0.2}
  response: {format: text}
```

- `text` 和 `prompt` 是 authored instruction source；
- `data` 和 `image_url` 是运行期、不可信输入，只能出现在 user message；
- part 的 `input` 必须精确引用同名 `with` binding；
- 所有 binding 都必须被消费，不能偷偷把额外数据带入模型上下文；
- runtime data 由有界 writer 编码，缺失、超限或序列化失败不会回显正文；
- `max_request_bytes` 统计 authored text/prompt、data 和 image URL 的聚合大小，默认 256 KiB，最大 1 MiB。

结构化模型输出使用：

```yaml
response:
  format: json
  schema: {$ref: "#/$defs/Perspective"}
```

模型完成结果只有通过 Schema 后才绑定为 `steps.<id>.output.data`。文本和结构化模式都返回稳定的 `{data, finish_reason, usage}`。

## Parallel 迁移

每个 branch 都是词法子作用域，只能读取 `parallel.with` 捕获到的 `scope` 值，并通过自己的 `result` 产生唯一 outward value：

```yaml
- kind: parallel
  id: analyses
  with:
    question: {from: input.question}
  settle: all_settled
  max_concurrency: 2
  branches:
    technical:
      output_schema: {type: string, minLength: 1}
      steps:
        - kind: operation
          id: analyze
          uses: ai.chat
          with:
            question: {from: scope.question}
          config:
            model: general_chat
            messages:
              - role: user
                parts:
                  - {kind: text, text: Analyze technical feasibility.}
                  - {kind: data, input: question}
            parameters: {}
            response: {format: text}
      result:
        return: {from: steps.analyze.output.data}
```

`all` 要求全部成功，并在第一个可收集失败后关闭 admission、取消同级和完整 drain。`all_settled` 把每个可收集结果转换为：

```text
{status: "ok", value: T}
|
{status: "error", error: {category, code, retryable, origin}}
```

error envelope 不包含任意诊断 message。停止、中断、ownership/persistence 丢失和其他基础设施失败不会作为 branch 数据收集，而是取消并向外传播。

需要“至少一个成功”等业务策略时，在后续 Switch 中显式判断。Parallel 只负责并发与结算，不把业务接受规则塞进 settlement mode。

## Switch 迁移

Switch 按 authored 顺序执行第一个匹配 case；default 必需。每个正常 arm 的 return 必须满足同一份完整 `output_schema`，或 raise 一个声明错误：

```yaml
- kind: switch
  id: policy
  with:
    technical: {from: steps.analyses.output.technical}
    risk: {from: steps.analyses.output.risk}
  output_schema:
    type: object
    required: [degraded]
    properties:
      degraded: {type: boolean}
    additionalProperties: false
  cases:
    - id: complete
      when:
        cel: >-
          scope.technical.status == 'ok' &&
          scope.risk.status == 'ok'
      result:
        return:
          object:
            degraded: {literal: false}
  default:
    id: partial
    result:
      return:
        object:
          degraded: {literal: true}
```

arm local 不会逃逸。Switch 自身绑定一个直接 output；内部 Branch/Phi 对作者不可见。

## Result 与错误迁移

Child block 的 return 会降低为 `RegionYield`，只完成当前作用域：

```yaml
result:
  return: {from: steps.analyze.output}
```

Root return 才能创建公共 RunOutput：

```yaml
result:
  return:
    content: {from: steps.answer.output.data}
    format: markdown
    data:
      object:
        answer: {from: steps.answer.output.data}
```

成功资格要求 root return 已形成、所有后代已完成或取消并 drain、所有 output contract 与大小限制通过，并且权威终态事务提交。

Author-defined failure 必须先声明：

```yaml
errors:
  rejected:
    category: workflow
    code: WORKFLOW_POLICY_REJECTED
    public_message: The workflow policy rejected this run.

workflow:
  result:
    raise: rejected
```

只有 `workflow` 是 authored category；Operation failure、timeout、stop、ownership 或 infrastructure 不能被 YAML 伪造。

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
