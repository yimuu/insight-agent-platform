# Insight Agent Platform

一个面向平台自有 Agent 的通用 Rust 运行时基线。它把严格 DSL 编译为不可变执行图，通过可扩展的节点、模型和 Action 注册表运行，并提供 live-only Attached SSE、Detached 轮询、显式取消以及 SQLite/PostgreSQL 事件历史。

医学报告单解读只是仓库中的一个多模态示例，不是平台的领域边界。

## 架构

启动链路是确定的：

```text
严格平台配置 + 命名资源
          ↓
节点/模型/Action 注册表
          ↓
AgentCompiler（Schema、模板、引用、DAG、能力校验）
          ↓
不可变 CompiledAgent
          ↓
RunService → RunCoordinator → EventHub/Journal → RunRepository
          ↓
             /v1 JSON + SSE
```

DSL 使用显式 `entry + nodes` DAG，而不是隐式步骤数组。核心理由是：执行顺序、条件跳转和数据依赖本质上属于图语义；显式 DAG 可以在启动前统一发现缺边、环、不可达节点和非法前驱引用，也让新增节点类型无需修改调度器。

正式 V1 内置五种节点：

| 节点 | 作用 |
|---|---|
| `core.template` | 递归渲染字符串、数组或对象 |
| `core.chat` | 调用命名 Chat 模型，支持文本、多模态消息和私有/公开增量 |
| `core.action` | 通过严格 JSON Schema 调用本地或受限外部能力 |
| `core.condition` | 按顺序执行预编译 CEL 条件并选择分支 |
| `core.output` | 唯一成功终点，明确最终内容、格式和结构化数据 |

条件节点和其他节点一样通过注册表解析。新增节点是静态链接的 Rust 扩展：实现 `NodeType` 负责编译期 config、envelope、边和引用声明，实现 `NodeExecutor` 负责运行期执行；两者分别注册到编译期和运行期注册表。注册后，自定义节点走同一套 DSL 解析、图校验、调度、事件、节点输出和终态提交路径，核心节点源码、调度器和 HTTP 层不需要增加分支：

```rust,ignore
let mut types = NodeTypeRegistry::default();
types.register(MyNode)?;

let mut executors = NodeExecutorRegistry::default();
executors.register(MyNode)?;
```

这不是动态插件系统：V1 不加载外部动态库、WASM、远程插件或下载代码。扩展代码由平台进程在构建/启动时显式链接和注册；如果编译期类型和运行期 executor 注册不一致，Run 会按普通运行时错误路径失败并写入终态。

业务能力优先实现为 Action。Action 声明输入/输出 Schema、幂等元数据和是否允许流式内容，再由 `core.action` 调用：

```rust,ignore
#[async_trait]
impl Action for ClassifyAction {
    fn descriptor(&self) -> ActionDescriptor { /* strict JSON contracts */ }
    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        /* cooperative cancellation through context */
    }
}

actions.register(ClassifyAction)?;
```

## 配置与启动

复制环境变量模板并填写模型密钥：

```bash
cp .env.example .env
# 编辑 .env，设置 OPENAI_API_KEY
cargo run
```

默认配置只监听 `127.0.0.1:3000`，并显式关闭鉴权。`/health` 始终公开；运行时可接受请求时返回 `200/OK`，journal 永久失败或服务关停后返回 `503/RUNTIME_UNHEALTHY`。`/v1` 可切换到从环境变量读取的 Bearer token：

INFO 日志是结构化且 body-free 的：Run、节点、Chat 和 provider 记录只包含 `run_id`、`request_id`、`agent_id`、`agent_version`、节点 ID/type、状态、耗时、计数和序列化字节数。日志不记录请求输入值、prompt、模型输出、Action 输入/输出、事件 payload、带 query 的完整 URL、请求/响应头或凭据。当前基线只提供结构化日志，不包含 metrics backend 或 exporter。

```yaml
version: 1
bind_addr: 127.0.0.1:3000
auth:
  mode: bearer_env
  token_env: AGENT_RUNTIME_TOKEN
agents:
  directory: ../agents
  enabled: [code_node_demo, medical_report_interpreter, researcher]
models:
  config: models.yaml
actions:
  enabled: [current_time, example.text_metrics]
history:
  provider: sqlite
  path: ../data/formal_v1.sqlite3
runtime:
  max_concurrent_runs: 32
  max_fork_branches: 32
  max_parallel_node_executions: 32
  max_parallel_branches_per_run: 8
  default_node_timeout: 60s
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 128
  journal_capacity: 1024
  journal_batch_size: 32
  journal_operation_timeout: 30s
```

`agents.enabled` 默认是空集合，不会意外暴露目录中的 Agent。相对路径从平台配置文件所在目录解析。未知字段、零容量、零超时、缺失文件和缺失/空密钥都会阻止启动。

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

`open_ai_chat.base_url` 默认必须使用 HTTPS。明文 HTTP 只能显式声明：

```yaml
transport:
  plaintext_http: loopback        # 仅 127.0.0.1 / localhost / [::1]
```

或：

```yaml
transport:
  plaintext_http: trusted_private # 部署方明确接受的私有网络模型服务链路
```

`trusted_private` 不会自动证明目标地址在内网，也不会做 DNS/IP 私网判定；它表示部署方确认该 HTTP 链路处于可信私有边界内。公网或其他不可信模型服务必须使用 HTTPS。模型 URL 不允许携带 username/password；密钥只通过 `api_key_env` 指向的环境变量注入。

`limits` 可省略；省略时使用上述默认字节上限。零值非法，会以 `MODEL_CONFIG_INVALID` 阻止启动。上游响应体、SSE 行、单个 `data:` payload、单个文本 delta、usage JSON 和最终累计文本都会在写入或继续累计前检查。超限 Run 使用稳定错误 `MODEL_RESPONSE_TOO_LARGE`，错误消息为 `chat provider response exceeded the configured size limit`，不会包含 provider body、prompt、API key、URL query、响应头、usage、响应片段或配置的具体限制值。

## Agent DSL 示例

```yaml
version: 1
id: answerer
name: Answerer
input:
  schema:
    type: object
    required: [question]
    additionalProperties: false
    properties:
      question: {type: string, minLength: 1}
prompts:
  answer: prompts/answer.md
entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    emit: content
    config:
      model: general_chat
      messages:
        - role: user
          content: {template_ref: answer}
      parameters: {}
  result:
    type: core.output
    config:
      content:
        template: "{{ nodes.answer.output.text }}"
      format: markdown
```

`emit: none` 保持节点增量私有，`emit: content` 才发布 `content.delta`。模板上下文只暴露 `input`、`run` 和已完成的 `nodes.<node_id>.output`。节点 ID 和 fork branch ID 必须匹配 `[A-Za-z_][A-Za-z0-9_]*`；跨节点引用只能使用 `nodes.<node_id>.output`，不能使用 `nodes["id"]`、computed access 或直接访问 `nodes` map。

节点 `timeout` 使用正式 V1 窄语法：正整数紧跟 `ms`、`s` 或 `m`，例如 `250ms`、`5s`、`2m`。不接受空格、复合值、别名、分数、`h/d` 等更大单位或前导零。

## HTTP 与 Run 生命周期

JSON 响应统一使用字符串码：

```json
{"code":"OK","message":"ok","data":{}}
```

公开健康检查：

```bash
curl --silent http://127.0.0.1:3000/health
```

列出 Agent：

```bash
curl --silent http://127.0.0.1:3000/v1/agents
```

创建 detached Run 会返回 HTTP 202。它与 SSE 订阅独立，客户端断开后继续执行，直到完成、失败、超时、显式取消或进程关闭：

```bash
curl --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs
```

创建 attached Run 会原子地订阅实时事件并启动执行，响应头包含 X-Run-Id 和 X-Request-Id。终态事件写入历史后发送，发送后 SSE 立即关闭。客户端断开会立即取消仍在运行的 attached Run；该接口不支持重连补发。

SSE 每 5 秒发送 keepalive 注释用于尽快发现半开连接；注释不是协议事件，不占用 seq。网络栈、代理和调度会影响实际发现时间，因此 5 秒是检测目标而不是硬实时保证。

```bash
curl --no-buffer \
  --header 'x-request-id: req_demo_001' \
  --header 'content-type: application/json' \
  --data '{"question":"解释这个运行时的扩展边界"}' \
  http://127.0.0.1:3000/v1/agents/researcher/runs/stream
```

事件 envelope 包含 `schema_version: 1`、单 Run 单调递增的 `seq`、字符串 `code` 和点分事件类型。SSE keepalive 是注释，不占用序号：

```text
id: 3
event: content.delta
data: {"schema_version":1,"type":"content.delta","seq":3,"request_id":"req_demo_001","run_id":"run_...","agent_id":"researcher","agent_version":"sha256:...","node_id":"answer","time":"2026-07-10T00:00:00Z","code":"OK","message":"ok","data":{"content":"Rust"}}
```

Detached：创建后通过 Run 资源轮询，断开不会停止任务：

```bash
# Detached：创建后通过 Run 资源轮询，断开不会停止任务
curl --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs

curl --silent http://127.0.0.1:3000/v1/runs/run_xxx
curl --silent --request DELETE http://127.0.0.1:3000/v1/runs/run_xxx
```

`GET /v1/runs/{run_id}/events` 和 `after_seq` 已删除。`seq` 与 SSE `id` 只用于单 Run 事件排序和审计关联，不是恢复游标。`DELETE` 仍然幂等：活动 Run 返回取消后的记录，已终止 Run 原样返回。

以下命令可在默认开发配置下完整验证确定性 action-only 路径（需要 `jq`）：

```bash
curl --fail --silent http://127.0.0.1:3000/health | jq
curl --fail --silent http://127.0.0.1:3000/v1/agents | jq
CREATED=$(curl --fail --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs)
RUN_ID=$(printf '%s' "$CREATED" | jq --exit-status --raw-output '.data.run_id')
curl --fail --silent "http://127.0.0.1:3000/v1/runs/$RUN_ID" | jq
curl --fail --silent --request DELETE \
  "http://127.0.0.1:3000/v1/runs/$RUN_ID" | jq
```

## 历史后端

SQLite 是本地默认后端。PostgreSQL 使用环境变量保存连接密钥：

```yaml
history:
  provider: postgres
  database_url_env: RUN_HISTORY_DATABASE_URL
```

本地运行 PostgreSQL 合同测试：

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

A0 Action 校验错误安全修复不兼容既有 Run 历史；部署前按[正式 V1 破坏性变更中的 A0 重置流程](docs/formal-v1-breaking-changes.md#a0-action-validation-error-containment)停止服务并显式清空历史。A5 会让静态非法 Action input、hyphenated node/branch ID、indexed/computed `nodes` access 在启动编译期失败；迁移理由见[正式 V1 破坏性变更中的 A5 语义编译期校验](docs/formal-v1-breaking-changes.md#a5-semantic-compile-time-validation)。应用不会自动删除数据。

正式 V1 不存储原始输入，只保存顶层键和序列化字节数摘要。journal 只有在数据库确认事件持久化后才向订阅者广播；单次数据库操作受 `journal_operation_timeout` 限制。失败时先停止 journal worker，恢复事务锁定 Run，并基于持久化的 `MAX(seq)` 原子派生终态序号；这也能判定超时发生在 `COMMIT` 附近时的实际结果。journal 永久关闭后拒绝新 Run。进程启动时遗留的 `created/running` 记录会被标记为 `interrupted`，V1 不恢复工作。

## 示例

- `agents/researcher`：私有计划 + 公开答案。
- `agents/code_node_demo`：`example.text_metrics` 原生 Action，不调用模型，适合确定性冒烟测试。
- `agents/medical_report_interpreter`：使用同一个通用 `core.chat` 多模态协议的垂直示例。
- `agents/parallel_researcher`：两个多节点分支汇聚后再综合：

```yaml
fanout:
  type: core.fork
  config:
    branches: {perspective_a: analyze_a, perspective_b: analyze_b}
    join: collect
collect:
  type: core.join
  next: synthesize
  config: {mode: all_settled}
```

每个分支依次执行 `analyze_* (core.chat) -> normalize_* (core.template) -> collect`；`synthesize` 只允许引用 `nodes.collect.output`。分支就绪并进入执行队列时发布 `branch.started`，完成或失败时分别发布 `branch.completed` / `branch.failed`。事件 `data` 的精确形状为：`branch.started` `{fork_id, branch_id}`；`branch.completed` 在此基础上增加 `terminal_node_id`；`branch.failed` 增加 `terminal_node_id` 和已清理的 `error:{code,message}`。`branch.started` 表示 ready-queue activation，不代表模型已开始返回内容。

`core.join` 的输出是固定聚合对象（不是 Run 终态 envelope）：

```json
{"branches":{"perspective_a":{"status":"succeeded","terminal_node_id":"normalize_a","output":{"text":"..."}},"perspective_b":{"status":"failed","terminal_node_id":"analyze_b","error":{"code":"UPSTREAM_FAILURE","message":"..."}}},"summary":{"total":2,"succeeded":1,"failed":1}}
```

所有分支失败时 join 仍成功并返回 `summary.failed == summary.total`；这与 Run 的 `run.completed`、`run.failed` 或 `run.cancelled` 终态事件分开。`max_concurrent_runs` 和 `max_parallel_node_executions` 是进程范围上限；`max_parallel_branches_per_run` 与 `max_fork_branches` 分别限制单 Run 并发分支和单 fork 分支数。取消会停止整个 Run；单个分支失败只结算该分支，`all_settled` 仍等待其余分支并可产生部分成功。V1 不支持嵌套 fork、resume、新 join 模式，也不允许 post-join 节点直接引用分支节点。

## 验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

完整接口迁移理由见 [正式 V1 破坏性变更](docs/formal-v1-breaking-changes.md)。
