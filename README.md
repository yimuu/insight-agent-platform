# Insight Agent Platform

一个面向平台自有 Agent 的通用 Rust 运行时基线。它把严格 DSL 编译为不可变执行图，通过可扩展的节点、模型和 Action 注册表运行，并提供可重连的 SSE、显式取消以及 SQLite/PostgreSQL 事件历史。

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

条件节点和其他节点一样通过注册表解析。新增静态链接节点只需实现 `NodeType` 和 `NodeExecutor`，然后在启动注册表中注册；DSL 解析器、图校验器、协调器、事件系统和 HTTP 层不需要增加分支：

```rust,ignore
let mut types = NodeTypeRegistry::default();
types.register(MyNode)?;

let mut executors = NodeExecutorRegistry::default();
executors.register(MyNode)?;
```

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
  default_node_timeout: 60s
  run_timeout: 5m
  attached_reconnect_grace: 10s
  subscriber_capacity: 128
  replay_ring_capacity: 512
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
```

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

`emit: none` 保持节点增量私有，`emit: content` 才发布 `content.delta`。模板上下文只暴露 `input`、`run` 和已完成的 `nodes.<id>.output`。

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

创建 attached Run 会直接返回 SSE，并在响应头给出 `X-Run-Id` 和 `X-Request-Id`。最后一个订阅断开后进入重连宽限期；宽限期内没有订阅恢复时，运行会被取消：

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

查询、按序补发和显式取消：

```bash
curl --silent http://127.0.0.1:3000/v1/runs/run_xxx
curl --no-buffer 'http://127.0.0.1:3000/v1/runs/run_xxx/events?after_seq=3'
curl --silent --request DELETE http://127.0.0.1:3000/v1/runs/run_xxx
```

`DELETE` 幂等：活动 Run 返回取消后的记录，已终止 Run 原样返回。客户端应保存最后处理成功的 `seq`，重连时把它作为 `after_seq`；服务分批补发有界数量的持久事件，再接入活动事件，不产生重复序号。若单批尚未追平或订阅落后，服务发送不带 `id/seq` 的 `transport.error`，其中 `data.after_seq` 是下一次重连游标。

以下命令可在默认开发配置下完整验证确定性 action-only 路径（需要 `jq`）：

```bash
curl --fail --silent http://127.0.0.1:3000/health | jq
curl --fail --silent http://127.0.0.1:3000/v1/agents | jq
CREATED=$(curl --fail --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs)
RUN_ID=$(printf '%s' "$CREATED" | jq --exit-status --raw-output '.data.run_id')
curl --fail --silent --no-buffer \
  "http://127.0.0.1:3000/v1/runs/$RUN_ID/events?after_seq=0"
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

正式 V1 不存储原始输入，只保存顶层键和序列化字节数摘要。journal 只有在数据库确认事件持久化后才向订阅者广播；单次数据库操作受 `journal_operation_timeout` 限制。失败时先停止 journal worker，恢复事务锁定 Run，并基于持久化的 `MAX(seq)` 原子派生终态序号；这也能判定超时发生在 `COMMIT` 附近时的实际结果。journal 永久关闭后拒绝新 Run。进程启动时遗留的 `created/running` 记录会被标记为 `interrupted`，V1 不恢复工作。

## 示例

- `agents/researcher`：私有计划 + 公开答案。
- `agents/code_node_demo`：`example.text_metrics` 原生 Action，不调用模型，适合确定性冒烟测试。
- `agents/medical_report_interpreter`：使用同一个通用 `core.chat` 多模态协议的垂直示例。

## 验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

完整接口迁移理由见 [正式 V1 破坏性变更](docs/formal-v1-breaking-changes.md)。
