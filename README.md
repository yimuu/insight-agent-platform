# Insight Agent Platform

Insight Agent Platform 是一个以 Rust 实现的 DSL v3 持久化图执行运行时。Agent 作者编写结构化 YAML；平台在启动时将其编译为不可变、类型化的 Canonical Plan，并由数据库驱动的调度器执行。进程重启、Worker lease 过期、信号/超时竞态和取消都通过持久化状态与 first-winner 事务恢复，不依赖进程内执行栈重放。

当前规范是 [DSL v3 Durable Graph Execution（中文）](docs/superpowers/specs/2026-07-18-dsl-v3-durable-graph-execution-design.md)。仓库仍处于快速演进期，不提供旧 DSL 或旧运行内核兼容层。

## 运行模型

```text
Agent YAML
    ↓ parse / type-check / link
Canonical Typed Plan（不可变 revision）
    ↓ durable admission
Run → Scope → Activation → Attempt
    ↓ lease / heartbeat / fenced commit
PostgreSQL（生产）/ SQLite（单进程开发）+ content-addressed artifacts
```

核心合同：

- `Run`、`Scope`、`Activation`、`Attempt`、控制 token、timer、signal 和 task outbox 都有稳定身份并持久化；
- scheduler 只根据 Plan、持久化事实和数据库时间作确定性决策；
- Worker 通过带 epoch/fence 的 lease 提交结果，过期 Worker 不能覆盖新权威结果；
- 每个终态、join、signal/timeout 和 cancel/success 竞态都由数据库事务决定唯一赢家；
- 小值内联保存，大值写入 content-addressed Artifact store，并在结果事务中提交引用；生产 runtime 必须共享同一物理 store；
- SQLite 覆盖确定性的单进程语义子集；多 runtime lease、fencing 与生产恢复合同只由真实 PostgreSQL 16 门禁承诺。

## 快速启动

仓库要求 Rust `1.94.1`。Quickstart 只启用本地 `action_demo`，不需要模型密钥：

```bash
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

服务默认监听 `127.0.0.1:3000`：

```bash
curl http://127.0.0.1:3000/health/ready

curl -X POST http://127.0.0.1:3000/v1/agents/action_demo/runs \
  -H 'content-type: application/json' \
  -H 'x-request-id: example-1' \
  -d '{"text":"hello durable graph"}'
```

创建接口返回 `202 Accepted` 和 `run_id`。随后查询：

```bash
curl http://127.0.0.1:3000/v1/runs/RUN_ID
```

生产配置入口由 `PLATFORM_CONFIG` 指定，默认是 `config/platform.yaml`。`deployment_mode: production` 强制使用 PostgreSQL 和显式的 `artifacts.provider: shared_filesystem`；共享存储必须声明 `namespace`，所有连接同一数据库的 runtime 必须挂载同一物理目录。第一次启动会把 marker 中的随机 `store_id` 原子绑定到数据库的不可变 Artifact-store authority，后续实例只有 identity 完全一致才能在任何 catalog/publication 写入或 GC 领取前启动。相同路径的不同挂载别名不影响 identity，而同 namespace 的不同物理根目录会 fail-closed。`local_filesystem` 禁止 namespace，只允许 `single_process_development`。SQLite 不承诺多进程所有权、HA 或生产部署语义。Quickstart 已显式选择单进程开发模式。配置中的路径相对于配置文件解析。运行时配置只接受当前 durable v3 真正生效的参数，旧内核的进程内 journal、template 和协作取消窗口参数会作为未知字段拒绝。`runtime.public_event_retention` 只控制已发布的非终态 Public Event，`runtime.public_event_prune_interval` 控制有界清理周期；terminal Public Event 不受该保留策略影响并持续作为 durable delivery authority。

### PostgreSQL 启动迁移

生产进程连接 PostgreSQL 后会自动执行唯一的 durable-v3 前向 migration manifest，成功后才创建 `RunService` 或读取运行表。并发启动的多个实例由固定的事务级 advisory lock 串行化；`durable_v3_schema_migrations` 独立记录每个 migration 的 version、文件名、SQL SHA-256 和应用时间。已应用记录必须是当前 manifest 的精确前缀；版本空洞、未知或更高版本、文件名或 checksum 漂移，以及已有 v3 表但没有 ledger，都会使进程 fail-closed 退出，不会自动认领或猜测旧 schema。

数据库角色需要目标 database/schema 的连接与使用权限、在目标 schema 中创建对象的权限，以及后续 migration 对既有表、索引、函数和 trigger 的 owner/ALTER 权限，同时需要读写 migration ledger。每个缺失 migration 的 SQL 与 ledger 记录处于同一事务；任一步失败都会回滚，`RunService` 不会在半迁移 schema 上启动。

## DSL v3

最小 Agent：

```yaml
api_version: insight.agent/v3
kind: agent

inputs:
  text: string

output: TextMetrics

types:
  TextMetrics:
    fields:
      characters: integer
      words: integer
      lines: integer

workflow:
  steps:
    - type: action
      id: analyze_text
      call: example.text_metrics
      inputs:
        text: $text
      response: TextMetrics

    - return: $analyze_text
```

作者层直接表达结构化控制流和数据：

- `if / elif / else` 做流程选择，`match` 只选择值；
- `parallel` 配合 `all_success` 或 `all_settled` 表达 fork/join；
- `map`、`loop`、`agent_loop` 表达动态重复执行；
- `call` 表达固定 revision/interface 的子流程；
- `try / catch / finally`、`human_task`、signal wait 和 timer wait 表达可恢复的长流程；
- `yield` 产生局部结构结果，`return` 与 `raise` 终结工作流。

`human_task` 是独立的持久化人工工作项，不是普通 signal 的别名。`request` 是分派给处理人的类型化上下文，节点结果由 `response` 类型约束；候选人、候选组和 claim lease 都是显式合同：

```yaml
types:
  Approval:
    fields:
      decision: {type: string, enum: [approved, rejected]}
      comment: string

inputs:
  report_id: string

workflow:
  steps:
    - id: review
      human_task:
        signal: medical_review
        request: {report_id: $report_id}
        response: Approval
        assignees: [reviewer-alice]
        candidate_groups: [medical-reviewers]
        claim_lease_ms: 300000

    - return: $review
```

运行时引用使用 `$name` 或 `$object.field`；普通字符串保持普通字符串。LLM `messages` 是标准的有序对象列表，历史消息可作为列表项直接拼入，内容项使用 `text` 或 `image_url`。完整示例见 [agents](agents) 与 [v3 fixtures](tests/fixtures/v3)。

## HTTP API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health/live` | 进程存活 |
| `GET` | `/health/ready` | repository 与 runtime 就绪 |
| `GET` | `/v1/agents` | Agent discovery 与输入 schema |
| `POST` | `/v1/graph-agents/{agent_id}/revisions` | 严格校验并发布不可变 Graph Definition/Deployment Revision |
| `GET` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}` | 读取已发布 Graph 作者文档 |
| `POST` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/semantic-edits` | 以 base hash/head 双 CAS 原子提交一组拓扑语义编辑，并发布新 revision |
| `GET/PUT` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/view` | 独立读取或按 `expected_version` CAS 更新 ViewDocument |
| `POST` | `/v1/agents/{agent_id}/deployments/{deployment_revision_id}/runs` | 从指定不可变 Deployment Revision 创建 pinned Run |
| `POST` | `/v1/agents/{agent_id}/runs` | 创建 detached Run |
| `POST` | `/v1/agents/{agent_id}/runs/stream` | 创建 attached SSE Run |
| `GET` | `/v1/runs/{run_id}` | 查询持久化 Run |
| `GET` | `/v1/runs/{run_id}/execution-graph` | 读取该 Run 固定 revision 的只读执行图 |
| `GET` | `/v1/runs/{run_id}/trace` | 读取按稳定节点/Activation ID 关联的 trace overlay |
| `DELETE` | `/v1/runs/{run_id}` | 请求取消 |
| `POST` | `/v1/runs/{run_id}/pause` | 暂停新的调度 admission |
| `POST` | `/v1/runs/{run_id}/resume` | 恢复调度 |
| `POST` | `/v1/runs/{run_id}/signals/{name}` | 以 `message_id` 幂等提交 typed signal |
| `POST` | `/v1/runs/{run_id}/redrive` | 使用原始不可变 revision，并可按 `reuse_compatible` 复用闭合前缀 |
| `POST` | `/v1/runs/{run_id}/fork` | 选择 Deployment Revision、checkpoint ID 与输入覆盖创建分叉 Run |
| `POST` | `/v1/runs/{run_id}/migrate` | 两阶段迁移到已部署目标 Agent revision |
| `POST` | `/v1/runs/{run_id}/continue-as-new` | 终结当前 Run 并以同一 revision 开启下一 generation |
| `GET` | `/v1/human-tasks?limit=100` | 列出当前人工身份有权处理的 open/claimed 工作项 |
| `POST` | `/v1/human-tasks/{work_item_id}/claim` | 使用 `{}` 请求体和 `X-Request-ID` 幂等抢占工作项 |
| `POST` | `/v1/human-tasks/{work_item_id}/complete` | 以 `{claim_fence, value}` 和 `X-Request-ID` 幂等完成工作项 |

Attached SSE 是实时投影，不是历史重放接口；非终态连接断开会立即提交取消意图，并按结构化并发合同完成 drain。断开本身不能伪造终态或覆盖已经提交的数据库赢家。需要脱离连接继续执行时应创建 Detached Run；Detached 查询和所有控制接口都读取或写入同一个持久化 Run。
GraphAuthorDocument 发布时会重新验证并编译为 Canonical Plan；Run 只执行固定的不可变 Plan，布局专用 ViewDocument 与 trace overlay 都不是执行真相。View 的 CAS 冲突或损坏不会改变已发布 revision，也不会影响已有或后续 pinned Run。
恢复接口要求稳定的 `X-Request-ID` 和 `expected_projection_version`。Fork 的 `target_deployment_revision_id`、`checkpoint_id` 与 `input` 只是选择器：checkpoint content hash、复用 candidate、effect proof、revision/interface/schema 兼容证据仍由服务端从 durable authority 推导，客户端不能注入这些低层证明。

人工任务 API 使用独立的 request-scoped principal resolver，与普通 Run 管理 API 的 bearer 身份隔离：人工凭据不能创建、取消或恢复 Run，管理凭据也不会自动获得人工任务权限。claim 返回单调递增的 `claim_fence`；complete 必须回传该 fence，租约过期后的旧处理人无法提交。相同 `X-Request-ID`、身份、fence 和 payload 可安全重放，任一内容变化都会返回冲突。工作流取消或超时会把尚未完成的工作项分别闭合为 `cancelled` 或 `expired`。

正式二进制通过环境变量配置相互独立的平台凭据和人工身份；配置只保存环境变量名，不接受明文 token：

```yaml
auth:
  mode: bearer_env
  token_env: PLATFORM_ADMIN_TOKEN
  human_task_credentials:
    - identity: alice
      groups: [medical-reviewers, triage]
      token_env: HUMAN_ALICE_TOKEN
    - identity: bob
      groups: [medical-reviewers]
      token_env: HUMAN_BOB_TOKEN
```

缺失或空环境变量、重复身份、重复 token（包括与平台管理 token 重复）会阻止启动；未配置 `human_task_credentials` 时，三个 HumanTask 路由保持 fail-closed。token 不进入 Debug、错误或运行日志。

## 验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

CI 还运行 cutover residual scan、真实 PostgreSQL 16 合同测试、real-process restart/shutdown 测试和依赖策略检查。PostgreSQL 测试使用 `V3_TEST_POSTGRES_URL`，Artifact repository 专项门禁使用 `V3_ARTIFACT_TEST_POSTGRES_URL`；在 CI 中这些变量必须存在，不能静默跳过。

## 代码导航

- `src/dsl/v3/`：作者文档、类型检查与 lowering；
- `src/engine/plan/`：Canonical Typed Plan 与 verifier；
- `src/engine/scheduler/`：纯计划决策与稳定身份；
- `src/engine/repository/`：SQLite/PostgreSQL 状态机、lease、checkpoint、recovery；
- `src/runtime/v3_service.rs`：生产 Run 服务、Worker pump 与外部 ingress；
- `src/catalog_v3.rs`：编译、绑定并固定 deployment revision；
- `migrations/durable_v3/`：持久化 schema；
- `tests/v3_*`：编译器、repository、scheduler、恢复和真实数据库门禁。

设计文档的权威关系见 [docs/superpowers/README.md](docs/superpowers/README.md)。
