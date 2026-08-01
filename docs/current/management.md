# Agent 与 Provider 管理面

状态：Current

适用合同：`management.version: 1`、`agent-management/v1`、`provider-management/v1`

## 权威与认证

`/v1/admin/agents/**`、`/v1/admin/providers/**` 和 `/v1/admin/mcp/**` 是 installation-scoped
Operator 控制面。它们共用顶层 `management.operator_credentials`，但 capability 不互相隐含：read、
write、validate/test、publish、deploy、activate、archive/suspend/retire 和 debug 分别授权。所有响应都
是 `Cache-Control: private, no-store`；mutation 要求稳定 `X-Request-ID`，Draft/entity/current pointer
mutation 还要求强 `If-Match`。

Durable store 是 managed Agent、Provider 和 MCP 的唯一运行权威。API 不修改平台 YAML 或工作区文件；
进程 catalog/registry 是可重建投影。每个成功 mutation 在同一事务提交状态、version、幂等 receipt、
body-free audit 与 bounded outbox。相同 request ID + 相同 canonical body replay 原结果；不同 body 返回
idempotency conflict。

## Agent 生命周期

```text
Agent Entity
  → mutable Draft
  → immutable Validation
  → immutable Definition Revision
  → immutable Deployment Resolution
  → immutable Deployment Revision
  → CAS Active Deployment
  → pinned Run
```

Agent API 主路径如下：

| 阶段 | API | 关键并发合同 |
|---|---|---|
| Entity | `POST/GET /v1/admin/agents`、`GET/PATCH/DELETE /{id}` | entity ETag `"agent-N"` |
| Draft | `GET/PUT /{id}/draft` | Draft ETag `"draft-N"`；完整 package 原子替换 |
| Graph | `POST /draft/semantic-edits`、`GET/PUT /draft/view` | semantic edit 用 Draft CAS；View 用独立 `"view-N"` |
| Validation | `POST /validations`、`GET /validations/{id}` | 固定 Draft version/hash 与 policy digest |
| Definition | `POST/GET /revisions`、`GET /revisions/{id}` | publish 只安装 Definition，不解析可变依赖、不激活 |
| Resolution | `POST /deployment-resolutions`、`GET /.../{id}` | 固定 catalog snapshot、dependency heads 和期限 |
| Deployment | `POST/GET /deployments`、`GET /.../{id}` | 事务复验 resolution/head，冻结 exact bindings，不激活 |
| Route | `GET/PUT/DELETE /active-deployment` | entity CAS；PUT 历史 Deployment 即回滚 |
| Lifecycle | `POST /archive`、`POST /restore` | archive 原子撤下 public head；restore 只回 inactive |
| Debug | `/debug-sessions/**` | admin-only `debugrun_*`，永不写 publication head |

`yaml_package` 和 `graph` 是创建时不可变的 authoring mode。YAML prompt path 必须是规范相对路径；Graph
semantic edit 不再隐式发布。ViewDocument 不进入 `author_hash`/`semantic_hash`。物理 DELETE 仅允许从未
发布、部署、调试或运行且无引用的 inactive Agent；其他对象使用 archive。

普通 `POST /v1/agents/{id}/runs` 只能解析 durable active head。公共 historical Deployment admission
与旧 `/v1/graph-agents/**` 不存在；历史 revision 只供服务端 recovery/redrive/fork/migrate 证明链和
admin Debug 使用。

## Debug Session

创建请求选择 Draft version、Definition Revision 或 Deployment Revision，并引用预配置 execution
profile：

```json
{
  "source":{"type":"draft","draft_version":12},
  "execution_profile_id":"author-sandbox",
  "input":{"query":"test"}
}
```

Draft 的 version、author hash 与正文快照在 session 创建事务内复验并固定。临时 exact plan 只安装到
immutable Definition/Deployment archive，不修改 `agent_publication_heads`。执行 Run 使用
`debugrun_*` namespace；普通 Run API 对其统一返回 404。`GET .../stream` 使用相同
`run-stream/v1` envelope，但认证、endpoint、cache 与 visibility 独立。

状态为 `queued | running | succeeded | failed | cancelled | expired`。Sandbox 当前对无法由受信任 mock
完整替换的任何 Provider、Action、Retrieval 或 Subflow fail closed；请求不能提交 endpoint、credential、
executable 或任意 mock response。Live 要求 `agent.debug.live` 和 `live_confirmation: true`，并继续受
Provider suspension、MCP disable、approval/interaction 与 exact binding fence 约束。普通 `agent.read`
只获得 body-free summary；读取 source/input 和 SSE 至少要求对应 debug capability。

Debug profile 的 content retention 到期后，maintenance transaction 会把 Session 和幂等 receipt 中的
source/input 改成只含 `content_deleted` 的 tombstone，同时保留 immutable ID、source hash、状态、计数和
引用。此后管理 stream 固定返回 `410 Gone`。底层 Run payload/artifact 不再能由 Debug API 读取，并由
既有 bounded Run/artifact retention worker 完成物理回收。

## Provider 生命周期

```text
Provider Entity
  → mutable Draft
  → immutable Discovery/Test/Validation evidence
  → immutable Provider Revision
  → CAS Active Revision
  → Agent Deployment exact binding
```

| 阶段 | API | 语义 |
|---|---|---|
| Template | `GET /v1/admin/provider-templates` | 只读受信任 adapter/template manifest |
| Entity/Draft | `POST/GET /providers`、`GET/DELETE /{id}`、`GET/PUT /draft` | adapter type 创建后不可变；secret 只用 reference |
| Discovery | `POST/GET /discoveries/**`、`GET /model-candidates` | 外部列表是 immutable untrusted evidence，不自动导入 |
| Import | `POST /model-import-previews`、`PUT /draft/models` | “本次全部”立即展开为逐项 ID/fingerprint/policy |
| Evidence | `POST/GET /validations/**`、`POST/GET /connection-tests/**` | validation 无网络；test 只用 adapter 固定 fixture |
| Revision | `POST/GET /revisions`、`GET /revisions/{id}` | publish 冻结 adapter/worker、endpoint、credential ref、models |
| Route | `GET/PUT/DELETE /active-revision` | 只影响新的 Agent resolution，不改历史 Deployment |
| Safety | `POST/DELETE /suspension`、`POST /retirement` | suspension 可恢复并增加 fence；retirement 不可逆 |

Provider Draft 不允许任意 header、secret value、动态 adapter 代码、model wildcard、regex 或
`auto_import`。Capability provenance 只能是 `template_verified`、`adapter_verified`、
`probe_verified` 或 `operator_asserted`，且仍需 adapter/compiler policy 允许。相同 credential reference
下的 secret value 轮换不改变 revision/binding hash；reference、slot 或 credential type 变化必须发布新
Provider Revision。

带 credential 的 Provider 必须先由平台配置在 `management.provider_secret_resolver.allowed_names` 中
允许对应环境变量名，并把 value 只注入服务进程。管理 Draft 使用
`secret://environment/<NAME>`；`providerctl` 只迁移名称，既不需要也不读取 value。旧
`providers.extensions` 不能隐式扩大 managed Provider 的 secret 白名单。

## 组合与安全门

给 Agent 增加第三方 MCP Tool 的完整链为：MCP discovery → 显式 import → MCP Revision activate →
把生成的 Action ID 显式加入 Agent Draft → validation → Definition → resolution → Deployment → Agent
activate。Provider 模型链同理。MCP/Provider active pointer 切换不会改写已有 Deployment；新 resolution
才选择新 active revision。

Agent activate 会在同一事务复验 exact Provider Revision、Provider operational state、MCP Server state
和 Subflow publication heads。普通 Run admission再次读取 durable Agent head；Provider/MCP leaf 在真正
开始外部调用前再次检查 suspension/disable fence。通知与进程 cache只加速收敛，不能替代数据库权威。
PostgreSQL Provider mutation 的 schema trigger 只发送 schema-scoped opaque wake hint，不包含 Provider、
Revision、endpoint、model 或 secret identity。consumer 收到丢失、重复或乱序 hint 时都重新读取完整
durable final state；listener 断开时 generation safety poll 继续收敛并定期重连。SQLite 不发送跨进程
通知，只保留同一 authoritative poll 合同。

## Bootstrap 与 clean-cut migration

运行时不再把 `agents.directory` 或 `providers.extensions` 当作 live authority。它们只能作为显式导入输入；
所有命令默认不激活，只有给出 `--activate` 才移动 public/current pointer。

```bash
# 本地编译 package；dry-run 不读取管理 token，也不上传 Prompt 正文
cargo run --bin agentctl -- import \
  --server http://127.0.0.1:8080 \
  --token-env MANAGEMENT_TOKEN \
  --agent-dir agents/action_demo \
  --dry-run

# 创建 Draft、Validation、Definition、Resolution、Deployment，并显式激活
cargo run --bin agentctl -- import \
  --server http://127.0.0.1:8080 \
  --token-env MANAGEMENT_TOKEN \
  --agent-dir agents/action_demo \
  --activate

# 把旧 platform YAML 中的 Provider extensions 展开为 managed Provider Revision
cargo run --bin providerctl -- import-extensions \
  --server http://127.0.0.1:8080 \
  --token-env MANAGEMENT_TOKEN \
  --platform-config config/platform.yaml \
  --catalog catalog/provider-catalog.yaml \
  --dry-run
```

已有数据库的 publication head 使用离线事务工具接管。先停写并备份数据库；`--dry-run` 会执行全部校验
和临时写入后回滚，正式执行保留原 Definition/Deployment ID、历史 Run foreign key、Canonical Plan、
resolved bindings 和 binding hash，不重新编译历史对象。默认保留当前 active route；`--inactive` 只建立
managed 历史并撤下 route。

```bash
cargo run --bin management-migrate -- adopt-agent-heads \
  --database-url sqlite:///var/lib/insight/history.sqlite3 \
  --dry-run

cargo run --bin management-migrate -- adopt-agent-heads \
  --database-url "$DATABASE_URL"

# Provider Revision 发布后，把 pre-cutover Deployment 中的旧 model_binding_hash
# 映射成 immutable archive evidence；不会重写 resolved_bindings
cargo run --bin management-migrate -- map-provider-history \
  --database-url "$DATABASE_URL" \
  --provider-id imported-company \
  --revision-id prev_... \
  --dry-run
```

失败发生在 immutable publish 前时，API 导入工具会删除刚创建且仍无引用的 Entity；一旦产生 immutable
Revision 就保留证据并返回失败，不做破坏性回滚。离线 migration 是单事务，任一 Agent 无效会使整批回滚；
重复执行只跳过已经接管的 managed head。

## 运维信号

启用 management 后，readiness 会直接探测 Agent/Provider store，并要求 Provider ModelRegistry 最近一次
投影成功且未超过 5 秒。`/metrics` 提供 spec 规定的低基数 management 指标以及
`provider_registry_projection_healthy`；ID、endpoint、secret reference、Prompt hash 和 request ID 均不
进入 label 或指标正文。

## Machine contracts 与验证

- [`agent-management-v1.json`](../../schemas/agent-management-v1.json) / [OpenAPI](../../schemas/agent-management-v1.openapi.json) / [samples](../../schemas/agent-management-v1.samples.json)
- [`provider-management-v1.json`](../../schemas/provider-management-v1.json) / [OpenAPI](../../schemas/provider-management-v1.openapi.json) / [samples](../../schemas/provider-management-v1.samples.json)
- [`mcp-management-v1.json`](../../schemas/mcp-management-v1.json) / [OpenAPI](../../schemas/mcp-management-v1.openapi.json)

核心验证命令：

```bash
cargo test -p insight-api agent_management
cargo test --test agent_management_api
cargo test -p insight-storage --test agent_management
cargo test -p insight-api provider_management
cargo test --test provider_management_api
cargo test -p insight-storage --test provider_management
cargo test --test management_import_tools --test management_migration
cargo test --test binary_smoke binary_starts_and_observes_success_and_workflow_failure_runs
```

设置 `TEST_POSTGRES_URL` 后，storage 合同对 PostgreSQL 16 运行同一状态机；CI 不允许静默缺失该变量。
正式资格结果见
[Agent 与 Provider 管理 Control Plane v1 资格验收](../archive/qualifications/2026-08-01-agent-provider-management-v1-qualification.md)。
