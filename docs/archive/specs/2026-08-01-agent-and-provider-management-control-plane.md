# Agent 与 Provider 管理 Control Plane v1 规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 日期 | 2026-08-01 |
| 平台协议 | `insight.agent/v1`、`agent-management/v1`、`provider-management/v1`、`mcp-management/v1`、`run-stream/v1` |
| 变更类型 | Breaking Management API / Breaking Configuration / Durable Catalog / Agent Authoring / Provider Runtime / Authorization |
| 影响范围 | `insight-dsl`、`insight-durable`、`insight-resources`、`insight-storage`、`insight-runtime`、`insight-api`、根 composition、平台配置、公开 schema、测试与文档 |

> 归档说明：本规范完成定义 1～15 已于 2026-08-02 CST 全部复验通过。当前可执行合同见
> [`docs/current`](../../current/README.md)，正式证据见
> [Agent 与 Provider 管理 Control Plane v1 资格验收](../qualifications/2026-08-01-agent-provider-management-v1-qualification.md)。

## 1. 决策摘要

平台增加两个 installation-scoped 管理控制面，并与已经交付的 MCP 管理面形成统一资源生命周期：

```text
Provider 管理：平台可连接哪些模型服务和模型
MCP 管理：平台可连接哪些第三方 Tool/Resource/Prompt
Agent 管理：某个 Agent 的作者定义，以及它精确绑定哪些 Provider、Action、Retrieval 和 Subflow
Run：固定一个不可变 Deployment Revision 执行
```

本规范采用以下决定：

1. **先交付完整 API，不把 UI 作为完成前置。** UI 以后只作为这些严格 API 的普通客户端；
2. **Agent 与 Provider 的 managed 对象以 durable store 为唯一运行权威。** API 不修改工作区 YAML，
   runtime 不对同一个对象同时读取文件和数据库；
3. **Agent 使用 `Draft -> Validation -> Definition Revision -> Deployment Resolution -> Deployment
   Revision -> Activate`。** Draft 可变；Definition、Resolution、Deployment 和 Run pin 均不可变；
4. **Provider 使用 `Draft -> Discovery/Test -> Validation -> Provider Revision -> Activate`。**
   Provider discovery 只产生模型候选，不自动授权或导入模型；
5. **发布不隐式激活。** Agent Definition 发布、Agent Deployment 创建、Agent route 激活是三个动作；
   Provider Revision 发布和 Provider route 激活也是两个动作；
6. **复用现有 Definition/Deployment Revision 和 Run pinning。** 不另造一套只供管理 UI 使用的执行版本；
7. **现有 Graph 的“语义编辑后立即发布并切换 current”被 clean-cut。** Graph 和 YAML 都在 Agent
   Draft 上编辑，只有显式 publish/deploy/activate 才改变运行路由；
8. **Agent Deployment 固定精确依赖。** Provider Revision、模型 ID、adapter/worker 版本、MCP
   binding、Action、Retrieval 和 Subflow 都进入 resolved bindings；运行时不追随“最新”；
9. **Provider adapter 是受信任代码，Provider route 是数据。** API 只能实例化已经安装的 adapter，
   不能上传 Rust、JavaScript、Python 或任意动态代码；
10. **Secret 只以 reference 进入对象。** Secret value 不进入请求/响应回读、revision hash、日志、错误、
    metric label、trace、audit 或 outbox；
11. **普通停用和紧急关闭是不同权威。** active revision 只决定以后发布绑定什么；Provider suspension、
    Agent archive 和 MCP disable 可以阻止新的 admission 或尚未开始的外部调用；
12. **Agent 调试使用独立 Debug Session。** Draft 调试产生不可公开路由的临时、固定 deployment；
    sandbox 与 live 权限分离，live 调试可能计费或产生副作用；
13. **文件 Agent 和内置 Provider Catalog 变为导入源/模板，不再是动态运行实例。** runtime 启动不再
    因扫描 `agents/*/agent.yaml` 或 `providers.extensions` 自动改写 durable active route；
14. **没有旧管理客户端需要兼容。** `/v1/graph-agents/**` 和静态 Provider extension 可以直接迁移，
    不提供双写、shadow routing 或隐式优先级。

## 2. 当前基线与缺口

### 2.1 Agent 基线

当前平台已经具备：

- `agents/<agent_id>/agent.yaml` 与 prompt 文件的结构化 DSL 编译；
- GraphAuthorDocument、Graph semantic edit、ViewDocument 和 TraceOverlay；
- 不可变 Definition Revision、Deployment Revision、resolved bindings 和 Run pin；
- `agent_publication_heads` 当前路由与 deployment archive；
- `GET /v1/agents`、`GET /v1/agents/{agent_id}` 与普通/Attached/pinned Run admission；
- `/v1/graph-agents/{agent_id}/revisions` 发布 Graph，并把部署安装为 current。

缺口是：

- 文件 Agent 只能通过修改磁盘和重启更新；
- 没有稳定 Agent entity、mutable Draft、Draft ETag、版本列表、归档与恢复；
- Graph semantic edit 直接形成新 published revision，不能保存尚未发布的协作草稿；
- Graph publish 同时完成编译、依赖 linking 和 current route 切换，无法审核、灰度或回滚前预览；
- 没有 Agent Definition Validation 与 Deployment Resolution 的独立 durable evidence；
- 没有受控的 Draft Debug Session；
- MCP API 激活 Tool 后，没有管理 API 把 Action ID 加入某个 Agent Draft；
- 普通 public pinned Run 如果能任意选择历史 deployment，会绕过 active route 的发布治理。

### 2.2 Provider 基线

当前平台随二进制发布只读 `catalog/provider-catalog.yaml`，并在平台 YAML 中接受
`providers.extensions`。启动时构造进程内 `ModelRegistry`，Agent 使用 `{provider, id}` selector，
部署时冻结 endpoint identity、credential environment reference、adapter/worker 版本、模型能力与
Catalog/extension digest。

该边界已经消除了模型别名，但仍缺少管理控制面：

- Provider route、模型和 credential reference 只能改文件并重启；
- 内置 Catalog、deployment extension 与进程内 Registry 共同承担运行权威；
- 没有 Provider Draft、discovery candidate、connection test、validation、revision、active pointer；
- 没有多 runtime 共享的 Provider Revision archive 与动态恢复；
- 没有明确区分“新 deployment 不再选择该 Provider”和“紧急阻止历史 deployment 的后续调用”；
- `/v1/models` 等第三方列表即使存在，也没有 durable candidate、显式导入和能力来源合同；
- 凭据轮换、endpoint 变更和模型能力变更没有独立生命周期。

### 2.3 MCP 已建立的参考边界

MCP 管理 API 已经证明以下模式，本规范复用而不削弱：

- 管理平面和 tenant/user 运行平面隔离；
- Draft、异步 discovery、显式 import、validation、immutable revision、CAS activate；
- 外部 I/O 不在数据库事务和锁内执行；
- secret reference 与 secret value 分离；
- active revision 只服务以后 Agent deployment binding；
- disable 是独立运行时安全门；
- mutation idempotency、body-free audit 与 bounded outbox 原子提交；
- SQLite 与 PostgreSQL 共享同一逻辑状态机。

Agent 和 Provider 管理不能发明更弱的并发、安全或持久化语义。

## 3. 目标与非目标

### 3.1 目标

- 通过严格、分页、可审计的 HTTP API 创建、编辑、校验、发布、部署、激活、回滚和归档 Agent；
- 同时支持 `yaml_package` 与 `graph` 两种 Agent authoring mode；
- 对 Graph 和 YAML 使用同一个 Canonical Plan、Definition Revision、Deployment Revision 和 Run 引擎；
- 通过 API 把显式 MCP Action ID 加入 Agent Draft，并在 Deployment 中冻结精确 MCP binding；
- 提供无公共路由污染的 Draft/Revision Debug Session；
- 通过 HTTP API 管理 Provider route、模型候选、模型显式导入、连接测试、Revision 和 active pointer；
- 保留平台对 adapter、网络、secret resolver、预算和模型治理的硬限制；
- 支持安全的 Provider credential rotation、suspension、resume 和 retirement；
- 保证已发布 Deployment 和活动 Run 不受普通 Catalog 漂移、Draft 编辑或新 Revision 发布影响；
- 对 current route 变化使用 CAS，并支持把 active pointer 指回历史 Deployment/Provider Revision；
- 在多 runtime 部署中以 durable store 为权威，以进程缓存和 outbox/notification 为加速；
- 交付 machine-readable JSON Schema、OpenAPI、错误码、审计、指标、SQLite/PostgreSQL parity tests；
- 提供文件 Agent 与 Provider extension 的一次性离线/API 导入路径。

### 3.2 非目标

- 不实现 Agent/Provider 管理 UI、模型市场、价格目录、评分或插件市场；
- 不实现多人富文本 OT/CRDT；v1 使用完整 Draft PUT、闭合 semantic edit batch 和 CAS；
- 不允许 API 修改 Git 工作区、`agent.yaml`、平台 YAML、进程环境变量或 Kubernetes Secret；
- 不增加 Agent `tools: ["*"]`、MCP `auto_import`、Provider model wildcard 或运行时自动授权；
- 不根据模型 ID、description、Provider 营销元数据自动授予 tool calling、JSON Schema 或图像能力；
- 不允许 Agent Draft 覆盖 Provider endpoint、credential、adapter 或平台网络策略；
- 不实现跨 Provider、跨区域或跨账户自动 failover；这需要独立、可冻结的路由策略规范；
- 不允许 Debug Session 绕过 Action approval、MCP interaction、tenant ownership 或安全关闭；
- 不保证第三方 Provider/MCP 副作用 exactly-once；现有 effect/idempotency 合同保持；
- 不让 published Revision 可变，也不通过“更新版本”接口原地改写历史；
- 不物理删除仍被 Definition、Deployment、Debug Session、Run、Conversation 或审计引用的对象；
- 不让 Provider API 动态安装新的 adapter 代码；adapter 安装属于受信任发布/插件边界。

## 4. 核心术语与不变量

### 4.1 API 平面

| 平面 | 路径 | principal | 权威 |
|---|---|---|---|
| Agent 管理 | `/v1/admin/agents/**` | Operator | Agent entity、Draft、validation、revision、deployment、activation、debug |
| Provider 管理 | `/v1/admin/providers/**` | Operator | Provider Draft、model discovery/import、test、revision、activation、suspension |
| MCP 管理 | `/v1/admin/mcp/**` | Operator | MCP Server Draft、discovery/import、revision、activation、disable |
| 用户运行 | `/v1/agents/**`、`/v1/runs/**`、`/v1/mcp/**` | tenant/user | 当前 active Agent discovery、Run、Context、Interaction |

管理 API 不接受普通 tenant/user token；用户运行 API 不接受 Operator token 作为提权方式。Debug live
所需的运行 principal 只能来自预配置 debug execution profile，不能由 Operator 任意提交 tenant/user header
来冒充。

### 4.2 Agent 版本链

```text
Agent Entity（稳定 agent_id）
  -> Agent Draft（mutable, draft_version + author_hash）
  -> Agent Validation（immutable evidence）
  -> Definition Revision（immutable authored semantics + Canonical Plan）
  -> Deployment Resolution（immutable dependency proposal）
  -> Deployment Revision（immutable exact bindings）
  -> Agent Active Deployment Pointer（mutable, CAS）
  -> Run（immutable deployment pin）
```

Definition Revision 表示作者语义；Deployment Revision 表示可执行绑定。同一个 Definition Revision 可以
因为 Provider、Action、Retrieval、Subflow、worker 或 deployment policy 的精确版本不同，产生多个
Deployment Revision。

### 4.3 Provider 版本链

```text
Provider Entity（稳定 provider_id，同时是 DSL provider route）
  -> Provider Draft（mutable, draft_version + input_hash）
  -> Discovery Snapshot / Connection Test（immutable external evidence）
  -> Provider Validation（immutable evidence）
  -> Provider Revision（immutable connection/model facts）
  -> Provider Active Revision Pointer（mutable, CAS）
  -> Agent Deployment exact model binding
```

Provider active pointer 不在 Run 开始时重新解析。Agent Deployment 创建后，其 `provider_revision_id`、
模型 ID、adapter/worker 版本、capability digest、endpoint identity 和 credential reference identity 都固定。

### 4.4 三种 Hash

必须区分：

| Hash | 输入 | 用途 |
|---|---|---|
| `author_hash` | Agent author package 的规范化文件名、字节、authoring mode，不含 ViewDocument | Draft CAS evidence、精确作者内容 |
| `semantic_hash` | 验证后的 Canonical Plan | 判断执行语义是否等价、Graph trace 对齐 |
| `binding_hash` | Definition + 所有 exact resolved bindings + deployment policy | Deployment identity、Run 恢复 |

Provider 使用：

| Hash | 输入 | 用途 |
|---|---|---|
| `provider_input_hash` | Draft canonical JSON，含 credential reference，不含 secret value | validation/discovery 关联 |
| `provider_revision_hash` | Published Provider document、模型与 capability provenance | exact binding、恢复与审计 |

ViewDocument、debug viewport、标签、最后编辑者、secret value、健康状态和实时延迟不得进入执行 hash。

### 4.5 全局不变量

- 所有客户端输入对象拒绝未知字段、重复 JSON key、非规范 ID、越界字符串和无界集合；
- Draft mutation 成功后 `draft_version` 单调递增；失败、冲突或 exact replay 不制造额外版本；
- Revision 创建后不可更新；重复幂等请求只能返回同一对象或明确冲突；
- publish 不修改 active pointer，deploy 不修改 active pointer；
- active pointer、archive/suspension/retirement 都使用 entity ETag 和事务 CAS；
- Agent Draft 中的 Tool 是显式 Action ID 列表，不保存通配符或“当前全部”；
- Provider Draft 中的模型是显式模型 ID 列表，不保存通配符、动态 selector 或 `auto_import`；
- discovery/test 期间不持有数据库事务、行锁或 advisory lock；
- 外部返回的 model/tool metadata 是 untrusted evidence，不能直接成为平台安全权威；
- 普通 active pointer 切换不改写历史 Deployment 或 Run；
- Provider suspension、MCP disable、Agent archive/retirement 是额外 admission/execution fence；
- Secret value 永不进入可序列化 domain object；
- 所有 current route 的 durable head 是多 runtime 权威，进程内 Registry 只能作为可重建缓存。

## 5. 统一管理认证与平台硬策略

### 5.1 `management.version: 1`

平台增加共享管理配置。Agent、Provider 和 MCP 管理路由使用同一个 Operator principal/capability
框架，避免 `/v1/admin` 下出现互不兼容的认证体系：

```yaml
management:
  version: 1
  enabled: true
  operator_credentials:
    - identity: platform-author
      token_env: INSIGHT_PLATFORM_AUTHOR_TOKEN
      capabilities:
        - agent.read
        - agent.write
        - agent.validate
        - agent.publish
        - agent.deploy
        - agent.activate
        - agent.debug.sandbox
        - provider.read
        - provider.write
        - provider.discover
        - provider.test
        - provider.publish
        - provider.activate
        - mcp.server.read

  limits:
    max_agent_draft_bytes: 4194304
    max_agent_prompt_files: 128
    max_provider_models: 4096
    max_pending_operations: 256
    operation_retention_days: 30

  debug_execution_profiles:
    author-sandbox:
      mode: sandbox
      max_concurrent_sessions: 4
      session_timeout: 10m
      retention: 24h
      allow_external_actions: false
      allow_live_provider_credentials: false
```

生产模式启用任一管理路由时，`management.enabled` 必须为 true 且至少存在一个合法 Operator credential；
`auth.mode: disabled` 不能使管理路由匿名可用。旧的 MCP 专用 Operator credential 配置在实现本规范时
迁入共享 management 配置；不进行双重 token lookup。

### 5.2 Capability 集合

Agent capability 为闭合集合：

```text
agent.read
agent.write
agent.validate
agent.publish
agent.deploy
agent.activate
agent.archive
agent.debug.sandbox
agent.debug.live
```

Provider capability 为闭合集合：

```text
provider.read
provider.write
provider.discover
provider.test
provider.publish
provider.activate
provider.suspend
provider.retire
```

Capability 互不隐含：`write` 不隐含 `publish`，`publish` 不隐含 `activate`，`debug.sandbox` 不隐含
`debug.live`。读取 Debug Session 私密 trace 至少要求对应 debug capability；普通 `agent.read` 只能看到
body-free debug summary。

### 5.3 API 不能放宽的硬策略

平台 YAML 或受信任 binary manifest 继续拥有：

- 允许安装的 Provider adapter 类型和版本；
- DNS、IP range、TLS、redirect、proxy、private network 与 loopback policy；
- Secret resolver、允许的 reference namespace 和 keyring；
- Provider/MCP 请求与响应大小、timeout、并发和 rate budget 上限；
- Debug execution profile、可用 sandbox adapter、tenant/principal 映射和费用上限；
- 全局 model governance allow/deny policy；
- Agent Draft、prompt file、Graph node/edge、validation diagnostic 和 revision retention 上限。

管理对象只能在硬上限内收窄，不能提交 executable、任意 proxy、证书私钥、secret value、动态 adapter
模块或未经批准的 debug principal。

## 6. Agent 管理

### 6.1 Agent Entity

`agent_id` 是稳定、全局唯一的 public route ID。创建请求同时选择不可变 `authoring_mode`：

```text
yaml_package
graph
```

v1 不在同一个 Agent identity 上切换 authoring mode，因为 YAML 注释/文件布局和 Graph node/view identity
之间不存在无损双向转换。需要转换时，客户端读取已发布定义、显式转换并创建新的 Agent；平台可以提供
clone/import helper，但不能静默改写原 Agent。

Entity 包含：

- `agent_id`；
- immutable `authoring_mode`；
- admin-only mutable labels；
- `draft_version`、`entity_version` 与 ETag；
- `active_deployment_revision_id`，可为空；
- `lifecycle: editable | archived`；
- 创建/更新时间与 body-free actor identity。

公开 name、description、input/output schema 和 streaming capability 来自 active Definition/Deployment，
不是 entity labels。修改它们必须形成新 Definition Revision。

### 6.2 Agent 生命周期

| 状态 | active deployment | 新 Draft 编辑 | 普通 Run admission | 允许操作 |
|---|---:|---:|---:|---|
| `editable_inactive` | 无 | 是 | 否 | 编辑、校验、发布、部署、激活、删除条件检查 |
| `editable_active` | 有 | 是 | 是 | 编辑下一版、发布、部署、CAS 切换、归档 |
| `archived` | 保留历史 pointer 证据但不公开 | 否 | 否 | 读取历史、恢复为 inactive |

Archive 是可恢复安全门：原子移除 public active route、阻止新的普通 admission，但不删除历史或中断
已经成功 admission 的 Run。Restore 只恢复为 `editable_inactive`，不能隐式重新激活旧 deployment。

只有从未发布 Definition、从未创建 Deployment、没有 Debug Session/Run/Conversation 引用且没有进行中
operation 的 inactive Agent 才允许物理 DELETE。其他对象只能 archive。

### 6.3 Agent Draft

Agent 同时只有一个 mutable Draft。完整 Draft PUT 使用：

```http
PUT /v1/admin/agents/{agent_id}/draft
If-Match: "draft-12"
X-Request-ID: agent-draft-update-001
```

`yaml_package` Draft 是原子 package，不能逐文件产生半完成版本：

```json
{
  "source": {
    "type": "yaml_package",
    "agent_yaml": "api_version: insight.agent/v1\n...",
    "prompt_files": [
      {"path": "prompts/system.md", "content": "..."}
    ]
  }
}
```

文件 path 必须是规范相对路径，只允许 `/` 分隔，不允许空段、`.`、`..`、反斜线、绝对路径、symlink、
重复 path 或非 UTF-8 内容。总字节数、单文件大小和文件数有平台硬上限。

`graph` Draft 保存 GraphAuthorDocument 的 author semantics，但 draft wire 不允许客户端选择最终
`definition_revision_id`。Revision ID 由服务端在 publish 时生成并注入。Graph node identity 稳定；
ViewDocument 独立存储，不进入 author/semantic hash。

Graph semantic edit 改为 Draft mutation：

```http
POST /v1/admin/agents/{agent_id}/draft/semantic-edits
If-Match: "draft-12"
```

请求包含 `expected_semantic_hash` 和闭合 edit batch。成功只形成 `draft-13`，不发布 Definition、
不创建 Deployment、不修改 active route。View 使用独立 `view-N` ETag；View 冲突不能改变 Draft。

### 6.4 Definition Validation

```http
POST /v1/admin/agents/{agent_id}/validations
GET  /v1/admin/agents/{agent_id}/validations/{validation_id}
```

Validation 是 durable、不可变、无外部副作用的 operation，固定：

- `agent_id`、`draft_version`、`author_hash`；
- DSL/compiler/expression engine/Graph schema 版本；
- Canonical Plan `semantic_hash`；
- 输入输出 schema；
- logical Provider selector、Action ID、Retrieval ID 和 Subflow reference；
- bounded diagnostics 与 risk diagnostics；
- validation policy digest。

Validation 检查作者语义和引用形状，但不把“当前 active dependency”冻结为 Deployment。Provider/MCP
active pointer 在 validation 后变化不会改写报告；真正 exact linking 由 Deployment Resolution 完成。

Validation 状态为 `queued | running | succeeded | failed | cancelled`。即使编译当前是 CPU-local，仍采用
durable operation，以便大 Draft、并发限流、多 runtime claim/lease 和未来外部 schema registry 不改变
API 语义。进程内 fast path 可以同步完成，但响应合同一致。

### 6.5 Definition Revision 发布

```http
POST /v1/admin/agents/{agent_id}/revisions
GET  /v1/admin/agents/{agent_id}/revisions
GET  /v1/admin/agents/{agent_id}/revisions/{definition_revision_id}
```

发布请求必须携带：

```json
{
  "draft_version": 12,
  "validation_id": "agentval_..."
}
```

服务端要求 validation 成功且其 `draft_version + author_hash + policy_digest` 与当前 Draft 完全一致。
发布事务持久化：

- server-minted Definition Revision ID；
- 原子 author package/Graph document；
- Canonical Plan；
- author hash、semantic hash；
- compiler 和 schema 版本；
- public metadata、input/output contract；
- validation evidence reference；
- body-free audit、idempotency receipt 和 outbox hint。

发布不解析 current Provider/MCP revision，不创建 public route，不修改 `agent_publication_heads`。

### 6.6 Deployment Resolution 与 Deployment Revision

Deployment linking 分两步，允许 UI/Operator 审阅精确依赖：

```http
POST /v1/admin/agents/{agent_id}/deployment-resolutions
GET  /v1/admin/agents/{agent_id}/deployment-resolutions/{resolution_id}
POST /v1/admin/agents/{agent_id}/deployments
GET  /v1/admin/agents/{agent_id}/deployments
GET  /v1/admin/agents/{agent_id}/deployments/{deployment_revision_id}
```

Resolution 请求引用一个 Definition Revision。服务端从同一 durable catalog snapshot 解析：

- 每个 `{provider, id}` 的 active Provider Revision、model profile、adapter/worker 与 credential ref；
- 每个 native/MCP Action 的 exact revision、schema、public policy 与 MCP binding hash；
- Retrieval revision；
- Subflow 的 active Deployment Revision；
- persistence policy、stream broker/worker capability 与平台预算；
- dependency publication heads/generations。

Resolution 是 immutable proposal，包含 `catalog_snapshot_hash`、完整 body-safe binding summary、风险和
过期时间。外部网络 I/O 不参与 resolution。Provider connection test 和 MCP discovery 不能藏在部署事务中。

创建 Deployment 时必须引用成功且未过期的 resolution：

```json
{
  "definition_revision_id": "defrev_...",
  "resolution_id": "agentres_..."
}
```

事务重新锁定并校验所有 dependency head/generation、安全状态和 resolution hash。任一变化返回
`AGENT_DEPENDENCY_HEAD_CHANGED`，不产生部分 Deployment。成功后沿用现有 `VersionedPlan` 拆分存储：
`workflow_definition_revisions` 保留 Definition/Canonical Plan，`deployment_revisions` 保存 exact resolved
bindings、worker contracts 和 `binding_hash`；`binding_hash` 决定 Deployment identity。

Deployment 创建仍不修改 public route。历史 Deployment 永久可用于既有 Run 恢复、redrive/fork/migrate
的服务端证明，但普通用户不能任意创建指向 inactive 历史 Deployment 的新 Run。

### 6.7 激活、停用与回滚

```http
GET    /v1/admin/agents/{agent_id}/active-deployment
PUT    /v1/admin/agents/{agent_id}/active-deployment
DELETE /v1/admin/agents/{agent_id}/active-deployment
```

PUT 请求使用 `If-Match: "agent-N"`，引用已存在且当前 admissible 的 Deployment Revision。激活事务：

1. 锁定 Agent entity/current head；
2. 验证 expected entity version；
3. 验证 Deployment 属于该 Agent，且 Provider/MCP/Action/Subflow 没有被安全关闭；
4. 更新 `agent_publication_heads`；
5. 增加 `entity_version`；
6. 原子写 body-free audit、idempotency receipt 和 outbox hint。

回滚就是 PUT 一个历史 Deployment Revision，不创建特殊 rollback revision。DELETE 只移除普通 public
route，使 Agent 变为 inactive；不删除 Draft、Revision 或历史 Run。

普通 `POST /v1/agents/{agent_id}/runs` 始终解析 active deployment。现有公开的
`POST /v1/agents/{agent_id}/deployments/{deployment_revision_id}/runs` clean-cut 删除；历史 Deployment
只允许由服务端 recovery/redrive/fork/migrate 证明链和 admin Debug Session 使用。否则 active route、
回滚和归档都可被普通客户端绕过。

### 6.8 Debug Session

```http
POST   /v1/admin/agents/{agent_id}/debug-sessions
GET    /v1/admin/agents/{agent_id}/debug-sessions
GET    /v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}
GET    /v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}/stream
DELETE /v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}
```

Debug source 为闭合选择：

```json
{
  "source": {"type": "draft", "draft_version": 12},
  "execution_profile_id": "author-sandbox",
  "input": {"query": "test"}
}
```

也可引用 Definition 或 Deployment Revision。Draft Debug 执行以下步骤：

1. 固定 exact draft version/hash；
2. 执行与 publish 相同的编译验证；
3. 解析 exact dependency snapshot；
4. 安装 `publication_origin=debug` 的临时 Definition/Deployment；
5. 创建 `visibility=admin_debug` 的 Run；
6. 永不写入 `agent_publication_heads`，也不出现在 `/v1/agents`；
7. 按 debug profile TTL、容量和内容 retention 回收未被引用的数据。

Debug Session 状态为 `queued | running | succeeded | failed | cancelled | expired`。SSE 复用
`run-stream/v1` envelope，但 endpoint、认证、cache policy 和 visibility 独立。

`sandbox` profile 必须把所有外部 leaf 解析到预配置 sandbox adapter/mock，或明确拒绝无法 sandbox 的节点；
请求不能自己提交 mock executable、endpoint、credential 或任意 response body。`live` profile 使用真实 exact
bindings，至少要求 `agent.debug.live`，并在创建前返回可审阅的费用/副作用风险。Action approval、MCP
interaction 和 Provider suspension 继续生效。

用户 OAuth MCP connection 不能被 Operator token 借用。需要 user-scoped MCP 的 live debug 必须使用
平台预配置、可审计的 debug principal flow；没有合法 principal 时 fail closed。

Debug trace 可以在 admin-only retention 窗口保存 prompt、tool arguments 和模型输出的受控投影，但：

- 不进入普通 Run/Conversation API；
- 不进入日志、metric label、body-free audit 或 outbox；
- secret、authorization header、OAuth token 和 Provider raw frame 始终不可见；
- privacy delete 和 retention 必须覆盖 debug content。

### 6.9 Agent API 总表

| 方法 | 路径 | capability | 用途 |
|---|---|---|---|
| `POST/GET` | `/v1/admin/agents` | `agent.write/read` | 创建与分页列表 |
| `GET/DELETE` | `/v1/admin/agents/{id}` | `agent.read/write` | 读取；仅删除从未发布对象 |
| `GET/PUT` | `/v1/admin/agents/{id}/draft` | `agent.read/write` | 读取或 CAS 替换 Draft |
| `POST` | `/v1/admin/agents/{id}/draft/semantic-edits` | `agent.write` | Graph Draft 闭合编辑事务 |
| `GET/PUT` | `/v1/admin/agents/{id}/draft/view` | `agent.read/write` | 非语义 ViewDocument |
| `POST/GET` | `/v1/admin/agents/{id}/validations/**` | `agent.validate/read` | durable Definition Validation |
| `POST/GET` | `/v1/admin/agents/{id}/revisions/**` | `agent.publish/read` | immutable Definition Revision |
| `POST/GET` | `/v1/admin/agents/{id}/deployment-resolutions/**` | `agent.deploy/read` | exact dependency proposal |
| `POST/GET` | `/v1/admin/agents/{id}/deployments/**` | `agent.deploy/read` | immutable Deployment Revision |
| `GET/PUT/DELETE` | `/v1/admin/agents/{id}/active-deployment` | `agent.read/activate` | CAS 激活、回滚或停用 |
| `POST` | `/v1/admin/agents/{id}/archive` | `agent.archive` | 移除 public route 并关闭编辑 |
| `POST` | `/v1/admin/agents/{id}/restore` | `agent.archive` | 恢复为 inactive editable |
| `POST/GET/DELETE` | `/v1/admin/agents/{id}/debug-sessions/**` | `agent.debug.*` | 创建、观察、取消调试 |

所有 list 使用稳定 cursor、`limit` 上限和确定性排序；不能返回全量 Revision、diagnostic 或 debug trace。

## 7. Provider 管理

### 7.1 Provider Template、Adapter 与 Provider Entity

必须区分：

| 概念 | 所有者 | 是否可由 API 创建 | 是否直接可执行 |
|---|---|---:|---:|
| Provider Adapter | 受信任 binary/plugin | 否 | 是，供 Revision 实例化 |
| Provider Template | 随平台版本发布的只读 manifest | 否，只能读取/引用 | 否 |
| Provider Entity/Route | durable management store | 是 | active revision 后可供 Agent deploy |
| Provider Revision | durable immutable document | 由 publish 产生 | 是，且可被 exact binding |

当前 `catalog/provider-catalog.yaml` 改为 template catalog。模板可以提供 adapter、canonical endpoint、默认
credential reference name、已验证模型事实，但不能仅因 binary 启动就自动成为 live Provider route。

`provider_id` 同时是 Agent DSL 中 `model.provider` 的稳定 route ID。Entity 包含 Draft/version、active
revision pointer、operational state 和 admin labels。Provider adapter 类型在创建后不可变；从
`open_ai_compatible` 转为原生 Anthropic 等必须创建新 Provider route，避免同一 ID 的协议语义漂移。

### 7.2 Provider Draft

```http
PUT /v1/admin/providers/{provider_id}/draft
If-Match: "draft-4"
```

自定义 OpenAI-compatible Draft 示例：

```json
{
  "adapter": {"type": "open_ai_compatible"},
  "endpoint": "https://llm.company.internal/v1",
  "credential": {
    "type": "bearer",
    "reference": "secret://production/company-llm"
  },
  "transport": {
    "tls": "required",
    "redirects": "deny",
    "connect_timeout_ms": 5000,
    "request_timeout_ms": 120000
  },
  "models": [
    {
      "id": "vendor/chat-v1",
      "input": ["text"],
      "capabilities": ["complete", "streaming"],
      "provenance": {"type": "operator_asserted"}
    }
  ]
}
```

Endpoint canonicalization、credential reference 解析、transport 收窄和 model capability schema 都必须由
adapter + platform policy 验证。Draft 不保存 secret value，不允许任意 header；需要鉴权/组织 header 时，
使用 adapter 定义的闭合 credential slots，每个 slot 只接收 secret reference。

### 7.3 Model Discovery 与显式导入

Provider discovery 是 adapter-specific durable operation：

```http
POST /v1/admin/providers/{provider_id}/discoveries
GET  /v1/admin/providers/{provider_id}/discoveries/{discovery_id}
GET  /v1/admin/providers/{provider_id}/discoveries/{discovery_id}/model-candidates
POST /v1/admin/providers/{provider_id}/model-import-previews
PUT  /v1/admin/providers/{provider_id}/draft/models
```

OpenAI-compatible adapter 可以调用 `/v1/models`；其他 adapter 可以使用自己的 metadata API；不支持可靠
列表的 adapter 必须明确返回 `discovery_not_supported`，Operator 仍可手工逐项声明模型。

必要链条为：

```text
Provider remote model list
  -> immutable Discovery Snapshot
  -> untrusted Model Candidate
  -> Operator 显式 Model Import + capability provenance
  -> Provider Validation
  -> immutable Provider Revision
  -> activate
  -> Agent Definition 显式 {provider, id}
  -> Agent Deployment exact Provider binding
```

第三方通常只能可靠返回 model ID，不能自动证明 tools、image、structured output、context window、reasoning
参数或 streaming 完成语义。每个 capability 必须记录来源：

```text
template_verified
adapter_verified
probe_verified
operator_asserted
```

安全或协议关键能力不能仅靠 Provider 自报。平台只能在 adapter 明确支持 lowering/validation 时把能力
暴露给 Agent compiler。

允许“选择本次 discovery 全部模型”的 preview 操作，但结果必须立即展开为逐项 model ID、candidate
fingerprint 和 capability policy。Revision/Draft 中不存在 `*`、regex、动态远端列表或 `auto_import`。

### 7.4 Validation 与 Connection Test

```http
POST /v1/admin/providers/{provider_id}/validations
GET  /v1/admin/providers/{provider_id}/validations/{validation_id}

POST /v1/admin/providers/{provider_id}/connection-tests
GET  /v1/admin/providers/{provider_id}/connection-tests/{test_id}
```

Validation 不发网络请求，检查：

- adapter 已安装且版本被允许；
- endpoint、TLS、DNS/IP、redirect、proxy 和 timeout policy；
- credential reference 语法、namespace 和所需 slot，不读取/返回 value；
- 模型 ID 唯一、有界、原样保持；
- input modality 与 capability 组合合法；
- capability provenance 满足 adapter 要求；
- response/request size 和并发限制不超过平台 hard limit；
- model governance policy 允许该 Provider/model。

Connection Test 是有外部 I/O 的 durable operation，模式为闭合集合：

| 模式 | 行为 | 风险 |
|---|---|---|
| `metadata` | 认证并访问 health/models 等无生成接口 | 通常不计生成费用 |
| `canary` | 对指定已导入模型发送平台固定的最小请求 | 可能计费 |
| `capability_probe` | 使用 adapter 固定用例验证 streaming/tool/image 等 | 可能计费并产生 Provider 日志 |

请求不能提交任意 prompt、tool schema 或 Provider raw payload。Probe fixture 由受信任 adapter 定义；响应只
持久化 body-free outcome、stable failure taxonomy、timing、byte counts 和验证事实，不保存 Provider 正文。

### 7.5 Provider Revision

```http
POST /v1/admin/providers/{provider_id}/revisions
GET  /v1/admin/providers/{provider_id}/revisions
GET  /v1/admin/providers/{provider_id}/revisions/{provider_revision_id}
```

发布请求引用当前 Draft version 和成功 Validation；如果 adapter policy 要求近期 connection test，还必须
引用尚未过期且 input hash 相同的 test。Published document 冻结：

- provider ID、adapter ID/version、worker version；
- canonical endpoint identity；
- credential type、slot 与 reference identity；
- 非秘密 transport policy、timeouts、limits；
- 显式模型列表、modalities、capabilities 与 provenance；
- template/adapter manifest digest；
- validation/test/discovery evidence reference；
- provider revision hash。

Secret value、实时 health、rate-limit remaining 和 test response body 不进入 Revision。

### 7.6 Active Revision、Suspension 与 Retirement

```http
GET    /v1/admin/providers/{provider_id}/active-revision
PUT    /v1/admin/providers/{provider_id}/active-revision
DELETE /v1/admin/providers/{provider_id}/active-revision

POST   /v1/admin/providers/{provider_id}/suspension
DELETE /v1/admin/providers/{provider_id}/suspension
POST   /v1/admin/providers/{provider_id}/retirement
```

Provider 同时具有两个正交状态：

```text
publication route: active_revision_id | none
operational state: enabled | suspended | retired
```

- active revision 只决定新的 Agent Deployment Resolution 选择什么；
- DELETE active revision 阻止新的 binding，但不影响已固定 Deployment 的可恢复性；
- suspension 是紧急运行门：阻止新的 Run admission 和尚未开始的 Provider call；
- 已经在网络中的调用收到 best-effort cancellation，最终结果仍按现有 effect/terminal authority 提交；
- resume 不改变 active revision；
- retirement 是不可逆终态，清除 active route 并永久阻止新调用，但保留历史恢复证据。

Provider active 切换不会自动更新 Agent Deployment。要升级 Agent：创建新的 Deployment Resolution 和
Deployment Revision，再显式激活 Agent。这样 Provider rollout 和 Agent rollout 可以独立审核、灰度和回滚。

### 7.7 Credential Rotation

Revision 固定的是 credential reference identity，不是 secret value。Secret resolver 每次按安全缓存策略
读取当前 value，因此：

- 相同 reference 的 secret value 轮换不要求重发 Provider/Agent Revision；
- credential type、slot 或 reference 路径变化必须形成新 Provider Revision；
- secret resolver 可以提供 body-free version/updated evidence 用于审计，但 secret version 不进入
  Deployment hash；
- credential missing、empty、denied 和 resolver unavailable 使用稳定、脱敏错误码；
- suspension 可用于泄漏事件的立即阻断，轮换完成后显式 resume。

### 7.8 Provider API 总表

| 方法 | 路径 | capability | 用途 |
|---|---|---|---|
| `GET` | `/v1/admin/provider-templates` | `provider.read` | 读取只读 adapter/template manifest |
| `POST/GET` | `/v1/admin/providers` | `provider.write/read` | 创建与分页列表 |
| `GET/DELETE` | `/v1/admin/providers/{id}` | `provider.read/write` | 读取；仅删除从未发布对象 |
| `GET/PUT` | `/v1/admin/providers/{id}/draft` | `provider.read/write` | Draft CAS |
| `POST/GET` | `/v1/admin/providers/{id}/discoveries/**` | `provider.discover/read` | durable model discovery |
| `POST` | `/v1/admin/providers/{id}/model-import-previews` | `provider.write` | 展开显式模型选择 |
| `PUT` | `/v1/admin/providers/{id}/draft/models` | `provider.write` | 原子替换显式模型列表 |
| `POST/GET` | `/v1/admin/providers/{id}/validations/**` | `provider.publish/read` | 无网络 Validation |
| `POST/GET` | `/v1/admin/providers/{id}/connection-tests/**` | `provider.test/read` | metadata/canary/probe |
| `POST/GET` | `/v1/admin/providers/{id}/revisions/**` | `provider.publish/read` | immutable Provider Revision |
| `GET/PUT/DELETE` | `/v1/admin/providers/{id}/active-revision` | `provider.read/activate` | CAS 激活、回滚、停用 |
| `POST/DELETE` | `/v1/admin/providers/{id}/suspension` | `provider.suspend` | 紧急关闭与恢复 |
| `POST` | `/v1/admin/providers/{id}/retirement` | `provider.retire` | 不可逆退役 |

## 8. Agent、Provider 与 MCP 的组合语义

### 8.1 给 Agent 增加 MCP Tool

```text
MCP discovery -> 显式 import -> MCP Revision -> activate
  -> 更新 Agent Draft.tools，加入生成后的 Action ID
  -> Agent Validation -> Definition Revision
  -> Deployment Resolution 固定 server_revision_id + tool_binding_hash
  -> Deployment Revision -> Agent activate
```

MCP activation 不修改 Agent Draft；Agent Draft 更新也不能激活未发布 MCP Tool。任一环节缺失，Agent 都
不能调用该 Tool。

### 8.2 给 Agent 更换模型或 Provider

```text
Provider Draft -> 模型显式导入 -> Provider Revision -> activate
  -> 更新 Agent Draft 的 {provider, id}
  -> Agent Validation -> Definition Revision
  -> Deployment Resolution 固定 Provider Revision/model/adapter
  -> Deployment Revision -> Agent activate
```

如果只升级 Provider endpoint/credential reference/model capability，而 Agent authored selector 不变，
可以复用原 Definition Revision，直接创建新的 Deployment Resolution/Deployment Revision。

### 8.3 Catalog 变化矩阵

| 变化 | Agent Draft | Definition Revision | 已有 Deployment | 已有 Run | 新 Deployment |
|---|---|---|---|---|---|
| MCP remote `tools/listChanged` | 不变 | 不变 | 不变 | 不变 | 继续绑定旧 active revision，直到 MCP 新 revision 激活 |
| MCP active revision 切换 | 不变 | 不变 | 不变 | 不变 | 解析新 active MCP revision |
| Provider active revision 切换 | 不变 | 不变 | 不变 | 不变 | 解析新 active Provider revision |
| Provider secret value 轮换，同 ref | 不变 | 不变 | identity 不变 | 后续调用使用新 value | identity 不变 |
| Agent Draft 编辑 | 改变 | 不变 | 不变 | 不变 | 必须先发布新 Definition |
| Agent active deployment 切换 | 不变 | 不变 | 历史保留 | 已 admission 不变 | 普通新 Run 使用新 active |
| Provider suspension | 不变 | 不变 | 不改写但 inadmissible | 未开始调用被 fence | resolution/activation 失败 |
| Agent archive | 关闭编辑 | 不变 | 历史保留 | 已 admission 不变 | 普通 admission 失败 |

### 8.4 不使用跨资源分布式事务

Provider/MCP publish 与 Agent deploy 是独立事务。Deployment Resolution 记录依赖 head，最终 Deployment
创建在一个本地数据库事务中重新验证所有 head/generation。依赖在两步之间变化时返回 conflict，客户端
重新创建 resolution；不能通过长事务、数据库锁包住网络 discovery 或跨 API 两阶段锁定全部资源。

## 9. Durable Repository 与 Runtime 投影

### 9.1 复用现有执行表

以下现有权威继续使用：

- `workflow_definition_revisions`：immutable author document、Canonical Plan 与 Definition metadata；
- `deployment_revisions`：immutable resolved bindings、worker contracts 与 binding hash；
- `agent_publication_heads`：只表示当前 active public Agent deployment；
- Run、checkpoint、effect、activation 与 recovery 表：继续固定 Deployment Revision；
- Graph View 表：保存非语义 ViewDocument；
- MCP management/revision 表：继续保存 exact MCP binding authority。

当前 `publish_versioned_plan` 的“安装 Definition/Deployment + 更新 publication head”必须拆成三个 durable
command；现有 `install_versioned_plan` 可以作为前两个 command 的内部组合 helper，但不能再作为唯一发布
边界：

```text
install_definition_revision     # immutable Definition insert only
install_deployment_revision     # immutable Deployment insert only
activate_agent_deployment       # CAS update current head only
```

Graph/YAML Definition publish 只调用第一个 command；Deployment create 只调用第二个；activate/rollback/
deactivate 只通过第三个 command 改变 current head。

### 9.2 新增 Agent 管理表

逻辑表至少包括：

```text
managed_agents
agent_drafts
agent_validations
agent_deployment_resolutions
agent_debug_sessions
agent_debug_content_retention
agent_management_idempotency_receipts
agent_management_audit
agent_management_outbox
```

Definition/Deployment 正文不复制到另一套管理 revision 表；管理对象通过外键引用现有 immutable workflow
revision。Agent Draft 保存 author package，Revision 保存发布时快照；View 与 trace 分离。

### 9.3 新增 Provider 管理表

逻辑表至少包括：

```text
managed_providers
provider_drafts
provider_discoveries
provider_model_candidates
provider_connection_tests
provider_validations
provider_revisions
provider_revision_models
provider_management_idempotency_receipts
provider_management_audit
provider_management_outbox
```

Candidate、test 和 validation 使用 immutable operation ID。成功 Revision 引用的 evidence 不受普通 operation
retention 删除；失败/cancelled 且未引用 operation 可按配置清理。

### 9.4 事务边界

每个成功 mutation 在同一数据库事务中提交：

1. 状态变化或 immutable insert；
2. entity/draft version；
3. idempotency receipt；
4. body-free audit；
5. bounded outbox/invalidation hint。

响应序列化失败不能回滚已提交 mutation。客户端对 transport failure/5xx 使用同一 `X-Request-ID` 重试；
exact replay 返回原结果，request ID 与不同 body hash 组合返回 idempotency conflict。

### 9.5 多 Runtime Registry

新增 `ProviderRevisionRuntime`，与 MCP revision runtime 相同地：

- 启动时从 durable active Provider Revision 重建 current `ModelRegistry`；
- 同时恢复被现有 Agent Deployment/Run 引用的 archived Provider Revision；
- 通过 body-free outbox/notification 加速 active/suspension 变化；
- 通知丢失时用 generation poll 修复；
- readiness 必须证明 durable heads、adapter availability 和本地投影一致；
- 任一 runtime 缺少 exact adapter/worker version 时不得声明 ready。

Agent current discovery 每次以 durable `agent_publication_heads` 为权威；进程 catalog 可加速 exact restore，
不能覆盖 durable route。

## 10. HTTP、并发与错误合同

### 10.1 通用请求合同

- 所有管理响应使用 `Cache-Control: private, no-store` 和 `X-Content-Type-Options: nosniff`；
- 所有 mutation 要求稳定 `X-Request-ID`；
- Draft/entity/active pointer mutation 要求强 ETag `If-Match`；
- create 使用 `If-None-Match: *` 或 server-side uniqueness + idempotency；
- JSON 只接受 UTF-8、单一顶层对象、无重复 key、无 trailing document；
- YAML 只作为 Agent package 内字符串，由现有 strict YAML decoder 解析；
- list 使用 opaque cursor，排序键至少包含 `created_at + stable_id`；
- operation create 返回 `202 Accepted`，immutable Revision/Deployment 创建返回 `201 Created`；
- CAS conflict 返回 `409` 或 `412`，实现必须在 OpenAPI 中统一；本规范选择 ETag mismatch 为 `412`，
  domain head changed 为 `409`；
- 错误正文不回显 Provider response、Prompt、tool arguments、secret reference value 或 arbitrary upstream text。

### 10.2 ETag

```text
Agent Draft:       "draft-N"
Agent Entity:      "agent-N"
Agent View:        "view-N"
Provider Draft:    "draft-N"
Provider Entity:   "provider-N"
```

ETag 只来自 durable monotonic version，不使用时间戳。读取响应与 mutation 结果必须同时返回 DTO version 和
HTTP ETag，二者不一致视为 repository corruption/readiness failure。

### 10.3 主要错误码

Agent：

```text
AGENT_NOT_FOUND
AGENT_ALREADY_EXISTS
AGENT_DRAFT_CONFLICT
AGENT_DRAFT_INVALID
AGENT_AUTHORING_MODE_CONFLICT
AGENT_VALIDATION_STALE
AGENT_VALIDATION_FAILED
AGENT_REVISION_NOT_FOUND
AGENT_DEPENDENCY_NOT_FOUND
AGENT_DEPENDENCY_HEAD_CHANGED
AGENT_DEPLOYMENT_INVALID
AGENT_ACTIVATION_CONFLICT
AGENT_ARCHIVED
AGENT_DEBUG_PROFILE_DENIED
AGENT_DEBUG_LIVE_CONFIRMATION_REQUIRED
```

Provider：

```text
PROVIDER_NOT_FOUND
PROVIDER_ALREADY_EXISTS
PROVIDER_DRAFT_CONFLICT
PROVIDER_DRAFT_INVALID
PROVIDER_ADAPTER_NOT_AVAILABLE
PROVIDER_CREDENTIAL_REFERENCE_INVALID
PROVIDER_DISCOVERY_NOT_SUPPORTED
PROVIDER_DISCOVERY_STALE
PROVIDER_MODEL_IMPORT_INVALID
PROVIDER_VALIDATION_STALE
PROVIDER_VALIDATION_FAILED
PROVIDER_TEST_FAILED
PROVIDER_REVISION_NOT_FOUND
PROVIDER_ACTIVATION_CONFLICT
PROVIDER_SUSPENDED
PROVIDER_RETIRED
```

Upstream failure映射继续使用现有稳定 taxonomy，例如 authentication、permission、connection、timeout、
rate-limited、unavailable、response-invalid 和 response-too-large；错误 message 固定且 body-free。

## 11. 安全、隐私与审计

### 11.1 SSRF 与网络

Provider endpoint 创建、discovery、test 和实际调用使用同一或更严格的 DNS/IP/TLS/redirect policy：

- production 默认只允许 HTTPS；
- DNS 解析后及每次 redirect 都复验目标 IP；
- private/link-local/metadata/loopback 默认拒绝；
- development loopback 例外必须显式且不能带 production secret；
- endpoint identity 去除 userinfo、query 和 fragment；
- 日志不能记录完整 URL query、authorization header 或 response header；
- connection test 不能成为任意 URL fetch primitive。

### 11.2 Agent 内容

Agent Draft、Prompt、Graph literal、Debug input/output 和 tool arguments 都是私密正文：

- 不进入 INFO/WARN/ERROR 日志；
- 不进入 metric label、audit、outbox 或 readiness detail；
- API 只对具备对应 capability 的 Operator 返回；
- 列表默认只返回 hash、字节数、状态、时间和 bounded diagnostic summary；
- Definition Revision 读取正文需要 `agent.read`，Debug 正文需要 debug capability；
- retention 删除正文时保留 immutable identity、hash、状态、计数和引用完整性。

### 11.3 审计

Audit 至少记录：

- actor identity 和 capability；
- request ID；
- object type/ID；
- operation；
- before/after version 或 revision ID；
- result、stable failure code；
- timestamp；
- body/content/hash 之外的安全计数。

Audit 不保存 Draft body、Prompt、Provider model response、secret ref resolved value、debug input/output、MCP
interaction body 或 arbitrary error detail。需要关联内容时只保存 cryptographic hash 和内部 object ID。

## 12. 配置与 API Clean Cut

### 12.1 Agent 文件权威迁移

目标运行配置不再使用 `agents.directory + agents.enabled` 作为 live route authority。仓库中的
`agents/*/agent.yaml` 继续作为：

- 示例和测试 fixture；
- `agentctl import` 或管理 API package import 的输入；
- release/quickstart bootstrap artifact。

服务进程不监视、不修改这些文件，也不在每次启动用文件覆盖 managed Draft/active route。迁移工具读取
文件、调用管理 API、输出创建的 Definition/Deployment ID，并要求显式 activate；它不是 runtime path。

### 12.2 Provider 配置迁移

目标配置删除 live `providers.extensions`。平台 YAML 只保留 adapter allowlist、Provider hard network
policy、secret resolver、management worker/limit 与 model governance。`catalog/provider-catalog.yaml` 成为
只读 template manifest，不直接注册 live model。

一次性迁移工具把当前内置/extension route 展开为：

```text
Provider create -> Draft -> optional validation/test -> Revision -> activate
```

Secret 仍只迁移 reference name，不读取或传输 secret value。

### 12.3 Graph API 迁移

以下接口 clean-cut：

```text
POST /v1/graph-agents/{agent_id}/revisions
GET  /v1/graph-agents/{agent_id}/revisions/{definition_revision_id}
POST /v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/semantic-edits
GET/PUT .../view
```

替换为 `/v1/admin/agents/**` 下的 Draft、semantic edits、revision 和 view API。旧路径不做转发，因为转发
无法保留“编辑不发布、发布不部署、部署不激活”的新语义。

### 12.4 数据迁移

升级工具必须：

1. 备份并验证现有 workflow revision/publication head；
2. 把当前 built-in/file Agent head 建立为 managed Agent entity 的历史 Definition/Deployment；
3. 保持原 Deployment Revision ID 与 Run foreign key 不变；
4. 把当前 route 显式写为 active deployment，或按 operator 选择保持 inactive；
5. 把 Provider extension 转为 Provider Revision，并让已有 Deployment resolved binding 指向可恢复的
   imported provider revision evidence；
6. 不重新编译历史 Canonical Plan，不用当前 Catalog 替换旧 resolved bindings；
7. 支持 dry-run、body-free report 和事务性失败回滚；
8. 在所有 runtime 升级到支持新 schema/adapter archive 前禁止 mixed-version 激活。

## 13. 实施阶段

### Phase 0：机器合同与共享 authority

- 冻结 `agent-management/v1`、`provider-management/v1` JSON Schema/OpenAPI；
- 增加 shared Operator auth/capability、request ID、ETag、pagination、audit/outbox domain types；
- 定义 SQLite/PostgreSQL migrations、repository ports 和 transaction outcomes；
- 将 existing `publish_versioned_plan` 拆为 Definition install、Deployment install 与 activate；
- 增加 fault injection 和 secret/body leak scanner。

### Phase 1：Provider durable management

- Provider Entity/Draft/discovery/import/validation/test/revision/activate/suspension；
- Provider template catalog 与 installed adapter registry；
- `ProviderRevisionRuntime` current/archive 恢复；
- Agent deployment resolver 支持 exact durable Provider Revision；
- 多 runtime notification/poll/readiness；
- 静态 Provider extension migration tool。

### Phase 2：Agent authoring management

- Agent Entity、YAML package Draft、Graph Draft、View 与 semantic edit CAS；
- durable validation、Definition publish、revision list/read；
- clean-cut `/v1/graph-agents`；
- file Agent import/migration；
- current public Agent discovery 只读取 durable active head。

### Phase 3：Deployment 与 activation

- Deployment Resolution 和 exact dependency head CAS；
- immutable Deployment create 与 active pointer update 分离；
- rollback、deactivate、archive/restore；
- 限制普通历史 pinned admission；
- Provider/MCP safety fence 在 admission 与 leaf start 双重校验。

### Phase 4：Debug Session

- sandbox/live profile；
- temporary debug origin Definition/Deployment；
- admin-only run-stream、trace、cancel、TTL/retention；
- cost/effect risk confirmation 和 capability；
- user-scoped MCP fail-closed principal flow。

### Phase 5：资格、运维与文档

- SQLite/PostgreSQL parity、multi-process、restart/recovery、race、leak tests；
- Quickstart/bootstrap、Helm、operations、API、architecture、DSL 文档更新；
- 删除静态 live Provider/Agent runtime 路径和旧 Graph API；
- 形成 qualification 文档后才把本规范归档。

每个 Phase 可以独立合并内部实现，但 public management API 只能在其完整状态机、OpenAPI、auth、audit、
SQLite/PostgreSQL parity 和故障测试一起完成后声明可用。

## 14. 验证矩阵

### 14.1 Agent

- YAML package 路径逃逸、重复文件、超限、无效 UTF-8、未知字段全部 fail closed；
- Graph complete PUT、semantic edit、View 分别使用正确 ETag，View 不改变 semantic hash；
- 两个并发 Draft writer 只有一个 CAS 成功；exact retry 不重复增加版本；
- stale validation 不能发布；failed validation 不创建 Definition；
- publish 只安装 Definition，不改变 public route；
- dependency head 在 resolution 后变化，Deployment create 原子冲突且无孤儿部分写；
- deploy 不激活；activate CAS 第一胜者；回滚指向历史 Deployment；
- Provider/MCP active 切换不改变已有 Deployment hash；
- Agent archive 后普通 admission 失败，已 admission Run 继续按原 pin；
- 普通用户不能选择 inactive 历史 Deployment 创建新 Run；
- Debug Draft 固定 exact version，永不出现在 `/v1/agents` 或 public publication head；
- sandbox 无法覆盖的外部 leaf fail closed；live debug 缺少 capability/confirmation fail closed；
- Debug cancellation、TTL、privacy delete 和 content retention 保持引用完整性。

### 14.2 Provider

- endpoint scheme/DNS/IP/redirect/TLS matrix 与 actual call 使用同一 policy；
- invalid/unknown adapter、credential slot、secret reference、模型 ID、capability 组合 fail closed；
- discovery 外部 I/O 时数据库无长事务/锁；lease 到期可恢复且只有一个成功 snapshot；
- `/models` candidate 不自动进入 Draft；preview all 展开为逐项列表；
- duplicate/renamed model、candidate fingerprint mismatch 和 stale discovery 被拒绝；
- validation 不访问网络；metadata/canary/probe 使用 adapter 固定 fixture；
- Provider response body/header 不进入 error、audit、outbox、trace 或日志；
- publish 不激活；activate CAS 第一胜者；active 切换不改已有 Agent Deployment；
- DELETE active 只阻止新 binding；suspension 同时阻止新 admission 和尚未开始的 call；
- secret value 同 reference 轮换不改变 revision/binding hash；reference 变化必须新 revision；
- retirement 不可逆，历史 Revision 仍可读取但不可执行。

### 14.3 Repository 与多 Runtime

- SQLite 与 PostgreSQL 对每个 command 返回相同 transition outcome、ETag、错误和 idempotency 语义；
- mutation、receipt、audit、outbox 在故障注入下全有或全无；
- response serialization/connection drop 后相同 request ID replay 原结果；
- notification 丢失、重复、乱序时 generation poll 收敛；
- runtime restart 能恢复 active Provider、archived exact revision 和 Agent current/archive；
- 缺少 exact adapter/worker version 的 runtime readiness 失败；
- mixed-version schema 或 catalog template digest 不一致不能参与 production admission；
- 所有分页稳定、无重复/遗漏、上限正确；
- 通过全仓 secret、Prompt、Provider body、tool argument leak scanner。

## 15. 可观测性与运维

新增低基数、body-free 指标：

```text
agent_management_operations_total{operation,outcome}
agent_drafts_current
agent_validations_pending
agent_deployment_resolutions_pending
agent_activations_total{outcome}
agent_debug_sessions{state,profile_mode}
provider_management_operations_total{operation,outcome}
provider_discoveries_pending
provider_connection_tests_total{mode,outcome}
provider_activations_total{outcome}
provider_operational_state{state}
provider_registry_projection_lag_seconds
```

禁止把 `agent_id`、`provider_id`、model ID、endpoint、secret ref、Prompt hash、request ID 或 debug session ID
作为无界 metric label。详细对象关联只在受控日志中使用 stable internal ID，仍不得记录正文。

Readiness 至少检查：

- repository schema 与 management worker lease；
- durable active Agent/Provider head 可读取；
- 本 runtime 已安装 exact adapter/worker version；
- Provider/MCP safety fence projection 不落后于允许阈值；
- debug profile 引用的 sandbox adapter 存在；
- production 管理认证和 secret resolver 已配置。

## 16. 完成定义

本规范只有同时满足以下条件才可标记 Implemented / Verified：

1. Agent 与 Provider 全部 API 有严格 JSON Schema 和 OpenAPI，并通过 positive/negative conformance；
2. 管理 auth、capability、ETag、request ID、pagination、idempotency、audit 和 outbox 完整交付；
3. Agent Draft/Validation/Definition/Resolution/Deployment/Activate/Archive 状态机在 SQLite/PostgreSQL 等价；
4. Provider Draft/Discovery/Import/Test/Validation/Revision/Activate/Suspension 状态机在两种数据库等价；
5. Graph semantic edit 不再隐式发布，publish/deploy 不再隐式激活；
6. Provider active 切换和 MCP active 切换不改写已有 Deployment/Run；
7. Agent Deployment 冻结 exact Provider/MCP/Action/Retrieval/Subflow/worker evidence；
8. 普通用户不能绕过 Agent active route 运行任意历史 Deployment；
9. Provider suspension、MCP disable 和 Agent archive 的 admission/leaf-start fence 有 race tests；
10. Debug sandbox/live 权限、临时 deployment、public isolation、TTL 和 privacy tests 完整；
11. Secret value、Prompt、Provider body、tool arguments 不出现在非授权响应、日志、错误、audit、outbox、
    metrics 或 public stream；
12. 多 runtime notification 丢失、restart、archive restore 和 exact adapter recovery 测试通过；
13. 文件 Agent、Graph API 和 Provider extension clean-cut migration 有 dry-run、rollback 与历史 Run 保真测试；
14. Quickstart、生产样例、Helm、operations、architecture、DSL、API 与 migration 文档同步；
15. 形成独立 qualification 报告，记录测试命令、平台、数据库版本、并发/race/故障证据与剩余限制。

在这些条件完成前，文件 YAML、静态 Provider Catalog 和现有 Graph 发布行为仍是当前事实；不能仅凭本文
宣称管理 API 已可用。
