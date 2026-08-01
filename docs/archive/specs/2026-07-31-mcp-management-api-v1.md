# MCP 管理 API v1 与显式导入规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 日期 | 2026-07-31 |
| 目标协议 | MCP `2026-07-28`，独立兼容 profile 为 `2025-11-25` |
| 平台协议 | `insight.agent/v1`、`run-stream/v1`、`mcp-management/v1` |
| 变更类型 | Breaking Configuration / Management Control Plane / Durable Catalog / Authorization / Agent Publication |
| 影响范围 | `insight-mcp`、`insight-storage`、`insight-runtime`、`insight-api`、Agent 编译与发布、平台配置、数据库 schema、公开 schema、测试与文档 |

> 本文是已完成实现的历史设计与验收边界。当前可执行行为以
> [MCP 使用、运行与安全合同](../../current/mcp.md)、
> [MCP 管理 API v1 资格验收](../qualifications/2026-07-31-mcp-management-api-v1-qualification.md)、
> 公开 schema、实现和 conformance tests 为准。

## 1. 决策摘要

平台先提供完整的 MCP 管理 API，再考虑 UI。管理 API 负责注册第三方 MCP Server、执行可信边界内的
discovery、审阅并显式导入 Tool/Resource/Prompt、发布不可变 MCP Server Revision，以及控制哪个
revision 可供新的 Agent Deployment Revision 绑定。

本规范采用以下决定：

1. **不增加持久化的 `tools: ["*"]`、`auto_import: true`、正则 Tool 导入或运行时自动授权。**
   上游 `tools/list` 只产生候选项，不产生执行权限；
2. **允许“选择本次 discovery 的全部 Tool”这一管理操作，但结果必须立即展开为逐项、可审阅、
   可哈希的显式导入清单。**“全部”不是保存到 Draft 或 Revision 的策略；
3. **管理 API 的 durable store 是 MCP Client Server 定义的唯一权威。**目标配置 clean-cut 到
   `mcp.version: 2`，删除 `mcp.client.servers`，不让 YAML 与数据库双写同一 Server；
4. **Draft 可变，Discovery Snapshot 与 Published Revision 不可变。**发布和激活分离；激活使用 CAS，
   只影响以后发布的 Agent Deployment Revision；
5. **Agent 仍逐项列出 Action ID。**Agent 发布时把每个 MCP Action 解析为精确的
   `server_revision_id + tool_binding_hash`，不能在 Run 开始时追随“最新”；
6. **管理面与用户运行面分离。**`/v1/admin/mcp/**` 只接受 Operator principal；现有
   `/v1/mcp/**` 继续服务 tenant/user principal，不能借其修改 Server 定义；
7. **Secret 只以引用进入管理对象。**v1 不提供读取或上传 secret value 的 API；secret value、OAuth
   token 与凭据版本不进入 revision hash、响应、日志、错误或审计正文；
8. **Discovery 是 durable 异步操作。**远程网络或 stdio I/O 不在数据库事务或行锁内执行；结果以
   不可变快照提交，并可被后续审阅和发布精确引用；
9. **关闭是独立安全权威。**禁用 Server 会阻止新的 Run admission 和尚未开始的 MCP 调用；不能仅靠
   固定 revision 绕过紧急关闭。

这套边界让未来 UI 可以安全地实现“新增连接 → 测试/发现 → 选择工具 → 配置策略 → 校验 → 发布”，
而不必让 UI 直接编辑 `platform.yaml`，也不必把第三方 Server 临时返回的 Catalog 当成权限配置。

## 2. 当前基线与缺口

当前平台已经完整支持 MCP Host/Client、MCP Server、Resources、Prompts、Completion、Subscriptions、
Elicitation、Tasks、OAuth 和标准传输，并具备以下安全基础：

- `mcp.client.servers` 在 YAML 中声明 Server、transport、discovery authority、authorization、imports
  和 limits；
- Tool import 逐项声明远端名称、模型可见 alias、effect、idempotency、cancellation、approval、
  input-required、Tasks、capability 与 public policy；
- 启动或发布期调用 MCP `server/discover` 以及适用的 `tools/list`、`resources/list`、`prompts/list`，
  冻结 Catalog fingerprint 与 binding；
- `GET /v1/mcp/servers/{server_id}/tools` 返回已经发布、当前 principal 可见的显式导入 Catalog，
  不是远端原始候选列表；
- `tools/listChanged` 只使候选 Catalog 失效，不修改已经发布的 Deployment Revision 或活动 Run；
- Agent 的 `tools` 字段列出生成后的 Action ID，例如
  `mcp.engineering.tool_engineering_search_repositories`。

目前缺少的是控制面，而不是 MCP wire 能力：

- 增删改 Server 必须修改文件并重启，无法由后续 UI 调用；
- discovery 候选、拒绝原因和 schema 差异没有独立、持久化、可分页的管理对象；
- 没有 Draft、校验报告、发布 revision、激活指针、CAS 与管理审计合同；
- Action Registry 主要由启动配置构建，尚不能安全承载持久化 MCP Revision；
- 若直接增加通配符，会把远端 Catalog 漂移升级为未经审阅的模型执行权限，扩大 prompt injection、
  数据外传、破坏性副作用和供应链风险。

因此本规范不会在当前 YAML 上补一个 CRUD 外壳，也不会先加自动导入。它建立单一权威、不可变发布和
精确绑定，再把 UI 作为该 API 的普通客户端。

## 3. 目标与非目标

### 3.1 目标

- 通过稳定、分页、严格 schema 的 HTTP API 管理第三方 MCP Client Server；
- 支持 Streamable HTTP 与 stdio，且不把任意网络访问或本地进程执行权限交给请求正文；
- 持久化每次 discovery 的协议、capability、Catalog、拒绝项、来源与 fingerprint；
- 让 Operator 明确选择 Tool，并逐项设置平台安全权威；
- 对 Resource/Prompt 提供与现有显式 allowlist、定义期快照和 user-invocation 规则一致的管理能力；
- 生成不可变、可哈希、可回滚、可审计的 MCP Server Revision；
- 在 Agent 发布期固定 MCP binding，并保持活动 Run 不受 Catalog 漂移影响；
- 支持安全的禁用、恢复激活、退役、secret 轮换和 `list_changed` 重新发现流程；
- SQLite 单进程开发和 PostgreSQL 16 生产共享同一逻辑状态机与事务结果；
- 为无 UI 的第一阶段提供完整 API、schema、示例、错误、测试和运维合同。

### 3.2 非目标

- 本规范不实现 UI、MCP 市场、Registry 搜索、安装推荐或第三方 Server 评分；
- 不自动信任 Provider 自报的 annotations、description、title、effect 或 destructive hints；
- 不根据 Tool 名称、description 或 JSON Schema 自动授予低风险策略；
- 不允许 Agent 或普通 tenant/user principal 注册 endpoint、stdio process 或 credential；
- 不增加 Agent `tools: ["*"]`、Server 级动态工具集合或运行期 alias 解析；
- 不通过管理 API 编辑 `platform.yaml`、写进程环境变量或返回 secret value；
- 不在 v1 托管 OAuth client secret、bearer token 或通用 Secret Manager UI；
- 不让 discovery 成功等价于 publish、activate、Agent 可见或 public-call 可见；
- 不改变 MCP Server profile 的 exports 管理；平台对外暴露 MCP 的配置仍属于独立边界；
- 不承诺第三方副作用 exactly-once；现有 at-least-once 边界保持不变。

## 4. 核心术语与不变量

### 4.1 两个 API 平面

| 平面 | 路径 | principal | 权威 |
|---|---|---|---|
| 管理控制面 | `/v1/admin/mcp/**` | Operator | Server Draft、discovery、import、revision、激活与关闭 |
| 用户运行面 | `/v1/mcp/**` | tenant/user | 已发布 Catalog、OAuth connection、Resource/Prompt 使用、interaction 与 Run |

两个平面可以读取相同 Published Revision 的不同投影，但不能共享写权限。管理响应不因为某个用户是否
完成 OAuth 而变化；用户运行面不能读取 service-account secret ref、stdio profile 参数或未发布候选。

### 4.2 上游列表不是执行权限

第三方 Tool 进入模型执行面的必要链条为：

```text
MCP tools/list 候选
  -> immutable Discovery Snapshot
  -> Operator 显式 Tool Import
  -> 校验通过
  -> immutable MCP Server Revision
  -> 激活
  -> Agent 显式列出生成 Action ID
  -> Agent Deployment Revision 固定 binding
  -> Run admission
```

任一环节缺失都不得调用该 Tool。Resources 与 Prompts 采用相同原则，但其 allowlist、内容快照和调用
方式仍遵守现有 MCP 合同。

### 4.3 生命周期

一个稳定 `server_id` 具有以下管理状态：

| 状态 | 含义 | 允许操作 |
|---|---|---|
| `draft` | 尚无 active revision | 修改 Draft、discovery、校验、发布、删除 |
| `active` | 指向一个可供新 Agent 发布绑定的 revision | 修改下一版 Draft、discovery、发布、CAS 切换、禁用、退役 |
| `disabled` | 保留历史和 active pointer，但安全门关闭 | 修改 Draft、discovery、发布、CAS 恢复或切换、退役 |
| `retired` | 终态，不再接受激活或新发布 | 只读历史和审计投影 |

`retired` 不物理删除被 Revision、Deployment 或 Run 引用的数据。只有从未发布且没有 discovery 运行中、
没有任何外键引用的 `draft` Server 可以通过 DELETE 物理删除。

Server 同时持有单个 mutable Draft。每次成功修改 Draft 都使 `draft_version` 单调递增并更新 ETag。
Discovery Snapshot 与 Server Revision 创建后不可改；修改需求必须形成新对象。

### 4.4 关键不变量

- 数据库中不存在 Tool 通配符、自动导入开关或“当前远端全部工具”语义；
- Published Revision 的 Tool alias、生成 Action ID 和 remote name 均唯一且排序稳定；
- discovery 记录发起时的精确 `draft_version`，并固定只覆盖连接、协议与发现 authority 的
  `discovery_input_hash`；publish 只能引用成功、非 stale 且该 hash 与当前 Draft 匹配的 snapshot；
- Provider 自报元数据只可作为 untrusted display/evidence，不能覆盖 Operator policy；
- 不在持有数据库锁或事务时发起 DNS、HTTP、OAuth metadata、stdio 或 MCP 请求；
- 任意 active pointer 变化都使用 `If-Match`，并记录前后 revision 与 server version；
- 任何已发布 Agent Deployment Revision 不追随 active pointer；
- disable/retire fence 是运行期额外校验，优先于历史 revision 的可执行性；
- secret value 永不进入 API DTO、canonical JSON、fingerprint、revision hash、日志、trace、metric label、
  error detail 或 audit payload。

## 5. 单一配置权威与 clean cut

### 5.1 `mcp.version: 2`

目标配置把 Client Server 实例从 YAML 移到 durable store。YAML 只保留全局协议、硬安全策略、
secret resolver、stdio launch profile、管理 worker 上限以及 MCP Server exports：

```yaml
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
    legacy_fallback: []

  client:
    enabled: true
    management_api:
      enabled: true
      discovery_workers: 4
      max_pending_discoveries: 128
      operator_credentials:
        - identity: mcp-platform-operator
          token_env: INSIGHT_MCP_OPERATOR_TOKEN
          capabilities:
            - mcp.server.read
            - mcp.server.write
            - mcp.server.discover
            - mcp.server.publish
    secret_encryption:
      active_key_version: v1
      keyring_env: INSIGHT_MCP_SECRET_KEYRING
    secret_resolver:
      type: environment_reference
      allowed_names: [ENGINEERING_MCP_TOKEN, REPOSITORY_MCP_TOKEN]
    signed_manifest_trust:
      max_validity: 24h
      trusted_signers:
        - key_id: platform-authoring-v1
          algorithm: ed25519
          public_key_file: /etc/insight-agent/mcp-authoring-v1.pub
    network_policy:
      allow_loopback_development: false
      allow_private_networks: false
      allow_redirects: false
    stdio_launch_profiles:
      repository-readonly:
        executable: /opt/mcp/bin/repository-server
        fixed_args: ["--mode", "readonly"]
        working_directory: /srv/mcp/repository
        allowed_parameters: []
        secret_environment:
          REPOSITORY_TOKEN: REPOSITORY_MCP_TOKEN
        isolation:
          profile: repository-readonly
    default_limits:
      connect_timeout: 5s
      request_timeout: 2m
      max_request_bytes: 1048576
      max_response_bytes: 16777216
      max_sse_line_bytes: 65536
      max_sse_event_bytes: 1048576
      max_content_items: 128
      max_catalog_items: 4096

  server:
    enabled: false
    endpoint: /mcp
    authorization:
      type: disabled
    exports: {agents: [], actions: [], resources: [], prompts: []}
```

所有对象继续拒绝未知字段。实现阶段必须为上例中的安全策略定义闭合 schema 和生产默认值；不能接受
无界 timeout、body、Catalog 或 worker 数。

`mcp.version: 2` 不接受 `mcp.client.servers`。`mcp.version: 1` binary/config 组合不与 v2 混跑，
不实现运行时双读、shadow write 或同名优先级。项目当前没有需要维持的旧管理客户端，因此这是
clean cut。

若实施时需要帮助开发环境搬迁，可以提供一次性、离线的“读取 v1 YAML、输出 v2 管理 API 请求”工具；
该工具不是服务启动路径，不写数据库，不构成兼容合同，也不是本规范的完成门槛。

### 5.2 平台策略与管理对象的边界

Operator 可以选择 Server、endpoint、协议 profile、credential reference、import 与 limit，但不能通过
API 放宽平台硬策略：

- HTTP endpoint 必须通过全局 scheme、DNS、IP range、redirect 与 TLS policy；
- stdio 只能引用 YAML 中已批准的 `stdio_launch_profile_id`，不能提交 executable、working directory、
  原始 argv、environment key 或 isolation 配置；
- per-Server limit 只能小于等于全局 hard limit；
- legacy protocol 只有在全局 `legacy_fallback` 允许时才能选择；
- secret ref 必须能被配置的 resolver 识别，管理 API 不能创建、读取或枚举 secret value；
- 生产启用 management API 时必须启用 Operator authentication，`auth.mode: disabled` 必须启动失败。

## 6. Durable 资源模型

### 6.1 `McpManagedServer`

稳定身份对象至少包含：

```json
{
  "server_id": "engineering",
  "display_name": "Engineering MCP",
  "state": "active",
  "server_version": 7,
  "draft_version": 4,
  "active_revision_id": "mrev_...",
  "created_at": "...",
  "updated_at": "..."
}
```

`server_id` 使用小写字母、数字和连字符，长度 1～64，创建后不可改。重命名必须创建新 Server，因为
`server_id` 进入 Action ID、binding 和 revision identity。`display_name` 是受限展示字段，不进入 Action ID。

`server_version` 覆盖 state、active pointer 和可变元数据；`draft_version` 只覆盖 Draft 内容。API 分别
返回彼此独立、用于强比较的 ETag，例如 `"server-7"` 和 `"draft-4"`，客户端不得自行构造版本。

### 6.2 `McpServerDraft`

Draft 是一个完整替换对象，不支持 JSON Merge Patch。它至少包含：

- transport：`streamable_http` endpoint，或 `stdio` 的预批准 launch profile 与受限参数；
- discovery authority：`live_service_account` 或 `signed_manifest`；
- runtime authorization：`none`、`bearer_secret_ref` 或 `oauth_user`；
- protocol policy 与 legacy opt-in；
- Tool imports、Resource allow policy、Prompt imports；
- per-Server limits；
- Draft description 与 Operator note；note 不进入运行时 revision。

所有字段闭合且有长度、数量、格式与总 body 上限。Draft 可以暂时不完整，因此 PUT 只做结构和硬安全
校验；语义完整性由 validation/publish 检查。每次 PUT 都要求 `If-Match: "draft-N"`。

系统另外计算 `discovery_input_hash`，只覆盖会改变 discovery 结果或 authority 的字段：transport、
discovery mode/manifest hash、authorization kind 与 discovery credential ref、protocol、Catalog/transport
limits，以及相关全局 policy fingerprint。修改 imports、display name 或 Operator note 会递增
`draft_version`，但只要这些 discovery 输入未变，已有 snapshot 仍可用于审阅和发布；修改任一 discovery
输入则使旧 snapshot 对新发布 stale。否则“先 discovery、再保存 imports”会形成无法发布的循环。

### 6.3 `McpDiscoverySnapshot`

Discovery 是 durable operation，状态闭合为：

```text
pending -> running -> succeeded
                   -> failed
pending/running -> cancelled
```

成功对象不可变，至少冻结：

- `discovery_id`、`server_id`、来源 `draft_version` 与 `discovery_input_hash`；
- 请求的 protocol profile、协商结果、Server info 和 capability matrix；
- transport/discovery authority 的非 secret identity；
- `tools/list`、`resources/list`、`prompts/list` 的完整分页结果；
- 每项 canonical schema/content metadata hash、排序键、byte count 与解析结论；
- 因 schema、名称、重复项、大小、capability 或策略拒绝的条目及稳定 reason code；
- `catalog_fingerprint`、开始/完成时间、受限诊断摘要；
- 是否已被 `list_changed`、Draft discovery input 修改或相关平台 policy 变化标记为 stale。

`stale` 是成功快照的派生标志，不改写快照正文和 fingerprint。stale snapshot 仍可审计，但不能用于新
publish。

失败对象只能保存阶段、稳定错误码、重试性、远端 request correlation hash 和安全摘要；不得保存
authorization header、stderr 原文、完整远端 body 或 secret-bearing URL。

### 6.4 `McpSignedManifest`

`signed_manifest` discovery 引用独立上传、不可变的 Manifest 对象，而不是 API Server 本地路径。
v1 manifest envelope 使用闭合的 `jcs-ed25519-v1` 格式，包含 base64url payload、`key_id` 和 signature；
payload 必须包含 `server_id`、protocol、discover result、完整分页 Catalog、生成/过期时间和 canonical
content hash。信任根与最大有效期只来自平台 YAML。

上传时验证 body/decoded byte 上限、JCS、signature、key ID、Server ID 和时间窗；创建 discovery 时
再次验证 trust policy 和 expiry。Manifest、decoded canonical payload 与 hash 创建后不可改，API 读取
只返回安全摘要；候选 Catalog 必须通过对应 Discovery Snapshot endpoint 查看。Manifest 不包含 access
token、Authorization header、用户正文或 secret ref 解析值。

### 6.5 `McpToolImport`

每个 Tool import 绑定某个 discovery candidate，至少包含：

```json
{
  "remote": "search_repositories",
  "candidate_schema_hash": "sha256:...",
  "as": "engineering_search_repositories",
  "title": "Search repositories",
  "description": {"mode": "disabled"},
  "effect": "read_only",
  "idempotency": "idempotent",
  "cancellation": "cooperative",
  "required_capabilities": [],
  "approval": "never",
  "input_required": "allowed",
  "tasks": "allowed",
  "terminal_only_compatible": false,
  "public": {"call": false}
}
```

枚举和值域与当前 MCP Tool import 合同一致。`candidate_schema_hash` 防止同名 Tool 的 input/output
schema 在审阅和保存之间被替换。Provider annotations 可以显示为 evidence，但不能代替这些字段。

系统提供保守的预览默认值：

```text
description = disabled
effect = mutating
idempotency = unknown
cancellation = not_supported
approval = always
input_required = denied
tasks = denied
terminal_only_compatible = false
public.call = false
required_capabilities = []
```

Operator 必须主动修改才能放宽。预览默认值不能根据远端 annotation 自动降为 `read_only`、
`idempotent` 或 `approval: never`。

### 6.6 Resource 与 Prompt import

Resource/Prompt 管理保持现有运行合同：

- Resource allow rule 必须显式列出 canonical URI、受限 URI template/pattern、MIME 和 size policy；
- Prompt import 必须列出 remote name、user-invocation、定义期参数与内容冻结方式；
- `oauth_user` 不具备发布期读取 Prompt 的 authority；需要 Definition Prompt snapshot 时只能使用
  signed manifest 或独立 service-account discovery authority；
- Resource/Prompt 候选和内容始终 untrusted，不能修改 system/tenant/tool/public policy；
- 发布时把规则展开/规范化后排序、去重并进入 Revision hash。

Resource URI pattern 是资源命名空间规则，不等于 Tool 自动导入。本规范禁止的 wildcard 特指模型可
调用 Tool 的动态授权。

### 6.7 `McpServerRevision`

Published Revision 不可变，至少包含：

- `revision_id`、`server_id`、单调 revision number；
- 来源 Draft version/hash、Discovery ID、Catalog fingerprint 与 validation report hash；
- transport、protocol、capability、authorization kind 与 secret reference identity；
- 完整展开、排序、去重的 Tool/Resource/Prompt binding；
- 每个 Tool 的生成 Action ID、模型可见 alias、input/output schema hash 与安全 policy；
- limits、全局安全 policy fingerprint、stdio launch profile fingerprint；
- canonical revision hash、创建 Operator、request ID 与时间。

Revision hash 使用 JCS canonical JSON 和 SHA-256。以下内容必须进入 hash：所有影响可连接目标、协议、
执行能力、schema、模型可见内容、安全策略、公开投影和资源选择的字段。以下内容不得进入 hash：
时间戳、display-only note、secret value、OAuth token、secret provider 当前版本、远端 request ID 与
未采纳候选。

secret reference 的稳定标识进入 hash，但 secret value/当前版本不进入，因此正常凭据轮换不需要重新
发布 Revision。endpoint、authorization kind 或 secret reference 本身变化必须发布新 Revision。

### 6.8 Idempotency 与审计记录

所有 mutation 使用非空、有长度上限的 `X-Request-ID`。Repository 对
`operator_principal + method + canonical_path + request_id` 建立唯一约束：

- 相同 request fingerprint 返回原结果，不重复创建对象或执行状态迁移；
- 相同 request ID 携带不同 fingerprint 返回 `409 IDEMPOTENCY_KEY_REUSED`；
- 失败于 durable commit 之前的 discovery worker 尝试可以由 lease/fence 重试，不重复提交 snapshot；
- HTTP 响应生成失败后，客户端必须使用相同 request ID 重试。

审计记录覆盖创建、Draft 修改/删除、discovery 请求/终态、import 修改、validation、publish、activate、
disable、reenable、retire 和拒绝事件。审计 payload 只保存稳定 ID、版本、hash、结果码和 Operator
identity；不保存 HTTP/MCP body、description、schema 正文、secret ref 的解析值或远端 stderr。

## 7. Discovery 合同

### 7.1 创建与执行

`POST /v1/admin/mcp/servers/{server_id}/discoveries` 只持久化 operation intent 并返回
`202 Accepted`。worker 通过 claim、lease 与 fence 执行：

1. 在短事务中读取并固定 Draft version、`discovery_input_hash`、全局 policy fingerprint 和 operation；
2. 事务外解析 secret ref、解析 DNS、建立 transport、执行 MCP `server/discover`；
3. 按协商 capability 分页调用 `tools/list`、`resources/list` 与 `prompts/list`；
4. 在内存/受限临时空间完成 schema 校验、canonicalization、排序、去重和 fingerprint；
5. 在短事务中验证 fence，原子提交成功 snapshot 或安全失败摘要与审计事件。

`tools/list` 是平台作为 MCP Host 向第三方 Server 发送的 MCP JSON-RPC operation，不是平台提供给
Operator 的 REST 接口。Operator 读取的是 Discovery Snapshot 下的候选投影。

单个 discovery 必须受 connect/request/overall timeout、最大分页次数、最大 Catalog items、响应 body、
SSE line/event、content items、schema 深度与总 canonical bytes 限制。重复 cursor、循环 pagination、
重复名称冲突、非法 JSON Schema 或超限必须稳定失败或逐项拒绝，不能无限拉取。

### 7.2 Discovery authority

- `live_service_account` 可以使用 `none` 或 `bearer_secret_ref`；凭据必须具有只读 discovery 所需的
  最小 scope；
- `oauth_user` runtime authorization 不能借用任意终端用户 token 做发布；它必须配套
  `signed_manifest`，或配置独立的 service-account discovery credential ref；
- signed manifest 必须验证签名、issuer、audience、有效期、Server identity、protocol 与 Catalog
  fingerprint，并保存验证证据 hash；
- discovery 成功不证明 Tool 无害，只证明 wire/schema/capability 在该时点可解析。

### 7.3 `list_changed`

订阅收到 `tools/listChanged`、resource update 或 prompt list change 时：

- 把对应 Server 最近成功 snapshot 标为 stale，并写有界 event/outbox；
- 不自动启动无限 discovery storm；worker 按 debounce、backoff 和每 Server 单飞限制调度；
- 可以按平台配置自动创建新的候选 snapshot，但不能修改 Draft imports、Published Revision、Agent
  Deployment Revision 或 Run；
- 管理 API 必须显示“存在更新候选”，由 Operator 重新审阅和发布。

## 8. 显式“全选”而不是自动导入

管理 API 提供 Tool import preview，以便 CLI/UI 快速处理大型 Catalog：

```json
{
  "discovery_id": "mdisc_...",
  "selection": {"mode": "all"},
  "alias_prefix": "engineering_"
}
```

也可使用 `{"mode":"names","names":[...]}`。服务端只对这一个不可变 discovery 做展开，返回逐项
`remote + candidate_schema_hash + alias + conservative policy`，并给出冲突、拒绝项和 preview hash。

该 endpoint 是纯预览：

- 不修改 Draft，不发布，不激活，不授予 Agent 权限；
- 响应中不存在 `*`，每个 Tool 都逐项出现；
- 超过响应上限时拒绝，不以分页结果的“当前页全部”冒充 Catalog 全部；
- alias 冲突、非法名称、被 discovery 拒绝的候选必须显式报告；
- 客户端审阅后，把完整显式数组 PUT 到 Draft；PUT 再次校验 discovery ID、schema hash、排序和唯一性。

Published Revision 永远只保存展开结果。以后远端增加 Tool，即使名称匹配 prefix，也不会进入旧 Draft、
Revision、Agent Deployment 或 Run。

## 9. Management HTTP API

### 9.1 通用合同

- Base path 固定为 `/v1/admin/mcp`；媒体类型为 `application/json`；对象拒绝未知字段；
- mutation 要求 `X-Request-ID`；修改已有对象还要求精确 `If-Match`；
- 成功读取返回 ETag；缺少前置条件返回 `428 PRECONDITION_REQUIRED`，版本不匹配返回
  `412 PRECONDITION_FAILED`；
- list 使用 `limit` 与 opaque `cursor`，默认 50、最大 200；排序稳定，不接受任意 sort 表达式；
- 时间使用 UTC RFC 3339，ID 是 opaque string，客户端不能从 ID 推断时间或分片；
- 所有响应 `Cache-Control: private, no-store`；secret-bearing 请求从设计上不存在；
- `HEAD`、`PATCH`、bulk delete 和任意 filter expression 不属于 v1；
- 错误使用平台统一闭合 envelope，`code` 稳定，message 安全且不回显远端 body。

### 9.2 Endpoint 列表

#### Server 与 Draft

| 方法 | 路径 | 语义 |
|---|---|---|
| `POST` | `/v1/admin/mcp/servers` | 创建稳定 Server identity 和空/初始 Draft |
| `GET` | `/v1/admin/mcp/servers?state=&limit=&cursor=` | 分页列出管理摘要 |
| `GET` | `/v1/admin/mcp/servers/{server_id}` | 读取 Server、Draft 摘要、active revision 与 stale 状态 |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/draft` | 使用 `If-Match` 完整替换 Draft |
| `GET` | `/v1/admin/mcp/servers/{server_id}/draft` | 读取 Draft 和 `draft-N` ETag |
| `DELETE` | `/v1/admin/mcp/servers/{server_id}` | 仅删除从未发布、无引用的 draft Server |

Create body 示例：

```json
{
  "server_id": "engineering",
  "display_name": "Engineering MCP",
  "draft": {
    "transport": {
      "type": "streamable_http",
      "endpoint": "https://mcp.example.com/mcp"
    },
    "discovery": {"type": "live_service_account"},
    "authorization": {
      "type": "bearer_secret_ref",
      "secret_ref": "ENGINEERING_MCP_TOKEN"
    },
    "protocol": {"preferred": "2026-07-28", "legacy_fallback": []},
    "imports": {"tools": [], "resources": {"allow": []}, "prompts": []},
    "limits": {}
  }
}
```

服务端补齐的是公开、版本化默认值，不得补齐安全权限。响应必须回显规范化后的完整 Draft。

#### Discovery 与候选 Catalog

| 方法 | 路径 | 语义 |
|---|---|---|
| `POST` | `/v1/admin/mcp/servers/{server_id}/manifests` | 上传并验证不可变 signed manifest |
| `GET` | `/v1/admin/mcp/servers/{server_id}/manifests?limit=&cursor=` | 列出 Manifest 安全摘要 |
| `GET` | `/v1/admin/mcp/servers/{server_id}/manifests/{manifest_id}` | 读取签名、hash、signer 与有效期摘要 |
| `POST` | `/v1/admin/mcp/servers/{server_id}/discoveries` | 对精确 Draft ETag 创建异步 discovery |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries?status=&limit=&cursor=` | 列出 discovery 摘要 |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}` | 读取状态、capability、fingerprint 与安全诊断 |
| `DELETE` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}` | 请求取消 pending/running operation |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/tools` | 分页读取原始候选的安全投影 |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/resources` | 分页读取 Resource 候选 |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/prompts` | 分页读取 Prompt 候选 |

Manifest upload body 为
`{"format":"jcs-ed25519-v1","payload":"...","key_id":"...","signature":"..."}`；成功返回
201 与 immutable `manifest_id`，不回显 decoded Catalog。Draft 的 `signed_manifest` discovery 配置只
保存该 ID 和 manifest content hash。

创建 discovery 要求 `If-Match: "draft-N"` 和 `X-Request-ID`，body 为闭合空对象 `{}`。返回 202、
operation location 和初始状态。取消为幂等 best-effort：已终态返回原终态；running worker 必须在下一
安全检查点观察 cancel fence。

候选 Tool 响应可以包含远端名称、title、description、安全裁剪后的 annotations、input/output schema、
schema hash、byte count、是否可导入及拒绝码。它不能包含 authorization、远端原始 headers、未受限
binary content 或 Server instructions 的特权解释。

#### Import 与校验

| 方法 | 路径 | 语义 |
|---|---|---|
| `POST` | `/v1/admin/mcp/servers/{server_id}/tool-import-previews` | 对 discovery 的 `all`/`names` 选择生成保守显式清单，不写状态 |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/draft/imports/tools` | 以完整显式数组替换 Tool imports |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/draft/imports/resources` | 完整替换 Resource allow rules |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/draft/imports/prompts` | 完整替换 Prompt imports |
| `POST` | `/v1/admin/mcp/servers/{server_id}/validations` | 对 Draft + discovery 生成不可变 validation report |
| `GET` | `/v1/admin/mcp/servers/{server_id}/validations/{validation_id}` | 读取排序后的 errors/warnings 与 report hash |

三个 imports PUT 都要求 Draft ETag 和 `X-Request-ID`，成功后共同递增 Draft version。Tool body 形如：

```json
{
  "discovery_id": "mdisc_...",
  "items": [
    {
      "remote": "search_repositories",
      "candidate_schema_hash": "sha256:...",
      "as": "engineering_search_repositories",
      "description": {"mode": "disabled"},
      "effect": "read_only",
      "idempotency": "idempotent",
      "cancellation": "cooperative",
      "required_capabilities": [],
      "approval": "never",
      "input_required": "allowed",
      "tasks": "allowed",
      "terminal_only_compatible": false,
      "public": {"call": false}
    }
  ]
}
```

不接受 `*`、`all`、pattern、regex 或省略 `items` 后隐式导入。服务端规范化、按 remote 排序并拒绝
重复，不依赖客户端数组顺序。

Validation 是本地、无远端网络的只读计算；它引用 `discovery_id` 和精确 Draft ETag，返回 201 与
不可变 report。至少检查 transport/policy、discovery freshness、协议/capability、candidate schema
hash、alias/Action ID、全部安全枚举、secret ref 可解析性但不读取/回显 secret value、terminal-only、
Tasks/InputRequired、Prompt snapshot authority、Resource pattern、public policy 和全局硬上限。

#### Publish、激活、禁用与退役

| 方法 | 路径 | 语义 |
|---|---|---|
| `POST` | `/v1/admin/mcp/servers/{server_id}/revisions` | 从精确 Draft、discovery、validation 发布不可变 Revision |
| `GET` | `/v1/admin/mcp/servers/{server_id}/revisions?limit=&cursor=` | 列出 Revision 摘要 |
| `GET` | `/v1/admin/mcp/servers/{server_id}/revisions/{revision_id}` | 读取不可变安全投影 |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/active-revision` | CAS 激活指定 Revision，或从 disabled 恢复 |
| `DELETE` | `/v1/admin/mcp/servers/{server_id}/active-revision` | 立即禁用 Server，保留 active pointer |
| `POST` | `/v1/admin/mcp/servers/{server_id}/retirement` | CAS 进入不可逆 retired 终态 |

Publish body：

```json
{
  "draft_version": 4,
  "discovery_id": "mdisc_...",
  "validation_id": "mval_..."
}
```

Publish 要求当前 Draft ETag。它不隐式激活；成功返回 `201 Created`、revision URI、revision hash。
若相同 request ID 重试则返回原 Revision。Publish 事务内必须重做所有不依赖网络的关键校验，不能只
信任旧 report。

激活 body 为 `{"revision_id":"mrev_..."}`，要求 Server ETag。只能激活同一 Server、平台 policy
仍兼容、未被撤销且 readiness probe 可通过的 Revision。Readiness probe 在事务外完成并生成限时
probe evidence；CAS 事务只验证 evidence、状态和版本后切换 pointer。并发激活仅一个成功。

DELETE active-revision 要求 Server ETag 与 `X-Request-ID`，且不接受请求 body；它不删除 pointer，
只把 state 改为 `disabled` 并递增 disable fence。恢复通过 PUT active-revision 显式完成。Retirement 请求包含有界
`reason_code`，不接受自由 secret-bearing note；retired 后不能恢复。

### 9.3 Operator authorization

管理 API 至少定义以下 capability：

| Capability | 权限 |
|---|---|
| `mcp.server.read` | 读取 Server、Draft、snapshot、validation 与 revision 安全投影 |
| `mcp.server.write` | 创建和修改 Draft、imports、删除未发布 Server |
| `mcp.server.discover` | 发起/取消 discovery 与 connectivity I/O |
| `mcp.server.publish` | 发布 Revision、激活、禁用与退役 |

默认拒绝。`publish` 不隐含 `write`，生产可以实施四眼流程：一个 Operator 编辑，另一个 Operator
发布。v1 的 Server 定义属于 installation scope；每次访问仍必须验证 Operator identity/capability，
不能把 URL 中的 `server_id` 当成授权证据。

普通 `ApiAuth` tenant/user bearer、MCP OAuth user token 和 MCP Server resource token 都不能访问
`/v1/admin/mcp/**`。未授权的对象读取按平台防枚举策略返回 404；已识别但缺 capability 的集合操作
返回 403。

## 10. 发布事务、并发与故障语义

### 10.1 Publish 原子边界

Publish 在一个短数据库事务内完成：

1. 锁定/比较 Server 与 Draft version；
2. 验证 snapshot 成功、非 stale、归属同一 Server 且 `discovery_input_hash` 与当前 Draft 匹配；
3. 验证 validation report 匹配，并重跑关键闭合校验；
4. 规范化、排序、去重并生成全部 binding、Action descriptor 与 revision hash；
5. 插入 Revision、Tool/Resource/Prompt binding、idempotency result、audit event 与 outbox；
6. commit 后返回 201。

任一步失败都不得留下部分 Revision、部分 Action 或已消费 request ID。事务中不解析 DNS、不访问
secret provider、不启动进程、不请求 MCP Server。

### 10.2 激活原子边界

激活事务原子更新 `active_revision_id + state + server_version + enable_fence`，写审计/outbox 和幂等
结果。Agent publisher 读取 active pointer 时必须在同一 storage snapshot 中获取 Revision 与全部
bindings，不能读到“新 pointer + 旧 binding”。

### 10.3 异步 discovery 的 uncertain outcome

客户端收到 202 后只以 discovery GET 为权威。HTTP 断线时用相同 request ID 重试。worker 在远端请求
成功但本地提交前崩溃，可以重新 discovery；远端 discovery/list 应为只读，但平台仍不声称远端只
执行一次。只有成功提交 snapshot 才能被 publish 引用。

### 10.4 错误码

至少提供以下稳定 code：

| HTTP | code | 含义 |
|---:|---|---|
| 400 | `MCP_MANAGEMENT_INVALID_REQUEST` | schema、枚举、ID 或 limit 非法 |
| 403 | `MCP_MANAGEMENT_FORBIDDEN` | Operator 缺 capability |
| 404 | `MCP_SERVER_NOT_FOUND` | 不存在或不可见 |
| 409 | `MCP_SERVER_STATE_CONFLICT` | 当前生命周期不允许操作 |
| 409 | `MCP_DISCOVERY_STALE` | snapshot 已被 Draft/policy/list change 失效 |
| 409 | `MCP_IMPORT_CANDIDATE_MISMATCH` | remote/schema hash 不属于 snapshot |
| 409 | `MCP_IDEMPOTENCY_KEY_REUSED` | request ID 被不同请求复用 |
| 412 | `MCP_MANAGEMENT_PRECONDITION_FAILED` | ETag 不匹配 |
| 422 | `MCP_VALIDATION_FAILED` | 语义校验不通过 |
| 428 | `MCP_MANAGEMENT_PRECONDITION_REQUIRED` | 缺少 If-Match |
| 429 | `MCP_DISCOVERY_CAPACITY_EXCEEDED` | pending/running discovery 达上限 |
| 503 | `MCP_SERVER_READINESS_FAILED` | 激活前的限时 readiness evidence 未通过 |

远端 JSON-RPC message、HTTP body、stderr、URL query 和 secret 不拼接进公开 `message`。详细原因通过
闭合 `stage`、`reason_code`、`retryable` 和 correlation ID 表达。

异步 discovery 的远端错误和超时不是 discovery GET 的 502/504：operation 进入 `failed`，GET 仍返回
200，并在 `failure.code` 中使用 `MCP_DISCOVERY_REMOTE_ERROR` 或 `MCP_DISCOVERY_TIMEOUT`。这样 HTTP
重试不会误创建第二个 operation。

## 11. Agent、Action Registry 与 Run 集成

### 11.1 Agent 仍显式列 Tool

Agent Definition 继续使用生成后的 Action ID：

```yaml
tools:
  - mcp.engineering.tool_engineering_search_repositories
```

不增加以下形式：

```yaml
tools: ["*"]
mcp_servers: [engineering]
auto_import: true
```

管理 Revision 发布或激活本身不使任何 Agent 获得工具。作者必须把 Action ID 加入 Agent，经过既有
schema/能力/公开策略编译和 Deployment 发布。

### 11.2 Revision-scoped Action Registry

实现必须把当前启动期全局 MCP Action 注册重构为 revision-scoped resolver：

- MCP Revision 发布时生成 immutable Action descriptor，但不修改正在运行的全局 map；
- Agent Deployment 发布时从同一 active MCP Revision snapshot 解析所有 MCP Action；
- Deployment Revision 冻结
  `server_id + server_revision_id + revision_hash + action_id + tool_binding_hash`；
- Plan hash、tool schema、模型可见 alias、effect、approval、capability 与 public policy 都来自该 binding；
- Run 只加载 Deployment Revision 固定的 descriptor，不能查询 active pointer 替换 schema；
- active MCP Revision 改变只影响以后发布的 Agent Deployment Revision，不热更新旧 Deployment。

找不到 active revision、Action 不存在、Server disabled/retired、binding hash 不匹配或 capability 不可用，
Agent 发布必须失败，不能静默删 Tool。

### 11.3 禁用 fence

固定 Revision 保证可复现，不等于绕过运维 kill switch：

- disabled/retired Server 阻止新的 Agent 发布和需要该 Server 的新 Run admission；
- 已 admission 但尚未发起的 MCP call 在 dispatch 前检查 enable/disable fence，失败为
  `MCP_SERVER_DISABLED`；
- 正在进行的 HTTP/stdio 请求收到 cooperative cancellation；完成结果若越过 disable fence 且未先
  durable commit，不得作为成功 Action 继续执行；
- 已 durable commit 的 Action outcome 不回滚，外部副作用也不伪装为未发生；Run 按现有
  at-least-once/uncertain-outcome 规则终止或等待人工处理；
- 恢复 active 不自动 redrive 失败 Run，必须使用既有显式 recovery API。

### 11.4 用户运行面

现有 `/v1/mcp/servers`、`/{server_id}/tools|resources|prompts` 改为投影 active Published Revision，
仍按 tenant/user principal、Agent 可见性和 OAuth connection 过滤。它们不返回 Draft、未采纳候选、
secret ref 或管理 note。

OAuth user connection 继续是 principal-scoped runtime object。Operator 激活 Server 不代表任何用户已
授权；用户撤销 connection 也不修改 Server Revision。

## 12. Secret、网络与进程安全

### 12.1 Secret authority

- v1 管理 API 只保存和返回 secret reference string，不提供 secret value CRUD；
- resolver 必须把引用限定在平台配置 allowlist/namespace，不能接受任意文件路径、URL、shell、模板或
  environment expansion；
- discovery、readiness 和 runtime 在最后责任时刻解析 secret，并在内存中最短持有；
- secret ref 是否存在可以作为 validation 结果，但响应只返回 boolean/reason code；
- keyring 继续用于 durable OAuth、interaction 与 task 敏感 body；管理对象不得成为旁路；
- secret 轮换不改 Revision，但应由外部 Secret authority 产生审计；平台记录下一次连接成功/失败和
  ref hash，不记录值或 provider version。

### 12.2 HTTP 与 SSRF

每次 discovery、readiness 和 runtime 连接都重新执行：

- URL canonicalization，拒绝 userinfo、fragment 和静态 credential query；
- production HTTPS 与 TLS verification；
- DNS resolution 后的 IP policy，连接目标与校验结果绑定；
- redirect 默认拒绝；允许时每一跳重复 policy，并限制次数；
- loopback/private/link-local/metadata/multicast/unspecified range 的闭合拒绝；
- connect/read/overall timeout 与 request/response/frame/content 上限；
- 日志只记录规范化 host hash、server_id、stage 和结果类，不记录完整 URL/query。

DNS rebinding、代理环境变量、IPv4-mapped IPv6、IDNA、多个 A/AAAA 记录和 redirect 换 host 必须有
conformance tests。

### 12.3 stdio

API 不能提交任意 executable。stdio Revision 引用预配置 launch profile 与 profile fingerprint；启动器
继续使用结构化 argv、不经 shell、最小环境、工作目录约束、资源上限和 isolation。参数必须通过 profile
自带的闭合 schema；secret 只能注入 profile 声明的环境槽位。

profile 从平台配置删除或 fingerprint 改变后，引用旧 profile 的 Revision 变为不可激活/不可新
admission；历史记录保留。运行中 process 按 shutdown/disable fence 有界终止。

### 12.4 不可信 Catalog

远端名称、title、description、instructions、annotations、schema description、Resource、Prompt 和
错误均为不可信内容：

- 管理 UI/CLI 投影需要长度限制和安全转义；
- Provider 声称 `readOnlyHint` 等不改变平台 effect/approval；
- Tool description 只有 Operator 明确选择 `remote` 或提供 override 才进入模型上下文；默认 disabled；
- schema 只作为数据验证结构，不能触发远程 `$ref` 获取、代码生成执行或文件读取；
- Prompt/Resource 内容不能升级为 system 指令或绕过 Agent/tenant policy。

## 13. Storage 与一致性

目标 logical schema 至少包含：

- `mcp_managed_servers`；
- `mcp_server_drafts`；
- `mcp_signed_manifests`；
- `mcp_discovery_operations`；
- `mcp_discovery_snapshots`；
- `mcp_discovery_tools`、`mcp_discovery_resources`、`mcp_discovery_prompts`；
- `mcp_validation_reports`；
- `mcp_server_revisions`；
- `mcp_revision_tools`、`mcp_revision_resources`、`mcp_revision_prompts`；
- `mcp_management_requests`；
- `mcp_management_audit_events` 与必要 outbox。

实现可以在证明相同约束的前提下合并物理表，但必须满足：

- Server ID、revision number、request idempotency、remote/alias/Action ID 唯一约束由数据库兜底；
- 所有 JSON 使用 canonical bytes/hash，并有原始/规范化字节上限；
- snapshot/revision 行不可 UPDATE 正文，只能追加派生状态或 tombstone/retention metadata；
- 外键保护被 Agent Deployment/Run 引用的 Revision；
- discovery claim/lease/fence、cancel 与终态 first-winner 在 SQLite/PostgreSQL 结果一致；
- 发布、激活、禁用、退役、审计和 outbox 各自在单事务内完成；
- PostgreSQL 16 是生产权威，SQLite 仅限单进程开发。

项目继续采用 pre-1.0 final-schema provisioning：实现阶段直接更新最终 schema 与 provisioner，不在
应用启动时运行 ad-hoc migration，也不保留 v1/v2 双表读路径。

Retention 至少保留所有被 Revision、Deployment、Run 或审计引用的 snapshot/revision。未引用的失败
discovery 和 preview 不得无界增长；清理使用单独、可审计、分批事务，不能由 GET 隐式触发。

## 14. 可观测性与运维

### 14.1 Metrics

新增或扩展以下有界指标：

- management request total/duration，label 为 route class、result class；
- draft validation outcome；
- discovery pending/running/oldest-age、duration、retry、cancel、stage outcome；
- discovered/imported/rejected Tool 数量的 histogram，不把 tool name 作为 label；
- publish/activate/disable/retire outcome；
- active/disabled/stale Server 数量；
- Agent publish binding failure reason；
- runtime disable-fence rejection 与 uncertain outcome。

禁止把 endpoint、tool/resource/prompt name、description、schema、secret ref、Operator/user/tenant ID、
request ID、revision ID 放入低基数 metrics label。

### 14.2 Readiness 与 shutdown

- management API enabled 但 store/schema、Operator auth、keyring、secret resolver policy 或 worker 配置
  无效时，readiness 失败；
- 单个未激活 Draft/discovery 失败不影响整个平台 readiness；
- active Server 的 readiness 只验证平台实际需要的 transport/profile/metadata，不把短暂第三方故障
  永久变成进程启动失败；具体 degraded 状态在 Server 投影中报告；
- shutdown 先停止新的 management mutation/discovery claim，再取消/归还 worker lease，最后关闭
  MCP subscription、stdio 和 HTTP transport；
- worker crash、lease takeover 和重复结果提交均有故障注入测试。

### 14.3 审计与诊断

Operator 可以通过平台既有审计 sink 关联 `server_id`、draft/revision/discovery ID hash、request ID hash、
actor 与结果。Management GET 只返回安全投影，不提供“下载原始远端报文”接口。需要深度诊断时使用
受控、短期、显式启用且仍做 secret redaction 的运维 trace；它不属于公共 API。

## 15. 实施阶段

### Phase 1：schema、权威与 Operator 边界

- 定义 `mcp-management/v1` DTO/JSON Schema、错误枚举与路由；
- 增加 Operator principal/capability，生产禁止 disabled auth；
- clean-cut 到 `mcp.version: 2`，删除 `mcp.client.servers`；
- provision durable 管理表、repository、ETag/CAS、idempotency 与 audit/outbox；
- 完成 Server/Draft CRUD，不接入 Agent runtime。

### Phase 2：durable discovery

- 实现 discovery operation、worker claim/lease/fence/cancel；
- 接入 modern/legacy profile、HTTP/stdio 安全 transport 与完整分页；
- 保存 immutable candidate snapshot、rejections、fingerprint 与 stale event；
- 完成候选分页 API、容量/超时/重试和双数据库故障测试。

### Phase 3：显式 import、validation 与 publish

- 实现 Tool preview 和三类 import PUT；
- 实现保守 policy、schema hash 防替换、完整 validation report；
- 实现 immutable Revision、JCS hash、publish 事务与读取 API；
- 实现 activate/disable/retire、readiness evidence 与 CAS 竞态测试。

### Phase 4：Agent 与运行时集成

- 把 MCP Action Registry 改为 revision-scoped resolver；
- Agent Deployment 发布固定 MCP bindings 并纳入 Plan/Revision hash；
- 现有用户 Catalog API 投影 active Revision；
- 实现 admission/dispatch disable fence、OAuth user 分离与 recovery 行为；
- 删除所有启动期从 YAML 注册 Client Server 的旧路径。

### Phase 5：文档与资格验收

- 更新 `docs/current/mcp.md`、`api.md`、`configuration.md`、`operations.md` 和开发指南；
- checked-in OpenAPI/JSON Schema、合法/非法样本与 curl/CLI 示例；
- 完成 MCP SDK interop、管理 API conformance、安全、并发和故障注入矩阵；
- 全部验收通过后把本文移入 `docs/archive/specs/`。

UI 不阻塞 Phase 1～5；后续 UI 只能调用本规范 API，不能获得额外 authority 或直接写配置文件。

## 16. 测试矩阵

### 16.1 Schema 与 API

- 每个 request/response/error DTO 的合法与非法 fixture；未知字段、空值、超长、重复、非法枚举拒绝；
- pagination 稳定、cursor 不可伪造、最大 limit 生效；
- mutation 缺 request ID、缺 If-Match、stale ETag、request ID 异体重放；
- 未授权、跨 scope、防枚举、四项 Operator capability 的正反例；
- secret、header、query、stderr、远端 body 不出现在任何响应、日志和审计 fixture。

### 16.2 Discovery

- modern 与 legacy profile、Streamable HTTP 与 stdio；
- 多页 Tool/Resource/Prompt、重复 cursor、循环分页、重复名称、非法/深层/超大 schema；
- capability 缺失、协议不兼容、authorization 失败、timeout、取消、worker crash/接管；
- DNS rebinding、redirect、private/link-local/metadata IP、userinfo、query credential、TLS 失败；
- `list_changed` 只产生 stale/candidate，不改变 Draft/Revision/Deployment/Run；
- OAuth user 没有 publication authority 时失败，signed manifest 验证正反例。

### 16.3 Import 与发布

- `all` preview 展开为逐项清单且不写状态；新增远端 Tool 不进入旧 preview/import/revision；
- `*`、regex、pattern、缺 schema hash、schema 替换、alias/Action ID 冲突全部拒绝；
- 保守默认不被 provider annotations 放宽；
- Resource/Prompt allow、snapshot authority、MIME/URI/argument 与 size 校验；
- publish 部分写故障全部回滚；相同 request ID 精确 replay；并发 publish/activate 只有合法结果；
- revision canonicalization 在不同输入顺序、SQLite/PostgreSQL 和重复执行下 hash 一致；
- secret value/rotation不改变 hash，secret ref/endpoint/policy 改变必须改变 hash。

### 16.4 Agent 与 Run

- Agent 未列 Action 时不可调用；列出不存在/未激活/disabled Action 时发布失败；
- MCP active Revision 切换后，旧 Agent Deployment 与 Run 继续使用原 binding；
- 新 Agent Deployment 使用新 binding，Plan/revision hash 相应变化；
- disable 阻止新 admission/dispatch，in-flight outcome 遵守 fence 与 uncertain-outcome 合同；
- reenable 不自动重驱，retire 不可恢复；
- 用户 Catalog 只显示 active、授权、显式导入投影，不泄漏 Draft/候选/secret ref。

### 16.5 数据库与运维

- SQLite 与 PostgreSQL 对 CAS、unique、claim/fence、cancel、terminal first-winner 结果一致；
- provisioner、空库启动、rolling-disabled binary、schema mismatch fail-fast；
- retention 不删除被引用 snapshot/revision，清理可中断、可恢复且有界；
- metrics 无高基数/敏感 label，audit body-free；
- shutdown 不留下永久 running discovery，lease 到期可安全接管。

## 17. 完成定义

以下完成门槛已于 2026-07-31 全部满足：

- `mcp.version: 2` 和 durable management store 成为 Client Server 的唯一权威，旧
  `mcp.client.servers` 路径已删除；
- `/v1/admin/mcp/**` 全部 endpoint、Operator authorization、ETag、idempotency、错误与分页通过
  checked-in conformance tests；
- discovery 对 modern/legacy、HTTP/stdio、Catalog 分页、stale、取消和故障恢复全部可用；
- 系统中不存在持久化或运行时 Tool wildcard/auto-import；“全选”只产生显式 immutable expansion；
- Tool/Resource/Prompt import、validation、Revision hash、publish/activate/disable/retire 事务完成双
  数据库验证；
- Agent Deployment 固定精确 MCP binding，active pointer 改变不能改写旧 Deployment/Run；
- secret、SSRF、stdio RCE、untrusted Catalog、权限与 body-free audit 的安全测试通过；
- 当前文档、schema、API 示例、运维手册与资格报告已更新；
- 全仓格式、lint、unit、integration、conformance、SDK interop 和 release profile 测试通过。

实施期间使用了以下 fail-closed 限制；完成后由当前合同和 conformance tests 持续守护：

- 未交付前不得把 `/v1/admin/mcp/**` 描述为当前可用 API；
- UI 不得依赖未交付 endpoint；本次交付仍不包含 UI；
- 不能以临时 `tools: ["*"]` 或直接修改内存 Action Registry 作为过渡实现；
- clean-cut 前 `docs/current/` 必须继续描述 `mcp.version: 1`；完成后已同步为 `mcp.version: 2`
  durable authority。

## 18. 取舍说明

逐项 import 比 `auto_import` 多一步，但把第三方 Catalog 变化与模型执行权限隔离开；保守 preview 和批量
PUT 已经消除了大部分机械工作。durable discovery 比同步“测试连接”接口复杂，但避免长网络请求占用
HTTP/数据库事务，并为 UI 轮询、取消、重试和审计提供统一对象。

把 Client Server 定义从 YAML clean-cut 到数据库会带来一次配置断点，却换来唯一 authority、CAS、
不可变 Revision 和可靠审计；长期比 YAML/API 双写及优先级规则更简单。stdio launch profile 继续放在
平台配置中，是因为批准可执行文件和隔离策略属于基础设施 authority，而不是业务管理对象。

最重要的边界是：**MCP discovery 解决“第三方提供了什么”，管理 Revision 解决“平台批准了什么”，
Agent Deployment 解决“这个 Agent 可以用什么”。三者不得合并。**
