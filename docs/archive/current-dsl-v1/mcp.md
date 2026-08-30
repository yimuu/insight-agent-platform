# MCP 使用、运行与安全合同

状态：Current / management API v1 Implemented & Verified（2026-07-31）

协议基线：MCP `2026-07-28`

正式验收记录：
[MCP 完整支持资格验收](../archive/qualifications/2026-07-30-complete-mcp-qualification.md)、
[MCP 管理 API v1 资格验收](../archive/qualifications/2026-07-31-mcp-management-api-v1-qualification.md)

平台同时提供两个彼此隔离的方向：

- MCP Host/Client 把显式允许的远程 Tools、Resources 和 Prompts 冻结为平台
  Action、Retrieval 和 Prompt snapshot；
- MCP Server 通过独立 `/mcp` Streamable HTTP endpoint 暴露显式配置的
  Agent、Action、Resource 和 Prompt。

MCP 不新增 DSL node。所有执行仍经过既有 schema、effect、public policy、tenant、
Deployment Revision 和 durable Run authority。动态 discovery 只更新候选 catalog，不会改变已经
发布的 revision 或正在运行的 Run。

## Profile 与能力发现

`GET /v1/mcp/profiles` 返回独立 profile，而不是含义不清的 `mcp: true`：

- `modern_client`：Tools、Resources、Prompts、Completion、Subscriptions、Elicitation、
  stdio、Streamable HTTP 和 HTTP Authorization；
- `modern_server`：`/mcp` discovery、显式 exports、同步执行、Completion、Subscriptions、
  Elicitation 和 HTTP Authorization；
- `tasks`：双向 `tasks/get`、`tasks/update`、`tasks/cancel` 和 status notification；
- `legacy_client`：独立的 `2025-11-25` compatibility profile。

每项分别返回 `implemented`、当前部署的 `enabled` 和有界 capability 集合。具体远程 Server
最终可用能力仍由 `GET /v1/mcp/servers` 中冻结的 discovery 结果决定；未协商的能力不得使用。

Legacy fallback 只有在全局显式允许 `2025-11-25`、Server 使用 live service-account discovery，
且 modern `server/discover` 明确返回 method-not-found 或有界探测超时时才发生。Legacy 独立执行
`initialize` / `notifications/initialized` 和 HTTP session 协商；选定 era 后连接不可切换。Modern
wire 不接受 legacy envelope，legacy wire 不携带 modern metadata、不声明 Roots/Sampling，也不支持
实验 Tasks。所选协议版本会进入 Server binding 和 Deployment Revision 证据。

## 最小关闭配置

仓库的 production 和 quickstart 配置都显式包含关闭状态：

```yaml
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
    legacy_fallback: []
  client:
    enabled: false
    management_api:
      enabled: false
  server:
    enabled: false
    endpoint: /mcp
    authorization:
      type: disabled
    exports:
      agents: []
      actions: []
      resources: []
      prompts: []
```

所有 MCP 配置对象拒绝未知字段。Server ID 只能使用小写字母、数字和连字符；timeout、body、
catalog、content 和 SSE frame 均有硬上限。生产 stdio 必须使用绝对 executable 和显式 isolation
profile；远程 HTTP 必须为无凭据 HTTPS URL，只有单进程开发的精确 loopback 可显式允许明文。

## Client 管理配置与 Server 创建

`mcp.version: 2` 把 Client Server 实例从 YAML 移到 durable management store。顶层
`management.version: 1` 保存共享 Operator credential；MCP YAML 只保存全局硬策略、secret resolver、
签名信任根、stdio launch profile 和默认上限；不应把
token、client secret 或签名私钥写入 YAML：

```yaml
management:
  version: 1
  enabled: true
  operator_credentials:
    - identity: mcp-platform-operator
      token_env: INSIGHT_PLATFORM_OPERATOR_TOKEN
      capabilities:
        - mcp.server.read
        - mcp.server.write
        - mcp.server.discover
        - mcp.server.publish

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
    secret_encryption:
      active_key_version: v1
      keyring_env: INSIGHT_MCP_SECRET_KEYRING
    secret_resolver:
      type: environment_reference
      allowed_names: [ENGINEERING_MCP_TOKEN]
    signed_manifest_trust:
      max_validity: 24h
      trusted_signers: []
    network_policy:
      allow_loopback_development: false
      allow_private_networks: false
      allow_redirects: false
    stdio_launch_profiles: {}
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

随后使用 `POST /v1/admin/mcp/servers` 创建 Draft，异步 discovery 得到候选 catalog，再通过三个
imports PUT 显式采纳 Tool/Resource/Prompt。Validation 和 publish 生成不可变 Revision；激活使用
Server ETag 做 CAS。远端 `tools/list`、`resources/list`、`resources/templates/list` 和
`prompts/list` 只提供候选，不授予 Agent 执行权限。系统不接受 `tools: ["*"]`、`auto_import` 或
Server 级动态授权；“全选” preview 只展开为当次 discovery 的逐项 immutable 清单。

管理 API 使用独立 Operator Bearer token，普通 tenant/user API token 与 MCP OAuth token 均不能访问。
所有 mutation 要求 `X-Request-ID`，修改 Draft/Server 还要求精确 `If-Match`。公开请求 schema 见
[`schemas/mcp-management-v1.json`](../../schemas/mcp-management-v1.json)，完整 HTTP 合同见
[`schemas/mcp-management-v1.openapi.json`](../../schemas/mcp-management-v1.openapi.json)。

`oauth_user` 连接使用 Authorization Code + PKCE。浏览器 callback 只接受高熵、单次、限时 state；
access/refresh token 和 interaction body 使用独立 keyring 加密，普通 API、日志、metrics、trace、
Run stream 和 terminal error 都不得输出明文。Definition Prompt snapshot 必须在发布期使用 authoring
credential 实际调用 `prompts/get`；`oauth_user` 不具备该 authority，因此这类组合启动时拒绝。

Resource allowlist 会在读取时再次校验 canonical URI、Server binding、MIME、严格 Base64 和解码后总
字节上限。Resource 作为稳定 ID 的 Retrieval 注册，不会因为 Server 声明 capability 自动进入模型
上下文。Prompt 内容始终标记为 untrusted，不能覆盖 system/tenant/tool/public policy。

## MCP Server 示例

生产建议使用 OAuth protected resource：

```yaml
mcp:
  version: 2
  protocol:
    preferred: "2026-07-28"
    legacy_fallback: []
  client:
    enabled: false
    management_api:
      enabled: false
  server:
    enabled: true
    endpoint: /mcp
    authorization:
      type: oauth_resource_server
      resource: https://agents.example.com/mcp
      authorization_servers: [https://identity.example.com]
      required_scopes: [mcp.invoke]
    exports:
      agents:
        - agent: researcher
          as: researcher
          execution: task_preferred
          input_required: allowed
          required_scope: mcp.invoke
      actions: []
      resources: []
      prompts: []
```

JWT access token 按 issuer、audience、signature、expiry、`typ=at+jwt` 及 RFC 9068 必需 claims
校验；JWKS 与 OAuth metadata 获取执行 HTTPS、重定向和 DNS/IP SSRF 防护。`/.well-known/`
protected-resource metadata 与 `WWW-Authenticate` challenge 只公布安全元数据。

Server 只列出配置 export。它不会自动公开 Run、Conversation、Artifact、trace、内部 Agent prompt
或 Action secret。每次 list/call/read/get/task 请求都重新执行 principal、scope 和 capability
检查。Tasks 只有双方协商 extension 后才能出现；过期 Server task 由后台维护先取消底层 Run，再删除
task authority。

## Interaction 与 run-stream/v1

Form、URL、Approval 和 OAuth Authorization 都使用 durable interaction。请求正文和响应正文加密
保存；公开 API 和 SSE 只返回安全摘要。等待的唯一 authority 是数据库中的 first-winner 状态：
respond、decline、cancel、expiry、Run terminal 和 retry completion 不能产生两个终态。
Run 进入终态时，同一 durable transaction 会先将所有尚未闭合的 interaction 以
first-winner 转为 `run_terminal`，再冻结全部安全摘要。摘要数组进入 canonical
`run_payload`，`snapshot_hash` 对包含该数组的完整 payload 计算，保证重启后返回同一
终态事实。每个 Run 最多 1024 个 interaction 摘要；超限必须 fail closed，不得
静默截断、丢弃早期交互或在 terminal commit 后补写。

主要接口：

| 方法 | 路径 |
|---|---|
| `GET` | `/v1/mcp/interactions` |
| `GET` | `/v1/mcp/interactions/{interaction_id}` |
| `POST` | `/v1/mcp/interactions/{interaction_id}/respond` |
| `POST` | `/v1/mcp/interactions/{interaction_id}/open` |
| `POST` | `/v1/mcp/interactions/{interaction_id}/decline` |
| `POST` | `/v1/mcp/interactions/{interaction_id}/cancel` |

Attached Run 直接在 `run-stream/v1` 中发送 `run.interaction.required` /
`run.interaction.closed`，并在 terminal Run snapshot 中加入安全 `interactions[]`。
live `required` / `closed` 事件只是通知，不是 interaction 或 Run 终态的恢复权威；
客户端断线、丢帧或重连后必须以 durable terminal snapshot 中冻结的 `interactions[]`
校准。
`run-stream/v1` 共有 27 个闭合事件；平台不使用请求 Header 协商另一个 run-stream
版本。公开合同见
[`schemas/run-stream-v1.json`](../../schemas/run-stream-v1.json)，覆盖全部 27 个事件的 checked-in
合法样本见
[`schemas/run-stream-v1.samples.json`](../../schemas/run-stream-v1.samples.json)。

## Operator 管理 API

| 方法 | 路径 |
|---|---|
| `POST/GET` | `/v1/admin/mcp/servers` |
| `GET/DELETE` | `/v1/admin/mcp/servers/{server_id}` |
| `GET/PUT` | `/v1/admin/mcp/servers/{server_id}/draft` |
| `POST/GET` | `/v1/admin/mcp/servers/{server_id}/manifests` |
| `GET` | `/v1/admin/mcp/servers/{server_id}/manifests/{manifest_id}` |
| `POST/GET` | `/v1/admin/mcp/servers/{server_id}/discoveries` |
| `GET/DELETE` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}` |
| `GET` | `/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/{tools\|resources\|prompts}` |
| `POST` | `/v1/admin/mcp/servers/{server_id}/tool-import-previews` |
| `PUT` | `/v1/admin/mcp/servers/{server_id}/draft/imports/{tools\|resources\|prompts}` |
| `POST` | `/v1/admin/mcp/servers/{server_id}/validations` |
| `GET` | `/v1/admin/mcp/servers/{server_id}/validations/{validation_id}` |
| `POST/GET` | `/v1/admin/mcp/servers/{server_id}/revisions` |
| `GET` | `/v1/admin/mcp/servers/{server_id}/revisions/{revision_id}` |
| `PUT/DELETE` | `/v1/admin/mcp/servers/{server_id}/active-revision` |
| `POST` | `/v1/admin/mcp/servers/{server_id}/retirement` |

Operator capability 是闭合集合：`mcp.server.read`、`mcp.server.write`、`mcp.server.discover` 和
`mcp.server.publish`，互不隐含。Draft、discovery snapshot、validation 与 Revision 都使用 durable
authority；publish、activate、disable、retire、idempotency receipt、body-free audit 与 outbox 分别在
短事务内原子提交。网络、DNS、OAuth metadata 和 stdio I/O 不在数据库事务中执行。

## Catalog、OAuth 与撤销 API

| 方法 | 路径 |
|---|---|
| `GET` | `/v1/mcp/profiles` |
| `GET` | `/v1/mcp/servers` |
| `GET` | `/v1/mcp/servers/{server_id}/tools` |
| `GET` | `/v1/mcp/servers/{server_id}/resources` |
| `POST` | `/v1/mcp/servers/{server_id}/resources/read` |
| `GET` | `/v1/mcp/servers/{server_id}/prompts` |
| `POST` | `/v1/mcp/servers/{server_id}/prompts/{prompt_name}/preview` |
| `POST` | `/v1/mcp/servers/{server_id}/completion` |
| `POST` | `/v1/mcp/servers/{server_id}/agents/{agent_id}/runs` |
| `POST` | `/v1/mcp/servers/{server_id}/authorize` |
| `GET` | `/v1/mcp/connections` |
| `DELETE` | `/v1/mcp/connections/{server_id}` |

Catalog 与 connection 接口按当前 `X-Tenant-ID` / `X-User-ID` principal 隔离并返回
`Cache-Control: private, no-store`。删除连接即使远程 revoke 失败也会先使本地 credential 不可用；
响应报告远程撤销结果，不恢复本地 token。

MCP context admission 接口在创建 Run 前显式选择最多 16 个已导入 Resource/Prompt，实时获取并冻结
安全快照，再把稳定 context map 作为普通 Run input 提交。选择必须属于路径中的 Server，仍受
principal、allowlist、大小、MIME、Prompt user-invocation 与 Agent input schema 约束；动态 catalog
变化不能改写已 admission 的 Run。

## 运维

- 对 MCP schema 或 durable 表的升级先运行 schema provisioner，再滚动部署 disabled binary；
- `INSIGHT_MCP_SECRET_KEYRING` 必须来自 Secret，包含版本化 32-byte key；轮换时保留仍被旧 ciphertext
  引用的 key version；
- readiness 只把已启用 Agent 实际导入的 binding 作为强依赖：service-account/legacy 连接执行真实
  discovery 或非弃用 primitive 探针；`oauth_user` 验证 protected-resource、authorization-server、
  PKCE 和 scope metadata，不发送无用户令牌的 MCP 请求。Keyring 在启动时完整解析；启用的 `/mcp`
  OAuth resource server 在接收流量前完成 issuer metadata 与 JWKS 获取；
- shutdown 先停止新 admission，再停止 subscription/task/OAuth maintenance，最后关闭 HTTP、
  stdio 和 RunService；
- metrics label 只能使用有界 Server ID、primitive、transport、principal kind 和结果类，不能使用
  tool name、URI、prompt name、task ID、Run ID 或 tenant ID。

`/metrics` 提供 `insight_mcp_operations_total`、operation duration、transport event、active
subscription、interaction open/oldest-age/outcome、remote task state/oldest-age、OAuth
transaction/refresh/revoke、stdio restart、cache hit/miss/invalidation、body/frame limit rejection
和 stale publication candidate 指标。管理面另外提供
`insight_mcp_management_requests_total`、`insight_mcp_management_request_duration_seconds`、
`insight_mcp_management_lifecycle_total`、`insight_mcp_management_lifecycle_duration_seconds`、
`insight_mcp_management_catalog_items` 和 `insight_mcp_management_objects`。后者覆盖 pending/running
discovery、oldest age，以及 active/disabled/stale Server 数。所有 label 都来自闭合低基数集合。

SQLite 仅用于单进程开发；生产的 durable interaction、OAuth、remote Tasks 与 Server Tasks 使用
PostgreSQL 16 和同一套已 provision schema。

外部 SDK 资格验收固定 TypeScript `@modelcontextprotocol/sdk@1.30.0` 与 Go SDK commit
`91e4e1a0b8ca01cfa680f142815b1152a0513326`。它覆盖平台 Client 到两个 SDK Server 的 stdio 与
Streamable HTTP、两个 SDK Client 到平台 `/mcp` 的 Streamable HTTP，以及双向 Tasks。复现命令、
适配器边界和报告位置见[开发指南](development.md)。
