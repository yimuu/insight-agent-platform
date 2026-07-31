# MCP 2026-07-28 完整支持规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 日期 | 2026-07-30 |
| 目标协议 | MCP `2026-07-28` |
| 兼容协议 | MCP `2025-11-25`（独立兼容 profile） |
| 平台协议 | `insight.agent/v1`、`run-stream/v2` |
| 变更类型 | Protocol Boundary / Resource SPI / Durable Interaction / Authorization / Public API |
| 影响范围 | 新 `insight-mcp` crate、`insight-resources`、`insight-engine`、`insight-durable`、`insight-storage`、`insight-runtime`、`insight-api`、平台配置、数据库 schema、公开 schema、测试与文档 |

> 本规范已于 2026-07-30 完成实现与验收，现作为历史设计记录归档。当前合同见
> [MCP 使用、运行与安全合同](../../current/mcp.md)，验收证据见
> [MCP 完整支持资格验收](../qualifications/2026-07-30-complete-mcp-qualification.md)。

## 1. 决策摘要

Insight Agent Platform 将增加完整、可协商、可审计的 MCP 支持，但不会把 MCP 简化为一个绕过现有
执行合同的动态插件入口。本规范定义两个一等 profile：

1. **MCP Host/Client profile**：平台连接外部 MCP Server，完整消费 Tools、Resources、Prompts、
   Completion、Subscriptions、Elicitation、标准传输和 HTTP Authorization；
2. **MCP Server profile**：平台通过独立 `/mcp` endpoint，把显式授权的 Agent、资源和 Prompt
   暴露给外部 MCP Host。

官方 Tasks extension 形成第三个独立协商 profile，用于长时间运行、断线恢复、进度和中途输入。
它不是 core MCP 的隐式组成部分，只有 client 与 server 双方明确声明
`io.modelcontextprotocol/tasks` 时才启用。

目标实现遵循以下架构决策：

- MCP Tool 通过 `McpToolProvider` 适配到现有 Action/LLM tool 执行合同，不新增 `type: mcp`；
- MCP Resource 通过 `McpResourceProvider` 接入 Retrieval、Artifact 和显式上下文选择；
- MCP Prompt 进入独立 Prompt Catalog；发布期导入必须冻结内容，运行期选择必须由用户或作者显式发起；
- `InputRequiredResult` 和 Tasks `input_required` 进入新的 durable interaction 状态机，不能伪装成
  Action retry、普通错误或进程内回调；
- MCP Server 的 `list_changed` 和资源更新只能使 Catalog cache 失效、触发重新发布候选或更新动态
  资源内容，不能修改已经发布的 Definition/Deployment Revision；
- Provider 自报的名称、版本、instructions、tool annotations 和 effect hints 均不构成安全权威；
- secret、OAuth token、PKCE verifier、MCP `requestState` 和用户表单响应不能进入 Plan、revision
  identity、公开事件、默认日志或错误正文；
- `full` persistence 支持可恢复的 Elicitation、远程 Tasks 和 approval wait；`terminal_only` 只允许
  明确声明为无交互、无远程 Task、可在单次 execution budget 内结束的 MCP 调用；
- 外部副作用继续只承诺 at-least-once。MCP 不扩大平台的 exactly-once 承诺。

本规范现为已完成的历史设计记录，不直接定义当前可执行合同。当前行为由公开 schema、实现、
双数据库约束、conformance tests 和 `docs/current/` 共同定义。

## 2. “完整支持”的精确定义

### 2.1 版本边界

规范基线固定为 MCP `2026-07-28`。该版本采用 modern、stateless wire：

- 删除 `initialize` / `notifications/initialized` handshake；
- 删除 Streamable HTTP 的 `Mcp-Session-Id`；
- 每个请求都携带协议版本、client info 和 client capabilities；
- Server 必须实现 `server/discover`；
- server 需要更多输入时，通过 `InputRequiredResult` 与新的 request ID 完成多轮请求；
- list/resource 更新通过 client 主动打开的 `subscriptions/listen` 流接收。

实现必须 vendor 一份上游 `2026-07-28` schema snapshot，并记录：

- 上游仓库、release/tag 或精确 commit；
- snapshot SHA-256；
- 生成 Rust wire types 所用的工具版本；
- 本地补丁清单；正常情况下补丁清单必须为空。

在线文档、SDK 当前行为或未固定的 `draft/` 文件不能在运行时动态改变协议解释。

### 2.2 Capability profile

“完整”不表示无条件声明所有能力。MCP 以 capability negotiation 为合同，平台只能声明已经由当前
部署配置、路由、持久化模式和 UI/API surface 真正支持的能力。

| Profile | 必须实现 | 可以按部署关闭 |
|---|---|---|
| `mcp-modern-client-v1` | modern envelope、`server/discover`、Tools、Resources、Prompts、Completion、Subscriptions、Elicitation、caching、pagination、progress、cancellation、stdio、Streamable HTTP、HTTP Authorization | 某个具体 Server 未声明的 primitive |
| `mcp-modern-server-v1` | `/mcp` modern endpoint、discovery、已配置 exports、标准错误、caching、pagination、subscriptions、HTTP Authorization resource behavior | 未配置 export 的 primitive |
| `mcp-tasks-v1` | `tasks/get`、`tasks/update`、`tasks/cancel`、status notification、client/server 双向映射 | 整个 extension；默认关闭 |
| `mcp-legacy-client-v1` | `2025-11-25` initialize/session compatibility 与非弃用 primitive 的交集 | 默认关闭，且不得影响 modern path |

产品对外必须报告 profile 和 capability 矩阵，不能只返回一个含义不明的 `mcp: true`。

### 2.3 弃用能力

MCP `2026-07-28` 已弃用 Roots 和 Sampling。新实现：

- 不声明 `roots`；
- 不声明 `sampling`；
- 不实现旧 `notifications/roots/list_changed`；
- 不实现旧 `ping` 或 `logging/setLevel`；
- 不允许 MCP Server 通过 Sampling 反向调用平台模型。

兼容 `2025-11-25` 时也只声明平台实际支持的非弃用能力，不因旧 Server 提供 Roots/Sampling 而自动
启用。未来若业务仍需要某项弃用能力，必须单独形成兼容规范和移除计划。

### 2.4 不属于“完整 core”的内容

以下内容不阻塞 `mcp-modern-client-v1` / `mcp-modern-server-v1` 完成：

- 任意第三方 MCP extension；
- MCP Apps/UI extension；
- Registry 市场、远程安装或自动信任；
- `2025-11-25` 之前的协议版本；
- Skills、A2A 或通用动态插件系统。

官方 Tasks extension 由独立 profile 验收，不能混入 core capability 后无条件发送。

## 3. 当前基线与缺口

### 3.1 已有可复用基础

当前平台已经具备：

- 不可变、类型化 Canonical Plan 与 Definition/Deployment Revision；
- Action input/output JSON Schema、descriptor hash、effect、idempotency、cancellation 和 capability；
- LLM tool 白名单、tool choice、轮数/次数上限、并行工具调用和 continuation；
- full runtime 的 durable scheduler、claim、lease/fence、checkpoint、wait 和恢复；
- terminal-only 的显式故障语义；
- Retrieval SPI、Artifact store、Prompt 文件固定和内容 hash；
- tenant/user principal、Conversation、human task、signal 和公开 Run API；
- `run.tool.started/progress/completed/failed` 与独立公开策略。

这些基础必须继续作为 MCP 执行权威，不能由远程 Server 的动态 metadata 替代。

### 3.2 当前工具合同缺口

当前 LLM tool binding 假设模型可见工具名等于 Action ID，并把模型工具 description 固定为 `None`。
MCP 工具名最长可达 128 字符、允许点号并只在单个 Server 内唯一，因此必须先完成通用工具合同重构：

- 区分 `action_id`、`remote_tool_name` 与 `model_tool_name`；
- `model_tool_name` 使用平台稳定 alias，解决长度、字符集和跨 Server 冲突；
- 增加可冻结的 `title`、`description`、icons/annotations 原始证据与安全投影；
- 保留 remote name 原样传输，不用 `serverInfo.name` 参与唯一性或安全判断；
- descriptor hash 覆盖模型实际看到的 name、description 和 input schema。

当前 `ActionDescriptor.id/version` 使用 `&'static str`，也不适合 publication-time discovery 得到的
owned descriptor。实现不能通过 `Box::leak` 制造伪静态生命周期；必须把 registry identity 改为
owned/`Arc<str>`，或增加等价的动态 provider descriptor。Native Action SemVer 与 MCP adapter worker
SemVer 继续冻结，remote Tool 本身的变化由 descriptor hash 表达，不能伪造上游 Tool SemVer。

### 3.3 当前执行返回值缺口

当前 `Action::call` 只返回成功 `Value` 或 `RunError`。完整 MCP 还会返回：

- `resultType: complete`；
- `resultType: input_required`；
- Tasks extension 的 `resultType: task`；
- request-scoped progress/log notifications；
- text、image、audio、resource link、embedded resource 和 structured content；
- protocol error 与 `isError: true` tool execution result。

因此 MCP 不能直接实现为一个普通 `Action`。平台需要一个能够返回
`Complete | PendingInteraction | RemoteTask` 的内部 leaf continuation contract，随后再通过 Action
和 LLM tool adapter 投影给现有执行模型。

### 3.4 当前认证缺口

Provider secret reference 只覆盖部署级凭据。完整 MCP HTTP Authorization 需要：

- protected resource metadata 与 authorization server discovery；
- OAuth authorization code + PKCE；
- client registration/metadata；
- tenant/user 与 token 的绑定；
- refresh、scope challenge 和 step-up；
- callback state、过期、撤销和 privacy deletion；
- MCP Server 作为 protected resource 时的 audience、scope 与 principal 映射。

这些 token 不能放入普通平台 YAML、Deployment Revision 或 Artifact 明文。

### 3.5 当前交互公开面缺口

现有 human task 是 Agent 作者显式声明的业务工作项。MCP Elicitation 是远程 Server 在某次协议请求
中提出的嵌套输入请求，二者不能共享同一个公共类型或权限模型。

`run-stream/v1` 是闭合的 25 事件协议，未包含 interaction 事件。完整 Host 需要：

- durable interaction 查询与回复 API；
- 来源、模式、schema、目标 URL host 和截止时间的安全展示；
- accept、decline、cancel 和 expire first-winner；
- Attached 客户端的实时通知；
- terminal snapshot 的安全校准。

因此本规范要求新增 `run-stream/v2`，而不是向 v1 静默增加事件。

## 4. 目标

1. 支持 MCP `2026-07-28` modern Host/Client 与 Server 两个角色；
2. 支持 stdio 和 Streamable HTTP 两种标准 transport；
3. 支持 Tools 的发现、调用、Schema 校验、结构化/多模态内容、进度、取消和错误语义；
4. 支持 Resources list/read/templates/subscriptions，并安全接入 Retrieval 和 Artifact；
5. 支持 Prompts list/get 和 Completion，保持 user-controlled 语义；
6. 支持 Elicitation form/URL mode 及 durable resume；
7. 支持完整 HTTP Authorization、用户级 credential 与 step-up；
8. 支持官方 Tasks extension 的 client/server 双向映射；
9. 所有运行都固定到精确 MCP server/tool/resource/prompt binding evidence；
10. dynamic list changes 不改变已发布 revision；
11. 外部内容、secret、错误、日志和公开事件遵循现有 non-interference 与 body bounds；
12. MCP Server 只暴露显式 export，不自动公开所有 Agent、Action、Prompt、Run 或 Artifact；
13. PostgreSQL 与 SQLite 实现相同的逻辑状态机；生产 HA 继续只承诺 PostgreSQL；
14. 失败、取消、超时、远程 task、用户输入和 Run terminal 竞态具有数据库决定的唯一赢家；
15. 提供可自动执行的 wire、恢复、安全、双 SDK interoperability 和 real-process 验收证据。

## 5. 非目标

- 不新增通用自定义 DSL node 或动态 Rust/WASM plugin ABI；
- 不允许 Agent YAML 直接声明任意 MCP endpoint、command、credential 或 OAuth scope；
- 不根据 remote annotations 自动判定 read-only、destructive、idempotent 或 safe-to-publish；
- 不允许 `list_changed` 原地修改正在运行的 Plan、Deployment Revision 或 model tool list；
- 不承诺远程副作用 exactly-once；
- 不自动把 Server instructions、Prompt 或 Resource 注入模型上下文；
- 不执行从 MCP Resource、Prompt 或 Tool result 下载的脚本；
- 不把 stdio command 当作 shell 字符串执行；
- 不跨 tenant 共享 private/user-scoped discovery、completion、resource 或 token cache；
- 不公开 remote raw error、stderr、OAuth token、request state 或用户表单正文；
- 不把 MCP human interaction 伪装成现有业务 human task；
- 不在本规范中实现 Skills、A2A、MCP Apps 或第三方扩展市场；
- 不支持当前规范已弃用的 Roots 和 Sampling；
- 不要求 terminal-only 获得与 full 相同的可恢复交互能力。

## 6. 上游协议对齐

### 6.1 规范来源

实施和评审使用以下版本化上游文档：

- [Architecture](https://modelcontextprotocol.io/specification/2026-07-28/architecture)
- [Key Changes](https://modelcontextprotocol.io/specification/2026-07-28/changelog)
- [Discovery](https://modelcontextprotocol.io/specification/2026-07-28/server/discover)
- [Transports](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports)
- [Authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization)
- [Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
- [Resources](https://modelcontextprotocol.io/specification/2026-07-28/server/resources)
- [Prompts](https://modelcontextprotocol.io/specification/2026-07-28/server/prompts)
- [Completion](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/completion)
- [Elicitation](https://modelcontextprotocol.io/specification/2026-07-28/client/elicitation)
- [Tasks extension](https://modelcontextprotocol.io/extensions/tasks/overview)

版本化 schema snapshot 是 wire 的最终机器权威；网页用于解释，不得替代 checked-in snapshot。

### 6.2 Core method coverage

Host/Client 必须覆盖：

| Primitive | 请求/流 |
|---|---|
| Discovery | `server/discover` |
| Tools | `tools/list`、`tools/call` |
| Resources | `resources/list`、`resources/templates/list`、`resources/read` |
| Prompts | `prompts/list`、`prompts/get` |
| Completion | `completion/complete` |
| Subscriptions | `subscriptions/listen` |
| Multi-round trip | `InputRequiredResult`、retry with `inputResponses` / `requestState` |
| Utilities | pagination、caching、request-scoped progress/message、cancellation、标准 JSON-RPC errors |

Server profile 对已声明 capability 实现相同的入站方法，并拒绝 capability 与方法不一致、body/header
不一致、规范定义为闭合位置的未知字段和非法 schema。规范允许扩展的 `_meta`、capabilities 和
extension map 必须按上游 schema 保留为有界 opaque 数据；平台不解释的扩展不能改变本地行为。

### 6.3 Modern request envelope

每个 modern 请求必须携带：

- `io.modelcontextprotocol/protocolVersion`；
- `io.modelcontextprotocol/clientCapabilities`；
- `io.modelcontextprotocol/clientInfo`，除非上游明确允许且配置要求省略；
- request-scoped log/progress、extension 和 authorization 所需 metadata。

Streamable HTTP 必须同时校验标准 header 与 body metadata。任何不一致都在进入业务适配层前失败。

### 6.4 `server/discover`

平台默认在发布、连接测试或 cache 过期时调用 `server/discover`，并：

- 从 `supportedVersions` 选择精确版本；
- 验证 capability 与后续 list/call 响应一致；
- 按 `ttlMs` 和 `cacheScope` 缓存；
- 保存 `serverInfo` 和 instructions 作为不可信、非安全证据；
- 禁止使用自报 `serverInfo.name` 作为 credential、cache 或 tool namespace 的唯一键。

`server/discover` 失败时，不通过逐个方法猜测能力来掩盖配置错误。只有启用 legacy compatibility
profile 时，才按第 21 节执行有界 fallback。

### 6.5 多轮请求

Server 不向 Client 发起独立 JSON-RPC request。需要更多输入时返回 `InputRequiredResult`。平台重试
原始方法时必须：

- 使用新的 JSON-RPC request ID；
- 原样带回受保护的 `requestState`；
- 只提交当前 outstanding key 对应的 `inputResponses`；
- 在本地 schema 和用户 action 校验通过后发送；
- 对重复、过期、未知或已闭合 interaction fail closed。

## 7. 目标架构与 crate 边界

```text
Platform Config / OAuth Principal / Agent Revision
                         │
                 McpConnectionCatalog
                         │
              ┌──────────┴──────────┐
              │ insight-mcp         │
              │ codec / transport   │
              │ auth / discovery    │
              └──────────┬──────────┘
                         │
       ┌─────────────────┼──────────────────┐
       │                 │                  │
 McpToolProvider  McpResourceProvider  McpPromptProvider
       │                 │                  │
 Action / LLM Tool   Retrieval/Artifact   Prompt Catalog
       └─────────────────┼──────────────────┘
                         │
              Durable MCP Continuation
          interaction / remote task / retry
                         │
          PostgreSQL / SQLite + Artifact Store
                         │
         RunService / run-stream/v2 / HTTP API
                         │
              MCP Server export adapter
```

### 7.1 `insight-mcp`

新增 workspace crate `crates/mcp`，package 名为 `insight-mcp`，只拥有协议与 transport：

- vendor-derived core wire types 与规范允许的有界 extension/meta maps；
- version/capability negotiation；
- JSON-RPC request/response correlation；
- stdio 与 Streamable HTTP framing；
- subscriptions、progress、cancellation 和 multi-round request driver；
- Authorization discovery、PKCE、token exchange/refresh 的协议客户端；
- client/server conformance helpers；
- body/frame/schema/depth limits。

它不依赖 `insight-runtime`、数据库、Agent DSL 或具体 Action。它不能决定 effect、idempotency、
public policy、tenant principal 或 Deployment Revision。

### 7.2 `insight-resources`

`insight-resources` 增加：

- `McpToolProvider`；
- `McpResourceProvider`；
- `McpPromptProvider`；
- remote content 到平台 typed content 的无状态转换；
- tool/resource/prompt descriptor normalization；
- result schema、MIME、URI 和 content bounds 校验。

Resource 层不持久化 OAuth token、interaction、remote task 或 subscription cursor。

### 7.3 `insight-engine`

`insight-engine` 增加无 I/O 合同：

- `LeafExecutionDisposition::{Complete, InputRequired, RemoteTask}`；
- `ExternalInteraction`、`InteractionMode`、`InteractionOutcome`；
- remote task 状态机和 first-winner action；
- MCP content、descriptor、binding 和 safe public projection DTO；
- `run-stream/v2` interaction 事件与 terminal snapshot 投影；
- terminal-only compatibility verifier 所需 capability facts。

这些类型不得导入 HTTP、OAuth、stdio 或数据库实现。

### 7.4 `insight-durable`

`insight-durable` 拥有：

- interaction/task repository ports、commands、claims、receipts；
- interaction reply/cancel/expire first-winner；
- remote task poll lease/fence；
- retry request identity 与 committed response receipt；
- OAuth credential metadata、transaction metadata 和 privacy deletion command；
- cache invalidation hint，不把 subscription 当执行权威。

### 7.5 `insight-storage`

`insight-storage` 为 SQLite/PostgreSQL 实现同一逻辑合同：

- MCP interaction；
- MCP remote task；
- OAuth authorization transaction 和加密 credential reference；
- publication snapshot metadata；
- inbox/outbox 与 public projection；
- retention、privacy deletion 和 orphan secret cleanup。

生产多 runtime claim/lease、通知和恢复仍只承诺 PostgreSQL。

### 7.6 `insight-runtime`

`insight-runtime` 负责：

- config/catalog 解析和 publication-time discovery；
- MCP binding freeze 与 Deployment Revision digest；
- worker 调用、interaction suspend/resume 和 remote task polling；
- principal credential 解析；
- subscription manager 与 cache invalidation；
- Agent export 到 MCP Server result/task；
- readiness、maintenance 和 shutdown。

### 7.7 `insight-api` 与根 composition

`insight-api` 增加：

- MCP interaction、connection 和 authorization API；
- `run-stream/v2` transport；
- `/mcp` Server endpoint；
- OAuth callback 与 protected resource metadata 路由；
- MCP principal 到 tenant/user 的映射。

根 package 只组合具体 transport、store、registry 和 policy，不重新导出秘密值或协议内部状态。

## 8. 身份、Catalog 与 Revision 冻结

### 8.1 Server binding identity

每个配置 Server 使用平台拥有的 `server_id`。binding evidence 至少包含：

- `server_id`；
- transport kind；
- canonical HTTP endpoint，或 stdio executable/args/working-directory digest；
- protocol profile 与选择的 protocol version；
- authorization mode 和非秘密 scope names；
- `server/discover` capability snapshot hash；
- self-reported `serverInfo` 证据；
- transport、request、content 和 interaction limits；
- adapter/worker version。

HTTP URL query、Authorization header、token、stdio secret env value 和 PKCE material不得进入 evidence。

### 8.2 Tool identity

冻结的 MCP Tool binding 至少包含：

- `server_id`；
- `remote_tool_name`；
- 平台 `action_id`；
- 模型可见 `model_tool_name`；
- title/description 的冻结安全副本；
- input/output schema 与 schema draft；
- descriptor hash；
- effect、idempotency、cancellation、required capability；
- approval、input-required、Tasks 和 public policy；
- HTTP `x-mcp-header` 合同；
- discovery snapshot hash。

MCP Tool 没有平台可依赖的 SemVer。平台使用 descriptor hash 和 server binding digest 形成精确版本
身份，不能用 remote Server 自报版本替代。

### 8.3 Resource identity

Resource binding 冻结 Server、URI 或 URI template、MIME allowlist、size policy、public policy 和
authorization mode。Resource 内容可以变化；每次 read 必须记录：

- canonical URI；
- observed MIME/type；
- 内容 hash；
- 读取时间；
- server binding；
- principal-scoped authorization identity 的非秘密引用。

需要复现的内容必须内联或写入 Artifact，并在 Activation result 中保存内容引用。

### 8.4 Prompt identity

Prompt 有两种互斥使用方式：

1. **Definition import**：发布时执行 `prompts/get`，把完整 messages、arguments、Server binding 和
   内容 hash 写入 Definition Revision；后续 list change 不影响该 revision；
2. **User invocation**：用户在 admission 前显式选择 Prompt，平台获取结果并把完整快照固定到该
   Run 的输入/context evidence，不修改 Agent Definition。

禁止在已经开始的 LLM node 内根据自然语言自动选择任意 MCP Prompt。

### 8.5 List change

`notifications/tools/list_changed`、`prompts/list_changed`、`resources/list_changed` 的处理只有：

- 使对应 principal/cache scope 的 Catalog cache 失效；
- 提交不含业务正文的 reconcile hint；
- 标记受影响的 publication candidate 为 stale；
- 按运维策略报警或重新编译候选 revision。

已经发布或正在执行的 revision 绝不原地变更。自动发布默认关闭；即使启用，也必须重新完成 parse、
schema normalization、policy review、link、hash 和完整 publication transaction。

### 8.6 Principal-scoped discovery

MCP list 结果可以因每请求 authorization 不同而变化，但 executable Plan 不能因当前 user 动态改写。
对 `oauth_user` Server：

- publication 必须使用平台受控 authoring credential 完成 discovery，或使用经过签名/审核的
  checked-in import manifest；
- authoring credential 只建立 descriptor，不代替运行用户 credential；
- 运行时用户授权集合可以是冻结 Catalog 的子集；无权调用时返回 auth/operation outcome；
- 运行时 `tools/list` 不能新增、替换或改变 LLM tool descriptor；
- 没有 authoring credential/manifest 时，只允许 principal-scoped Catalog 浏览，不允许把结果链接进
  Deployment Revision。

这样既保留 MCP 按 authorization 过滤 list 的语义，也避免为每个用户生成不可管理的 Agent revision。

## 9. 平台配置

### 9.1 顶层结构

目标配置使用严格、版本化的 `mcp` 顶层对象：

```yaml
mcp:
  version: 1

  protocol:
    preferred: "2026-07-28"
    legacy_fallback: ["2025-11-25"]

  client:
    enabled: true
    servers:
      engineering:
        transport:
          type: streamable_http
          endpoint: https://mcp.example.com/mcp
        discovery:
          type: signed_manifest
          path: ../mcp/engineering-tools.json
        authorization:
          type: oauth_user
          scopes: [repo.read]
          client_id: insight-agent-platform
          redirect_uri: https://agents.example.com/v1/mcp/oauth/callback
        imports:
          tools:
            - remote: search_repositories
              as: engineering_search_repositories
              effect: read_only
              idempotency: idempotent
              cancellation: cooperative
              approval: never
              input_required: allowed
              tasks: allowed
              public:
                call: true
                arguments: private
                progress: none
                result: none
          resources:
            allow:
              - repo://*
          prompts:
            allow: [review_change]
        limits:
          connect_timeout: 5s
          request_timeout: 2m
          max_request_bytes: 1048576
          max_response_bytes: 16777216
          max_content_items: 128

  server:
    enabled: true
    endpoint: /mcp
    authorization:
      type: oauth_resource_server
      resource: https://agents.example.com/mcp
      authorization_servers:
        - https://identity.example.com
      required_scopes: [mcp.invoke]
    exports:
      agents:
        - agent: researcher
          as: researcher
          execution: task_preferred
          input_required: allowed
          required_scope: mcp.invoke
      resources: []
      prompts: []
```

所有对象 `deny_unknown_fields`。`mcp.version != 1`、未知 protocol、空 alias、重复 export/import、
未声明 secret、无界 timeout 或无界 body limit 必须在启动时失败。

### 9.2 Server ID

`server_id` 来自 `client.servers` mapping key：

- 使用小写字母、数字、`-`，长度 1～64；
- 在单个平台配置中唯一；
- 进入 Deployment Revision identity；
- 不从 endpoint host、stdio command 或自报 `serverInfo.name` 推导；
- 重命名是显式 breaking binding change。

### 9.3 Streamable HTTP transport

HTTP transport 配置至少包含：

- HTTPS endpoint；
- connect/request/idle timeout；
- request/response/frame/content bounds；
- redirect policy，默认拒绝；
- DNS/private-network policy；
- TLS trust policy；
- authorization mode；
- subscription reconnect/backoff bounds。

生产默认只允许 HTTPS。明文 HTTP 只允许 loopback/single-process development 显式 opt-in，并进入
binding evidence。URL 禁止 userinfo 和 fragment；query 不进入 identity、日志或错误，生产默认拒绝
静态 query credential。

### 9.4 stdio transport

stdio 使用结构化 argv，不通过 shell：

```yaml
transport:
  type: stdio
  executable: /opt/mcp/bin/repository-server
  args: ["--mode", "readonly"]
  working_directory: /srv/mcp/repository
  environment:
    REPOSITORY_TOKEN:
      secret_env: REPOSITORY_TOKEN
  isolation:
    profile: repository-readonly
```

约束：

- executable 必须是绝对路径或命中部署 allowlist；
- environment 默认清空，只注入显式变量；
- secret 只允许引用，不能在 YAML 写明文；
- stdout 只承载 MCP frame；非协议输出使连接失败；
- stderr 有独立大小/频率限制，默认不进入正文日志；
- 子进程具有 startup、request、idle、shutdown 和 restart backoff；
- 取消/超时后终止请求，必要时终止整个进程树；
- production 必须声明可审计 isolation profile；缺失时 readiness 失败；
- stdio Server crash 不能让 runtime 进程崩溃。

平台不承诺本地 stdio Server 进程内 handle 在进程重启或跨 runtime worker 后仍有效。Server 必须按
MCP 约定对未知/过期 handle 返回可恢复的 tool execution error。

### 9.5 Authorization modes

Client 允许：

| 模式 | 用途 | secret 所有权 |
|---|---|---|
| `none` | 公开或本地 Server | 无 |
| `bearer_env` | 部署 service account | 部署 secret reference |
| `oauth_user` | 代表 tenant/user 调用 | 加密 credential store |

stdio 不执行 HTTP Authorization flow，凭据通过显式 environment secret reference 注入。

Server 允许：

- `disabled`：只允许 single-process development loopback；
- `bearer_compatible`：兼容现有受控部署，但不能宣称完整 MCP Authorization；
- `oauth_resource_server`：生产完整 profile，发布 protected resource metadata、验证 token audience/
  scope，并映射到 tenant/user principal。

### 9.6 Import policy

远程 list 结果从不自动变为可执行工具。每个 import 必须有显式 allowlist 和本地 policy。

`discovery` 必须选择：

- `live_service_account`：使用部署 secret reference 获取 publication descriptor；
- `signed_manifest`：加载由管理流程生成并签名的完整 discovery/list snapshot。

`oauth_user` 不能直接充当 publication discovery credential。

Signed manifest 必须包含 `server_id`、protocol、discover result、完整分页 list、生成/过期时间、
canonical content hash、signer key ID 和 signature。信任根来自部署配置；manifest 不包含 access
token、Authorization header 或用户正文。签名、过期或 Server ID 不匹配时 publication 失败。

Tool policy 必须声明：

- remote name 与平台 alias；
- effect、idempotency、cancellation；
- required capabilities；
- LLM 可见 description 采用 remote、override 或禁用；
- approval；
- 是否允许 `input_required`；
- 是否允许 Tasks；
- public projection；
- 可选 idempotency injection 合同；
- terminal-only compatibility。

Remote annotation 只能产生 review diagnostic，不能填充缺失 policy。

## 10. MCP Tool 支持

### 10.1 发现和导入

发布流程：

1. 解析 Server config 与 credential binding；
2. 调用 `server/discover` 并选择协议；
3. 分页读取完整 `tools/list`；
4. 应用 import allowlist；
5. 校验 name、description、input/output schema、icons、annotations 和 `x-mcp-header`；
6. 生成平台 alias；
7. 叠加本地 effect/idempotency/cancellation/approval/public policy；
8. 规范化 descriptor 并计算 hash；
9. 注册 `McpToolProvider`；
10. 在 Deployment Revision 中冻结完整 binding。

list 顺序不参与 identity；规范化后按 remote name 字节序排序。重复 name、分页 cursor 循环、cache
scope 变化或分页期间 list version 不稳定都会使发布失败。

### 10.2 名称和 description

Remote tool name 原样保留用于 `tools/call`。模型可见 alias：

- 长度 1～64；
- 只含 ASCII 字母、数字、`_`、`-`；
- 在单个 LLM node 白名单内唯一；
- 不从 Server 自报名称隐式生成安全 namespace；
- 若自动建议 alias，最终值仍必须写入配置或生成的受审 import manifest。

Description 是模型决策输入，必须冻结并受长度、控制字符和 Unicode normalization 约束。默认最多
4096 UTF-8 bytes。Server instructions 不自动拼接到每个 tool description。

### 10.3 JSON Schema

完整 Tool 支持至少接受 MCP 允许的 JSON Schema Draft 2020-12 与 Draft 7：

- schema 必须是 object；
- input root 必须接受 object 参数；
- `$ref` 默认只允许文档内引用；
- 不在发布时通过网络获取 remote schema；
- 限制 schema bytes、depth、property count、regex length 和 validator complexity；
- `x-mcp-header` 只允许规范定义的 primitive、静态可达 property；
- header name/value 在 transport 前再次验证；
- secret/PII property 默认禁止通过 `x-mcp-header`；
- output schema 存在时必须验证 `structuredContent`；
- output schema 缺失时仍使用平台闭合 `McpToolResultEnvelope`。

一个非法工具不能污染其他工具。显式 import 指向非法工具时 publication fail closed；纯浏览 API
可以返回该工具被拒绝的 body-free diagnostic。

### 10.4 执行结果

平台内部使用闭合结果：

```text
McpToolResultEnvelope {
  is_error,
  content[],
  structured_content?,
  observed_output_schema_hash?,
  remote_metadata_safe
}
```

`content` 支持：

- text；
- image；
- audio；
- resource link；
- embedded text/binary resource。

大 image/audio/binary 写入 Artifact store；Run result 只保存受租户隔离的引用。MIME、decoded
bytes、item count、单 item 和总响应大小全部有硬上限。

LLM continuation 对内容执行 provider capability projection：

- text 与合法 structured content 可以直接进入模型 tool result；
- image/audio 只有冻结模型 request capability 支持时才发送；
- 不支持的二进制内容转换为不含原始正文的 typed artifact/resource reference；
- 任何投影都不能把 private content 送入公开 Run stream。

### 10.5 `isError` 与错误分类

MCP `isError: true` 是 tool execution result，不是 JSON-RPC protocol error：

- LLM-originated call：发布安全的 `run.tool.failed`，并把经大小和隐私处理的 actionable result
  返回模型继续推理；
- 直接 workflow Action：转换为 catchable operation failure；
- 禁止把 remote raw body 直接用作 Run terminal error。

JSON-RPC error、transport failure、auth failure、schema mismatch 和 cancellation 分别映射到平台闭合
错误类。已冻结工具在运行时返回 unknown tool 或不兼容 schema，视为 binding drift/infrastructure
failure，不自动改绑到同名新工具。

### 10.6 副作用和幂等

Remote annotation 中的 `readOnlyHint`、`destructiveHint` 等只用于 review。平台 policy 才决定：

- `Pure | ReadOnly | Mutating`；
- `Idempotent | NonIdempotent`；
- `Cooperative | LeaseOnly` cancellation；
- retry 上限；
- approval 和 durable fence requirement。

默认未分类 MCP Tool 是 `Mutating + NonIdempotent + LeaseOnly + approval required`。发布可以拒绝
缺少显式分类的 import。

MCP 不提供通用 idempotency key。若 Server 有明确合同，配置可把
`ActionContext.idempotency_key` 注入：

- 已声明的 input property；
- 规范允许且已冻结的 `x-mcp-header`；
- 受审 transport extension。

没有明确合同不得猜测注入位置。远程成功、本地 commit 前 crash 仍可能重复副作用，文档和 risk
diagnostic 必须保持 at-least-once 表述。

### 10.7 Approval

`approval` 支持：

- `never`；
- `model_tool_only`；
- `mutating`；
- `always`。

Approval 在发送 remote request 前形成 durable interaction。批准内容包含 Server、tool、effect 和
安全参数摘要，不包含默认私有参数正文。Run cancel、deadline、decline 和 approve 使用 first-winner。

terminal-only 只能绑定 `approval: never`。

### 10.8 Progress 与 cancellation

request-scoped progress 默认私有。显式 public policy 只允许规范化的数值进度：

```json
{"progress": 30, "total": 100}
```

remote message 默认剥离，除非通过冻结的安全 schema。公开进度继续是 best-effort、live-only，不能
决定 Action 结果。

取消规则：

- stdio 发送 `notifications/cancelled`；
- Streamable HTTP 关闭 request response stream；
- remote Tasks 调用 `tasks/cancel`；
- cancellation 是意图，不承诺 Server 已停止；
- local terminal first-winner 后的迟到 remote completion 不能覆盖权威结果。

## 11. MCP Resource 支持

### 11.1 定位

Resource 是 application-controlled context，不自动成为 LLM tool。平台提供三种显式入口：

1. Agent `retrieval` binding 读取固定 URI/template；
2. admission API 由用户选择 Resource 并固定到 Run context；
3. MCP Server export 公开受授权的平台 Resource。

禁止因为 Server 声明 resources capability 就把所有 Resource 自动加入每次模型上下文。

### 11.2 List、template 和 completion

平台支持：

- paginated `resources/list`；
- paginated `resources/templates/list`；
- URI template 参数校验；
- `completion/complete` 的 resource reference；
- per-principal/cache-scope cache；
- list changed 和 resource update subscription。

Completion 结果只用于交互式建议，不直接执行读取。值数量、字符串长度、cache key 和 rate 都有上限。

### 11.3 Read 与内容处理

每次 `resources/read`：

- 重新执行 principal authorization；
- 校验 URI scheme、canonicalization 和 import allowlist；
- 校验 MIME 与 content bounds；
- text 使用明确 encoding，拒绝非法 UTF-8 或按 MIME 规则处理；
- binary 写入 Artifact；
- embedded URI 不能绕过原 Server binding 和 tenant policy；
- 记录内容 hash 和 binding evidence；
- 失败不回退到未授权的缓存副本。

`file://` 只允许 stdio/local Server 且必须受部署 filesystem policy；remote HTTP Server 返回的
`file://` URI 不意味着平台本机文件访问权。

### 11.4 Subscription

`subscriptions/listen` 是非权威 hint channel：

- reconnect 使用有界 exponential backoff；
- 断线不删除 cache 中仍在 TTL 内的内容；
- update 只使对应 URI/cache key 失效；
- durable Run 已固定的 Artifact 内容不被 update 改写；
- notification 丢失最终由 TTL/read 校验恢复；
- subscription stream 不占 operation permit。

## 12. MCP Prompt 与 Completion 支持

### 12.1 User-controlled 原则

MCP Prompt 只能由以下动作触发：

- Agent 作者在 Definition 中显式 import；
- 用户通过 API/UI 显式选择；
- 运维创建受审的 Prompt export。

LLM 不能仅凭 Server instructions 自动调用 `prompts/get`。Prompt 内容按不可信外部输入处理，不能
覆盖平台 system policy、tenant policy、tool allowlist 或 public policy。

### 12.2 Prompt Catalog API

Host 提供：

- 按 Server 和 principal 分页列出 Prompt；
- 获取参数 schema/说明；
- 调用 `completion/complete`；
- 预览 `prompts/get` 结果；
- 将结果显式固定到 Definition 或 Run。

Prompt message 中的 text/image/audio/resource link/embedded resource 使用与 Tool content 相同的
大小、Artifact、MIME 和公开策略。

### 12.3 Server instructions

`server/discover.instructions` 默认只在管理/调试 API 展示，不进入 system prompt。若 Agent 显式引用：

- 内容必须固定到 revision；
- 使用独立、标记为 untrusted 的 message boundary；
- 受长度和 content policy 限制；
- 不能声明或扩大工具权限；
- list/discovery refresh 不改变旧 revision。

## 13. Durable Elicitation

### 13.1 与 human task 的边界

MCP interaction 与 human task 使用不同公开资源：

| 对象 | 发起者 | 目的 | claim 模型 |
|---|---|---|---|
| Human task | Agent 作者 | 业务审批/人工工作 | candidate/claim fence |
| MCP interaction | 远程 MCP Server 或平台 Tool policy | 完成一个协议请求 | tenant/user principal + interaction version |

底层 first-winner、deadline 和审计 helper 可以共享，但 wire、权限和 DTO 不能混用。

### 13.2 状态机

```text
Requested
   ├─ accept  ─→ Responded ─→ Retrying ─→ Closed
   ├─ decline ─────────────────────────→ Closed
   ├─ cancel  ─────────────────────────→ Closed
   ├─ expire  ─────────────────────────→ Closed
   └─ Run terminal/timeout ────────────→ Closed
```

`Responded` 与 retry request receipt 必须事务关联。Crash 后可以重新领取 retry，但相同 interaction
response 只能产生一个 committed MCP retry result。

### 13.3 Form mode

Form mode：

- 保存并验证规范限制的 requested JSON Schema；
- 只接受 flat object，以及 string、number、integer、boolean、单选 enum 和 enum multi-select array；
- 只接受规范列出的 string format 和 primitive constraints，拒绝嵌套 object、普通 object array、
  remote reference 和其他高级 JSON Schema；
- 禁止 password、API key、access token、payment credential 等 secret；
- UI 必须显示请求 Server、message、字段和用途；
- 用户可以 review、修改、accept、decline 或 cancel；
- response 只对原 Server 和原 outstanding key 可见；
- 正文加密存储，并按 interaction retention 删除；
- 公开事件只包含 interaction identity/status，不包含字段值。

### 13.4 URL mode

URL mode：

- 解析并展示 scheme、canonical host、port；
- 默认只允许 HTTPS；
- 拒绝 userinfo、控制字符、危险 scheme 和隐式 redirect；
- 导航前取得用户 consent；
- URL 中的敏感 query 不进入日志、revision、事件或 terminal snapshot；
- 完成信号必须通过受保护的 callback/state 或明确用户 action；
- URL mode 不能被降级成 form mode 以穿过平台收集 secret。

### 13.5 request state

MCP `requestState` 是 remote Server 提供的不透明 continuation：

- 有严格 byte limit；
- 以 tenant/run/server/operation scope 加密保存；
- 不反序列化为业务对象；
- 不进入 Debug、metrics、trace、public API 或错误；
- retry 时原样返回；
- interaction terminal 或 retention 到期后安全删除。

### 13.6 API

新增：

| 方法 | 路由 | 语义 |
|---|---|---|
| `GET` | `/v1/mcp/interactions?run_id=&status=&limit=&cursor=` | 列出当前 principal 可见 interaction |
| `GET` | `/v1/mcp/interactions/{interaction_id}` | 获取安全请求详情 |
| `POST` | `/v1/mcp/interactions/{interaction_id}/respond` | accept 并提交 schema-valid 内容 |
| `POST` | `/v1/mcp/interactions/{interaction_id}/decline` | 显式拒绝 |
| `POST` | `/v1/mcp/interactions/{interaction_id}/cancel` | 取消 |

所有 mutation 要求 `X-Request-ID`，repository 使用 interaction version/fence 防止重复和迟到覆盖。

### 13.7 `run-stream/v2`

`run-stream/v2` 保留 v1 的 25 个事件语义，并增加：

- `run.interaction.required`；
- `run.interaction.closed`。

`required` 只携带 `interaction_id`、source kind、Server ID、mode、safe message、URL host、deadline
和详情 API link。`closed` 只携带 outcome，不携带 response body。

terminal Run snapshot 增加安全 `interactions[]` 状态摘要。v1 不增加 alias 或未知新事件。实现阶段
对受控客户端执行一次明确协议升级，并同步 discovery、schema、baselines 和 current 文档。

## 14. MCP Tasks extension

### 14.1 协商

Tasks 只有在以下条件同时满足时启用：

- client request capabilities 声明 `io.modelcontextprotocol/tasks`；
- `server/discover` 声明相同 extension；
- Server config/import policy 允许 Tasks；
- 当前 persistence mode 支持该任务预期的等待语义。

未协商时，Client 不能接受 `resultType: task`，Server 不能返回 task。

### 14.2 Client 映射

收到 `CreateTaskResult` 后，平台事务写入：

- tenant/run/operation；
- Server binding；
- opaque remote `taskId`；
- status、TTL、poll interval；
- remote task capability/version；
- poll lease/fence；
- terminal receipt；
- 可选 outstanding input requests。

`taskId` 视为敏感 opaque handle，不进入公开日志。poll 使用 WorkCoordinator 的 wait/deadline，不在
等待期间占 operation permit。

支持：

- `tasks/get`；
- `tasks/update`；
- `tasks/cancel`；
- `notifications/tasks/status`；
- TTL expiration；
- polling 与 notification first-observed convergence；
- crash/restart 后继续 poll；
- terminal result exactly-once local commit。

Remote task 本身是否恰好执行一次由 Server 负责；平台只保证本地 terminal receipt 不重复提交。

### 14.3 Input required

Tasks `input_required` 复用第 13 节 durable interaction，但：

- response 经 `tasks/update` 提交，而不是重试原始 `tools/call`；
- outstanding keys 与 remote task version 绑定；
- task 回到 `working` 后 interaction 闭合；
- 同一 task 可以顺序产生多个 interaction generation；
- 每代 response、cancel、expire 继续使用 first-winner。

### 14.4 Server 映射

平台作为 MCP Server 时，长时间 Agent Run 默认使用 Tasks：

- `tools/call` admission 成功后先 durable 创建 Run，再返回 task handle；
- `tasks/get` 从权威 Run/interaction 状态构建 task；
- `tasks/update` 路由到对应 MCP interaction 或显式允许的 Agent input；
- `tasks/cancel` 提交 Run cancel intent；
- terminal Run 投影为 completed/failed/cancelled；
- progress notification 只投影已授权的安全 Run activity。

若 client 未声明 Tasks：

- `execution: synchronous` 的 export 可以在配置的短超时内等待；
- `execution: task_required` 直接返回 capability error；
- 不在超时后悄悄把同一次调用转成后台 Run；
- client disconnect 对 synchronous 调用按 export policy 提交 cancel intent。

## 15. HTTP Authorization 与 Credential

### 15.1 Client authorization flow

`oauth_user` 必须实现当前 MCP Authorization 规定的：

- protected resource metadata discovery；
- authorization server metadata discovery；
- authorization code + PKCE；
- state/issuer/redirect URI 验证；
- client ID metadata 或受支持的 registration；
- resource indicator；
- access token audience/scope 检查；
- refresh token rotation；
- insufficient scope challenge；
- step-up authorization；
- revoke/disconnect 和 privacy deletion。

浏览器只接触 authorization URL 和 callback；token exchange 在服务端完成。

### 15.2 Credential store

credential key 至少包含：

- tenant ID；
- user ID；
- MCP `server_id`；
- authorization issuer；
- client identity；
- granted resource/scope set；
- credential generation。

access/refresh token 使用独立、版本化的 envelope encryption keyring。它不能复用“把任意 secret 当
Artifact”的普通读取 API，也不能被 Artifact endpoint 取回。

数据库只保存：

- ciphertext；
- key version；
- token type；
- expiry；
- scope/resource 的非秘密事实；
- rotation/revocation metadata；
- content hash/AEAD associated-data evidence。

Debug 必须 body-free；metrics 只能按 Server ID、结果类和 principal kind 聚合。

### 15.3 OAuth transaction

authorization transaction 保存 state hash、PKCE verifier ciphertext、issuer、Server、principal、
redirect target 和 expiry。约束：

- state 单次消费；
- callback 与发起 principal/tenant 绑定；
- 过期或重复 callback 失败；
- callback error 不回显 token、code 或完整 query；
- 未完成 transaction 定期回收；
- 登录 CSRF、open redirect、issuer mix-up 和 code substitution 有负向测试。

### 15.4 Scope 与 publication

Deployment Revision 只冻结要求的 scope name，不冻结 token 或实际用户 grant。运行时：

- principal 无 credential：进入 authorization interaction 或返回 auth required；
- scope 不足：进入 step-up interaction；
- 用户 decline：当前 operation 以可预测的 operation outcome 收敛；
- credential 被撤销：不回退到 service account；
- 不允许一个用户的 list/cache/tool 结果被另一个用户复用。

### 15.5 平台作为 OAuth protected resource

生产 MCP Server profile：

- `/mcp` 对应 canonical resource URI；
- 提供 protected resource metadata；
- 发现或配置可信 authorization server；
- 验证 issuer、signature、audience、expiry、scope；
- 把 subject/claims 映射到平台 tenant/user；
- tool/resource/prompt exports 各自要求显式 scope；
- MCP Authorization 不绕过现有 tenant ownership。

平台可以依赖外部 IdP，不要求自身成为通用 authorization server。

## 16. MCP Server profile

### 16.1 独立 endpoint

MCP Server 使用 `/mcp`，不伪装成 `/v1` REST，也不把现有 Run SSE 当 MCP Streamable HTTP。
endpoint：

- 支持 modern per-request metadata；
- 校验 `MCP-Protocol-Version`、`Mcp-Method`、`Mcp-Name` 和 body；
- 每次请求重新执行 auth/principal/capability；
- request-scoped SSE 与平台 Run SSE 相互独立；
- 不创建 modern HTTP session。

### 16.2 Discovery

`server/discover` 返回：

- 平台实现名和版本；
- 精确协议版本；
- 当前 principal/config 可用 capability；
- 配置的安全 instructions；
- cache TTL/scope；
- Tasks 等 extension。

结果可因每请求 authorization 不同而变化，但不能因同一请求外的连接内隐式状态变化。

### 16.3 Agent Tool export

每个 export 显式声明：

- `agent_id`；
- MCP name/title/description；
- input/output schema projection；
- required OAuth scope；
- sync/task execution policy；
- public result projection；
- Conversation 是否允许以及 principal 绑定；
- max Run timeout、result bytes 和 Artifact link policy。

MCP Tool input 通过 Agent public input schema 校验，再用普通 Run admission 创建固定 revision Run。
Agent typed output 进入 `structuredContent`；同时提供规范要求的兼容 text content，但 text 序列化必须
稳定、受限且不能泄漏 private field。

Synchronous export 遇到显式允许公开的 `ExternalInteraction` 时：

- client 已声明 Elicitation capability：返回 `InputRequiredResult`，平台生成与原 Run/operation
  绑定的 opaque request state；client 使用新 request ID 重试同一 `tools/call` 后恢复该 Run；
- client 未声明 Elicitation：返回闭合 capability error，不能把 Run 留成无人可完成的 wait；
- task export：改由 task `input_required` 与 `tasks/update` 处理。

现有业务 human task 不自动投影为 MCP Elicitation。只有 export 明确声明 candidate/principal、
公开 schema 和交互映射时才允许转换。

### 16.4 Action export

Action 默认不直接导出。若显式允许：

- 仍通过平台 Run/operation authority 执行；
- 不能绕过 capability、effect、approval 或 public policy；
- secret injection 保持 server-only；
- mutating/non-idempotent Action 必须有额外 scope 和 risk diagnostic；
- 不能因为 MCP caller 提供同名参数而覆盖 server-only 值。

### 16.5 Resource export

只允许显式 provider/URI mapping。Run、Conversation、Artifact 和 trace 不自动成为 Resource。

Artifact link：

- 使用短期、principal-bound URI 或 MCP read indirection；
- 不能暴露本地 filesystem path；
- 不能把 tenant object hash 当 bearer capability；
- scope、MIME、size 和 retention 每次 read 重新验证。

### 16.6 Prompt export

只导出专门定义的公共 Prompt，不导出 Agent 内部 system prompt、policy、隐藏 instructions 或
Conversation summary。Prompt 参数有闭合定义，结果经过内容和资源安全检查。

### 16.7 Completion 和 subscriptions

Server 对已导出的 Prompt/Resource template 提供 completion，并：

- 最多返回 100 项；
- rate limit；
- principal-scoped cache；
- 防止 completion 侧信道枚举无权资源。

export Catalog 发生变化时，只有成功提交的新 publication/config generation 才发送 list changed。
通知不是 export authority；订阅者重连后通过 list/read 恢复。

## 17. Persistence、恢复与竞态

### 17.1 逻辑记录

目标 schema 至少包含下列逻辑对象；具体表名可在实施计划中调整，但权威关系不能改变：

| 对象 | 权威内容 |
|---|---|
| MCP publication snapshot | Server/tool/resource/prompt binding hash、协议和 capability evidence |
| MCP credential | principal-scoped encrypted token 与 generation |
| OAuth transaction | 单次 state/PKCE/callback 状态 |
| MCP interaction | request、mode、version、deadline、outcome |
| MCP interaction secret | requestState、form response、URL sensitive state ciphertext |
| MCP remote task | task ID ciphertext、status、TTL、poll lease/fence、terminal receipt |
| MCP cache metadata | scope key、TTL、descriptor/content hash |

Tool/Resource/Prompt 大正文继续进入 Artifact 或受控 encrypted object，不在 hot scheduler row 重复保存。

### 17.2 Interaction first-winner

以下动作竞争同一个 interaction terminal：

- accept/respond；
- decline；
- cancel；
- expiry；
- Run timeout/cancel/terminal；
- OAuth transaction expiry；
- remote task terminal。

数据库事务决定唯一赢家。迟到动作返回闭合 conflict/capability response，不能重新打开 interaction。

### 17.3 Retry identity

每次 MCP wire request 有新的 JSON-RPC ID；平台另有稳定 logical request identity：

```text
run_id + operation_id + interaction_generation + protocol_method
```

它用于本地去重、receipt 和恢复，但不伪装成 remote idempotency key。

### 17.4 Crash windows

必须覆盖：

1. remote complete，本地 commit 前 crash；
2. interaction response commit，retry send 前 crash；
3. retry complete，本地 commit 前 crash；
4. remote task create，本地 task receipt 前 crash；
5. poll terminal，本地 terminal commit 前 crash；
6. cancellation send，本地 cancel first-winner 前后 crash；
7. OAuth refresh 成功、本地 credential rotation commit 前 crash；
8. subscription notification 与 cache invalidation commit 竞态。

测试必须证明不会出现两个本地 terminal authority、secret 明文或未绑定 response。远程副作用重复风险
按 at-least-once 明确保留。

### 17.5 Full persistence

full Run：

- interaction/task wait 不占 operation permit；
- remote task poll 使用有界 claim/lease/fence；
- process restart 后从数据库恢复；
- Run deadline、cancel 和 remote terminal first-winner；
- immutable binding 在恢复时重新验证；
- credential generation 变化不改变 revision，但每次调用重新授权；
- remote binding drift fail closed。

### 17.6 Terminal-only

terminal-only publication 只允许 MCP binding 同时满足：

- `input_required: forbidden`；
- `tasks: forbidden`；
- `approval: never`；
- 不依赖 OAuth redirect/step-up；
- request timeout 小于剩余 terminal execution budget；
- 不要求 durable effect fence；
- stdio process lifecycle 在 owner budget 内闭合。

若 Server 违反冻结声明并返回 input-required/task，当前 Run 以
`MCP_TERMINAL_ONLY_INTERACTION_UNSUPPORTED` 失败，不隐式切换 full。

## 18. Public API 与 Run Stream

### 18.1 Connection/Catalog API

目标 API：

| 方法 | 路由 | 说明 |
|---|---|---|
| `GET` | `/v1/mcp/servers` | 当前 principal 可见 Server 与 capability |
| `GET` | `/v1/mcp/servers/{server_id}/tools` | paginated tool catalog |
| `GET` | `/v1/mcp/servers/{server_id}/resources` | paginated resource catalog |
| `GET` | `/v1/mcp/servers/{server_id}/prompts` | paginated prompt catalog |
| `POST` | `/v1/mcp/servers/{server_id}/completion` | prompt/resource argument completion |
| `POST` | `/v1/mcp/servers/{server_id}/authorize` | 创建 OAuth authorization transaction |
| `GET` | `/v1/mcp/connections` | 列出 principal credential 状态，不含 token |
| `DELETE` | `/v1/mcp/connections/{server_id}` | 撤销并删除 principal credential |

管理端的 import/republication API 必须与普通 user catalog API 分权，不能让任意 user 把动态工具加入
生产 Agent。

### 18.2 Interaction API

第 13.6 节 API 是 durable authority。普通 Run signal API 不能代替 interaction response。

### 18.3 Agent discovery

Agent discovery 增加：

- MCP Server/tool/resource/prompt capability 的安全摘要；
- 是否可能产生 interaction；
- 是否支持 remote Tasks；
- 要求的 connection/server IDs 和 scope names；
- `full|terminal_only` compatibility；
- streaming protocol `run-stream/v2`。

不公开 endpoint query、stdio argv secret、token、requestState 或 remote raw instructions。

### 18.4 Terminal snapshot

Run terminal snapshot：

- 保留现有 tool result、retrieval 和 typed result；
- MCP tool result 只按 frozen public policy 投影；
- Resource 内容只按 Retrieval/Artifact authorization 投影；
- interaction 只保存 identity、mode、outcome 和时间；
- remote task 只保存 safe status/duration，不保存 opaque task ID；
- OAuth/credential 事实最多保存 `authorization_required|authorized|declined`，不保存 scope grant 正文。

## 19. 安全与隐私

### 19.1 Trust boundary

以下全部是不可信输入：

- Server capability、identity 和 instructions；
- Tool name/title/description/schema/annotations；
- Resource URI、MIME 和内容；
- Prompt messages；
- Completion suggestion；
- progress/log message；
- JSON-RPC error/body；
- Elicitation message/schema/URL；
- remote task status text。

进入 compiler、model、database、public API 或日志前必须经过各自的闭合校验和大小限制。

### 19.2 Prompt injection

平台不能证明远程文字没有 prompt injection，因此采用结构性隔离：

- Server instructions 默认不注入；
- Prompt 必须显式选择；
- Resource 标记来源和不可信边界；
- remote content 不能扩大 tool allowlist、OAuth scope 或 public policy；
- LLM tool result 只在当前冻结 call context 内使用；
- 高风险 Tool 仍由本地 approval/effect policy 决定。

### 19.3 SSRF 与网络

Streamable HTTP：

- production HTTPS；
- DNS 解析和连接地址都执行网络 policy；
- 防止 DNS rebinding；
- 默认拒绝 redirect；
- 禁止 loopback/link-local/private range，除非显式部署 allowlist；
- 限制 header、body、SSE frame 和总字节；
- endpoint 不由 Agent/user 输入动态构造；
- Resource link 不能绕过 Server endpoint policy。

### 19.4 stdio

- 不使用 shell；
- 最小环境；
- secret env 不出现在 Debug；
- bounded stdout/stderr；
- kill tree；
- sandbox/isolation profile；
- executable digest/allowlist；
- 禁止 Server 通过相对路径逃逸工作目录；
- 进程 crash/panic/invalid frame 只影响 MCP binding，不影响平台进程。

### 19.5 Multi-tenant

cache、credential、interaction、remote task、Artifact 和 subscription 都必须包含 tenant/principal
scope。`cacheScope: public` 只有在 Server binding、authorization mode 和平台 policy 都证明公共时
才能跨 principal 复用；默认仍按 principal 隔离。

### 19.6 Public non-interference

默认正文日志、metrics、trace、Run stream 和 terminal error 不得包含：

- tool arguments/result；
- Resource/Prompt 正文；
- OAuth code/token/refresh token；
- requestState；
- form response；
- URL query；
- remote task ID；
- stderr；
- raw protocol error data。

需要诊断时只保存 code、Server ID、method、descriptor hash、byte counts、duration 和 body-free
validation location。

## 20. 错误模型

### 20.1 Publication/config errors

稳定 code 至少覆盖：

- `MCP_CONFIG_INVALID`
- `MCP_PROTOCOL_VERSION_UNSUPPORTED`
- `MCP_SERVER_DISCOVERY_FAILED`
- `MCP_SERVER_CAPABILITY_MISMATCH`
- `MCP_IMPORT_NOT_FOUND`
- `MCP_TOOL_SCHEMA_INVALID`
- `MCP_TOOL_ALIAS_INVALID`
- `MCP_BINDING_POLICY_REQUIRED`
- `MCP_TERMINAL_ONLY_INCOMPATIBLE`
- `MCP_EXPORT_INVALID`

错误可以包含 Server ID、tool/resource/prompt name 和非秘密字段路径；不能包含 credential、URL query
或 remote body。

### 20.2 Runtime errors

Runtime 使用现有 operation/infrastructure/timeout/stop 分类，增加稳定 MCP code：

- `MCP_AUTH_REQUIRED`
- `MCP_AUTHORIZATION_FAILED`
- `MCP_TRANSPORT_UNAVAILABLE`
- `MCP_PROTOCOL_INVALID`
- `MCP_BINDING_CHANGED`
- `MCP_CONTENT_LIMIT_EXCEEDED`
- `MCP_INTERACTION_INVALID`
- `MCP_INTERACTION_EXPIRED`
- `MCP_REMOTE_TASK_EXPIRED`
- `MCP_REMOTE_TASK_FAILED`
- `MCP_TERMINAL_ONLY_INTERACTION_UNSUPPORTED`

remote raw message 不成为稳定平台 error message。LLM 可恢复的 `isError` 内容通过安全 tool result
返回，不混入基础设施错误。

### 20.3 Retry

默认：

- discovery/list/read/completion 可按 read-only policy 有界 retry；
- mutating/non-idempotent `tools/call` 不自动 retry；
- input-required retry 是协议 continuation，不消耗普通 Action retry budget；
- task polling 有独立 deadline/backoff；
- OAuth refresh 只对明确可重试 transport failure retry；
- schema、auth denial、binding drift 和 invalid protocol 不 retry。

## 21. 兼容 `2025-11-25`

Legacy compatibility 是独立后置 profile：

- modern 优先；
- stdio 先有界调用 `server/discover`，明确 method-not-found/探测超时后才 fallback；
- HTTP 按版本协商和状态/header 规则 fallback；
- legacy 使用 `initialize`/`initialized` 与 session transport；
- adapter 把 legacy notifications/subscriptions 映射到相同内部事实；
- 不向 legacy Server 声明 Roots/Sampling；
- 不实现 `2025-11-25` 实验 Tasks；
- modern 和 legacy codec/connection state 严格分离；
- 一个 logical connection 选定 era 后不能中途切换；
- legacy evidence 明确进入 Server binding 和 Deployment Revision。

`2024-*`、`2025-03-26`、`2025-06-18` 不在本规范范围。需要时必须新增 profile 和 conformance matrix。

## 22. Observability 与运维

### 22.1 Metrics

至少提供：

- discovery/list/call/read/get/completion 请求计数和 duration；
- transport/protocol/auth/schema/result classes；
- active subscription；
- interaction open/age/outcome；
- remote task working/input-required/terminal/age；
- OAuth transaction/credential refresh/revoke；
- stdio process restart；
- cache hit/miss/invalidation；
- body/frame limit rejection；
- stale publication candidate；
- per Server bounded labels。

tool name、URI、prompt name 默认不作为无界 metrics label。

### 22.2 Readiness

只有被已启用 Agent/Server export 实际引用的 MCP binding 参与强 readiness：

- required Server discovery/version/capability 可用；
- stdio executable/isolation 可用；
- OAuth metadata 可用或已有合法配置；
- credential store/keyring 可用；
- Tasks/interaction worker 已启动；
- `/mcp` auth metadata 和 signing/JWKS dependency 可用。

未被引用的 optional Server 不阻止进程启动，但其 catalog API 返回明确 unavailable。

### 22.3 Shutdown

shutdown 顺序：

1. 停止新 MCP admission/export request；
2. 停止创建 interaction/task；
3. quiesce subscription 与 poll claim；
4. 发送可行的 cancellation；
5. 提交/释放 claim 和 owner fence；
6. 关闭 HTTP response stream；
7. 终止 stdio 子进程；
8. 在 hard deadline 前完成 secret buffer 清理。

shutdown 不能把 remote task 错误标记为 cancelled；full runtime 下由后续 owner 恢复。

### 22.4 Schema provisioning 与 rollout

MCP client/server、OAuth、interaction 和 Tasks 默认全部关闭。新增数据库对象：

- 同时写入 PostgreSQL/SQLite authoritative full schema；
- bump schema contract ID 和 startup verifier；
- 继续由部署前 provisioner 安装，业务进程零 DDL；
- 不添加 runtime auto-migration；
- pre-1.0 开发数据库按当前 clean-rebuild policy 重建；
- 生产升级必须先 provision compatible schema，再滚动发布 disabled binary，最后按 profile 启用。

rollout 顺序固定为：

1. schema/store 与 `insight-mcp` disabled；
2. read-only service-account Client/Tools；
3. Resources/Prompts/Subscriptions；
4. Elicitation/OAuth；
5. stdio；
6. MCP Server；
7. Tasks；
8. legacy compatibility。

每一步都需要独立 readiness、metrics 和 rollback。关闭新 admission 后，已经存在的 full interaction/
remote task 仍必须由兼容 runtime 排空；不能在有未闭合权威记录时卸载对应 worker/version。

## 23. 实施阶段

### Phase A：协议与工具合同基础

- vendor MCP schema snapshot；
- 新增 `insight-mcp`；
- modern codec、core types、bounded extension/meta maps 和 conformance fixtures；
- 重构 owned Action identity 与 model tool alias/title/description/action identity；
- 增加 MCP binding DTO 和 hash；
- crate boundary/public API baselines。

完成门槛：纯协议测试覆盖所有 core request/result/content/error variant，现有本地 Action/LLM tool 行为
无回归。

### Phase B：Modern Client + Tools

- `server/discover`；
- Streamable HTTP；
- tools list/call；
- schema draft、`x-mcp-header`、content、isError、progress/cancel；
- publication freeze；
- service-account auth；
- full/terminal-only 无交互工具。

完成门槛：外部 reference Server 的 read-only 与 mutating fixture 通过 SQLite/PostgreSQL、restart 和
non-interference gates。

### Phase C：Resources、Prompts、Completion、Subscriptions

- Resource/Prompt Provider；
- list/read/templates/get/completion；
- Artifact projection；
- principal-scoped cache；
- `subscriptions/listen`、reconnect 和 list-change publication staleness。

完成门槛：动态资源更新不改变固定 Run/Revision，cache/tenant/size/prompt-injection 负向测试通过。

### Phase D：Durable Interaction 与 run-stream/v2

- interaction engine/durable/storage/API；
- form/URL Elicitation；
- approval；
- `run-stream/v2`；
- first-winner、restart、timeout/cancel；
- terminal-only verifier。

完成门槛：所有 crash window 和 SQLite/PostgreSQL 竞态测试通过，用户回复正文与 requestState
non-interference 通过。

### Phase E：OAuth

- client discovery/PKCE/token/refresh/step-up；
- encrypted credential store；
- callback/connection APIs；
- protected resource server behavior；
- privacy deletion/retention；
- auth attack negative suite。

完成门槛：两个 tenant/两个 user 并发授权无混淆，token 从 Plan/DB plaintext/log/error/public wire
完全不可见。

### Phase F：stdio

- process supervisor；
- structured argv/minimal env；
- isolation、restart/backoff、cancel/shutdown；
- invalid stdout/stderr/crash/handle expiry tests。

完成门槛：real-process fault injection 不影响 runtime authority，secret 与 stderr 不泄漏。

### Phase G：MCP Server

- `/mcp`；
- Agent/Action/Resource/Prompt export；
- OAuth resource server；
- synchronous execution 与 core InputRequired；
- subscriptions/completion；
- external SDK interoperability。

完成门槛：受控 export allowlist 外无对象可枚举或调用，Go/TypeScript reference client 通过。

### Phase H：Tasks extension

- Client remote task；
- input-required/update/cancel/poll/status notification；
- Server Agent Run task export；
- reconnect/restart/TTL/first-winner。

完成门槛：长 Run 跨 client/runtime restart 完成，任务 terminal 与 Run terminal 一致。

### Phase I：Legacy compatibility

- `2025-11-25` probe/fallback；
- initialize/session；
- legacy notification adapter；
- era separation tests。

完成门槛：modern wire 无 legacy field，legacy connection 无 modern/session 混用。

各 Phase 可以独立合并，但在 A～H 全部完成前产品只能声明具体已交付 capability，不能宣称完整 MCP。
Phase I 不阻塞 modern 完整 profile。

## 24. 测试与验收矩阵

### 24.1 Wire conformance

- vendor schema encode/decode；
- 必填字段、闭合对象未知字段，以及扩展位置的 opaque 字段保留；
- 每请求 `_meta`；
- protocol/header/body mismatch；
- JSON-RPC ID correlation；
- all result types；
- pagination/caching；
- subscriptions；
- cancellation；
- all content variants；
- `x-mcp-header`；
- malformed/oversized/deep schema；
- unsupported version/capability。

### 24.2 Interoperability

至少与两个独立官方/参考 SDK 互测：

- 平台 Client ↔ TypeScript reference Server；
- 平台 Client ↔ Go 或 C# reference Server；
- TypeScript reference Client ↔ 平台 Server；
- Go 或 C# reference Client ↔ 平台 Server；
- stdio 与 Streamable HTTP 都覆盖；
- Tasks 单独覆盖。

测试必须固定 SDK 版本和上游 commit，不能依赖 floating latest。

### 24.3 Durable conformance

SQLite 与 PostgreSQL：

- interaction first-winner；
- task poll lease/fence；
- retry receipt；
- Run cancel/timeout/remote terminal；
- process restart；
- duplicate notification；
- lost notification；
- OAuth callback replay；
- credential rotation；
- retention/privacy deletion；
- terminal exactly-once local authority。

生产资格测试必须使用 PostgreSQL 16 和 real processes。

### 24.4 Security

- SSRF/DNS rebinding/redirect；
- stdio command/env/path injection；
- OAuth CSRF/state/PKCE/issuer/audience/scope；
- cross-tenant cache/token/interaction/task；
- prompt injection boundary；
- malicious schema/regex/depth；
- decompression/base64/body bombs；
- MIME spoofing；
- `x-mcp-header` secret/header injection；
- URL elicitation phishing；
- form secret request rejection；
- raw error/stderr/token/requestState leakage；
- export enumeration/scope bypass。

### 24.5 Failure injection

- Server unavailable/slow/malformed；
- subscription disconnect；
- stdio crash/hang/partial frame；
- remote task lost/expired；
- unknown handle；
- schema drift；
- OAuth issuer/metadata outage；
- refresh token invalidation；
- artifact write failure；
- database commit failure at every crash window；
- runtime SIGINT/panic/restart；
- remote completes after local cancel/timeout。

### 24.6 Public protocol

- `run-stream/v2` schema samples 覆盖全部 v1 事件与两个 interaction 事件；
- v1 decoder 拒绝 v2 identity；
- interaction body 永不进入事件/terminal；
- MCP tool public policy 双重授权；
- attached stream gap/terminal calibration；
- Agent discovery capability 准确；
- `/mcp` 与 `/v1` principal/transport 不混淆。

## 25. 完成定义

只有以下全部满足，`mcp-modern-client-v1` 才可标记 complete：

1. Phase A～F 实现并通过门禁；
2. Tools、Resources、Prompts、Completion、Subscriptions、Elicitation 全覆盖；
3. stdio 与 Streamable HTTP 全覆盖；
4. HTTP Authorization 和 principal credential 全覆盖；
5. full durable interaction recovery 通过 SQLite/PostgreSQL；
6. terminal-only fail-closed verifier 通过；
7. 两个外部 SDK 的双 transport interop 通过；
8. schema、body、secret、tenant 和 prompt-injection security gates 通过；
9. current 文档、config 示例、Helm、schema provisioner 和运维手册同步。

只有以下全部满足，`mcp-modern-server-v1` 才可标记 complete：

1. Phase G 实现并通过门禁；
2. `/mcp` discovery、exports、auth、synchronous execution、completion/subscriptions 全覆盖；
3. export allowlist、scope、tenant 和 public projection fail closed；
4. 两个外部 SDK interoperability 通过；
5. 未协商 Tasks 时绝不返回 task；
6. Server 不泄漏内部 Agent prompt、Action secret、Run/Artifact 或 trace。

只有以下全部满足，`mcp-tasks-v1` 才可标记 complete：

1. Phase H 的 Client 与 Server 两个方向都实现并通过门禁；
2. long Agent Run 通过 Tasks 在 client/runtime 断线或重启后完成；
3. `tasks/get/update/cancel`、status notification、TTL 和 input-required 全覆盖；
4. task terminal、Run terminal、interaction response 和 cancel/timeout first-winner 一致；
5. capability 未双向协商时，Client 拒绝 task result，Server 不返回 task；
6. 两个外部 SDK 的 Tasks interoperability 通过。

完成后：

- 将本文移入 `docs/archive/specs/` 并标记 `Implemented / Verified`；
- 在 `docs/current/` 增加 MCP 使用、API、运维、安全与开发合同；
- 保存完整 conformance/qualification 报告；
- 只有 profile 完成定义满足后才在 README/版本说明宣称对应完整支持。

产品只有在 `mcp-modern-client-v1`、`mcp-modern-server-v1` 和 `mcp-tasks-v1` 三个 profile 都完成后，
才能使用不带 profile 限定的“完整 MCP 支持”表述。`mcp-legacy-client-v1` 仍是独立兼容声明。

## 26. 关键取舍

### 26.1 MCP 不是新的 DSL node

Tools、Resources 和 Prompts 已有明确平台资源边界。新增通用 MCP node 会绕过类型、effect、public
policy 和 revision link，因此拒绝。

### 26.2 Dynamic discovery 不等于 dynamic execution authority

MCP list 可以变化，但 durable workflow 必须可恢复和可审计。平台在发布边界冻结 descriptor，运行
边界只执行精确 binding。变化通过新 revision 收敛。

### 26.3 Elicitation 不是普通错误

把 input-required 转成失败会丢失完整协议能力；在内存中等待又破坏 full runtime 恢复。独立 durable
interaction 是唯一同时满足协议、用户控制和恢复不变量的方案。

### 26.4 Tasks 是 extension，不是假定

长 Run 与 Tasks 高度匹配，但 Server 未必支持，Client 也未必声明。显式 capability profile 防止 wire
污染和互操作误判。

### 26.5 最新协议优先，legacy 隔离

modern MCP 已删除 session/initialize。把 modern 与 legacy 逻辑混在一个隐式状态机会产生难以验证的
半连接，因此使用独立 codec/profile，并把 compatibility 后置。

### 26.6 完整支持不等于自动信任

协议互操作只证明 wire 正确，不证明远程 Server、description、annotation、Prompt、Resource 或
Tool 是安全的。平台本地 policy、approval、tenant、OAuth scope 和公开投影始终拥有最终权威。
