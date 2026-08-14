# Platform v2 Management 与 Runtime API 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-09 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)～[`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) 的全部领域合同 |
| 直接下游 | 18 |

> Persistence ruling：API 不拥有第二套业务状态。operation 使用共享 Job/Invocation，idempotency/callback 使用 Receipt，
> public stream 使用 Run 上的 sequence 与共享 Event/Outbox；历史 API audit/operation/stream-head 专用表已废止。

## 1. 决策摘要

Platform v2 对外提供两个逻辑 API surface：Management API 管理 Draft、Validation、Revision、Deployment、Head、
Discovery、Policy、Suspension 和 Operation；Runtime API 管理 Run admission、snapshot、result、cancel、pause、resume、
signal、interaction、Artifact 和安全事件流。二者共享 `/v1` 版本、认证、错误、幂等、etag、cursor、schema 和审计
合同，但使用独立服务身份、路由授权、连接池和容量。

外部 API 使用 HTTPS + JSON/OpenAPI 3.1；Run observation 使用单向 SSE。平台不提供 GraphQL、任意 RPC、Provider/
MCP/Sandbox pass-through 或双向 WebSocket。内部服务使用 mTLS + versioned protobuf/gRPC；NATS 只传 wake/live hint
和 committed outbox projection，不能作为 API authority。

所有 mutation 都是 command，具有 Idempotency-Key/request digest；所有可变资源使用 ETag/If-Match；耗时操作创建
durable Operation。请求 unknown field fail closed。响应、错误和事件只暴露安全 projection，不返回 Secret、raw
backend handle、object key、Prompt/代码/文档正文或内部 policy expression。

## 2. 目标与非目标

### 2.1 目标

- 给新平台的全部 `/v1` 外部路由、资源、命令、Operation、错误、分页和流式事件统一合同；
- 让动态管理与 Run exact binding 同时成立；
- 让客户端安全重试 mutation、恢复 SSE、处理 CAS 冲突和长 Operation；
- 分离普通 tenant、Operator、Human approver 和 workload identity 权限；
- 让 OpenAPI/JSON Schema/protobuf、Rust types、数据库约束和 conformance fixture 可自动比较；
- 对 payload、list、filter、cursor、stream、connection、rate 和 retention 设硬限制；
- 对 cross-tenant enumeration、CSRF/CORS、SSRF、token、错误和 audit 提供一致安全语义；
- 为 SDK、CLI、UI 和外部集成提供稳定、typed、无后端泄漏的 surface。

### 2.2 非目标

- 不兼容当前`insight.agent/v1`的URL语义、DTO、SSE event、Action、DSL或错误码；相同`/v1`前缀只表示
  clean replacement后的首版新合同；
- 不提供通用 `/resources/{kind}`、任意 SQL、任意 JSON-RPC 或 backend extension bag；
- 不让客户端直接 claim work、提交 Worker outcome、创建 ChildRun 或 CapabilityInvocation；
- 不提供 Provider、MCP、Sandbox、S3 或 Secret Manager 原始 pass-through；
- 不把 API 请求连接作为 Run、Operation、upload 或 stream 的 durable authority；
- 不提供 server-side mutable Conversation/Memory；跨 Run history 必须显式成为 Run input、Artifact 或 ContextSource；
- 不在公共 API 暴露内部 transition ledger、lease、epoch、node stack 或 reconciliation evidence 正文；
- 不提供 public webhook、GraphQL subscription、WebSocket control channel 或跨租户批量导出。

## 3. Protocol 与规范权威

外部合同由以下 checked-in artifacts 共同构成：

```text
contracts/platform-v1/openapi.yaml
contracts/platform-v1/schemas/*.json
contracts/platform-v1/events/*.json
contracts/platform-v1/errors.json
contracts/platform-v1/examples/*
```

- HTTP schema 使用 JSON Schema 2020-12 / OpenAPI 3.1 可表达 subset；
- Agent/Capability/Context/Model业务payload引用05的`insight.closed-json-schema/1` nominal schema；OpenAPI DTO
  不能用`additionalProperties: true`或自由`object`弱化它；
- internal gRPC 使用 `proto/insight/platform/v1/*.proto`；
- internal gRPC按authority拆分service与workload identity。Egress调用Security Authority的Secret resolution/prepared-registration
  method时，服务端只接受exact `spiffe://insight.platform/workload/egress-broker` URI SAN；请求与响应使用closed、bounded、canonical
  envelope并绑定payload digest。该endpoint不经公共Ingress，不接受human token或tenant header，且不能暴露通用SQL/通用security command；
- ID、digest、timestamp、money、ArtifactRef、ValueRef 等共享 scalar fixture；
- request body 使用 closed object、`additionalProperties: false`；
- JSON parser拒绝duplicate key、非UTF-8、NaN/Infinity、越界整数、过深/过大对象；
- response client必须容忍新optional非语义字段，但状态/event/error code新增视为breaking profile change；
- breaking change先更新并接受本组规范与machine contract，再直接替换`/v1`；不保留并行兼容命名空间；
- OpenAPI/protobuf生成物和Rust DTO必须在CI进行round-trip、negative和unknown-variant conformance；
- 文档示例不是绕过machine schema的第二权威。

## 4. Endpoint 与服务边界

```text
https://api.example/v1/...       Runtime API
https://manage.example/v1/...    Management API
https://artifacts.example/v1/... Artifact transfer API
```

可以共用同一外部域名与Gateway，但后端Service、route permission、rate bucket、DB pool、timeout和readiness必须分离。
Runtime token不能访问Management route；Operator token默认不能代表tenant运行Agent。Artifact transfer credential只对
exact operation/object有效。

公共health route不放在 `/v1` resource namespace：`/health/live`、`/health/ready`。Metrics、debug、pprof、admin
repair和internal gRPC不经公共Ingress。

## 5. Authentication 与 Principal

- human/API client使用OIDC/OAuth 2.1 access token，验证issuer、audience、signature、expiry、not-before和revocation；
- workload使用mTLS workload identity，不能以human token调用internal result/callback API；
- access token只放Authorization header，不接受query/body/cookie token；
- tenant-scoped API从verified principal membership/active tenant binding派生tenant，不接受 `X-Tenant-ID` override；
- 跨tenant Operator使用独立admin audience、显式tenant path和break-glass/audit，不复用普通route；
- PrincipalSnapshot在Run/Operation/Interaction创建时固定append-only identity/binding generation evidence，不冻结current
  binding的后续CAS；持续权限在敏感读取/响应时重新检查；
- session/display identity不作为授权；
- authentication失败统一 `unauthenticated`，不泄露issuer、user或tenant existence。

所有route和internal method必须逐项映射04的closed permission registry。Management、Runtime、Artifact、Operator和
workload audience不能仅凭拥有同名permission跨surface复用token；authorization同时检查audience、principal kind、
tenant/support session、resource scope与permission。

OpenAPI `operationId -> Permission` registry是启动与CI校验的唯一route映射；以下family表冻结首版规则，但运行时不得
根据URL字符串临时推导permission：

| Route/command family | Permission |
|---|---|
| Agent read/author/publish/deploy/activate/admit Run | `agent.read/write/publish/deploy/activate/run`逐项 |
| Skill read/author/publish/bind/activate | `skill.read/write/publish/bind/activate`逐项 |
| Capability Interface/Implementation read/author/publish/deploy/activate | `capability.read/write/publish/deploy/activate`逐项 |
| Context read/author/publish/deploy/activate/build dataset | `context.read/write/publish/deploy/activate/build_dataset`逐项 |
| MCP read/author/discover/import/publish/deploy/activate | `mcp.read/write/discover/import/publish/deploy/activate`逐项 |
| Model Provider/Profile read/author/discover/import/publish/deploy/activate | `model.read/write/discover/import/publish/deploy/activate`逐项 |
| Sandbox read/author/build/publish/activate | `sandbox.read/write/build/publish/activate`逐项 |
| Policy read/author/publish/activate | `policy.read/write/publish/activate`逐项 |
| Run snapshot/result/events/control/signal | `runtime.read/control/signal`逐项；admission另用`agent.run` |
| Interaction/Approval read/respond | `interaction.read/respond`或`approval.read/respond`逐项 |
| Artifact read/prepare-delete/hold/rescan | `artifact.read/write/delete/hold/rescan`逐项 |
| Operation read/cancel | `operation.read/cancel`逐项 |
| SecretBinding metadata/create-rotate-revoke | `secret.inspect/bind/rotate/revoke`逐项 |

Internal claim/invoke另要求workload audience与service authorization，并分别映射`capability.invoke`、`context.query`、
`mcp.invoke`、`model.invoke`、`sandbox.execute`或`skill.activate`；public token即使带同名permission也不能调用internal
method。archive、suspend/resume和clear-active分别使用对应resource的`write`、`activate`权限，且仍受更严格Policy。

Browser public client使用Authorization Code + PKCE。API不使用ambient cookie，因此普通mutation不依赖CSRF token；
CORS只允许配置的exact origin/method/header，禁止credentialed wildcard origin。

MCP OAuth固定redirect `GET /v1/mcp/oauth/callback`是匿名浏览器入口，但不是无认证入口：唯一认证因子是AEAD保护、绑定固定
callback audience且短TTL的一次性`state`。该route无Authorization header或tenant override，只接受空body与总长度不超过API
`url_bytes` hard max（当前8192 bytes）的raw query；字段闭集为`state`、`iss`以及`code | error`二选一，重复/unknown字段fail closed。
它使用独立`internal_callback` rate class与Callback Receipt幂等，业务winner原子写Event/Outbox；响应固定为不反射输入的
`text/plain`并强制`no-store`、`no-referrer`、`nosniff`与closed CSP。200表示已有durable authorized/declined winner，202表示外部
prepared write或数据库commit结果不确定并交由durable reconcile，503只表示尚无durable winner的依赖暂不可用。

## 6. 通用请求合同

每个请求至少受以下约束：

```text
Authorization: Bearer ...
Content-Type: application/json
Accept: application/json | text/event-stream
Idempotency-Key: <opaque bounded key>        # mutation required
If-Match: <opaque etag>                      # mutable CAS required
X-Request-Id: <optional client correlation>  # untrusted, bounded
```

- server生成canonical request ID；client correlation不替代idempotency key；
- Idempotency-Key按tenant + principal class + route command scope隔离；
- request digest包含canonical method/path/query/body和影响语义的header，不含Authorization/request ID；
- body、query、header count/length、URL length、decompression ratio和total deadline有硬限制；
- request timeout不自动cancel已提交Run/Operation；客户端读取receipt后决定后续command；
- unknown query/filter/sort/header extension不改变语义，业务unknown字段直接400；
- timestamp必须UTC RFC3339 microsecond precision，duration使用bounded integer milliseconds或ISO合同指定形式；
- decimal money不使用binary float。

## 7. 通用响应合同

- create同步提交返回 `201 Created` + resource + `Location`；
- 异步 Operation 返回 `202 Accepted` + Operation + `Location`；
- 幂等重放返回原 status/receipt 语义，可附 `Idempotent-Replay: true`；
- successful command不使用 `200` 包装任意 `{data:any}`；每个route有closed response schema；
- immutable Revision/Deployment可使用long-lived private cache和strong ETag；
- mutable Draft/Head/Snapshot使用 `ETag`、`Cache-Control: no-store` 或短private cache；
- 含 transfer credential、OAuth challenge、Secret metadata 或 interaction response 的响应必须 `no-store`；
- response不回显Authorization、Idempotency-Key、raw request body、backend endpoint/handle或内部stack；
- server request ID通过 `Request-Id` 返回并进入safe log/trace。

## 8. ETag 与并发控制

ETag是opaque strong validator，由resource identity、generation和canonical representation digest生成。客户端不能
解析其内部格式。

- PUT Draft、activate/clear Head、suspend/resume、archive/unarchive/retire和policy update必须`If-Match`；
- create使用 `If-None-Match: *` 仅在route明确支持client-chosen external key时允许；
- immutable Revision/Deployment不提供update；
- ETag/If-Match不匹配固定返回`etag_mismatch`/412；409只用于idempotency/domain state conflict。包含current ETag
  只在调用者仍有read permission时返回；
- If-Match与Idempotency-Key共同存在：先命中完全相同receipt，否则对current state执行CAS；
- retry不能通过省略ETag覆盖并发编辑；
- active head activate请求必须同时携带expected generation/ETag并固定target exact Revision/Deployment ID。

## 9. Idempotency

所有POST command、PUT mutation、interaction response、signal、Artifact prepare/complete/delete都要求
Idempotency-Key。规范行为：

```text
first request -> InProgress receipt -> committed response/failure
same key + same digest -> same logical receipt/response
same key + different digest -> idempotency_conflict
```

- receipt在执行业务mutation前同事务创建；
- 确定性validation、已认证后的authorization/policy failure可保存bounded stable receipt；unauthenticated或body无法安全
  解析时不创建receipt；
- transient gateway/DB unavailable且未提交receipt时不声称成功，客户端可重试；
- response body较大时receipt保存resource/result reference，不复制正文；
- receipt retention至少覆盖客户端最大retry window和资源业务要求；
- Idempotency-Key不成为公开resource ID或metric label；
- GET/HEAD天然安全，不使用Idempotency-Key改变cache。

## 10. Management 生命周期 API

每种资源使用typed route，不使用任意kind：

| 资源 | 基础路径 |
|---|---|
| Agent | `/v1/agents` |
| Skill | `/v1/skills` |
| Capability Interface | `/v1/capability-interfaces` |
| Capability Implementation | `/v1/capability-implementations` |
| ContextSource | `/v1/context-sources` |
| ContextSource Implementation | `/v1/context-source-implementations` |
| MCP Server | `/v1/mcp-servers` |
| Model Provider | `/v1/model-providers` |
| Model Profile | `/v1/model-profiles` |
| Policy | `/v1/policies` |
| Sandbox Runtime | `/v1/sandbox-runtimes` |
| Sandbox Package | `/v1/sandbox-packages` |
| Sandbox Profile | `/v1/sandbox-profiles` |

标准生命周期shape：

```text
POST /v1/{resources}
GET  /v1/{resources}/{entity_id}
GET  /v1/{resources}/{entity_id}/draft
PUT  /v1/{resources}/{entity_id}/draft
POST /v1/{resources}/{entity_id}/draft:validate
GET  /v1/{resources}/{entity_id}/revisions
GET  /v1/{resources}/{entity_id}/revisions/{revision_id}
POST /v1/{resources}/{entity_id}/revisions:publish
POST /v1/{resources}/{entity_id}:activate
POST /v1/{resources}/{entity_id}:clear-active
POST /v1/{resources}/{entity_id}:suspend
POST /v1/{resources}/{entity_id}:resume
POST /v1/{resources}/{entity_id}:archive
POST /v1/{resources}/{entity_id}:unarchive
POST /v1/{resources}/{entity_id}:retire
```

具体资源只能实现其domain允许的子集。例如Capability分Interface/Implementation，Context有Dataset Generation，
MCP/Model有Discovery，Artifact/Sandbox有Build/Scan evidence。API schema不能用generic document绕过各规范。
`:activate/:clear-active`只存在于02 head-owner matrix中的Entity route；Capability/Context Implementation Entity没有
active head，只支持authoring/read/archive与独立suspend/resume。activate body的target kind必须与02矩阵完全匹配。

只有存在环境绑定的资源实现Deployment collection，首版路径闭集为：

| Deployment | Collection |
|---|---|
| Agent | `/v1/agent-deployments` |
| Capability | `/v1/capability-deployments` |
| ContextSource | `/v1/context-deployments` |
| MCP Server | `/v1/mcp-deployments` |
| Model Provider | `/v1/model-provider-deployments` |
| Model Profile | `/v1/model-deployments` |

每个collection只支持其typed `POST`和`GET /{deployment_id}`schema，不存在通用`/{kind}-deployments`dispatcher。
Skill和Sandbox Runtime/Package/Profile在首版直接以exact immutable Revision被上层Deployment绑定，不虚构独立
Deployment资源。

## 11. Draft、Validation、Publish 与 Deployment

- POST Entity创建空/initial Draft并返回Entity + Draft ETag；
- PUT Draft是完整replacement，要求If-Match，不使用JSON Merge Patch的null/array歧义；
- Validate创建async Operation，固定Draft digest和validator versions；
- Publish只接受current Draft digest、有效mandatory evidence和explicit mutation；
- Publish成功返回immutable Revision；相同semantic digest是否复用遵守resource spec；
- Deployment request只能引用exact Revision、Policy Revision、SecretBinding和implementation IDs；SecretPurpose只声明
  credential需求，不能替代实际绑定。客户端只提交SecretBinding ID；repository在创建事务中派生并
  冻结04的`ExactSecretBindingRef`，不接受客户端提交generation、provider、purpose或policy digest；
- Deployment resolution失败返回typed field errors，不自动追随head或discovery；
- publish/deploy不activate；Activate是独立CAS command；
- suspension/resume不修改Revision/Deployment/head；
- archive/unarchive遵守02 lifecycle CAS；retire不可逆且不hard-delete仍被Run/Reference保留的Revision。

SecretBinding不套用Entity/Draft/Revision生命周期，使用专用typed API：

```text
GET  /v1/secret-bindings
POST /v1/secret-bindings
GET  /v1/secret-bindings/{secret_binding_id}
POST /v1/secret-bindings/{secret_binding_id}:rotate
POST /v1/secret-bindings/{secret_binding_id}:revoke
```

响应只含04允许的safe metadata projection；`opaque_reference`、Secret value和resolver response永不回读。create/rotate/
revoke要求Idempotency-Key，rotate/revoke还要求If-Match；Revoked不可恢复。

## 12. Discovery、Build 与 Dataset API

耗时管理操作统一创建Operation：

```text
POST /v1/mcp-deployments/{deployment_id}:discover
POST /v1/model-provider-deployments/{deployment_id}:discover-models
POST /v1/context-deployments/{deployment_id}:build-dataset
POST /v1/sandbox-packages/{id}:build
POST /v1/artifacts/{id}:rescan
```

- MCP/Model/Context command固定exact Deployment；Sandbox build固定Package Draft/Revision digest；Artifact rescan固定
  Artifact generation。所有command还固定profile、deadline和principal；
- Operation result是DiscoverySnapshot/Evidence/DatasetGeneration/Package Revision reference；
- candidate不会自动publish/activate；
- list/import candidate需要explicit selection和后续validation；
- request连接断开不cancel Operation；
- Operation cancel是best-effort durable intent，不删除已生成evidence/Artifact；
- progress是bounded stage/count，不包含remote目录、code、document、raw error或Secret。

## 13. Operation 资源

```rust
struct ManagementOperation {
    operation_id: OperationId,
    tenant_id: TenantId,
    kind: ManagementOperationKind,
    target: OperationTarget,
    state: ManagementOperationState,
    progress: Option<SafeOperationProgress>,
    result: Option<OperationResultRef>,
    failure: Option<Failure>,
    created_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
    terminal_at: Option<DateTime<Utc>>,
    projection_version: u64,
}

enum ManagementOperationKind {
    Validation,
    Import,
    Discovery,
    Build,
    ArtifactUpload,
    ArtifactVerify,
    ArtifactRescan,
    ArtifactDelete,
    Export,
}

struct ManagementOperationTarget {
    resource_kind: ResourceKind,
    resource_id: ResourceId,
}

enum ManagementOperationState {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

Operation kind进入machine registry。Target永远是预分配的typed ResourceId；目标aggregate尚不存在时，Operation handler必须在
同一事务创建目标row并由deferred typed-source constraint闭合。禁止target携带任意表名、qualified name、URL、backend handle
或开放JSON。首个Artifact批次只开放`ArtifactUpload | ArtifactVerify | ArtifactRescan | ArtifactDelete`到`ArtifactId`的
组合，其他kind在各自typed target verifier交付前fail closed。

`result`只表示Operation新产生或选定的typed resource，而不是“成功”本身。产生资源的Succeeded Operation必须返回exact
typed reference；`ArtifactDelete`这类破坏性Operation的Succeeded终态必须保持`result=None`，由target标识被删资源，并由领域
内部append-only deletion receipt强绑Attempt、GC candidate与closed Blob disposition；只有`blob_generation`处置包含后端删除
与absence evidence，`artifact_only`处置必须保留共享Blob并绑定exact alias witness。禁止为满足通用result shape而把receipt digest冒充Artifact content
digest，或把Deleted Artifact重新投影为可绑定exact resource。

```text
Queued -> Running | Cancelled | TimedOut
Running -> Succeeded | Failed | Cancelling | TimedOut
Cancelling -> Cancelled | Failed | TimedOut
```

```text
GET  /v1/operations/{operation_id}
POST /v1/operations/{operation_id}:cancel
```

终态不可离开；cancel、timeout与worker outcome由projection version/Attempt fence first-winner决定。Operation不是Run，
不能调用Capability/ChildAgent或持有用户会话。它可以驱动bounded discovery、
validation、build、scan和export。非空Operation result必须是typed resource reference，不能返回任意backend JSON；不产生资源的
破坏性Operation必须由领域receipt闭合，不能返回伪资源。

### 13.1 已撤销 persistence 记录（非规范性）

旧 migration 24～28 与 Artifact-specific ManagementOperation 实施 checkpoint 已撤销，不属于当前 baseline、API 行为
或资格证据；详细记录只保留在 Git 历史。

Operation 的目标持久化统一复用 03 的共享 `Job`、`Invocation`、`Receipt`、`Event` 与对应领域聚合。
Phase 5 尚未开放任何公共 Operation route；只有完成本规范的实现与资格门禁后，才能把它声明为 `/v1` 当前行为。

## 14. List、Filter、Sort 与 Cursor

- list只提供route allowlist中的exact filter/sort字段；
- unknown operator、wildcard field、regex、raw SQL和任意expression被拒绝；
- 默认 stable order 为 `(created_at DESC, opaque_id DESC)`，客户端可选项是 closed enum；
- page size有default/hard max，offset pagination不公开；
- `OpaqueListCursor`是signed/encrypted opaque token，绑定tenant、principal permission digest、route、filters、sort、snapshot/high-water、
  page size和expiry；
- cursor不可跨tenant/principal/route/filter复用；
- permission revoke、expiry或schema generation变化返回 `cursor_invalid`/`cursor_expired`；
- list item是summary projection，不包含Draft/Revision正文；
- total count默认不返回，避免昂贵查询/存在性泄露；需要时使用bounded approximate count字段并显式标记。

List使用`OpaqueListCursor`，Run SSE使用`OpaqueRunEventCursor`。二者共享versioned cryptographic envelope实现，但purpose
discriminator和payload schema不同，任何跨类型复用固定返回`cursor_invalid`。

## 15. Run Admission API

```text
POST /v1/runs
```

```rust
struct CreateRunRequest {
    agent: AgentAdmissionSelector,
    input: ValueRef,
    deadline: Option<DateTime<Utc>>,
    service_class: Option<ServiceClass>,
    client_correlation: Option<BoundedClientCorrelation>,
}

enum AgentAdmissionSelector {
    ExactDeployment { agent_deployment_id: DeploymentId },
    ActiveHead {
        agent_id: AgentId,
        expected_head_generation: Option<u64>,
    },
}
```

Admission同一事务中验证principal、tenant、input/Artifact、quota、suspension、deadline和Deployment closure，固定完整
RunBindings并创建Queued Run/outbox。ActiveHead selector只在该事务读取一个CAS generation，receipt返回exact
Deployment和bindings digest。expected generation不匹配显式失败。
该事务固定使用PostgreSQL serializable isolation；所有参与admission的Principal generation、tenant/Agent gate和exact
binding closure读取都进入SSI依赖，序列化失败按同一Idempotency-Key重试，不能降级为read-committed的check-then-insert。

成功admission固定返回`201 Created`、RunSnapshot、`Location: /v1/runs/{run_id}`和ETag；Run执行异步不等于
admission Operation，因此不返回202。只有尚未创建目标resource的durable ManagementOperation返回202。

请求不能覆盖Model、Skill、Capability、Context、Policy、Secret、Sandbox、network、retry、tool或provider参数。
input必须通过Agent input schema。`service_class`是policy可选请求，只能选择公开closed等级，不能请求07的
`critical_control`或扩大quota。ServiceClass machine wire固定为`low | normal | high`；它只是受policy约束的公开请求，
不能直接写Scheduler Priority。Runtime API不接受client-supplied Run ID；Idempotency-Key保证重试只创建一个Run。

## 16. Run Query 与 Result API

```text
GET /v1/runs/{run_id}
GET /v1/runs/{run_id}/result
GET /v1/runs/{run_id}/children
GET /v1/runs/{run_id}/interactions
```

RunSnapshot使用06定义的public projection：state、exact Agent Deployment、bindings digest、timestamps、deadline、
bounded wait/budget summary、safe failure和授权的input/output ArtifactRef。默认不返回内部Node/Attempt/lease/binding
details。

`PublicRunStatus`闭集为`queued | running | waiting | paused | cancelling | succeeded | failed | cancelled | timed_out`。
除`paused`外它与06 RunState一一映射；当pause intent有效且没有in-flight业务Attempt时投影`paused`，同时保留
`pause_generation`。恢复/重连必须从相同committed facts得到同一status，不能根据API进程内permit推断。

`/result`在非terminal返回 `run_not_terminal`/409；Succeeded返回typedValueRef，其他terminal返回safe terminal
failure/cancel/timeout summary。下载Artifact仍需单独grant。`children`只返回授权的ChildRun summary，不暴露child
private input/output。Operator diagnostic使用独立route/audience和审计，不扩大普通Run projection。

## 17. Run Control API

```text
POST /v1/runs/{run_id}:pause
POST /v1/runs/{run_id}:resume
POST /v1/runs/{run_id}:cancel
POST /v1/runs/{run_id}/signals
```

- 每个 command 要求 Idempotency-Key；pause/resume 可携带 expected projection version/ETag；
- pause只设置admission intent，不阻止cancel/timeout/signal/cleanup；
- resume不能恢复terminal Run或改变Deployment/Bindings；
- cancel body只有closed reason code和可选safe client correlation，不持久化任意自由文本；
- signal包含published signal kind/key、closed response schema value和expected continuation generation；
- signal key在Run/kind内幂等，未声明/迟到/错误generation不能推进Run；
- client disconnect不撤销已提交command；
- command receipt返回current/newRunSnapshot或accepted control generation。

## 18. Interaction 与 Approval API

```text
GET  /v1/interactions/{interaction_id}
POST /v1/interactions/{interaction_id}:respond
POST /v1/interactions/{interaction_id}:decline
POST /v1/approvals/{approval_id}:approve
POST /v1/approvals/{approval_id}:deny
```

- task/approval projection显示请求来源、safe prompt key、schema、Effect summary、deadline和可选ArtifactRefs；
- response绑定tenant、principal capability、interaction generation、Run/Invocation和request schema；
- 错误 approver、过期、已响应、跨 Run/tenant 或 body digest 变化被拒绝；
- Form interaction禁止Secret/password/token/payment credential；URL interaction遵守13的consent/origin规则；
- approval固定input/binding/effect digest，任何变化需要新approval；
- duplicate相同response返回receipt，不同response返回idempotency conflict；
- decline/deny是明确terminal response，不等同于timeout；
- rawbackend prompt、policy expression、SecretPurpose或opaque continuation handle不公开。

## 19. Artifact API

15定义的route在公共v1合同中使用：

```text
POST /v1/artifacts:prepare
POST /v1/artifacts/{artifact_id}:complete-upload
GET  /v1/artifacts/{artifact_id}
POST /v1/artifacts/{artifact_id}:issue-download
POST /v1/artifacts/{artifact_id}:delete
```

Prepare/complete/delete要求Idempotency-Key。transfer response `Cache-Control: no-store`，grant只对exact object/
operation/audience/deadline有效。普通client不能调用Finalize/Reference或指定object key、Ready state、classification
override、bucket/KMS。Range/download由Artifact Gateway验证current permission。

Prepare同步提交Upload ManagementOperation、Staging Artifact与UploadGrant时返回201，`Location`指向Artifact且closed
response包含Operation reference；complete-upload、rescan和需要异步引用/物理处理的delete返回202，`Location`指向统一
`/v1/operations/{operation_id}`。不存在第二套Artifact专用Operation状态机或ID。Operation成功只表示15规定
的目标状态已提交，不能绕过Verified/Ready、reference、retention或legal hold门。

## 20. Run SSE

```text
GET /v1/runs/{run_id}/events
Accept: text/event-stream
Last-Event-ID: <opaque durable cursor>
```

连接算法：

1. authentication/authorization/rate/connection quota；
2. 解析Last-Event-ID或`after`，两者不可同时提供；
3. 无cursor时在一致读中取得RunSnapshot和durable high-water；
4. 先发送 `run.snapshot`，其SSE id为high-water cursor；
5. 查询并发送cursor后的durable public events；
6. 订阅NATS live/wake只作加速，每次durable wake回PostgreSQL high-water查询；
7. heartbeat comment不改变cursor；
8. permission revoke、Run retention、server drain或backpressure时安全关闭。

客户端只以SSE `id`/event envelope的durable cursor恢复，不解析cursor。cursor过期返回410 `cursor_expired`并提供
RunSnapshot URI；客户端读取snapshot后重新连接。

## 21. Public Event Envelope

```rust
struct PublicRunEvent {
    event_id: Option<EventId>,
    run_id: RunId,
    cursor: Option<OpaqueRunEventCursor>,
    sequence: Option<u64>,
    schema_version: EventSchemaVersion,
    event_type: PublicRunEventType,
    durability: EventDurability,
    occurred_at: DateTime<Utc>,
    data: ClosedEventPayload,
}

enum EventDurability {
    Snapshot,
    Durable,
    LiveOnly,
}
```

三类envelope约束如下：

- `Snapshot` 只用于合成的 `run.snapshot`，`event_id=None`、`sequence=None`，`cursor` 是快照覆盖的durable high-water；
- `Durable` 必须有 `event_id`、严格per-Run `sequence` 和opaque `cursor`，并来自committed outbox；
- `LiveOnly` 的 `event_id`、`sequence`、`cursor` 都是 `None`，不得推进客户端状态；
- SSE `id` 只出现在Snapshot/Durable帧并等于envelope `cursor`；LiveOnly帧和heartbeat没有SSE `id`；
- `run.snapshot` 不是outbox event，不能参与domain replay或被当成下一个Run sequence。

Durable `sequence`来自 Run 聚合上的原子 CAS sequence，不是任一leaf aggregate version。Opaque cursor封装tenant、Run、
sequence、permission digest、schema generation、issued/expiry和key ID并签名/加密；服务端不为每次连接或cursor创建
数据库row。key rotation保留覆盖最长cursor TTL的验证key，revoke通过permission digest与重新授权使cursor失效。
因此immutable PublicRun outbox payload不得保存cursor：outbox columns与typed projection保存EventId、Run sequence、
schema version和occurred_at，JSON payload严格符合`public-run-payloads.schema.json`，只含event type与closed safe
source projection。API在完成当前principal授权后组合envelope并为响应materialize cursor；同一durable event可为不同principal
产生不同opaque cursor，但都引用同一Run sequence。该边界关闭CR-075，不能由dispatcher缓存某个principal的cursor替代。

最小closed event types：

```text
run.snapshot
run.queued
run.started
run.waiting
run.paused
run.resumed
run.cancelling
run.completed
run.failed
run.cancelled
run.timed_out
node.started
node.completed
node.failed
node.cancelled
node.timed_out
skill.selected
skill.activated
skill.rejected
model.started
model.delta                 # live-only
model.tool_intent
model.completed
model.failed
model.cancelled
model.timed_out
capability.started
capability.waiting
capability.input_required
capability.progress         # live-only or coarse durable profile
capability.completed
capability.failed
capability.cancelled
capability.timed_out
context.started
context.completed
context.failed
context.cancelled
context.timed_out
child.started
child.waiting
child.progress              # live-only or coarse durable profile
child.completed
child.failed
child.cancelled
child.timed_out
interaction.required
interaction.resolved
approval.required
approval.resolved
stream.live_gap             # live-only
```

事件只公开Agent/Interface允许的名称、safe summary和ArtifactRef。内部implementation、provider、endpoint、Secret、
tool arguments/output、Context正文、Child private data、raw failure和lease不公开。未知event type是protocol break，
不能用generic `custom`绕过schema。

event type闭集不等于payload合同闭合。每个event type必须在
`contracts/platform-v1/events/public-run-payloads.schema.json`拥有以event type为discriminator的closed payload schema，
并由Rust type、OpenAPI、Event repository validator和F-EVENT共同消费。未注册的source kind与任意`jsonb data`均fail closed。

## 22. SSE Backpressure、重连与保留

- 每 principal/tenant/Run/IP 有 connection limit 和 rate bucket；
- server 每 connection 使用 bounded buffer；先丢 live delta/progress 并发送 `stream.live_gap` hint；
- durable event不因慢client丢失，buffer满时关闭连接，client凭cursor重连；
- heartbeat interval、maxconnection age、idle/write deadline和max event bytes有硬限制；
- SSE不得在内存中长期持有DB transaction/listener或per-client NATS connection；
- fan-out 使用共享 subscription/connection 和 tenant-safe projection；
- event正文超限使用ArtifactRef或safe summary，不能拆无限delta；
- terminal event送达失败不改变Run terminal；
- durable public event retention与Run retention/cursor SLA由18冻结；
- Run不存在/跨tenant统一404，不能通过SSE timing枚举。

## 23. Error 模型

错误使用 `application/problem+json`：

```rust
struct ApiProblem {
    type_uri: String,
    title: String,
    status: u16,
    code: ApiProblemCode,
    detail: Option<SafeMessage>,
    request_id: RequestId,
    retryable: bool,
    retry_after_ms: Option<u64>,
    field_errors: Vec<FieldError>,
}
```

`ApiProblemCode`首版闭集：

```text
invalid_request
schema_validation_failed
unauthenticated
permission_denied
resource_not_found
etag_mismatch
idempotency_conflict
invalid_state_transition
policy_denied
approval_required
quota_exceeded
rate_limited
resource_suspended
secret_unavailable
network_denied
isolation_unavailable
content_rejected
cursor_invalid
cursor_expired
run_not_terminal
operation_not_terminal
deadline_exceeded
temporarily_unavailable
internal_error
```

`ApiProblem.code`只描述当前HTTP/gRPC command是否被接受或完成；Run、ManagementOperation和leaf terminal resource中的
业务失败使用05的`Failure`/safe `FailureProjection`。二者即使文本相似也不共享enum，Gateway不得把terminal Failure
伪造成请求级500，也不得把未提交command错误写入Run。

跨tenant、不可见资源和部分授权枚举统一404。detail/field error不回显Secret、policy expression、endpoint、SQL、
stack、rawbackend error或其他tenant存在性。`retryable=true`不等于mutation可无Idempotency-Key重试；client仍遵守
command/Effect语义。

## 24. HTTP 状态映射

| HTTP | 语义 |
|---|---|
| 200 | query或同步command成功 |
| 201 | resource/Run已原子创建 |
| 202 | durable Operation已接受 |
| 204 | 无body的幂等同步command成功 |
| 400 | wire/schema/field非法 |
| 401 | unauthenticated |
| 403 | permission/policy denied且不需隐藏existence |
| 404 | 不存在或不可见 |
| 409 | idempotency/state/conflict |
| 410 | cursor/resource retention已过期 |
| 412 | If-Match/precondition失败 |
| 413 | request/upload声明超限 |
| 415 | media/content type不支持 |
| 422 | schema合法但domain validation失败 |
| 429 | rate/quota暂时限制，可能含Retry-After |
| 500 | 未分类platform invariant failure |
| 503 | 依赖/容量暂不可用且没有已提交receipt |
| 504 | gateway deadline，不能推断backend command未提交 |

API Gateway不能把所有domain failure压成200或500。同一`ApiProblem.code`在REST/internal gRPC/SDK中语义一致；SSE和
terminal resource使用05的`FailureProjection`，即使某个code拼写相同也必须保留layer/discriminator，不能混为同一enum。

## 25. Internal gRPC

内部接口按所有权拆分：

```text
RegistryCommandService
RunCommandService
RunQueryService
WorkerClaimCommitService
ModelExecutionService
McpHostService
SandboxGatewayService
ArtifactBrokerService
SecretBrokerService
CallbackInboxService
```

- protobuf package/version固定，unknown enum fail closed；
- 所有 call 使用 mTLS workload identity、service authorization、deadline、message size 和 retry policy；
- tenant/Run/Invocation等从durable command派生，backend不能body override；
- Worker claim/commit必须含work/attempt/epoch/fence；
- callback使用scoped one-time identity/inbox dedupe；
- Secret value只在专用broker response且不进入generic envelope；
- gRPC retry仅对明确idempotent method和stable request ID；
- publicIngress不能路由internal service；
- internal API不绕过PostgreSQL transaction/outboxauthority。

## 26. Security Headers 与 Gateway

- HTTPS only，TLS policy和certificate rotation由18定义；
- HSTS、nosniff、strict content-security policy用于UI/download origin；
- API response默认 `Cache-Control: no-store`，immutable public-safe metadata可显式private cache；
- request smuggling、ambiguous Content-Length/Transfer-Encoding、header normalization和duplicate authheader在edge拒绝；
- decompression在size/rate limit下进行；
- Artifact/upload/download使用独立origin或path policy，防止active content污染API origin；
- redirect只用于明确OAuth/Artifact transfer flow，不用302隐藏mutation result；
- error页面不返回HTML stack/debug；
- source IP仅作rate/safety signal，不作为Principal identity；
- Gateway不能注入tenant/permission结论，backend重新验证signed identity context。

## 27. Rate Limit、Quota 与 Backpressure

层级限制：

```text
installation -> service/route class -> tenant -> principal/workload -> resource/Run
```

- management mutation、Run admission、query/list、SSE、interaction、Artifact transfer和internal callback独立bucket；
- rate limit是短窗口保护，Quota是durable业务预算，两者错误码不同；
- admission在内存queue前做auth/size/rate，在DB transaction内做durable quota reservation；
- overloaded dependency只影响对应route/bulkhead；Sandbox/MCP/Model饱和不使GET Run/API readiness失败；
- 429使用bounded Retry-After，不暴露其他tenant load；
- API不创建无界in-processfuture等待Operation/Run；
- list/SSE/Artifact large responses流式backpressure并有byte/deadline limit；
- critical cancel/interaction/security command使用reserved capacity；
- DB pool按Runtime/Management/Stream/Artifact/Internalrole分离并有硬budget。

## 28. Persistence 与审计

API 不拥有业务表。Management Operation 映射为共享 Job 或 Invocation；command/callback/idempotency 使用 Receipt；请求审计、
业务 transition 与 public event 使用共享 Event，外发使用 Outbox。严格 per-Run `sequence` 是 Run 的 CAS 字段，事件 payload
保存 safe projection；PrincipalSnapshot 嵌入 Receipt/Task/Event。短窗口 rate limit 是 gateway 状态，durable quota 使用 04
的共享 quota 聚合。任何 API projection 都不得复制 Resource、Run、Artifact、Invocation、Job 或 Task 的当前状态。

## 29. 可观测性与隐私

```text
http_requests_total{service,route_template,method,status_class,error_code}
http_request_duration_seconds{service,route_template,method,status_class}
http_in_flight{service,route_class}
api_idempotency_total{command,outcome}
api_etag_total{command,outcome}
api_rate_limit_total{route_class,outcome}
sse_connections{state}
sse_events_total{durability,event_class,outcome}
sse_replay_lag_seconds{outcome}
management_operations_total{kind,outcome}
```

route使用template，不以ID/tenant/principal/filter/error detail作label。Trace span记录request ID、authorized tenant的受控
hash、route template、Operation/Run opaque ID和outcome，不记录Authorization/body/Artifact URL/Secret。Access log默认
不记录query value，敏感route强制redaction。

## 30. 配置与部署

- Management API、Runtime API、SSE Gateway、Artifact Gateway和internal gRPC role独立Deployment/DB pool/HPA；
- OpenAPI/event/error schema随image构建并暴露受控static endpoint，digest进入release evidence；
- server启动校验route/permission/schema/error registry完整性，unknown config fail fast；
- readiness检查自身DB pool、policy resolver和command path；不因任一Provider/MCP/Sandbox远端失败全局unready；
- rollingdeploy先readiness false/drainHTTP-SSE，在grace后关闭；durableRun/Operation不依赖连接；
- ingress/body/connection/deadlinelimit与application hardlimit一致或更严；
- production debug/repair endpoint默认不编译/不暴露；
- API base URL、issuer、audience等部署配置不能由tenant resource覆盖。

## 31. 测试矩阵

- OpenAPI/JSON Schema/protobuf与Rust DTO的positive/negative/round-trip/unknown-field fixture；
- duplicate JSON key、invalid UTF-8、NaN、overflow、deep/nested/large body和request smuggling；
- every mutation的Idempotency-Key replay/conflict/crash window；
- Draft/head/suspension并发If-Match/ETag first-winner；
- cross-tenant ID/list/cursor/SSE/Artifact/Operation/Interaction不可见且404不可区分；
- Run admission与head切换并发只冻结一个完整binding；
- cancel/pause/resume/signal/interaction/timeout/result 竞态；
- SSE snapshot/replay/live gap/slow client/disconnect/reconnect/cursor expiry/permission revoke；
- outbox/NATS 丢失、重复、乱序下 SSE durable sequence 收敛；
- Operation worker/API crash、cancel、deadline和result receipt恢复；
- Error code/HTTP mapping无raw backend/Secret/policy/stack泄漏；
- rate/connection/payload/list/filter/cursor/backpressure hard limit；
- Management/Runtime/Operator/workload token audience和route隔离；
- Artifact transfer grant no-store/exact audience和active content origin隔离。

## 32. 验收标准

- 所有公开 route 都有 OpenAPI、closed schema、permission、idempotency、rate 和 audit 声明；
- 不存在 generic resource/backend pass-through 或客户端可覆盖 binding/tenant/Secret 字段；
- mutation在response丢失/并发重放时只产生一个逻辑结果；
- mutable resource无法无If-Match覆盖，immutableRevision无法update；
- long work全部返回durableOperation，不在HTTP request内持有后台future；
- Run create事务同时固定exactBindings并返回digest；
- public Run snapshot/result/event在终态和故障恢复后完全一致；
- SSE无per-client DB listener/NATS connection，slow client不会造成无界buffer；
- 跨 tenant、Secret、backend handle、Prompt/正文 canary 不进入 response/event/error/log/metric；
- API各bulkhead饱和/远端故障不破坏cancel、安全控制和其他route readiness；
- generated SDK/conformance suite在至少Rust/TypeScript两个client实现通过；
- route/error/event breaking change在CI被识别并阻止静默改变同一`insight.platform/v1`合同。

## 33. 明确推迟的工作

- GraphQL、WebSocket双向控制、public webhook和batch mutation；
- mutable Conversation/Memory公共资源；
- public Run fork/redrive/continue-as-new；
- cross-tenant bulk export/import；
- v1兼容gateway/DTO/event translation；
- anonymous/public Artifact sharing；
- end-user manual reconciliation UI；
- 外部 client 自定义 event/filter 表达式。

## 34. 未决问题

没有阻止部署和资格验收的未决问题。具体SDK生成器、Ingress产品和OIDC provider可以替换，但 `/v1` typed
resource/command、Idempotency、ETag、Operation、SSE cursor、错误模型和服务bulkhead不得改变。
