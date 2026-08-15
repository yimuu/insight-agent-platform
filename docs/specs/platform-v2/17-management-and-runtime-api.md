# Platform v2 Management 与 Runtime API 规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | 02～16全部领域合同，以及18的deployment/release、Candidate与InstallationReleaseState章节 |
| 直接下游 | 18的qualification/API conformance章节 |

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

可以共用同一外部域名与edge Ingress，但后端Service、route permission、rate bucket、DB pool、timeout和readiness必须分离。
Runtime token不能访问Management route；Operator token默认不能代表tenant运行Agent。Artifact transfer credential只对
exact operation/object有效。

`artifacts.example`是一个public hostname与HTTP contract，不是一个共享进程。Ingress的closed route+method registry把只接受15
`Principal + OpaqueBearer`的StagingWrite/upload stream路由到`Artifact Upload Gateway` Deployment，把opaque read grant对应的
GET/HEAD/Range只路由到`Artifact Download Gateway` Deployment；public Upload不得接受workload mTLS、`JobAttempt + WorkloadBound`或internal
request binding。不能根据body、token内容或运行时header动态选后端。两者使用不同Service、ServiceAccount、数据库pool、S3/KMS identity、
permit与HPA，且都不redirect到object store或返回direct object URL。

公共health route不放在 `/v1` resource namespace：`/health/live`、`/health/ready`。Metrics、debug、pprof、admin
repair和internal gRPC不经公共Ingress。`ArtifactWorkloadBrokerService`、`ArtifactModelBrokerService`、
`ArtifactSandboxBrokerService`、`ArtifactWorkloadProducerService`、`ArtifactMaintenanceAuthorityService`与
`ModelArtifactProducerService`六者都没有public route、
OpenAPI operation、外部LoadBalancer或Ingress/Gateway映射；内部Kubernetes Service discovery不能被投影为tenant API。
public `Artifact Gateway`只指上述统一hostname/HTTP合同；其Upload与Download Gateway都不注册或转发上述任一internal gRPC service/method。

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
| Tenant encryption-domain read/add-rebind/revoke | read要求`tenant.read`；add/rebind要求`tenant.manage + secret.bind`；revoke要求`tenant.manage + secret.revoke` |
| Installation Release read/promote/rollback | `installation.support`读取；`InstallationOperator + installation.manage`变更 |

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
- Idempotency-Key按03 `AuthorityScope` + principal class + route command scope隔离；普通command使用tenant scope，只有exact Installation Release
  promote/rollback使用configured `InstallationId` scope，不能伪造tenant或让NULL唯一键绕过dedupe；
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
- installation Release promote/rollback必须`If-Match`18 current state ETag；只有该public precondition失配才返回`etag_mismatch`。root Run
  admission等内部generation race不得伪装为客户端ETag错误。
- Artifact public prepare是创建新Artifact，固定不接收`If-Match`；`issue-download`只创建独立Grant且在winner事务重验current Artifact/policy，
  也固定不接收`If-Match`。这是首批closed例外，不允许扩展到已有Artifact的state mutation。
- Artifact complete-upload、rescan和delete都要求path中current Artifact的strong `If-Match`；missing header在Receipt claim前返回400，terminal
  same-key/same-digest replay先于current ETag/state，只有新claim的stale header才terminalize `etag_mismatch`/412。

## 9. Idempotency

所有POST command、PUT mutation、interaction response、signal、Artifact prepare/complete/delete都要求
Idempotency-Key。规范行为：

```text
first request -> Processing receipt -> committed response/failure
same key + same digest -> same logical receipt/response
same key + different digest -> idempotency_conflict
```

- 普通短command在业务mutation的同一事务claim并terminalize Receipt；03注册的可恢复长preflight（首个为Installation Release）可先用短事务
  提交Processing Receipt/lease/capture，但最终业务mutation、success Receipt、Event与Outbox仍在同一final winner事务；
- 确定性validation、已认证后的authorization/policy failure可保存bounded stable receipt；unauthenticated或body无法安全
  解析时不创建receipt；
- transient gateway/DB unavailable且未提交receipt时不声称成功，客户端可重试；
- response body较大时receipt保存resource/result reference，不复制正文；
- receipt retention至少覆盖客户端最大retry window和资源业务要求；
- Idempotency-Key不成为公开resource ID或metric label；
- GET/HEAD天然安全，不使用Idempotency-Key改变cache。

下列API command使用03 `ReceiptKind::Command`与tenant `AuthorityScope`，并在首版closed route-to-operation registry中固定为互不相等的
`ClosedOperation` wire discriminator；method/path相近、共享DTO或共享Task aggregate都不得合并operation：

| API command | Receipt operation |
|---|---|
| `POST /v1/encryption-domain-change-approvals` | `tenant.encryption_domain.approval_request.v1` |
| `POST /v1/encryption-domains` | `tenant.encryption_domain.add.v1` |
| `POST /v1/encryption-domains/{encryption_domain_id}:rebind` | `tenant.encryption_domain.rebind.v1` |
| `POST /v1/encryption-domains/{encryption_domain_id}:revoke` | `tenant.encryption_domain.revoke.v1` |
| `POST /v1/approvals/{approval_task_id}:approve` | `approval_task.approve.v1` |
| `POST /v1/approvals/{approval_task_id}:deny` | `approval_task.deny.v1` |

exact discriminator进入Receipt dedupe key及request-digest domain separator；同一Idempotency-Key用于表中不同command不会命中同一Receipt。

这六项tenant command与§13.2两项installation command使用以下closed Receipt payload；它们不是第二套API状态：

```rust
struct TenantCommandDedupeOwnerV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    principal: PrincipalSnapshot,
}

struct ApprovalCommandDedupeOwnerV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    approval_task_id: ApprovalTaskId,
    task_generation: u64,
    principal: PrincipalSnapshot,
}

struct InstallationCommandDedupeOwnerV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    principal: PrincipalSnapshot,
}

struct RequestEncryptionDomainApprovalReceiptV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    proposal: EncryptionDomainChangeProposalV1,
    if_match: Etag,
}

#[serde(tag = "change_kind", rename_all = "snake_case", deny_unknown_fields)]
enum EncryptionDomainApplyTargetV1 {
    Add,
    Rebind { encryption_domain_id: EncryptionDomainId },
    Revoke { encryption_domain_id: EncryptionDomainId },
}

struct ApplyEncryptionDomainChangeReceiptV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    target: EncryptionDomainApplyTargetV1,
    request: ApplyApprovedEncryptionDomainChangeV1,
    if_match: Etag,
}

#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
enum ApprovalResolutionDecisionV1 { Approve, Deny }

struct ResolveApprovalTaskReceiptV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    approval_task_id: ApprovalTaskId,
    decision: ApprovalResolutionDecisionV1,
    request: ResolveApprovalTaskRequestV1,
    if_match: Etag,
}

#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
enum InstallationReleaseTransitionV1 { Promote, Rollback }

struct ChangeInstallationReleaseReceiptV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    transition: InstallationReleaseTransitionV1,
    request: ChangeInstallationReleaseRequestV1,
    if_match: Etag,
}

#[serde(tag = "result_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ApiCommandReceiptResultV1 {
    ApprovalTaskCreated {
        status: u16,
        task: ApprovalTaskViewV1,
        etag: Etag,
    },
    EncryptionDomainChanged {
        status: u16,
        binding: EncryptionDomainBindingViewV1,
        collection_etag: Etag,
    },
    ApprovalTaskResolved {
        status: u16,
        task: ApprovalTaskViewV1,
        etag: Etag,
    },
    InstallationReleaseChanged {
        status: u16,
        release: InstallationReleaseViewV1,
        etag: Etag,
    },
    Rejected { status: u16, problem: ApiProblem },
}
```

03 registry的八个exact entry如下；表中的名字均为schema ID，version固定1：

| ClosedOperation | scope | dedupe owner schema | request schema |
|---|---|---|---|
| `tenant.encryption_domain.approval_request.v1` | Tenant | `api.tenant-command.dedupe-owner.v1` | `api.encryption-domain-approval-request.v1` |
| `tenant.encryption_domain.add.v1` | Tenant | `api.tenant-command.dedupe-owner.v1` | `api.encryption-domain-apply-request.v1` |
| `tenant.encryption_domain.rebind.v1` | Tenant | `api.tenant-command.dedupe-owner.v1` | `api.encryption-domain-apply-request.v1` |
| `tenant.encryption_domain.revoke.v1` | Tenant | `api.tenant-command.dedupe-owner.v1` | `api.encryption-domain-apply-request.v1` |
| `approval_task.approve.v1` | Tenant | `api.approval-command.dedupe-owner.v1` | `api.approval-resolution-request.v1` |
| `approval_task.deny.v1` | Tenant | `api.approval-command.dedupe-owner.v1` | `api.approval-resolution-request.v1` |
| `installation.release.promote.v1` | Installation | `api.installation-command.dedupe-owner.v1` | `api.installation-release-change-request.v1` |
| `installation.release.rollback.v1` | Installation | `api.installation-command.dedupe-owner.v1` | `api.installation-release-change-request.v1` |

所有entry均为`ReceiptKind::Command`、`CompleteAtClaim`，result schema均为
`api.command-receipt-result.v1`/version 1/path
`contracts/platform-v1/schemas/api/command-receipt-result.schema.json`/131072 canonical bytes。三个owner schema path依次为
`contracts/platform-v1/schemas/api/tenant-command-dedupe-owner.schema.json`、
`contracts/platform-v1/schemas/api/approval-command-dedupe-owner.schema.json`、
`contracts/platform-v1/schemas/api/installation-command-dedupe-owner.schema.json`，canonical maximum均为65536 bytes。四个request schema path依次为
`contracts/platform-v1/schemas/api/encryption-domain-approval-request.schema.json`（131072 bytes）、
`contracts/platform-v1/schemas/api/encryption-domain-apply-request.schema.json`（65536 bytes）、
`contracts/platform-v1/schemas/api/approval-resolution-request.schema.json`（65536 bytes）与
`contracts/platform-v1/schemas/api/installation-release-change-request.schema.json`（65536 bytes）。operation与request中的change/decision/transition
必须逐值相等；不允许用共享request schema把add/rebind/revoke或approve/deny/promote/rollback互换。

result允许矩阵固定为：approval-request只允许`ApprovalTaskCreated | Rejected`，三项encryption apply只允许
`EncryptionDomainChanged | Rejected`，approval resolution只允许`ApprovalTaskResolved | Rejected`，installation change只允许
`InstallationReleaseChanged | Rejected`。status只能是对应章节声明的成功码或稳定problem码；Receipt replay从result完整重建status/body/Location/ETag，
其中approval Location只由result内`approval_task_id`按固定route模板重建，不保存自由路径；不得重读current aggregate。所有schema及八个registry entry进入
root contract digest；缺失entry、schema、上限或错误scope时Candidate/server启动失败。

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

04 Tenant encryption-domain binding同样不套用Resource/Draft/Revision，使用tenant-scoped专用API；tenant只从verified principal membership派生，
普通route不接受tenant path/header override：

```text
GET  /v1/encryption-domains
GET  /v1/encryption-domains/{encryption_domain_id}
POST /v1/encryption-domain-change-approvals
POST /v1/encryption-domains
POST /v1/encryption-domains/{encryption_domain_id}:rebind
POST /v1/encryption-domains/{encryption_domain_id}:revoke
```

```rust
struct ApplyApprovedEncryptionDomainChangeV1 {
    schema_version: u32, // const 1
    approval_task_id: ApprovalTaskId,
}

struct EncryptionDomainBindingViewV1 {
    schema_version: u32, // const 1
    encryption_domain_id: EncryptionDomainId,
    storage_binding_digest: Digest,
    kms_binding_digest: Digest,
    state: EncryptionDomainBindingState,
    generation: u64,
}
```

`GET` list返回04 bounded wrapper的safe entries和strong collection ETag；item GET返回同一entry projection，ETag仍派生自current Tenant aggregate
version与wrapper digest，因此任一domain变更都会使旧precondition失效。read要求`tenant.read`。四个POST都要求Idempotency-Key、If-Match、
`management_mutation` rate class与closed body；approval-request body逐值复用04唯一`EncryptionDomainChangeProposalV1` schema，并按proposal要求
add/rebind的`tenant.manage + secret.bind`或revoke的
`tenant.manage + secret.revoke`。17不重新定义proposal nominal或Effect：Add/Rebind固定`IdempotentWrite`，Revoke固定`Irreversible`；服务从04
Tenant security aggregate的current exact `PolicyKind::Approval` binding选择对应`TenantEncryptionDomainApprovalPolicyV1` rule并创建03 shared
Approval Task，不创建EncryptionDomain change aggregate/table。Task typed owner冻结tenant、requester principal、完整proposal及canonical digest、
固定Effect、observed collection ETag、Policy Revision ID/semantic digest、完整approver rule和deadline；请求不得提交或覆盖这些字段。缺失、错误
kind、非active、digest漂移或不能满足04 Revoke职责分离下界的tenant Approval Policy返回403 `policy_denied`且不创建Task；成功同步返回201 `ApprovalTaskViewV1`及
`Location: /v1/approvals/{approval_task_id}`，same key/digest exact replay返回同一status/body/Location且只创建一个Task，body/ETag漂移产生冲突。

三个apply route只接受`approval_task_id`，不能重复提交或覆盖proposal字段；application service从shared Task加载04 frozen typed proposal并重算
`input_digest`。Task必须`Approved`且其tenant、operation、path target、proposal digest、
owner snapshot、generation、policy与current If-Match逐值相等；add的`enc_<uuidv7>`只在final transaction由服务端分配并由Command Receipt保证重放稳定。
apply按04锁序在一个事务完成Tenant aggregate CAS、installation compatibility generation推进、terminal Command Receipt及两scope Event/Outbox；
成功add返回201，rebind/revoke返回200及`EncryptionDomainBindingViewV1`和新collection ETag。判定顺序固定为terminal Receipt replay、public
If-Match、Approval Task、domain validation：header与current collection ETag不相等才terminalize 412 `etag_mismatch`；header仍current但Task冻结的
tenant version/wrapper/policy/owner snapshot已陈旧，或Task为Rejected/Expired/Cancelled、Revoked key复用、same-binding rebind、错误operation/target，
都terminalize 409 `invalid_state_transition`并要求新proposal+Approval；Task仍Pending返回409 `approval_required`。不存在或不可见的exact storage/KMS
binding返回404 `resource_not_found`且不泄露catalog。两个binding都已知但其组合与current installed storage/KMS manifest不兼容，或Add后的
wrapper会超过04的64项/65536 canonical-byte hard bound，固定terminalize 422 `invalid_request`、`retryable=false`；该映射同时用于
approval-request的当前proposal validation和apply的锁后domain validation，不能压成409/413/500。approval-request的确定性422不创建Task；apply的
确定性422不修改Tenant/installation/Event，只terminalize其Command Receipt。terminal same-key/same-digest replay仍先于current ETag/Task重验；
response、problem、Task/Event均不含KMS key、object locator或tenant Secret。

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
struct ManagementOperationAggregateV1 { // internal durable aggregate; not a wire DTO
    operation_id: OperationId,
    tenant_id: TenantId,
    kind: ManagementOperationKind,
    target: ManagementOperationTargetV1,
    binding_snapshot: VersionedSnapshot,
    current_job_id: Option<JobId>,
    state: ManagementOperationState,
    progress: Option<SafeOperationProgressV1>,
    result: Option<OperationResultRefV1>,
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
    ArtifactVerify,
    ArtifactRescan,
    ArtifactDelete,
    Export,
}

#[serde(tag = "target_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManagementOperationTargetV1 {
    Artifact { artifact_id: ArtifactId },
    ResourceVersion { version: ExactVersionRef },
    McpDeployment { deployment: ExactDeploymentRef },
    ModelProviderDeployment { deployment: ExactDeploymentRef },
    ContextDataset {
        dataset_resource_id: ResourceId,
        context_deployment: ExactDeploymentRef,
    },
    SandboxPackage { package_version: ExactVersionRef },
}

struct SafeOperationProgressV1 {
    schema_version: u32, // const 1
    stage_index: u16,
    stage_count: u16,
    completed_units: u64,
    total_units: Option<u64>,
    updated_at: DateTime<Utc>,
}

#[serde(tag = "result_kind", rename_all = "snake_case", deny_unknown_fields)]
enum OperationResultRefV1 {
    ResourceVersion { version: ExactVersionRef },
    DiscoverySnapshot {
        snapshot_id: DiscoverySnapshotId,
        snapshot_digest: Digest,
    },
    Evidence { evidence_id: EvidenceId, evidence_digest: Digest },
    Artifact {
        artifact_id: ArtifactId,
        artifact_projection_version: u64,
    },
}

struct FailureProjectionV1 {
    code: FailureCode,
    class: FailureClass,
    retryability: Retryability,
    safe_message: Option<SafeMessage>,
    source: FailureSource,
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

struct ManagementOperationViewV1 { // public GET/list projection
    operation_id: OperationId,
    tenant_id: TenantId,
    kind: ManagementOperationKind,
    target: ManagementOperationTargetV1,
    state: ManagementOperationState,
    binding_schema_version: u32,
    binding_digest: Digest,
    progress: Option<SafeOperationProgressV1>,
    result: Option<OperationResultRefV1>,
    failure: Option<FailureProjectionV1>,
    created_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
    terminal_at: Option<DateTime<Utc>>,
    projection_version: u64,
}
```

Operation kind与target组合进入machine registry；首版合法矩阵固定为：

| kind | 合法target |
|---|---|
| `Validation` | `ResourceVersion` |
| `Discovery` | `McpDeployment \| ModelProviderDeployment` |
| `Build` | `ContextDataset \| SandboxPackage` |
| `ArtifactVerify \| ArtifactRescan \| ArtifactDelete` | `Artifact` |

`Import | Export`在v1只保留枚举值，没有合法target，admission必须fail closed。每个exact ref还必须匹配该variant要求的
`ResourceKind`：MCP/Model deployment、Context deployment、ContextDataset root和SandboxPackage version不得互换。裸ID、
`ResourceKind + ResourceId`开放pair、`resource` alias、`null`或额外字段都非法。Target永远是预分配的typed nominal；目标aggregate尚不存在时，
Operation handler必须在同一事务建立目标aggregate并验证typed source完整性。禁止target携带任意表名、qualified name、URL、backend handle或
开放JSON。首版没有`ArtifactUpload` kind，upload只是`ArtifactVerify`开始执行前的Staging阶段。

`SafeOperationProgressV1`要求`stage_count > 0`、`stage_index < stage_count`、`total_units=None | Some(n >= completed_units)`；stage文案由
client按`kind + stage_index`映射，服务端不返回backend文本。`OperationResultRefV1`是唯一public/internal result union，所有digest与projection
version必须在terminal winner事务从authoritative row派生。`FailureProjectionV1`只复制05 `Failure`的safe字段，永不投影`details_ref`正文。

`binding_snapshot`必须是03同一closed、bounded、versioned snapshot envelope并保存完整typed payload及其canonical digest，不能只保存target、
散落列或运行时重查current Policy。ordinary Attempt start只在source Job attempt snapshot中预分配Operation/scan Job ID并冻结15
`ArtifactVerifyOperationBindingV1` preimage；此时不创建ManagementOperation或scan Job row。preimage只含当时已知的Artifact/candidate Blob identity、
`staging_artifact_version`、scan policy/rules、deadline与预分配ID，`scan_operation_binding_digest`逐值等于其canonical digest；未知的actual object
generation和`uploaded_artifact_version`不得伪造进preimage。任何pre-success失败只按15 stage/cleanup flow收敛，不留下Queued Operation orphan。

ordinary Workload stage success取得actual object generation与Uploaded version后，必须在同一事务创建exact `ArtifactVerify` Operation、把start冻结的
同一preimage安装为immutable `binding_snapshot`、构造15 closed `ArtifactVerifyJobBindingV1`、创建由该Operation拥有的唯一scan Job，并设置
`current_job_id=Some(scan_job_id)`。public prepare则在自己的winner事务已经创建同kind Operation及immutable binding，并以该Operation作为
Staging Artifact的exact owner；public `CompleteUpload` winner只能在该既有Operation上构造同一Job binding、创建预分配的唯一scan Job并设置pointer，
不得创建或替换第二个Operation。两条路径的Job binding都回绑Operation ID/binding digest并追加actual generation、
`uploaded_artifact_version`、length与digest，不能修改Operation binding。Scanner claim同时验证Operation仍为exact owner、pointer、Operation binding
digest、Job snapshot、actual generation及Job fence。
target只定位aggregate，不能替代执行授权与幂等binding。其他Operation
kind在其owner定义并注册typed binding schema前不得用开放JSON或空snapshot启动。公共create request不能提交/覆盖snapshot；
`ManagementOperationAggregateV1`不是OpenAPI DTO，`GET /v1/operations/{operation_id}`只返回closed `ManagementOperationViewV1`中的safe
binding schema version与digest，不回显可能含Policy、locator或backend细节的snapshot正文。

`current_job_id`是03 owner→Job current pointer的唯一落点。创建/切换Job必须在同一事务写Job immutable owner与Operation pointer；Job terminal不自动
清pointer，Operation merge事务锁定两者、验证exact ID/version/binding后才推进Operation并清空或原子切到下一Job。`Queued | Running | Cancelling`
凡需要后台执行都必须为`Some`且指向同tenant、owner为本Operation的非已消费Job。唯一`Queued + None`例外是public prepare创建、仍在deadline内、
作为同tenant exact Staging Artifact owner且仍绑定matching active public UploadGrant的`ArtifactVerify` Operation；其binding已冻结预分配scan Job ID，
但Job row尚不存在。upload放弃、失败后未重试或超时必须由15的deadline/delete authority把该Operation与Artifact/Grant一起收敛到匹配terminal状态，
不得永久保留Queued orphan；`CompleteUpload`成功后例外立即结束并原子设置`Some`。terminal Operation必须为`None`。pointer missing/mismatch、同一
Operation两个live Job、单边创建/清除或从Job表反查“最新”都属于invariant failure。

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

旧Artifact-specific ManagementOperation持久化设计与实施checkpoint已撤销，不属于当前baseline、API行为或资格证据；具体物理记录只保留在
Git历史与ADR。

Operation 的目标持久化统一复用 03 的共享 `Job`、`Invocation`、`Receipt`、`Event` 与对应领域聚合。
Phase 5 尚未开放任何公共 Operation route；只有完成本规范的实现与资格门禁后，才能把它声明为 `/v1` 当前行为。

### 13.2 Installation Release API目标

```text
GET  /v1/installation/release
POST /v1/installation/release:promote
POST /v1/installation/release:rollback
```

GET只对`installation.support | installation.manage`返回18 safe current state、strong ETag及exact Release/Candidate ID/digest/generation；
Uninitialized为显式状态，不返回null局部binding。两个command只允许`InstallationOperator + installation.manage`，必须携带
Idempotency-Key、If-Match和closed target：

```rust
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum InstallationReleaseViewV1 {
    Uninitialized {
        schema_version: u32, // const 1
        installation_id: InstallationId,
        compatibility_generation: u64,
    },
    Active {
        schema_version: u32, // const 1
        installation_id: InstallationId,
        compatibility_generation: u64,
        release_id: ReleaseId,
        release_manifest_digest: Digest,
        candidate_id: ReleaseCandidateId,
        candidate_manifest_digest: Digest,
        active_model_deployment_count: u32,
    },
}

struct ChangeInstallationReleaseRequestV1 {
    release_id: ReleaseId,
    release_manifest_digest: Digest,
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
}
```

GET与两个成功command都返回同一closed `InstallationReleaseViewV1`、strong ETag和`Cache-Control: no-store`。DTO不公开内部
`state_digest`、scan evidence、catalog或qualification正文；ETag以current installation identity/generation/state digest派生但保持opaque。

`promote`允许`Uninitialized -> Active`或`Active -> Active`切换到不同的approved target。`rollback`只允许从`Active`切到不同的approved、
database schema-compatible旧target；该机器判定与promote相同，均要求incoming Candidate的`database_schema_version`逐值等于18 startup verifier
产生的`ValidatedInstalledDatabaseSchemaVersion`，首版不接受compatibility range或down-migration。rollback不要求扫描Event证明target曾经Active。
rollback在Uninitialized、或任一新key指向current exact target，
固定返回409 `invalid_state_transition`。同key同digest的terminal Receipt必须在重新检查If-Match/current state前返回原结果，因此即使安装后来
再次切换也保持exact replay；同key请求漂移仍返回409 `idempotency_conflict`。两条command都是同步current-state command，不创建
ManagementOperation；Receipt operation和Release Event discriminator分别固定为`installation.release.promote.v1`与
`installation.release.rollback.v1`。

服务端按完整ID+digest解析approved immutable manifests并执行18 bounded scan/final CAS；rollback与promote使用同一请求/验证路径，区别只进入
上述closed Receipt/Event discriminator，不能down-migrate或采用未批准manifest。成功同步返回200及新state/ETag；同key同digest重放返回同一结果，同key请求
漂移返回409 `idempotency_conflict`。wire/schema错误返回400 `schema_validation_failed`；不存在、digest不匹配或不可见的exact manifest ref返回404
`resource_not_found`；已知但未批准的target、Candidate database schema version与startup-verified exact version不相等，或任一active Model
Deployment与目标Candidate确定性不兼容，都返回409
`invalid_state_transition`且不可重试。ApiProblem固定`detail=None`、safe message为空，至多包含一个allowlisted field path/reason和opaque
request ID，不含ID、digest、region、manifest正文、tenant catalog或backend detail。public If-Match在capture或final CAS失配都返回412
`etag_mismatch`并把同一Receipt terminalize为稳定Rejected结果；active-set或encryption mutation造成的generation/ETag漂移不能内部重试成503。
claim/capture后的deadline、transient dependency或serializable/CAS race只有经Receipt→InstallationReleaseState classification证明public ETag仍
相等时，才能在规定重试后返回503 `temporarily_unavailable`；state保持旧值且可以保留一个可续租/接管的非终态Processing Receipt，但不存在terminal
command winner。唯一例外是观察到另一个未过期same-key/same-digest Processing claim时返回的in-progress 503：它发生在If-Match前、只表示已有
命令仍在执行，不启动resolver/scan或第二claim，也不把dependency/CAS结果归类为可重试。该目标路由在实现、OpenAPI、安全和
PostgreSQL fixture完成前不是当前API。

稳定的state/count/EOF invariant损坏返回500 `internal_error`并阻止切换，不能伪装为可重试503。GET使用§27 closed `query_list` rate class，
promote/rollback使用closed `management_mutation` rate class；不创建未登记的installation专用class、limit authority或隐藏reserve。每个成功
state-transition winner都在同一事务写03 installation-scoped Command Receipt、Release Event与Outbox；稳定Rejected只terminalize bounded
Receipt结果且不追加Release Event，exact replay也不重复Event。

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

Admission先按02在事务外拒绝错误schema version、超过1 MiB、超过512个distinct Model refs及非法candidate set，并解析immutable exact
manifest closure。mutation transaction验证principal、tenant、input/Artifact、quota、suspension、deadline和Deployment closure，固定version 2
RunBindings后创建Queued Run/outbox。
ActiveHead selector只读取一个CAS generation，receipt返回exact Deployment、installation binding和bindings digest；任一非首选Model候选不兼容
也使整个admission失败，不能删候选或提前选择。
该事务固定使用PostgreSQL serializable isolation；Receipt claim/replay后按03 rank先锁18 Active InstallationReleaseState，再锁Tenant security
与按kind/ID排序的Resource/Deployment，逐项复验事务外确定集并调用16 compatibility port。所有Principal generation、tenant/Agent gate和
exact binding closure读取都进入SSI依赖，提交前再次比较已锁installation generation/state digest。序列化或internal generation失败按
同一Idempotency-Key最多重试18规定次数，耗尽返回503而不是412；不能降级为read-committed的check-then-insert。
installation为Uninitialized时返回409 `invalid_state_transition`且不创建Run/Receipt成功结果。`expected_head_generation`与current Agent head
不匹配同样是409 domain conflict，不是HTTP If-Match，因此不能返回412。

成功admission固定返回`201 Created`、RunSnapshot、`Location: /v1/runs/{run_id}`和ETag；Run执行异步不等于
admission Operation，因此不返回202。只有尚未创建目标resource的durable ManagementOperation返回202。

请求不能覆盖Model、Skill、Capability、Context、Policy、Secret、Sandbox、network、retry、tool或provider参数。
input必须通过Agent input schema。`service_class`是policy可选请求，只能选择公开closed等级，不能请求07的
`critical_control`或扩大quota。ServiceClass machine wire固定为`low | normal | high`；它只是受policy约束的公开请求，
不能直接写Scheduler Priority。Runtime API不接受client-supplied Run ID；Idempotency-Key保证重试只创建一个Run。
Child Run不是公共create route；08的内部child admission必须逐字段继承parent冻结的installation Release/Candidate binding，不能读取当前
installation state、重新选择Candidate或把历史binding替换为current head；它仍须针对该historical Candidate验证child的全部Model candidates，
并复验exact manifest resolver和所需runtime/adapter仍在retention内。

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
GET  /v1/approvals?state=<approval_state>&limit=<page_size>&cursor=<opaque_cursor>
GET  /v1/approvals/{approval_task_id}
POST /v1/approvals/{approval_task_id}:approve
POST /v1/approvals/{approval_task_id}:deny
```

```rust
#[serde(tag = "subject_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ApprovalSubjectViewV1 {
    CapabilityInvocation {
        run_id: RunId,
        invocation_id: InvocationId,
        input_digest: Digest,
    },
    TenantEncryptionDomainAdd {
        storage_binding_digest: Digest,
        kms_binding_digest: Digest,
    },
    TenantEncryptionDomainRebind {
        encryption_domain_id: EncryptionDomainId,
        storage_binding_digest: Digest,
        kms_binding_digest: Digest,
    },
    TenantEncryptionDomainRevoke {
        encryption_domain_id: EncryptionDomainId,
    },
}

struct ApprovalTaskViewV1 {
    schema_version: u32, // const 1
    approval_task_id: ApprovalTaskId,
    state: ApprovalState,
    subject: ApprovalSubjectViewV1,
    effect: Effect,
    safe_prompt_key: BoundedKey,
    policy_revision_id: ResourceVersionId,
    approver_rule_digest: Digest,
    evidence_artifacts: Vec<ArtifactRef>,
    deadline: DateTime<Utc>,
    generation: u64,
    projection_version: u64,
}

struct ResolveApprovalTaskRequestV1 {
    schema_version: u32, // const 1; no optional fields
}

struct ApprovalTaskListEntryV1 {
    task: ApprovalTaskViewV1,
    etag: Etag,
}

struct ApprovalTaskListPageV1 {
    schema_version: u32, // const 1
    items: Vec<ApprovalTaskListEntryV1>,
    next_cursor: Option<OpaqueListCursor>,
}
```

- interaction GET要求`interaction.read`，Approval exact GET要求`approval.read`，respond/decline与approve/deny分别要求
  `interaction.respond`/`approval.respond`；task/approval projection显示请求来源、safe prompt key、schema、Effect summary、deadline和可选ArtifactRefs，
  Tenant encryption-domain projection只显示04允许的safe change kind/target与binding digest，不显示KMS key/locator/Secret；
- Approval list是eligible approver的durable discovery surface：tenant只从verified active TenantPrincipalBinding派生，不接受tenant path/header/query
  override；调用者必须同时是该tenant的`HumanApprover`、拥有`approval.read + approval.respond`、满足Task冻结的完整approver rule与minimum authn，且
  rule要求职责分离时不能是Task冻结的requester。返回值只含同时满足这些条件的Task，不得把“知道`apr_` ID”当作assignment或授权证据；
- list只允许`state`、`limit`、`cursor`三个query字段。`state`省略时固定为`Pending`，提供时只能是04 `ApprovalState`单值；page size受18 effective
  `list_page_items` default/hard max约束，顺序固定为`(created_at DESC, approval_task_id DESC)`，不返回total count。cursor按§14绑定tenant、principal
  permission digest、state、page size、snapshot和expiry；每页仍重验current principal binding及各Task冻结rule，continuation参数漂移返回
  `cursor_invalid`。响应为closed `ApprovalTaskListPageV1`、使用`query_list` rate class并强制`Cache-Control: no-store`；entry携带当次观察到的
  strong ETag，后续approve/deny仍以If-Match复验，竞态返回412；
- `ApprovalTaskViewV1`是04 shared Task current authority的closed read projection，不是第二state；`evidence_artifacts`为0～8个同tenant Ready
  `ArtifactRef`并按Artifact ID严格升序且唯一，`safe_prompt_key`为1～128 byte machine key而非自由prompt。GET返回strong ETag与`no-store`；
  unknown/null/cross-subject字段、`tsk_` ID、正文、owner snapshot、raw approver rule或Policy expression均不进入public DTO；
- approve/deny都要求Idempotency-Key、If-Match和exact `ResolveApprovalTaskRequestV1`；ETag由ApprovalTask ID、generation、projection version、
  state与payload digest派生。missing If-Match在进入Receipt前拒绝，stale ETag terminalize 412 `etag_mismatch`；terminal same-key/same-digest
  replay先于current ETag/state，new key对非Pending Task稳定409，body不能携带comment、proposal、generation或approver override；approve/deny的
  Receipt operation分别固定为`approval_task.approve.v1`/`approval_task.deny.v1`，不能使用generic `approval.resolve`；
- response绑定tenant、principal capability、Task generation与closed owner variant：Capability Approval校验Run/Invocation及owner snapshot，
  Tenant encryption-domain Approval校验Tenant aggregate/change owner snapshot且不得伪造Run/Invocation；interaction另校验request schema；
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
POST /v1/artifacts/{artifact_id}:rescan
POST /v1/artifacts/{artifact_id}:delete
```

Prepare/complete/issue-download/rescan/delete都要求Idempotency-Key并逐项使用15 §9 Receipt table的exact request/result schema；operation依次固定为
`artifact.upload.prepare.v1`、`artifact.upload.complete.v1`、`artifact.download_grant.issue.v1`、`artifact.rescan.v1`、`artifact.delete.v1`。
transfer response `Cache-Control: no-store`，grant只对exact object/
closed capability/audience/subject/deadline有效。`issue-download` same key/digest
逐字节重放由15 deterministic sealed-token contract生成的同一grant/token，不创建第二能力，different digest返回409 `idempotency_conflict`。
`issue-download`只返回平台Artifact Download Gateway address + opaque token，绝不返回object-store URL/credential。
`rescan`要求`artifact.rescan`、Idempotency-Key、current Artifact strong If-Match、closed `ArtifactRescanRequestV1`与
`management_mutation` rate class；Receipt operation exact为`artifact.rescan.v1`。winner事务按15把Ready Artifact推进Quarantined，创建
`ArtifactRescan` ManagementOperation及其immutable binding、唯一scan Job并设置`current_job_id`；成功返回202及
`Location: /v1/operations/{operation_id}`。terminal same-key/same-digest重放同一202/Location且不创建第二Operation/Job，request或target漂移返回
`idempotency_conflict`，If-Match失配返回412，非Ready/hold/policy不允许的状态按closed transition错误拒绝。
`complete-upload`与`delete`都必须使用15 registered receipt request把path `artifact_id`、closed body及exact If-Match纳入request digest；缺header在
Receipt前拒绝，new claim的header与current strong ETag不等时把该Receipt terminalize为稳定412。`complete-upload`只允许current Staging、matching active
public upload Grant/owner/deadline；`delete`按15 legal hold/reference/retention transition guard。terminal replay在任何current Artifact/Grant/Operation
读取和If-Match检查之前只从Receipt result返回原status/body/Location/ETag，因此后来state或权限漂移都不改写既有response；new key遇到非允许state返回
closed 409/422，不得伪装成412。
普通client不能调用Finalize/Reference或指定object key、Ready state、classification override、bucket/KMS。每次Range/download都由Artifact
Download Gateway逐字段重验current permission、Principal subject、public-download port/purpose、single-use Active read variant、token/binding digest、
generation/version/use ordinal、encryption-domain fence和content-evidence freshness；token只放Authorization header，不放query/cookie，每个Range/
重连必须重新issue且旧token重放稳定拒绝。read-use耗尽不阻止并发revoke提升generation；I/O后还要用sealed ticket复验全部current事实。

Prepare winner在一个事务内创建Staging Artifact、以该Artifact为target的durable `ArtifactVerify` ManagementOperation、其immutable
`ArtifactVerifyOperationBindingV1`、public UploadGrant及对应Receipt，并使该Operation成为Staging Artifact的initial owner；Operation初始为
`Queued`且`current_job_id=None`，binding已冻结预分配的唯一scan
Job ID，但此时不得创建Job row。prepare返回201，`Location`指向Artifact，response body同时返回该可查询Verify Operation的typed reference；
same-key/same-digest replay只从`ArtifactPrepareTerminalResultV1`重建同一Artifact safe view、byte-identical upload token、Grant和Operation identity，
不读取current aggregate/config。public `CompleteUpload` winner取得actual object facts后，在同一事务只创建由该
既有Operation拥有的预分配scan Job、安装`ArtifactVerifyJobBindingV1`并设置`current_job_id=Some(scan_job_id)`，不得创建第二个Operation；成功返回202，
返回Uploaded Artifact的新strong ETag，`Location`指向该既有`/v1/operations/{operation_id}`。上传放弃、持续失败至deadline或delete必须由15的deadline/delete authority在Operation仍为
`Queued + None`时terminalize该Verify Operation并收敛Artifact/Grant，不得遗留可永久查询的Queued orphan。rescan和需要异步引用/物理处理的delete也
固定异步delete总是创建`ArtifactDelete` Operation并返回202、新Artifact ETag及统一Operation Location；不提供同一路由的204/sync隐式分支。
rescan也返回202及新Artifact ETag/统一Operation Location。不存在第二套Artifact专用Operation状态机或ID。Operation成功只表示15规定
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
业务失败内部使用05的`Failure`，公开投影使用本规范`FailureProjectionV1`。二者即使文本相似也不共享enum，Gateway不得把terminal Failure
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
| 503 | 依赖/容量暂不可用；可有非终态Processing receipt，但没有已提交terminal winner |
| 504 | gateway deadline，不能推断backend command未提交 |

API Gateway不能把所有domain failure压成200或500。同一`ApiProblem.code`在REST/internal gRPC/SDK中语义一致；SSE和
terminal resource使用本规范`FailureProjectionV1`，即使某个code拼写相同也必须保留layer/discriminator，不能混为同一enum。

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
ArtifactWorkloadBrokerService
ArtifactModelBrokerService
ArtifactSandboxBrokerService
ArtifactWorkloadProducerService
ArtifactMaintenanceAuthorityService
ModelArtifactProducerService
SecretBrokerService
CallbackInboxService
```

Artifact internal gRPC必须注册为以下六个不可合并的exact service，不能保留generic `ArtifactBrokerService`、按header/body动态选择
audience，或在同一listener上用method alias扩大权限。public Upload/Download Gateway都不是internal service；它们只实现各自§19 HTTPS transfer：

| Service | 唯一首版方法 | exact client mTLS URI SAN | authority上限 |
|---|---|---|---|
| `ArtifactWorkloadBrokerService` | `ReadRuntimeArtifact` | `spiffe://insight.platform/workload/runtime-worker` | 只读15 `workload_kind=Runtime`的exact Artifact request；返回bounded bytes，不返回locator/KMS plaintext |
| `ArtifactWorkloadBrokerService` | `ReadRegistryArtifact` | `spiffe://insight.platform/workload/registry-validation-worker` | 只读15 `workload_kind=RegistryValidation`的exact Artifact request；返回bounded bytes，不返回locator/KMS plaintext |
| `ArtifactWorkloadBrokerService` | `ReadCapabilityArtifact` | `spiffe://insight.platform/workload/capability-worker` | 只读15 `workload_kind=Capability`的exact Artifact request；返回bounded bytes，不返回locator/KMS plaintext |
| `ArtifactWorkloadBrokerService` | `ReadContextArtifact` | `spiffe://insight.platform/workload/context-worker` | 只读15 `workload_kind=Context`的exact Artifact request；返回bounded bytes，不返回locator/KMS plaintext |
| `ArtifactWorkloadBrokerService` | `ReadMcpArtifact` | `spiffe://insight.platform/workload/mcp-host` | 只读15 `workload_kind=Mcp`的exact Artifact request；返回bounded bytes，不返回locator/KMS plaintext |
| `ArtifactModelBrokerService` | `ReadModelRequest` | `spiffe://insight.platform/workload/model-worker` | 只读exact Model request Artifact；数据库SELECT-only，对象存储只允许HEAD/GET与KMS decrypt |
| `ArtifactSandboxBrokerService` | `ReadWasiArtifact`、`ReadMicroVmArtifact` | `spiffe://insight.platform/workload/sandbox-controller` | 只读exact Sandbox Job/package/input Artifact；数据库SELECT-only，对象存储只允许HEAD/GET与KMS decrypt |
| `ArtifactWorkloadProducerService` | client-streaming `StageRegistryArtifact` | `spiffe://insight.platform/workload/registry-validation-worker` | 只写exact Registry Validation Job Attempt的Staging Artifact，最多推进到Uploaded并触发既有scan Job |
| `ArtifactWorkloadProducerService` | client-streaming `StageCapabilityOutput` | `spiffe://insight.platform/workload/capability-worker` | 只写exact Capability Job Attempt output，最多推进到Uploaded并触发既有scan Job |
| `ArtifactWorkloadProducerService` | client-streaming `StageContextOutput` | `spiffe://insight.platform/workload/context-worker` | 只写exact Context Job Attempt output，最多推进到Uploaded并触发既有scan Job |
| `ArtifactWorkloadProducerService` | client-streaming `StageMcpOutput` | `spiffe://insight.platform/workload/mcp-host` | 只写exact MCP Job Attempt output，最多推进到Uploaded并触发既有scan Job |
| `ArtifactWorkloadProducerService` | client-streaming `StageSandboxOutput` | `spiffe://insight.platform/workload/sandbox-controller` | 只写exact Sandbox Job Attempt output，最多推进到Uploaded并触发既有scan Job |
| `ArtifactMaintenanceAuthorityService` | `ReadForScan` | `spiffe://insight.platform/workload/artifact-scanner-finalizer` | 只对exact scanner Job/fence流式读取15 `ScanRead`正文；不返回locator/KMS plaintext |
| `ArtifactMaintenanceAuthorityService` | `HeadExactGeneration` | `spiffe://insight.platform/workload/artifact-scanner-finalizer`或`spiffe://insight.platform/workload/artifact-gc-reconciler` | 只对各自exact Job/fence返回bounded HEAD evidence；两项是该method的完整closed allowlist |
| `ArtifactMaintenanceAuthorityService` | `DeleteExactGeneration` | `spiffe://insight.platform/workload/artifact-gc-reconciler` | 只执行15 lifecycle guard已授权的exact generation delete并返回bounded deletion/absence evidence |
| `ModelArtifactProducerService` | client-streaming `StageModelOutput` | `spiffe://insight.platform/workload/model-worker.artifact-output` | 只为exact Model Attempt写staging并最多推进`Staging -> Uploaded -> Verifying -> Verified` |

15 `ArtifactWorkloadStageKindV1`到Workload Producer method/audience/owner/Job/port的唯一机器映射固定在
`contracts/platform-v1/protocol/artifact-workload-stage-routes.json`及其closed schema
`contracts/platform-v1/schemas/protocol/artifact-workload-stage-routes.schema.json`。registry `schema_version=1`且恰有以下五行；按下表固定ordinal
顺序、无重复，全部字段required，purpose/WorkClass集合按wire bytes严格升序且唯一。stage-kind wire依次exact为
`registry_artifact | capability_output | context_output | mcp_output | sandbox_output`：

| stage kind | exact method | client startup profile set / exact URI SAN | Grant audience | Artifact owner / Job typed owner | JobKind / WorkClass | exact port source / purpose |
|---|---|---|---|---|---|---|
| `RegistryArtifact` | `StageRegistryArtifact` | `registry_validation_worker/v1` / `spiffe://insight.platform/workload/registry-validation-worker` | `RegistryWorker` | `Revision` / `ManagementOperation` | `RegistryValidation` / `RegistryValidation` | frozen Registry artifact slot / `Package \| Sbom \| BackendBinding` |
| `CapabilityOutput` | `StageCapabilityOutput` | `capability_native_worker/v1 \| capability_remote_worker/v1` / `spiffe://insight.platform/workload/capability-worker` | `CapabilityWorker` | `CapabilityInvocation` / `CapabilityInvocation` | `Capability` / `CapabilityNative \| CapabilityRemote` | frozen Interface output port / `CapabilityOutput` |
| `ContextOutput` | `StageContextOutput` | `context_worker/v1` / `spiffe://insight.platform/workload/context-worker` | `ContextWorker` | `ContextObservation` / `ContextQuery` | `Context` / `Context` | frozen Context output port / `ContextDerived \| McpResource` |
| `McpOutput` | `StageMcpOutput` | `mcp_host/v1` / `spiffe://insight.platform/workload/mcp-host` | `McpHost` | `CapabilityInvocation` / `CapabilityInvocation` | `Capability` / `CapabilityRemote` | frozen MCP-backed Interface output port / `CapabilityOutput` |
| `SandboxOutput` | `StageSandboxOutput` | `sandbox_controller/v1` / `spiffe://insight.platform/workload/sandbox-controller` | `SandboxGateway` | `CapabilityInvocation` / `CapabilityInvocation` | `Sandbox` / `Sandbox` | frozen Sandbox output port / `SandboxOutput` |

表中`|`表示closed set membership而非数组顺序；机器数组仍按wire bytes严格升序。enum wire、owner nominal、JobKind、WorkClass、port/purpose均直接
复用03/07/15定义，registry不得复制宽松字符串validator。每行`client_startup_profile_ids`为1～2项、严格升序且唯一，不同stage kind不得复用profile。
这些profile同时是18判断stage kind是否enabled的唯一输入：同一Candidate只要安装至少一个引用该行任一profile的client startup manifest，
对应kind即enabled；不能由Helm、环境变量、RPC字段或Producer配置增删。所有生产Candidate至少启用`RegistryArtifact`，Q1启用全部五项。
root `contract_digest`、Candidate builder、client/Producer readiness必须解析同一registry并逐值复验protobuf descriptor、profile、method、URI SAN、
audience、Artifact/Job owner、JobKind、WorkClass、port和purpose；不存在基于method字符串或相邻合法enum的fallback。

六个service都必须在TLS握手后的service authorization再次比较method-specific exact URI SAN；CN、DNS SAN、bearer/human token、tenant header、forwarded
identity、自报workload字段或拥有同名permission都不能替代该audience。请求tenant、Run/ModelTurn/Sandbox Job、Artifact、attempt/lease/
Worker generation、grant/reservation、purpose、Policy closure和deadline只能从durable command与current fence派生，stream body不能覆盖。
错误service、method、URI SAN或audience组合在解析正文、访问PostgreSQL/S3/KMS前拒绝，并形成body-free受限audit。
Model Worker必须使用不同client SVID与连接池访问read Broker和Producer：`.../model-worker`只能调用`ReadModelRequest`，
`.../model-worker.artifact-output`只能调用`StageModelOutput`；任一凭证互换都在进入authority前拒绝。

`ArtifactWorkloadBrokerService`、`ArtifactModelBrokerService`与`ArtifactSandboxBrokerService`是三个独立只读进程边界，不得注册
write/stage/upload/complete/verify/finalize/delete方法。Workload Broker的五个method都消费15同一closed
`ArtifactWorkloadReadRequestV1`，method discriminator、`workload_kind`、URI SAN和durable owner/Job必须逐值对应；任一role不能调用相邻method，
也不能通过请求字段把一种workload投影为另一种。

`ArtifactWorkloadProducerService`是独立ordinary-workload staging进程边界。五个client-stream method都只接受15 exact
`JobAttempt + WorkloadBound + StagingWrite`，并逐值比较method、URI SAN、Artifact audience/port/purpose、typed owner、Job/attempt、lease token/
generation、WorkerProcessGeneration、request binding、grant generation、exact staging identity、byte/digest ceiling与deadline；public
`Principal + OpaqueBearer`、`JobRequest`、错误method/SAN/owner或Model-output request全部在读取stream body和访问DB/S3/KMS前拒绝。
成功stream只能对exact staging generation执行有界写入并最多提交`Staging -> Uploaded`，随后通过15现有Artifact scan Job/Receipt流程创建或重放
同一scan work；它不能读取业务Artifact、执行scan、推进Verified/Ready、finalize/reference、处理Model output、修改owner Job terminal state，
也不能注册generic object API或public HTTP route。public Artifact Upload Gateway反向只接受`Principal + OpaqueBearer`，两条写入路径的
credential、grant delivery和request envelope不可互换。

`ArtifactMaintenanceAuthorityService`是独立maintenance进程边界，只接受15 closed `ArtifactMaintenanceRequestV1`：`ReadForScan`、
`HeadExactGeneration`和`DeleteExactGeneration`分别只匹配`ScanRead`、`HeadExactGeneration`和`DeleteExactGeneration` variant。它可以在
exact Job/lifecycle fence下使用受限对象存储/KMS identity执行该次操作，但只向worker返回bytes或bounded typed evidence；永不返回明文locator、
KMS plaintext、bucket credential，不注册业务read、upload/stage/finalize或generic object API。Artifact Scanner/Finalizer与GC/Reconciler
worker自身不得持有S3/KMS identity或直接调用object store。

`ModelArtifactProducerService`不得注册任一read Broker方法、Workload Producer方法、Maintenance方法、Sandbox方法、generic object API或公共HTTP route；
其restricted数据库角色不能修改Run、RunNode、Invocation/ModelTurn、Job current state、RunValue、业务Artifact Output Link、quota余额、
Event或Outbox。

六个internal service必须使用六组不可互换的Deployment、ServiceAccount、mTLS server identity、restricted数据库credential/pool、
storage/KMS identity、connection pool和process-local permit；同一binary/library可以复用无状态代码，但任一进程/listener只能安装一个service。
Artifact Upload Gateway与Artifact Download Gateway再使用两个独立Deployment、ServiceAccount、数据库credential/pool、storage/KMS identity、
connection pool、permit与HPA，且不复用任一internal service identity/listener或彼此凭证；统一hostname不改变这两个物理failure domain。

`StageModelOutput`必须使用15/16的closed header/chunk/terminal与same-Attempt `JobCommit` Receipt，在object I/O前后分别重验exact
current Job fence。Producer receipt只证明预留Artifact bytes已经Verified，不是Model terminal result。Producer无权将Artifact推进
Ready、创建Output Link/RunValue、推进ModelTurn/Job/Node/Run、settle quota或发布`model.completed`。只有Model owner repository的单一
PostgreSQL terminal first-winner事务可以原子完成Verified -> Ready、唯一ModelTurn Output Link、immutable Artifact-backed RunValue、
ModelTurn/Job terminal、quota settlement、Event与Outbox；任一步失败全部回滚，stale/cancel/timeout/loser仍保持非Ready并进入orphan GC。

- protobuf package/version固定，unknown enum fail closed；
- 所有 call 使用 mTLS workload identity、service authorization、deadline、message size 和 retry policy；
- tenant/Run/Invocation等从durable command派生，backend不能body override；
- Worker claim/commit必须含work/attempt/epoch/fence；
- callback使用scoped one-time identity/inbox dedupe；
- Secret value只在专用broker response且不进入generic envelope；
- gRPC retry仅对明确idempotent method和stable request ID；
- public Ingress/Gateway不能路由任何internal service，六个Artifact internal service也不得出现在OpenAPI或公共service registry；
- internal API不绕过PostgreSQL transaction/outboxauthority。

## 26. Security Headers 与 Gateway

- HTTPS only，TLS policy和certificate rotation由18定义；
- HSTS、nosniff、strict content-security policy用于UI/download origin；
- API response默认 `Cache-Control: no-store`，immutable public-safe metadata可显式private cache；
- request smuggling、ambiguous Content-Length/Transfer-Encoding、header normalization和duplicate authheader在edge拒绝；
- decompression在size/rate limit下进行；
- Artifact/upload/download使用独立origin或path policy，防止active content污染API origin；
- redirect只用于明确OAuth flow，不用302隐藏mutation result；Artifact download不得redirect到object store，必须经15 Gateway/Broker代理逐请求授权；
- error页面不返回HTML stack/debug；
- source IP仅作rate/safety signal，不作为Principal identity；
- Gateway不能注入tenant/permission结论，backend重新验证signed identity context。

## 27. Rate Limit、Quota 与 Backpressure

首版machine contract只定义每个请求恰好扣减一个`service/rate-class/scope/principal` composite短窗口bucket，不宣称存在未建模的
hierarchical token bucket；tenant/resource durable限制由04 Quota与各domain admission拥有，connection/in-flight总量由本规范bulkhead拥有。

```rust
struct ApiRateClassLimitV1 {
    rate_class: ApiRateClassV1,
    requests_per_window: u32,
    window_milliseconds: u32,
}

struct GatewayRateLimitProfileV1 {
    schema_version: u32, // const 1
    limits: Vec<ApiRateClassLimitV1>,
}
```

- `ApiRateClassV1` wire闭集固定为`management_mutation | run_admission | query_list | sse | interaction | artifact_transfer |
  internal_callback`；route registry/OpenAPI的`x-insight-rate-class`必须逐值命中其一，unknown class使startup失败；
- 唯一schema路径为`contracts/platform-v1/schemas/gateway-rate-limit-profile.schema.json`并进入root contract digest；`limits`必须恰有七项，按
  rate-class wire bytes严格升序且唯一，两个数值都为正。Candidate `deployment_config_digest`覆盖exact profile canonical bytes；请求、tenant
  resource、route handler和环境变量不能临时创建class或放宽Candidate值；
- tenant route的limiter key包含service/class、TenantId与principal/workload stable digest；Installation Release route没有tenant维度，key改为
  service/class、configured InstallationId与installation principal stable digest，不能填fake tenant或与tenant bucket共享identity；
- 每个请求只扣上述唯一composite key；不得暗中增加使用同一profile数值的installation/tenant/resource父bucket，也不得把一个principal的剩余额度
  转给另一个principal。需要新的聚合短窗口scope时必须扩展closed profile/schema/route registry后再发布；
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

- Management API、Runtime API、SSE Gateway、Artifact Upload Gateway、Artifact Download Gateway和每个internal gRPC role独立
  Deployment/DB pool/HPA；
- `ArtifactWorkloadBrokerService`、`ArtifactModelBrokerService`、`ArtifactSandboxBrokerService`、
  `ArtifactWorkloadProducerService`、`ArtifactMaintenanceAuthorityService`与`ModelArtifactProducerService`分别使用独立Deployment、
  ServiceAccount、mTLS identity/method allowlist、restricted DB credential/pool、storage/KMS identity、permit与NetworkPolicy；六者不得共享Pod或用一个
  process-wide listener/semaphore模拟隔离，Maintenance不得把delete identity借给worker，Producer也不得借用read Broker的SELECT-only credential；
- public Artifact Upload Gateway与Artifact Download Gateway分别使用独立Deployment、ServiceAccount、连接池、DB/storage/KMS identity、
  permit、HPA与readiness；两者只共享外部hostname/HTTP contract，且与六个internal service完全隔离；public Upload只安装
  `Principal + OpaqueBearer` adapter，Workload Producer只安装`JobAttempt + WorkloadBound` adapter；
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
- encryption-domain list/item与add/rebind/revoke OpenAPI覆盖tenant-derived scope、permission组合、closed proposal/apply DTO、collection ETag、
  Idempotency-Key和`management_mutation` rate class；approval request 201/Location/body及same-key exact replay只创建一个Task，proposal/ETag漂移冲突；
  proposal/effect/policy/rule/requester均由04 authority冻结，missing/wrong/inactive Approval Policy稳定403；六个Receipt operation discriminator逐route
  唯一且不同command复用同一Idempotency-Key不碰撞；64/N+1、65536/N+1及known-incompatible storage/KMS pair稳定422 `invalid_request`；
- Approval list/GET/approve/deny覆盖`apr_` kind、tenant-derived eligible discovery、state默认/闭集、page hard max、stable order、cursor
  tenant/principal/filter/rule-generation绑定、no-store、entry ETag、read/respond permission、requester separation、owner subject四variant、
  Pending/Approved/Rejected/Expired/Cancelled、strong ETag、
  missing/stale If-Match、same-key terminal replay、new-key terminal conflict、bounded evidence ArtifactRef及unknown/null/cross-variant/`tsk_`/
  sensitive-field负例；
- encryption-domain apply覆盖Pending/Approved/Rejected/Expired/Cancelled、approval tenant/operation/target/generation/policy swap、unknown storage/KMS、
  lower/equal/stale collection ETag、same-binding rebind、Revoked key复用及add ID response-loss replay；stale header稳定412，而header=current但Approval
  snapshot陈旧稳定409且两者均exact replay；并发domain mutation、Release preflight、Model
  terminal和Artifact read只能观察完整旧或新security/compatibility generation，成功只追加一次tenant+installation Event/Outbox；
- root Run admission对不超过02上限的全部Model candidates执行同一16 compatibility port；任一非首选candidate失败时整体拒绝，child Run只继承
  parent exact installation binding；
- Installation Release GET覆盖Uninitialized/Active safe projection与strong ETag；promote/rollback覆盖permission、If-Match缺失/失配、
  Idempotency-Key同请求重放/请求漂移、unknown/unapproved manifest和detail-free安全ApiProblem；
- installation GET分别覆盖support/manage正向及ServiceIdentity/tenant token负向；command覆盖exact InstallationOperator+manage正向和其余
  audience/principal负向，并验证query/control rate bucket与Receipt/Event discriminator；
- promote覆盖首次Uninitialized激活和Active切换；rollback覆盖Uninitialized拒绝、current target拒绝、approved旧target成功；已有terminal
  Receipt在state再次变化后仍原样重放且不追加第二Event；
- promote/rollback在resolver/scan前完成terminal Receipt replay/conflict与capture；Processing crash/lease takeover复用同一Receipt与递增
  `claim_generation`，resolver/scan期间不持有数据库事务或行锁，capture/final/classification transaction严格按
  Receipt→InstallationReleaseState锁序；
- promote/rollback都把与startup-verified installed database schema version不相等的Candidate terminalize为同Receipt稳定409
  `invalid_state_transition`；覆盖lower/higher/equal三种fixture，没有range、rollback特例或down-migration；
- 18最大4096项/每页256项的active Model scan覆盖零项、边界项、count不一致、任一项不兼容、deadline与最终state CAS失败；promotion、
  activate/deactivate/suspend/resume/archive/retire和root Run admission并发时只能提交完整旧或完整新generation；
- captured state未变时active Model不兼容terminalize同一Receipt为稳定409且不写Release Event；response loss后exact replay不重扫catalog；
- capture时rollback@Uninitialized/current-target等transition guard失败terminalize稳定409；capture后active-set/encryption mutation改变public
  ETag时稳定terminal replay 412；resolver/scan transient与serialization race都只有在classification证明ETag未变时才可在三次后返回带Processing
  Receipt的503；active same-key Processing observation单独覆盖其pre-If-Match in-progress 503且不产生第二scan；
- Installation Release GET/promote/rollback在OpenAPI分别精确声明`query_list`/`management_mutation`，unknown或自造rate class不能启动；
- Gateway rate profile覆盖七项exact set、排序/重复、零值/overflow、unknown/null与Candidate deployment-config digest漂移；installation key不含
  fake tenant且不会与tenant route碰撞；
- child的全部Model candidates针对parent inherited historical Candidate校验；current Candidate已切换时也不得fallback，resolver/runtime
  retention缺失时fail closed；
- cancel/pause/resume/signal/interaction/timeout/result 竞态；
- SSE snapshot/replay/live gap/slow client/disconnect/reconnect/cursor expiry/permission revoke；
- outbox/NATS 丢失、重复、乱序下 SSE durable sequence 收敛；
- ManagementOperation target fixture覆盖三个Artifact kind与`Artifact { artifact_id }`正向组合，证明首版不存在`ArtifactUpload` kind，并覆盖unknown/null/裸ID、
  `ResourceKind + ResourceId`、错prefix、额外字段、非Artifact kind↔target组合负例；DTO、schema、Rust nominal与kind-target registry逐值一致；
- ArtifactVerify Operation fixture逐字段比较15 `ArtifactVerifyOperationBindingV1` snapshot、canonical digest与
  `scan_operation_binding_digest`，覆盖target-only/空snapshot、schema version、Artifact/Blob/policy/rules/deadline/scan Job swap、
  start preimage伪造actual generation/uploaded version、client snapshot override和GET正文泄漏负例；ordinary workload pre-success所有失败均证明无
  Operation/scan Job row，stage success证明同一事务创建Operation+immutable binding、15 `ArtifactVerifyJobBindingV1`及唯一Job/pointer；public prepare
  则证明同一事务创建由Staging Artifact owner回绑的Queued Verify Operation、immutable binding与Grant，`current_job_id=None`且无Job row，201/replay都返回
  同一Artifact/Grant/Operation reference。public `CompleteUpload`证明只在既有Operation上创建preallocated scan Job、exact冻结actual generation/
  uploaded version、回绑Operation ID/binding digest并设置唯一pointer，不创建第二Operation；abandon、失败至deadline和delete fixture证明Queued-none
  例外被terminalize并cleanup。Job terminal/Operation merge、clear/switch及response-loss replay均保持pointer双向一致且不修改Operation binding；
- Operation worker/API crash、cancel、deadline和result receipt恢复；
- Error code/HTTP mapping无raw backend/Secret/policy/stack泄漏；
- rate/connection/payload/list/filter/cursor/backpressure hard limit；
- Management/Runtime/Operator/workload token audience和route隔离；
- Artifact transfer grant no-store/exact audience和active content origin隔离。
- Artifact rescan OpenAPI/fixture覆盖exact route、`artifact.rescan`、`management_mutation`、closed body、If-Match、
  `artifact.rescan.v1` Receipt replay/conflict、Ready→Quarantined与202 Operation Location；response loss/并发请求只形成一个Operation/current scan Job；
- 六个Artifact internal service均无OpenAPI/public route/Ingress/外部LoadBalancer，Artifact Upload/Download Gateway探测任一internal
  service/method均不可达；统一public hostname按closed route/method分别只到两个HTTP Gateway，任一Gateway都不能注册另一个lane或internal RPC；
- Artifact Upload/Download Gateway的route、token、ServiceAccount、DB credential、S3/KMS identity、permit与HPA逐项互换均fail closed；Upload
  只接受`Principal + OpaqueBearer`并拒绝workload mTLS/`WorkloadBound`；饱和/kill/rollout时Download仍可GET/HEAD/Range，Download
  饱和/kill/rollout时Upload仍可StagingWrite，且两侧都不返回object-store URL；
- Runtime、Registry Validation、Capability、Context、MCP、Model、Sandbox、Artifact scanner/GC的exact workload身份，以及human token、
  CN/DNS SAN和相邻ServiceAccount，对六个Artifact service逐项做URI SAN/service/method/variant互换负向fixture；未授权组合在
  DB/S3/KMS访问前拒绝；
- 三个read Broker的数据库写入与S3 PUT/DELETE/KMS encrypt被权限拒绝；Artifact Scanner/Finalizer和GC/Reconciler直接S3/KMS请求均被
  NetworkPolicy/cloud IAM拒绝，Maintenance只允许`ReadForScan`/`HeadExactGeneration`/`DeleteExactGeneration`各自closed Job、owner、generation、
  policy、Receipt与method-specific URI SAN，且不返回locator/credential；Workload Producer只能对exact staging generation执行PUT/HEAD，
  任意GET/list均被拒绝；Model Producer仅可额外执行16定义的same-reservation exact staging recovery GET。两者对任意Ready object GET/list与
  Run/ModelTurn/Job/RunValue/Output Link/quota/Event/Outbox写入都被权限拒绝；
- Workload Producer五个method逐项覆盖exact URI SAN、`JobAttempt + WorkloadBound + StagingWrite`、attempt/lease/worker/request/grant/staging
  fence与stream顺序/size/digest；stage kind、startup profile、Grant audience、Artifact/Job owner、JobKind、WorkClass、port、purpose、method或SAN任一逐字段
  互换都在Data/DB/S3/KMS前拒绝，尤其`RegistryArtifact`不得借用只属于scan/rescan/delete/blob-cleanup的`JobKind::Artifact`。public
  Principal/bearer、`JobRequest`、跨method/role/owner/Attempt、Model output全部拒绝。成功最多Uploaded且只
  创建或重放既有scan Job/Receipt；GET/list/scan/Verified/Ready/finalize/reference/owner terminal mutation均被DB/storage权限拒绝；单服务饱和、
  重启或DB pool耗尽不消耗另外五个internal service、Artifact Upload/Download Gateway或API/control容量；
- `StageModelOutput` pre/post Job-fence、same-Attempt replay/conflict、cancel/lease takeover/terminal first-winner竞态证明Producer最多Verified，
  owner terminal事务要么同时提交Ready/Output Link/RunValue/quota/outbox，要么全部回滚并保留非Ready orphan。

## 32. 验收标准

- 所有公开 route 都有 OpenAPI、closed schema、permission、idempotency、rate 和 audit 声明；
- 不存在 generic resource/backend pass-through 或客户端可覆盖 binding/tenant/Secret 字段；
- mutation在response丢失/并发重放时只产生一个逻辑结果；
- mutable resource无法无If-Match覆盖，immutableRevision无法update；
- long work全部返回durableOperation，不在HTTP request内持有后台future；
- root Run create事务同时固定version 2 exactBindings、current installation generation/state digest和全部Model candidate validation集合并返回
  bindings digest；child Run逐字段继承parent exact installation binding；
- public Run snapshot/result/event在终态和故障恢复后完全一致；
- SSE无per-client DB listener/NATS connection，slow client不会造成无界buffer；
- 跨 tenant、Secret、backend handle、Prompt/正文 canary 不进入 response/event/error/log/metric；
- API各bulkhead饱和/远端故障不破坏cancel、安全控制和其他route readiness；
- generated SDK/conformance suite在至少Rust/TypeScript两个client实现通过；
- route/error/event breaking change在CI被识别并阻止静默改变同一`insight.platform/v1`合同。
- Artifact internal gRPC只存在六个exact service及其列明方法；它们具有method-specific不可互换的mTLS audience、部署与最小权限，且均无
  public route/Ingress；public Artifact contract由独立Upload与Download Gateway只提供各自HTTPS transfer，不能互相借权或代理internal method；
- 六个internal service加Artifact Upload/Download Gateway形成八类物理lane；Q1单storage boundary中恰有八个独立
  Deployment/ServiceAccount/credential/pool/permit/HPA logical scope，多region/boundary按lane类增加独立scope而不合并；任一Q1 lane饱和或滚动
  不消耗另外七类lane及API/control准入容量；
- 每个Candidate至少安装一个完整Artifact Workload Producer scope；18从上述route registry与startup manifests派生enabled stage-kind集合，
  并要求它与全部Candidate storage binding的笛卡尔积恰被Workload Producer scopes覆盖一次。零scope、漏/重binding、漏/重route或运行时投影漂移
  均fail closed；
- Runtime/Registry/Capability/Context/MCP只经Workload Broker取得bounded bytes，scanner/GC只经Maintenance Authority取得scan/head/delete
  结果；这些调用方均无法取得object locator、S3/KMS credential或绕过exact Job/grant/lifecycle fence；
- Registry/Capability/Context/MCP/Sandbox普通输出只经Workload Producer写到Uploaded并进入既有scan flow；不能使用public bearer、read Broker或
  Model Producer，也不能绕过scan/finalize owner形成Ready Artifact；
- `ModelArtifactProducerService`无法创建可读或业务可引用的Model output；只有owner terminal单事务能形成Ready Artifact、Output Link与
  Artifact-backed RunValue，失败或stale输出只能保持非Ready并由GC收敛。
- Installation Release三个目标route具有closed OpenAPI/permission/error/ETag/idempotency合同；promote/rollback通过18 bounded scan与最终短CAS，
  不在锁内执行无界catalog遍历；public If-Match对应state漂移返回412，只有ETag未变的internal race或无public If-Match的root admission race
  才能在有界重试后返回503。

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

CR-165相关Installation Release route、RunBindings v2以及全Model candidate admission仍处于Architecture Revision；在02/03/04/05/06/07/08/
09/12/15/16/18与本规范完成全量cross-review并恢复Accepted前，它们不得生成OpenAPI、DTO或实现，也不得声明为当前`/v1`行为。具体SDK
生成器、Ingress产品和OIDC provider可以替换，但已接受后的`/v1` typed resource/command、Idempotency、ETag、Operation、SSE cursor、
错误模型和服务bulkhead不得改变。
