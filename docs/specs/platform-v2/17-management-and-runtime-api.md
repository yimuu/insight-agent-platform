# Platform v2 Management 与 Runtime API 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-21 |
| 依赖 | 02～16 |
| 直接下游 | 18 |

## 1. 决策摘要

平台公开协议保持`insight.platform/v1`和`/v1`，clean-cut替换，不建`/v2`、双写或兼容层。
首版API只暴露完成资源发布、Run、Task和Artifact核心流程所需的最小路由。

Operation不是独立aggregate。一切discovery、validation、dataset build、Artifact verify/delete等异步工作复用
shared Job，`GET /v1/operations/{operation_id}`是该Job的safe public projection，`operation_id == job_id`。
数据库不存在ManagementOperation current state或Installation Release API/state。

## 2. 协议权威与schema生成

OpenAPI是public HTTP projection，owner Rust nominal type和closed registry是领域语义authority。内部protobuf、JSON Schema、
Receipt result和OpenAPI component必须从owner type/registry生成或由conformance test逐字段验证，不手写多份平行合同。

每个边界合同都要求：

- closed object/enum，unknown field/value fail closed；
- nominal typed ID，不用裸UUID猜测kind；
- 有界list/string/bytes/depth和total request/response size；
- canonical serialization与digest；
- 稳定problem code和字段级error detail；
- Secret、token、内部定位和敏感正文不进public DTO；唯一例外是15明确规定、只在prepare-upload成功响应出现的短期
  `upload_target` bearer capability，它必须no-store/redact且不以明文进入Receipt/Event/log/trace。

## 3. Authentication、tenant 与通用请求

Gateway验证OIDC/JWT或等价workload credential，构建不可伪造的`PrincipalContext`：principal、tenant、roles/scopes、
auth strength、session/credential ID digest、region和trace identity。tenant来自credential/routing authority，不信任body/header自由值。

所有mutation必须包含：

- `Idempotency-Key`，每tenant + principal + operation scope唯一；
- 更改已存aggregate时的strong `If-Match`；
- bounded request deadline，服务端hard max不被调用方放大；
- trace/correlation context，但不允许用户覆盖内部tenant/owner identity。

Receipt在事务内claim并保存request digest、owner-generated typed result和status class。同key同digest重放返回同一
status/body/ETag/Location，同key异digest返回`idempotency_conflict`，不重读current aggregate伪造历史response。

## 4. 通用响应与problem

成功response返回typed body、strong ETag和`Cache-Control: no-store`（敏感/current resource）。异步command返回
`202 Accepted`、OperationView和`Location: /v1/operations/{job_id}`。

```rust
struct ApiProblemV1 {
    type_uri: String,
    title: String,
    status: u16,
    code: ProblemCode,
    detail: Option<String>,
    instance: String,
    trace_id: TraceId,
    retryable: bool,
    retry_after_seconds: Option<u32>,
    errors: Vec<FieldError>,
}
```

stable problem code至少包含`invalid_request | unauthenticated | forbidden | not_found | conflict | precondition_required |
precondition_failed | idempotency_conflict | quota_exceeded | rate_limited | dependency_unavailable | timeout |
output_too_large | internal`。domain terminal failure通过resource view/Event表示，不滥用HTTP status重写历史。

## 5. 最小Management API

类型化resource kind使用同一lifecycle，但public route仍保留domain noun，不暴露generic arbitrary JSON registry：

```text
POST   /v1/{agents|skills|capabilities|contexts|models|mcp-servers|policies|sandboxes}
GET    /v1/{kind}/{resource_id}
PUT    /v1/{kind}/{resource_id}/draft
POST   /v1/{kind}/{resource_id}/draft:validate
POST   /v1/{kind}/{resource_id}/draft:publish
GET    /v1/{kind}/{resource_id}/versions/{version_id}
POST   /v1/{kind}/{resource_id}/deployments
GET    /v1/{kind}/{resource_id}/deployments/{deployment_id}
POST   /v1/{kind}/{resource_id}/deployments/{deployment_id}:activate
POST   /v1/{kind}/{resource_id}/deployments/{deployment_id}:suspend
```

create/draft-update/validate/publish/deploy/activate/suspend语义由02拥有。Resource拥有唯一current editable Draft；Draft update使用
`If-Match`并推进generation、使旧validation失效。publish以Resource ETag + draft generation + digest为fence并创建immutable Version；
Published Version和Deployment immutable。activate/suspend都必须携带Resource `If-Match`：activate用path Deployment设置
Resource active binding并使gate为`Enabled`，suspend仅在path Deployment仍为active binding时将Resource gate设为`Suspended`。
它们只影响未来Run；已存Run使用冻结binding，Deployment row不被修改。validate/discovery/build返回Job
Operation，不建业务Operation aggregate。首版不暴露mutable Draft Version identity，也不提供尚未发布Version的GET route。

首版不提供Installation release/promote/rollback、Candidate、GateResult、ReleaseManifest、dynamic storage/KMS binding、
arbitrary runtime installer或generic plugin execution API。发布/回滚由GitOps/Kubernetes负责。

## 6. Discovery、build 与dataset API

```text
POST /v1/mcp-servers/{resource_id}/deployments/{deployment_id}:discover
POST /v1/contexts/{resource_id}/deployments/{deployment_id}:build-dataset
GET  /v1/context-datasets/{dataset_id}/versions/{generation_id}
```

discovery/build command冻结exact Deployment、source/config/schema/validator digest并创建shared Job。成功Job事务创建immutable
Discovery Snapshot或Dataset Generation、Ready manifest Artifact、Event/Outbox，并在expected active head CAS成功时移动未来绑定。
失败/取消/超时不创建半成品generation。

## 7. Operation 是Job projection

```rust
struct OperationViewV1 {
    operation_id: JobId,
    tenant_id: TenantId,
    kind: PublicJobKind,
    target: PublicJobTarget,
    state: PublicJobState,
    progress: Option<BoundedProgress>,
    result: Option<SafeJobResult>,
    error: Option<SafeJobFailure>,
    created_at: Timestamp,
    updated_at: Timestamp,
    etag: ETag,
}
```

`PublicJobKind`/`PublicJobTarget` 从03的closed Job kind-owner registry生成安全子集。首版public kind只包含
`ResourceValidation | McpDiscovery | ContextDatasetBuild | ArtifactVerify | ArtifactDelete`；target是该Job的typed direct
owner投影，可为ResourceVersion、Deployment、ContextDataset或Artifact，不限于Artifact。kind-target非法组合fail closed。

Job internal lease、payload、credential、retry evidence、object locator和diagnostic不进public view。Operation没有独立ETag/state/owner/table；
视图ETag直接来自Job projection version。取消必须调用owner-specific command，不提供generic Operation mutation。

```text
GET /v1/operations/{job_id}
```

## 8. Runtime Run API

```text
POST /v1/runs
GET  /v1/runs/{run_id}
GET  /v1/runs/{run_id}/result
POST /v1/runs/{run_id}:cancel
POST /v1/runs/{run_id}:pause
POST /v1/runs/{run_id}:resume
GET  /v1/runs/{run_id}/events
```

Run admission在一个事务中解析所选Agent Resource的tenant-scoped active Deployment、冻结02的RunBindingsSnapshot、验证input/policy/quota/deadline、
创建Run/initial Node/Job、Receipt/Event/Outbox。只有整个快照成功才返回`201 Created`。

`POST /v1/runs`使用closed `CreateRunRequestV1 { agent_id, input, deadline }`。`agent_id`选择该tenant内一个Agent Resource，
事务只接受其`Enabled` active Deployment；调用方不提交Deployment ID、Plan entry node、binding closure或Job/Node ID。`input`为
`{ classification, schema_digest, value: Inline | ArtifactRef }`，RunValue ID由服务端生成；Inline content digest由canonical JSON
计算，Artifact使用exact ArtifactRef digest并在事务内重验Ready/tenant/classification。deadline必须在HardLimitProfile允许窗口内。
Idempotency-Key按tenant/principal/agent collection scope绑定，重放返回第一次生成的Run及其原始投影。

control command使用If-Match和Receipt写durable intent，不直推任意leaf为terminal。cancel返回command accepted/current Run view，
真实terminal由06/07的收敛过程决定。result只在Run terminal且授权时返回typed RunValue/ArtifactRef。

## 9. Task 与interaction API

```text
GET  /v1/tasks/{task_id}
POST /v1/tasks/{task_id}:submit-input
POST /v1/tasks/{task_id}:approve
POST /v1/tasks/{task_id}:reject
POST /v1/tasks/{task_id}:cancel
```

Task view只暴露closed kind/state/schema/prompt metadata/deadline和owner-safe link，不暴露Secret/backend/session。每个mutation使用
Receipt + If-Match，验证principal、tenant、assignee/delegation、schema、deadline和current state。Task terminal与owner wake/Event/Outbox
同事务。

## 10. Artifact API

```text
POST /v1/artifacts:prepare-upload
POST /v1/artifacts/{artifact_id}:complete-upload
GET  /v1/artifacts/{artifact_id}
GET  /v1/artifacts/{artifact_id}/content
POST /v1/artifacts/{artifact_id}:delete
```

prepare返回Staging Artifact、short-lived upload grant和verify Job Operation。complete与object-store callback幂等，不直推Ready。
download经Gateway重新验证Artifact/Link/Grant/tenant/classification并有界stream。delete创建owner-specific maintenance Job。

closed public DTO冻结为：

```rust
struct PrepareArtifactUploadRequestV1 {
    schema_version: ConstU16<1>,
    purpose: ArtifactPurpose,
    classification: DataClassification,
    expected_size_bytes: BoundedU64,
    expected_digest: Option<Digest>,
    declared_media_type: Option<BoundedMediaType>,
    display_name: Option<BoundedSafeText>,
}

struct PrepareArtifactUploadResponseV1 {
    schema_version: ConstU16<1>,
    artifact_id: ArtifactId,
    operation_id: JobId,
    upload_grant_id: ArtifactLinkId,
    artifact_etag: StrongEtag,
    upload_target: SecretBearingUploadTargetV1,
    upload_expires_at: UtcTimestamp,
}

struct SecretBearingUploadTargetV1 {
    url: BoundedHttpsUrl,
    completion_proof: OpaqueUploadCompletionProof,
}

struct CompleteArtifactUploadRequestV1 {
    schema_version: ConstU16<1>,
    completion_proof: OpaqueUploadCompletionProof,
}

struct ArtifactMutationAcceptedV1 {
    schema_version: ConstU16<1>,
    artifact_id: ArtifactId,
    artifact_etag: StrongEtag,
    operation_id: JobId,
}
```

`prepare-upload`要求`Idempotency-Key`；`complete-upload`要求`Idempotency-Key`和prepare返回的current Artifact `If-Match`；
`delete`要求`Idempotency-Key`、current Artifact `If-Match`和空closed body。服务端生成Artifact之外的Blob/Grant/Job/Task/Receipt/Event/Outbox ID，
并从published policy/config选择retention、storage、scan、quota与deadline closure。public body不得接受这些内部ID、tenant/principal、object locator、
grant token、scan revision、retry参数或audit identity。complete中的opaque proof只引用服务端已冻结的grant/generation，Artifact Gateway仍须从provider
重新观察并验证generation/checksum/length；调用方不能用proof声明成功。

这里的policy/config选择不是模糊查找：retention与Artifact I/O/scan exact revision只读04 `TenantConfigV1`的两个current slot，quota按tenant
scope/work-class/metric唯一关系解析；缺失、重复、错kind、digest不符或disabled authority返回稳定拒绝，不采用“第一条active policy”。

`prepare-upload`返回`201`；`complete-upload`和`delete`返回`202`与`Location: /v1/operations/{job_id}`，其响应使用
`ArtifactMutationAcceptedV1`。`GET .../content`只在Ready且current authorization成立时返回bounded attachment stream；metadata、mutation和content
全部`no-store`。upload target/proof按15的Secret-bearing例外处理，不进入problem/Event/log/trace或明文Receipt result。

外部OIDC终止在Public Gateway；Artifact业务与storage I/O留在独立Artifact Gateway。内部hop使用mTLS exact workload audience并携带closed DTO、
通用header摘要及verified principal assertion，Artifact Gateway再从PostgreSQL重绑定current principal/permissions。不得把Public Gateway变成Artifact
状态机/storage client，不得以自由`x-platform-*` header、plain HTTP或NetworkPolicy来源代替服务身份认证。

不公开internal stage/read/verify/Ready、object key、bucket/KMS identity、generic grant、Model Artifact、dynamic storage binding或
Maintenance admin route。

## 11. List、filter 与cursor

首版只为已证明需要的Resource、Deployment、Run、Task和Artifact提供list，不为每个内部aggregate生成CRUD。
filter/sort是route-specific closed registry，unknown field/operator被拒绝。cursor绑定tenant、principal-scope digest、filter/sort digest、
snapshot position、page size和expiry，并签名或AEAD保护。page size只能在hard max内缩小。

## 12. SSE 与Event

```text
GET /v1/runs/{run_id}/events
```

SSE只投影已提交Event，NATS只唤醒。`Last-Event-ID`是opaque tenant/run-bound cursor，断线后从PostgreSQL重放。
活连接队列有item/byte/time上限，饱和时关闭连接并允许重连，不丢失durable state。

public envelope包含event ID、type/version、tenant-safe resource IDs、sequence、occurred/committed time、trace ID和bounded typed data。
Secret、prompt/response、tool arguments、Artifact URL/path、credential和内部diagnostic不进Event。

## 13. Internal service protocols

internal service只有跨物理信任边界时才存在，不为每个domain trait自动生成network service。首版至少包含：

| Service | 职责 | 关键限制 |
|---|---|---|
| EgressBroker | catalog-bound HTTP/provider/MCP egress与last-hop Secret resolution | 不接受自由URL/header/Secret |
| SandboxController | Submit/Cancel/Observe fenced Job | Executor无DB直连 |
| SandboxExecutor | WASI/gVisor physical execution | 不改写业务state |
| ArtifactGateway | 经Public Gateway转发的public upload/download HTTP语义 | exact public-gateway mTLS audience + current principal rebinding |
| ArtifactDataWorker | internal stage/read/verify/derive | exact workload capability + owner/Job fence |
| ArtifactMaintenance | delete/GC/quarantine/reconcile | closed maintenance transition |

没有Model Artifact Producer、Model/Sandbox专用Artifact Broker、microVM RPC、Managed stdio runner或Installation release service。
Artifact public hop保持同一个OpenAPI HTTP请求/响应语义，不生成一份字段对等protobuf；其余internal RPC使用protobuf。所有跨进程调用使用
mTLS workload identity、exact audience、tenant/owner/fence重绑定、bounded deadline/message/stream和stable status mapping。

## 14. Rate limit、quota 与backpressure

Gateway只做便宜的principal/tenant/route/token-bucket限流；durable quota由04/07拥有。rate limit和quota不复制current
counter authority。队列满、DB pool保留、downstream busy和stream背压使用stable `429/503`、bounded Retry-After或durable
Ready/Waiting state，不用无界内存队列。

## 15. Persistence、审计与可观测性

API process不拥有业务state；它在单一PostgreSQL事务中调用owner service并写Receipt/Event/Outbox。审计事件
记录principal/tenant/action/target/result/policy/evidence digest和timestamp，不记录Secret、token或正文。

metric至少包含request latency/status/problem code、idempotency replay/conflict、CAS conflict、rate/quota reject、SSE connections/drop、
operation age/outcome、internal RPC latency/denial和DB pool utilization。tenant/resource ID不进label。

## 16. 安全与部署

- Management API、Runtime API、Scheduler/Worker和Artifact三role使用独立ServiceAccount、DB role/pool和NetworkPolicy；
- API无Sandbox runtime、object-store admin、raw provider/MCP credential或Kubernetes mutation权限；
- CSRF/CORS/security headers按credential mode闭合，callback route使用专用ingress和Receipt；
- request smuggling、duplicate header、invalid UTF-8/JSON、unknown content type和oversized body在domain前拒绝；
- health/readiness只检查mandatory dependency和startup manifest，不因一个外部Provider/MCP不可用而整体fail。

## 17. 验收标准

- OpenAPI路由只使用`/v1`，没有`/v2`、双写或legacy fallback；
- 同Receipt key/digest重放返回同status/body/ETag/Location，异digest stable conflict；
- 所有mutation缺If-Match、旧ETag、错tenant/owner和unknown schema field均fail closed；
- Operation ID等于JobId，view直接投影Job，不存在ManagementOperation state/table；
- ResourceValidation、McpDiscovery、ContextDatasetBuild、ArtifactVerify/Delete的kind-target矩阵正负fixture通过；
- Run admission只有完整binding snapshot事务成功才返回201；
- SSE在NATS丢失/断线后从Event cursor恢复，慢client不使服务内存无界；
- public/internal route无Secret、object locator、DB payload、lease token或敏感正文泄漏；
- API、Sandbox、Artifact与critical-control饱和隔舱符合18资格合同。

## 18. 分层证据

owner domain tests、OpenAPI/schema conformance、repository transaction tests、HTTP/RPC integration tests、security negative tests和
production-equivalent load/fault qualification分层运行。不在每层重复同一证据，不以route/object count代替行为验证。

## 19. 明确推迟

- Installation Release/Candidate/Gate runtime API和dynamic storage/KMS management；
- generic GraphQL、arbitrary resource CRUD和public internal-RPC proxy；
- Managed stdio、microVM、Model Artifact与专用RPC；
- cross-region public cursor/stream migration和exactly-once SSE。

## 20. 未决问题

首版minimal `/v1`、Job Operation projection与internal service边界无未决设计问题。
