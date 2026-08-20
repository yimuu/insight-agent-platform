# Platform v2 Artifact 与 File 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-20 |
| 依赖 | 02、03、04、07、09、10、12、13 |
| 直接下游 | 16、17、18 |

## 1. 决策摘要

Artifact是tenant-scoped、immutable、可审计的大值或文件。PostgreSQL拥有metadata、state、link、grant与blob generation
identity，S3-compatible store拥有bytes，KMS/Secret Manager拥有key material。NATS不拥有Artifact状态。

首版只有三个Artifact物理role：

1. Artifact Gateway：public upload/download；
2. Artifact Data Worker：内部stage/read/verify/derive；
3. Artifact Maintenance：retention、quarantine、delete、GC与reconcile。

不建设Model、Sandbox、Context或Registry专用Broker/Producer。Data Worker使用closed caller capability与exact owner fence区分路径。
Model output首版Inline-only；文件或大输出由Capability/Sandbox通过普通Artifact port产生。

## 2. 模型与状态

```rust
struct Artifact {
    artifact_id: ArtifactId,
    tenant_id: TenantId,
    kind: ArtifactKind,
    purpose: ArtifactPurpose,
    state: ArtifactState,
    classification: DataClassification,
    media_type: MediaType,
    length: Option<u64>,
    digest: Option<Digest>,
    current_blob_id: Option<BlobId>,
    storage_binding_id: StorageBindingId,
    retention: RetentionSnapshot,
    projection_version: u64,
}

enum ArtifactState {
    Staging,
    Uploaded,
    Verifying,
    Verified,
    Ready,
    Quarantined,
    Rejected,
    Deleting,
    Deleted,
}
```

`Verified`表示bytes通过完整性和安全验证，但尚未形成业务引用。`Ready`只能由业务owner事务
从exact Verified candidate推进，并在同一事务中创建ArtifactLink/RunValue或其他typed reference。Data Worker不直推Ready。

terminal states为`Rejected | Deleted`。`Quarantined`是安全门，在复验前禁止新read grant。状态推进需要
expected projection version和Event/Outbox同事务。

## 3. ArtifactRef 与Blob

```rust
struct ArtifactRefV1 {
    schema_version: ConstU16<1>,
    artifact_id: ArtifactId,
    media_type: MediaType,
    length: u64,
    digest: Digest,
}
```

ArtifactRef是nominal typed reference，不包含bucket、object key、presigned URL、KMS key或storage credential。读取时
必须从authority重新验证tenant、state、link/grant、digest与length，不信任请求携带的metadata。

Blob表示exact object generation，并以`(storage_binding_id, object_key, generation)`唯一。content dedup只在同tenant、
classification、encryption boundary和retention允许时进行；禁止用digest作为跨tenant存在性oracle。Blob引用与
candidate generation都在PostgreSQL事务中记录，对象列表不是权威引用计数。

## 4. Storage binding

首版只支持部署时生成的一份`ArtifactStorageBindingManifestV1`，通过GitOps/Kubernetes configuration安装，
包含store endpoint identity、bucket/prefix、KMS reference identity、region、encryption mode、hard limit和digest。不提供tenant自助
Add/Rebind/Revoke API，不引入Installation Release、compatibility generation或EncryptionDomain current aggregate。

新Artifact使用当前安装binding；旧Artifact/Blob保留创建时exact binding identity并仍可读取/删除。运维更换存储或KMS时
必须使用经审核的数据迁移runbook和回滚点，不由业务API动态切换。

## 5. Prepare、upload、verify 与finalize

### 5.1 Public upload

`PrepareUpload`事务使用Receipt创建Staging Artifact、预分配Blob generation、UploadGrant和`ArtifactVerify`
shared Job，并返回short-lived upload target。公开Operation ID就是JobId；`GET /v1/operations/{job_id}`只是safe Job projection。

public request只携带业务意图：`schema_version=1`、`purpose`、`classification`、`expected_size_bytes`、可选
`expected_digest`、可选`declared_media_type`和可选`display_name`。tenant/principal、Artifact/Blob/Grant/Job/Receipt/Event/Outbox ID、
storage binding、retention policy revision、scan policy、deadline上限和quota account全部由服务端current authority选择并冻结，客户端不得提交。
public request中的`purpose`仍需通过principal permission与owner policy的closed allowlist，不能用它取得内部package/evidence权限。

成功响应只公开`schema_version=1`、`artifact_id`、`operation_id`、`upload_grant_id`、Artifact ETag、`upload_target`和
`upload_expires_at`。`upload_target`是一次短期、exact generation、method/length/media约束的bearer capability，可包含完成上传所需的
opaque completion proof；它是这个响应唯一允许出现的Secret-bearing字段，必须`no-store`、禁止日志/trace/Event记录且不得在Receipt result中
持久化明文。Receipt只保存可重放的非Secret结果与加密/摘要化的grant binding；同key重放若原target仍有效可重新签发绑定同一generation的
新target，不能创建第二Artifact/Blob/Job。

client只能向exact object generation上传指定media/length/digest上限内的bytes。`CompleteUpload`通过Receipt验证
由prepare响应获得的opaque completion proof、current Artifact ETag与provider completion evidence；公开request不接受Artifact以外的
内部ID、object key、bucket、KMS identity、scan policy或owner-generated audit ID。Artifact Gateway从exact provider binding重新观察
object generation、ETag/checksum与length，验证digest和expiry，然后推进`Staging -> Uploaded`并唤醒原verify Job。

Public Gateway只做外部credential验证、通用request limits和rate admission；Artifact Gateway才拥有upload/download业务处理与storage client。
两者之间必须使用mTLS认证exact `public-gateway -> artifact-gateway` workload audience，转发closed public DTO、Idempotency-Key/If-Match摘要和
verified principal identity。Artifact Gateway必须从PostgreSQL重新绑定current principal/permission，不能仅信任自由`x-platform-*` header；
plain HTTP、仅NetworkPolicy来源或调用方提供的tenant/principal/body ID均不是认证边界。

### 5.2 Internal workload stage

Capability、Context、MCP和Sandbox必须在owner Job开始前预分配Artifact/Blob identity和quota。Artifact Data Worker只接受
closed request，绑定tenant、caller capability、owner kind/ID、Job lease generation、declared port、media、maximum bytes、digest mode、
classification和retention。不接受object key或通用owner JSON。

同一owner generation重试必须返回同一Artifact identity或stable conflict，不得无界创建candidate。

### 5.3 Verification

verify按kind/purpose的published profile执行：

- object generation、length、digest、media sniffing和canonicalization；
- malware/archive-bomb、executable、document/script和格式专用scan；
- schema、classification、retention、SBOM/provenance等领域规则；
- scanner/ruleset/runtime version、deadline和evidence expiry；
- 不可变的VerificationEvidence digest。

success推进`Uploaded -> Verifying -> Verified`，failure推进`Rejected`或`Quarantined`。verification Job与Artifact状态
同事务协调，但Job不是第二Artifact current-state authority。

### 5.4 Ready commit

业务owner terminal事务必须重新锁定：

- current owner/Job fence和expected owner projection version；
- Artifact exact `Verified` state/version；
- Blob generation、digest、length、binding和current verification evidence；
- tenant、classification、purpose、port、quota和retention。

然后原子推进Ready、创建ArtifactLink/RunValue、提交owner outcome、关闭Job、settle quota并写Event/Outbox。
任一失败不得留下可读的无owner Artifact。

## 6. ArtifactLink 与所有权

```rust
struct ArtifactLinkV1 {
    link_id: ArtifactLinkId,
    tenant_id: TenantId,
    artifact_id: ArtifactId,
    owner: ArtifactOwnerRef,
    relation: ArtifactRelation,
    state: LinkState,
    created_with_owner_projection_version: u64,
    projection_version: u64,
}
```

owner闭集由03的typed registry拥有，至少包含ResourceVersion、Deployment、Run、RunValue、Invocation、Job、Task和Artifact。
不包含ManagementOperation。relation是closed enum，例如`Input | Output | Package | Manifest | Evidence | DerivedFrom` 。

`created_with_owner_projection_version`只是创建Link时CAS成功的evidence，不是永久关系generation。后续读取和授权只验证
Link Active、tenant、owner identity/relation与current authorization，不要求该值等于owner当前projection version。否则owner正常
状态推进会使有效Link意外失效。

release command另行携带操作时owner的current expected projection version与Link expected version，事务内复核后把
Link推进`Active -> Released`。owner deletion、retention和GC均以current Link state为authority。

## 7. Grant、read 与download

ArtifactGrant是short-lived、purpose-bound的访问许可，绑定tenant、Artifact、Link/owner、subject workload/principal、
operation、range/bytes、audience、expiry和single/multi-use policy。它不是presigned URL的别名，也不授予bucket权限。
物理持久化中Grant复用`artifact_links` 的closed grant kind/payload，不增加第四个Artifact current-state表。

read流程：

1. Gateway/Data Worker认证principal/workload identity；
2. 锁定Artifact、Link/owner与Grant，验证tenant、Ready、classification、retention、evidence和expiry；
3. 以exact storage binding/object generation从object store读取；
4. 核对length/digest/range并以有界stream返回；
5. 可选消耗single-use grant并写有界audit Event。

public download与internal read可共享domain library，但不共享server identity或public principal权限。Data Worker的closed caller
capability区分`ReadWorkloadArtifact | ReadSandboxArtifact | StageWorkloadArtifact | VerifyArtifact | DeriveArtifact`；不再区分
WASI/microVM/Model专用RPC。

## 8. Derived Artifact 与provenance

transform必须是published Capability/Sandbox Deployment，并冻结source Artifact digest集、transform revision/package、parameters、
output schema/media/classification/retention、Job generation和determinism mode。派生结果使用普通stage/verify/Ready流程，
provenance使用ArtifactLink和Event，不增加transform专用表。

## 9. 领域使用规则

- Model：首版request/response为有界Inline value，不读写Artifact-backed Model正文；
- Context：index/manifest/source body使用Artifact，query result保留citation/provenance；
- MCP：large Resource/Prompt body使用Artifact，但未reviewed Prompt仍不受信任；
- Sandbox：package/input/output file只通过declared Artifact ports，Executor无storage credential；
- Capability：Interface显式Artifact field才使用ArtifactRef；不得用metadata代替对物化正文的schema验证。

## 10. Inline 与Artifact value

RunValue storage shape可为`Inline | ArtifactBacked`，但逻辑schema始终描述物化正文。超过effective inline threshold
的Capability/Sandbox/Context value必须使用Artifact；调用方不能通过hint改变存储形状。每个信任边界
物化后都要重验digest、length和logical schema。

Model是首版例外：它的request/response hard limit必须不超过Inline上限；超过时返回stable
`model_output_too_large`，不自动建Model Artifact。用户需要文件时应让Model调用可产生Artifact的Capability。

## 11. Retention、hold、quarantine 与delete

retention snapshot在Artifact创建时冻结，包含minimum/maximum expiry、classification、legal hold eligibility和delete policy。
Active Link、legal hold、open grant、current verification/reconcile或owner policy可阻止删除。

Maintenance使用bounded scan和exact generation Job：

1. 锁定Artifact、Blob、Link/hold/grant和expected versions；
2. 推进`Ready/Quarantined -> Deleting`并写delete intent；
3. 以exact object generation执行删除；
4. 复核outcome后推进`Deleted`、settle quota并写Event/Outbox。

删除不确定时保持`Deleting`并reconcile，不把列表结果当作成功。candidate/orphan generation只能在数据库
证明无current reference且grace过期后GC。材料删除后不可恢复，操作前必须精确解析目标。

## 12. 配额、并发与隔舱

配额至少覆盖staging bytes/count、ready bytes/count、active grants、daily ingress/egress、verification/transform并发和
maintenance backlog。预留在object I/O前发生，terminal/recovery原子settle或release。

Gateway、Data Worker和Maintenance必须使用三个独立Deployment、ServiceAccount、DB pool、storage identity、queue、permit
和autoscaling signal。一个role饱和不得占用其他role、API、Scheduler、Model、MCP或Sandbox的保留容量。
Data Worker内部的read、stage、verify/derive使用独立有界queue/permit/byte budget，防止大写入或scanner饱和阻塞读取；
gVisor guest read使用独立token-authenticated listener：只接受专用audience的短期Pod-bound ServiceAccount JWT，离线验证
发布时安装的JWKS，并在每次package/input读取前复核exact Pod UID、Job fence、request digest与Artifact grant。该listener不接受
public principal，也不复用Controller mTLS identity。
这些是同一role的capacity lane，不创建新server identity、aggregate或state machine。

## 13. Persistence 与machine contract

Artifact/Blob/Link/Grant因独立存储与安全生命周期保留domain-specific persistence。verify、derive、delete、GC和
rescan的attempt复用Job，历史/审计复用Event，幂等复用Receipt，不建每阶段/每证据表。

Artifact DTO、internal RPC payload和DB snapshot各自有一个owner type。JSON Schema/OpenAPI/protobuf等边界投影由
owner type/registry生成或验证，不手写三份对等schema。所有JSONB有`schema_version`、closed validation、
size limit、canonical serialization和digest。

## 14. API 与事件

首版public `/v1`只提供：

- prepare/complete upload；
- Artifact metadata/detail；
- authorized download；
- delete request；
- safe Job Operation query；
- Run/Event SSE中的Artifact lifecycle observation。

public delete request是空closed body，使用`Idempotency-Key`与Artifact strong `If-Match`；服务端生成delete Job/Receipt/Event/Outbox identity，
并按policy创建或复用必要Approval Task。public content read不接受object locator或通用grant，响应只从current Ready Artifact + active typed Link +
current principal authorization重新派生，强制bounded stream、`Content-Length`、verified media type、digest ETag、attachment disposition与`no-store`。

不公开object key、storage binding/KMS细节、generic grant mint、internal stage/read/verify/Ready、dynamic storage management或
Maintenance API。Event记录stable IDs/state/reason/evidence digest，不记录bytes、URL、path、Secret或对象定位。

## 15. 可观测性与安全

metric至少包含upload/download/stage/verify latency和outcome、bytes、queue age、grant denial、integrity mismatch、quarantine、
delete/GC backlog、quota rejection和storage dependency health。tenant、ArtifactId、digest、path和object key不进label。

Gateway做principal authorization，Data Worker做workload capability与exact owner fence authorization，Maintenance只做闭集维护transition。
三者都不获得通用bucket admin或KMS admin权限。所有stream有byte/time/range/chunk上限，异常时fail closed。

## 16. 验收标准

- 不存在Ready Artifact而没有同事务typed reference；
- 错tenant、owner、port、Job generation、digest、length或storage generation的stage/read/commit被拒绝；
- public upload重放只产生一个Artifact和一个verify Job，Operation只是Job projection；
- Link在owner正常状态推进后仍可读；只有release command使用owner current expected version；
- Data Worker不能直推Ready或改写Run/Invocation/Job current state；
- Model超过Inline hard limit时返回stable error，不创建Model Artifact；
- Gateway、Data Worker和Maintenance三个role的identity、DB/storage权限与permit负向矩阵通过；
- object-store/KMS超时、重复callback、process kill和NATS丢失后可恢复，不伪造success；
- retention/hold/link/grant阻塞删除，orphan GC不删除可达generation。

## 17. 分层证据

domain state/property tests、PostgreSQL transaction/fence tests、S3/KMS adapter tests、三role mTLS/permission tests与
production-equivalent saturation/fault qualification分层运行。不以表数、trigger数或广泛snapshot作为功能完成证据。

## 18. 明确推迟

- Model Artifact-backed request/response和Model专用Producer/Broker；
- tenant self-service storage/KMS binding、cross-region replication和client-side encryption；
- 通用public grant API、public transform pipeline和跨tenant dedup；
- microVM专用Artifact protocol。

## 19. 未决问题

首版Artifact状态、三role与static storage binding合同无未决设计问题。
