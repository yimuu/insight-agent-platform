# Platform v2 Artifact 与 File 规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md)、[`12-context-and-retrieval.md`](12-context-and-retrieval.md) |
| 直接下游 | 14、16、17、18 |

> Persistence ruling：Artifact 只保留 Artifact、Blob 与 Link 三类领域事实；upload/scan/rescan/delete/GC 使用共享 Job、
> Receipt、Event 与 Task。历史 migration 24～28 及专用 transition/evidence/operation 表族已废止。

## 1. 决策摘要

Artifact 是 tenant-scoped、不可变的大值/文件对象；PostgreSQL 保存身份、metadata、状态、引用和授权权威，
S3-compatible object store 保存 blob 正文。业务对象只持有 ArtifactRef，不持有 object key、bucket credential、
宿主路径或永久 URL。

写入采用 prepare/upload/verify/finalize：先创建 intent 和 scoped grant，正文进入 staging，完成 digest/media/size/
content policy 后成为 Verified，最后在引用者的 PostgreSQL 事务中同时变为 Ready 并建立 ArtifactReference。只有
Ready Artifact 可读取。删除使用引用图、retention、legal hold 和两阶段 GC，不使用简单引用计数或立即物理删除。

Model canonical response 的 Artifact-backed 输出使用独立 Model Artifact Producer。它不是只读 Model Artifact Broker 的
写方法，也不是 Model Worker 内嵌 SDK：Producer 只接受 exact Model Attempt 的有界 canonical JSON stream，并且最多把预留
Artifact 推进到 Verified。只有 Model owner 的 terminal PostgreSQL 事务可以同时完成 Verified -> Ready、Output Link、RunValue、
ModelTurn/Job first-winner、quota settlement 与 outbox；任何失败窗口都不能产生可读的半成品 Model 输出。

内容寻址用于完整性与 tenant 内受控复用，不作为公开身份。平台禁止跨 tenant blob dedupe，避免存在性、大小、
digest、加密和访问时序侧信道。

## 2. 目标与非目标

### 2.1 目标

- 给 Artifact identity、metadata、state、ArtifactRef、Blob、Grant、Reference 和 Provenance 机器合同；
- 保证数据库与 object store 非事务写入在崩溃、重复、乱序和 GC 下最终一致；
- 对用户上传、Capability、Context、MCP、Sandbox、Model 和导出使用同一 scoped I/O 协议；
- 在内容进入 Run/Model/Sandbox/下载前验证 digest、size、media、classification 和内容安全；
- 支持小对象、流式大对象、multipart、range read、derived artifact 和短期 diagnostic；
- 隔离 tenant、principal、Run、Invocation、port、purpose 和 deadline；
- 让 retention、legal hold、delete、quarantine、corruption 与审计具有明确状态；
- 防止 Artifact I/O/扫描饱和影响 API、Scheduler 和执行 Worker 并发。

### 2.2 非目标

- 不将 S3 bucket/object key/presigned URL 作为平台稳定 API；
- 不允许公共匿名 bucket、永久 download URL 或跨租户共享 object；
- 不提供可变文件、POSIX 文件系统、协作编辑、append log 或用户目录；
- 不保证仅凭扩展名/Content-Type 的文件类型真实性；
- 不在 PostgreSQL 保存大型正文、模型输出、stdout/stderr 或 package archive；
- 不通过 hash 命中证明调用者有读取权限；
- 不支持跨 region active-active object replication 或跨云 transparent migration；
- 不让模型、Skill、Sandbox 或 MCP server 选择 object key、encryption key、retention 或 classification。

## 3. 术语与信任边界

| 术语 | 含义 |
|---|---|
| Artifact | 公开逻辑身份和不可变安全 metadata |
| Blob | object store 中的 tenant/security-domain scoped physical content |
| Artifact Intent | 尚未 Ready 的 prepare/upload/verify 生命周期 |
| ArtifactRef | 业务合同中的 nominal、完整性保护引用 |
| ArtifactReference | Artifact 到拥有者/Run/Value/Revision 的 durable 引用边 |
| ArtifactGrant | 对单 Artifact/Intent、operation、port、principal/workload 和 deadline 的临时能力 |
| Provenance Edge | source Artifact 到 derived Artifact 的不可变转换证据 |
| Content Evidence | digest/media/malware/parser/policy 验证结果 |
| Quarantine | 禁止正常读取、等待安全处置的状态 |

上传者声明的文件名、media、digest、classification、archive、document metadata、embedded link/macro/script、模型
输出和远端 MIME 全部不受信任。Object store 不做 tenant 授权；所有访问必须先经过 Artifact authority 或其签发的
短期 scoped grant。

## 4. Artifact 模型

```rust
struct ArtifactRecord {
    artifact_id: ArtifactId,
    tenant_id: TenantId,
    state: ArtifactState,
    classification: DataClassification,
    purpose: ArtifactPurpose,
    expected_size: BoundedSize,
    expected_digest: Option<Digest>,
    blob_id: Option<InternalBlobId>,
    verified_content: Option<VerifiedArtifactContent>,
    retention_policy_revision_id: RevisionId,
    retain_until: DateTime<Utc>,
    created_by: PrincipalId,
    created_at: DateTime<Utc>,
    projection_version: u64,
}

struct VerifiedArtifactContent {
    content_digest: Digest,
    byte_length: u64,
    verified_media_type: MediaType,
    encryption_domain_id: EncryptionDomainId,
}
```

`blob_id`与`verified_content`是不同阶段的事实：Staging Artifact可在PUT前绑定`Some(blob_id)`而
`verified_content=None`；未开始物化时两者都为空。`Verified | Ready`必须同时具有`blob_id`和完整verified content，并与同tenant
Verified Blob的digest/length/encryption domain一致；`blob_id=None, verified_content=Some`及任何sentinel组合均非法。

Artifact相关分类不是自由字符串，统一使用以下machine registry：

```rust
enum ArtifactPurpose {
    AuthoringDocument,
    InterfaceContract,
    TypedPlan,
    Package,
    Sbom,
    BackendBinding,
    ModelGenerationDefaults,
    RunInput,
    RunOutput,
    CapabilityInput,
    CapabilityOutput,
    ContextSource,
    ContextDerived,
    McpResource,
    SandboxInput,
    SandboxOutput,
    Diagnostic,
    Export,
}

enum ArtifactReferenceKind {
    Definition,
    Input,
    Output,
    Evidence,
    Package,
    Attachment,
    Result,
    Provenance,
}

enum BlobIntegrityState {
    Staging,
    Verified,
    Corrupt,
    Deleting,
    Deleted,
}
```

`ArtifactPurpose`描述内容被允许用于什么，`ArtifactReferenceKind`描述owner为何持有引用；两者必须同时验证，不能互相代替。
新purpose/reference kind必须先扩展machine registry、Policy和qualification fixture，不能由API、media type、文件名或owner port
临时发明。Blob integrity只描述physical content完整性，不拥有Artifact lifecycle。

Artifact ID 使用 02 的 `art_<uuidv7>`，不编码 tenant、digest、bucket、region、media 或 classification。Artifact
metadata 中不保存 Secret、raw filename path、prompt 或业务正文。`verified_content` 在 Staging/Uploaded/Verifying
可以为空，进入 Verified/Ready 必须非空；数据库 CHECK 约束强制状态与字段组合。授权owner可读取所有未删除状态的
safe `ArtifactSnapshot`，但只有Ready snapshot包含不可空`ArtifactRef`/verified content projection；其他状态只公开
expected metadata、state和safe reason class，不能被业务输入/下载引用。

## 5. ArtifactRef

```rust
struct ArtifactRef {
    artifact_id: ArtifactId,
    content_digest: Digest,
    byte_length: u64,
    media_type: MediaType,
    classification: DataClassification,
    display_name: Option<SafeDisplayName>,
}
```

- ArtifactRef 是 nominal type，不接受任意 JSON object 冒充；
- 使用时必须按 tenant + Artifact ID 查询，并重新比较 digest/length/media/classification；
- `display_name` 只用于 UI，经过 Unicode normalization、长度、控制字符和路径清理，不参与 object lookup；
- Ref 不包含 URL、bucket、key、filesystem path、Secret、grant 或 owner permission；
- Ref 被序列化到 Model/remote backend 前必须经过该 port 的 data-flow policy；
- 只持有 ArtifactRef 不代表有 read/download 权限。

## 6. Blob 与内容寻址

Blob 是内部物理对象：

```rust
struct BlobRecord {
    blob_id: InternalBlobId,
    tenant_id: TenantId,
    content_digest: Option<Digest>,
    byte_length: Option<u64>,
    storage_binding_digest: Digest,
    opaque_object_key: EncryptedObjectKey,
    object_generation: Option<ObjectGeneration>,
    encryption_domain_id: EncryptionDomainId,
    integrity_state: BlobIntegrityState,
    verified_at: Option<DateTime<Utc>>,
}
```

Staging Blob在唯一opaque locator封印后即可存在，因此`content_digest`、`byte_length`、`object_generation`和`verified_at`在
`Staging`可以为空；Uploaded/Verifying按已观察事实逐步填充。进入`Verified`后四者必须全部非空并与exact object generation、Artifact
verified content和HEAD evidence一致，数据库CHECK与domain validator共同强制该closed state/field invariant。不得用空digest、零长度
sentinel或虚构generation绕过未知状态；真实零字节对象仍以`Some(0)`表达并受purpose/content policy决定是否合法。

规则：

- digest 使用 02 定义的 `sha256:<lowercase-hex>`，由平台验证，不仅信任客户端 header；
- object key 由服务生成，使用 tenant-keyed opaque partition，不直接包含公开 tenant ID、filename 或裸 digest；
- 同一 tenant、encryption/classification/retention domain 内可以复用 verified Blob；
- 跨 tenant 永不 dedupe；同 digest 也产生独立 blob/encryption/object identity；
- dedupe lookup 在完成授权和 quota reservation 后进行，响应时序/错误不泄露其他内容是否存在；
- Artifact 与 Blob 分离：不同 purpose/retention/provenance 的 Artifact 可以在允许域内引用同一 Blob；
- Object version/generation 固定，禁止 overwrite existing key；
- object lock/versioning 是防御层，PostgreSQL 仍是 Artifact lifecycle 权威。
- `storage_binding_digest`引用本规范的installation-scoped storage/region/KMS binding，不是tenant
  Revision或可运行时选择的backend ID；本规范是该catalog与1～64 hard max的owner，Model output是否Inline-only
  不影响Package、request Artifact及其他Artifact路径对该catalog的需求；

### 6.1 Installation storage binding机器合同

```rust
#[serde(transparent)]
struct CanonicalRegion(String);

enum StorageBackend { S3 }
enum S3AddressingMode { VirtualHosted, PathStyle }
enum ObjectWriteMode { ConditionalCreateVersioned }
enum ExactKeyObservationContract { StrongAfterWriteQuiescence }

struct ArtifactStorageBindingManifestV1 {
    manifest_version: u32, // const 1
    backend: StorageBackend, // exact s3
    region: CanonicalRegion,
    endpoint_identity_digest: Digest,
    bucket_binding_digest: Digest,
    addressing_mode: S3AddressingMode,
    request_timeout_milliseconds: u32,
    maximum_object_bytes: u64,
    kms_binding_digest: Digest,
    write_mode: ObjectWriteMode, // exact ConditionalCreateVersioned
    exact_key_observation: ExactKeyObservationContract, // exact StrongAfterWriteQuiescence
    maximum_put_completion_uncertainty_milliseconds: u64,
}
```

schema路径固定为`contracts/platform-v1/schemas/artifact-storage-binding-manifest.schema.json`，所有object closed。
`StorageBackend::S3`、`S3AddressingMode`、`ObjectWriteMode`与`ExactKeyObservationContract`的wire分别exact为`s3`、
`virtual_hosted | path_style`、`conditional_create_versioned`与`strong_after_write_quiescence`。`CanonicalRegion`是1～63 bytes的transparent ASCII string，pattern固定
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`；不做lowercase、provider alias或Unicode normalization。uncertainty必须为正且不超过
`9007199254740991`，request timeout与maximum object bytes也必须为正，后者不超过JSON safe integer；所有digest必须使用02 exact wire。
endpoint/bucket/KMS字段引用Candidate安装的exact private endpoint、opaque bucket/prefix与workload-identity/KMS binding，不携带hostname、
bucket name、access key或Secret正文；runtime按digest解析后仍须逐字段复验region/addressing/timeout/byte limit。

`MAX_INSTALLATION_ARTIFACT_STORAGE_BINDINGS=64`由本规范唯一拥有。每个生产Candidate必须安装1～64份canonical manifest；digest按raw
bytes严格升序且唯一。未被某一时刻动态Deployment引用的binding不是orphan，因为同一catalog还服务Package、request Artifact及其他
Artifact路径。

纯`ArtifactStorageBindingManifestV1::validate_timing(StorageBindingTimingLimitsV1)`使用checked arithmetic计算
`required_write_quiescence_seconds = ceil(uncertainty_milliseconds / 1000) + 1`；任何add或换算溢出都拒绝。调用方提供的installation
`staging_seconds`必须严格大于该结果，引用该binding的ArtifactIo Policy `staging_grace_seconds`必须大于等于该结果。所有conditional PUT都
携带不晚于Attempt deadline的write deadline；从该deadline经过uncertainty后，不得再有此前admitted write创建新generation，exact-key
HEAD/DELETE/HEAD必须提供稳定观察。不满足该生产等价backend/proxy合同的binding不得进入Candidate。

## 7. 状态机

```rust
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
    Corrupt,
}
```

```text
Staging -> Uploaded | Quarantined | Rejected | Deleting
Uploaded -> Verifying | Quarantined | Rejected | Deleting
Verifying -> Verified | Quarantined | Rejected | Deleting
Verified -> Ready | Quarantined | Rejected | Deleting
Ready -> Quarantined | Deleting | Corrupt
Quarantined -> Ready | Rejected | Deleting | Corrupt
Rejected -> Deleting
Deleting -> Deleted
Corrupt -> Quarantined | Deleting
```

`Ready` 之后 content、digest、size、media、classification、purpose 和 Blob 不可修改；修改 metadata 需要新
Artifact 或独立 display projection。Quarantine 恢复为 Ready 需要新的 security evidence、授权 principal 和
generation CAS。Deleted 终态不可离开；物理 version 尚可由管理员灾难恢复时，也必须创建新 Artifact ID。

`Quarantined -> Ready`还有purpose-specific guard：对`purpose=RunOutput`且由16 Model output reservation/stage Receipt标识、但不存在
匹配的terminal Receipt、active ModelTurn Output Link与immutable RunValue三项证据的candidate，generic scan/rescan/security authority
禁止把它推进Ready，只能保持Quarantined、Reject或Delete。只有一个Artifact在此前曾由Model owner terminal原子形成Ready三元组，且
当前仍存在逐字段匹配的业务Link/RunValue/terminal Receipt时，rescan才可在同一事务恢复`Quarantined -> Ready`并保留既有Ready
retention。该guard阻止Producer integrity failure或orphan绕过Model first-winner形成孤立Ready。

Artifact metadata的current security projection必须包含最近一次accepted scan的exact scan Policy revision、scanner contract
digest、ruleset digest、object generation、evidence digest、observed/expiry time与disposition。它与Artifact version一起CAS，
是当前可读性/证据新鲜度的唯一current authority；完整scanner report仍保存为受限Artifact，Event只保存bounded摘要。

## 8. Prepare、Upload、Verify、Finalize

规范流程：

```text
1. PrepareArtifact
   -> reserve quota
   -> create Staging Artifact Intent
   -> issue scoped UploadGrant
2. Upload bytes to unique staging object
3. CompleteUpload
   -> HEAD/size/checksum/multipart validation
   -> Uploaded
4. Verify
   -> stream digest + media sniff + content policy
   -> Verified | Quarantined | Rejected
5. FinalizeAndReference in owner transaction
   -> validate current Verified evidence/grant/owner
   -> promote Artifact Ready
   -> insert ArtifactReference
   -> commit owner Value/Revision/Invocation result + outbox
6. asynchronous staging/orphan cleanup
```

S3 upload 成功而 DB 未提交只会产生 staging orphan；DB 不能先提交 Ready 再假设 object 存在。Finalize 必须验证
object generation、digest、evidence、tenant、classification、retention、owner 和当前 port contract。重复 finalize
返回已有 receipt；同一 Intent 不能绑定不相容 owner/purpose。

对于公共独立上传，prepare事务同时创建03的durable ManagementOperation作为初始owner、Staging Artifact和UploadGrant；
verify成功后该Operation finalize/reference并返回ArtifactRef，之后Agent Run/Revision创建新Reference，不复制正文。
对于Capability/Sandbox输出，owner transaction是Invocation output commit。首版不存在UploadObject、WorkspaceAttachment
或其他未注册owner kind。

Model output是同一状态机的受限内部路径：claim/start预留Staging intent与全部identity，独立Model Artifact Producer只推进到
Verified，随后由Model terminal owner transaction执行Ready/reference/RunValue/terminal commit。该路径不开放公共prepare/finalize，
也不允许Producer成为第二个Model current-state authority。

## 9. Upload 协议

```rust
struct PrepareArtifactRequest {
    idempotency_key: IdempotencyKey,
    purpose: ArtifactPurpose,
    expected_size: BoundedSize,
    expected_digest: Option<Digest>,
    declared_media_type: Option<MediaType>,
    classification: DataClassification,
    retention_policy_revision_id: RevisionId,
    target_owner: PendingOwnerBinding,
}

struct UploadGrant {
    artifact_grant_id: ArtifactGrantId,
    artifact_id: ArtifactId,
    allowed_operations: UploadOperations,
    exact_staging_identity: OpaqueStagingIdentity,
    max_bytes: u64,
    expected_digest: Option<Digest>,
    expires_at: DateTime<Utc>,
    generation: u64,
}
```

`target_owner`只存在于内部command，由authenticated route/workload context生成并验证；公共prepare DTO不含该字段，
平台为其创建Upload ManagementOperation owner。任何客户端提交owner type/ID、Run ID、Revision ID或object key覆盖字段
都按unknown/forbidden field拒绝。

- client/server upload 通过 Artifact Gateway；底层可使用短期 exact-object presign，但 URL/credential 是一次性响应，
  不进入业务 API、日志或持久化正文；
- multipart part number/count/size、并发、总时长和 abandoned upload 有硬上限；
- Gateway 不接受客户端 bucket/key、ACL、SSE key、public flag 或 storage class；
- expected digest 缺失时允许上传，但 Verify 必须计算；有值时不匹配直接 Rejected；
- streaming upload 在超过 quota/size 时立即终止，不先落完整超限文件；
- resume 绑定同一 grant generation、principal/workload 和 staging object；
- browser filename 和 Content-Type 只作为 hint/display，不能决定 verified media。

## 10. Content Verification

验证 pipeline 按 purpose/media/profile 选择，但最低包含：

- object existence、generation、byte length、streaming cryptographic digest；
- magic-byte/media sniff、declared/verified media mismatch policy；
- archive entry count/path/depth/expanded size/compression ratio/symlink/device 检查；
- malware、known-bad digest、active content、macro、embedded object/link 和 parser exploit scan；
- image dimension/frame/decompression、PDF page/object、Office relationship、text encoding 等格式限制；
- executable/package/runtime 特有 signature、SBOM、lock、entrypoint 检查；
- classification/purpose/port media allowlist；
- content evidence 的 validator version、ruleset digest、observed object generation 和 expiry。

Scanner/parser 不在 API 或 Artifact Gateway 进程运行；复杂解析进入独立Scan Worker。Scan Worker可以在实现层通过
14的typed Sandbox Capability执行，但Artifact domain、状态机和Scanner port不依赖Sandbox协议。扫描工具
失败、超时、OOM、未知格式或 evidence 过期默认 Quarantined/失败，不以“无法扫描”等于安全。

公共上传、Capability/Sandbox及其他普通producer的`CompleteUpload`只提交Uploaded事实。随后durable scheduler command把Uploaded原子
推进Verifying并创建`WorkClass::Artifact` scan Job；Job payload冻结exact Artifact/Blob/object generation、scan Policy、scanner contract
与ruleset。

exact `purpose=RunOutput + owner=ModelTurn + ModelOutputArtifactReservation + ModelArtifactProducer`组合是唯一首版例外：它不调用generic
`CompleteUpload`、不创建或claim Artifact scan Job，而由§15.1/16的同Attempt Producer执行固定canonical Model-response同步verifier并以
stage Receipt `claim_generation`推进到Verified。generic scheduler/scanner必须拒绝该route，Producer也必须拒绝所有其他purpose/owner/
audience；routing predicate与negative fixture进入machine contract，防止两个verifier对同一Artifact竞推进。
rescan创建独立`ArtifactRescan` ManagementOperation和同结构Job；Ready Artifact在rescan排队事务先进入Quarantined，避免旧证据
过期期间继续读取。scan/rescan物理结果必须由exact WorkerProcessGeneration lease fence和`JobCommit` Receipt提交。

## 11. Reference 与所有权

```rust
struct ArtifactReference {
    tenant_id: TenantId,
    artifact_id: ArtifactId,
    owner: ArtifactOwner,
    owner_generation: u64,
    port_or_purpose: ArtifactPortName,
    reference_kind: ReferenceKind,
    created_at: DateTime<Utc>,
}

enum ArtifactOwner {
    Run(RunId),
    Revision(RevisionId),
    CapabilityInvocation(InvocationId),
    ContextObservation(ContextObservationId),
    SandboxJob(SandboxJobId),
    ModelTurn(ModelTurnId),
    ManagementOperation(OperationId),
}
```

- Reference 必须由 owner domain 在同一 PostgreSQL 事务创建/删除；
- owner是closed tagged union，不能接受任意表名/string或不匹配variant的ID prefix；异步scan/transform/delete产物由
  03的统一ManagementOperation aggregate拥有并由17投影API，不创建第二种Artifact operation资源；
- tenant-scoped foreign/integrity constraint 或 central reference service 验证 owner；
- RunBinding、Revision、Invocation、ContextObservation、SkillPackage、SandboxPackage、Model input/output 和 user
  attachment 都使用 Reference；
- public/share/export 是单独授权 projection，不改变 Artifact 本体；
- 删除 owner 先按其 retention policy释放 reference，不能直接删 object；
- 不以缓存、消息或 object store tag 作为引用权威；
- reference graph 支持 legal hold、lineage、incident 和 GC 查询。

## 12. ArtifactGrant

```rust
enum ArtifactGrantOperation {
    ReadWhole,
    ReadRange,
    WriteStaging,
    CommitStaging,
}

enum ArtifactWorkloadAudience {
    Principal,
    Runtime,
    RegistryWorker,
    CapabilityWorker,
    ContextWorker,
    ModelWorker,
    ModelArtifactProducer,
    McpHost,
    SandboxGateway,
    ArtifactWorker,
}

struct ArtifactGrant {
    artifact_grant_id: ArtifactGrantId,
    tenant_id: TenantId,
    subject: GrantSubject,
    artifact_id: ArtifactId,
    operations: BTreeSet<ArtifactGrantOperation>,
    byte_range: Option<BoundedRange>,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    audience: WorkloadAudience,
    expires_at: DateTime<Utc>,
    max_uses: u32,
    generation: u64,
}
```

Grant 是 capability-style authorization，不是 ArtifactRef：

`ArtifactGrantOperation`和`ArtifactWorkloadAudience`均进入machine registry；audience必须与签发时的workload identity和
端口purpose一起进入grant digest。不得用任意service-account字符串扩展audience，也不得把`ReadRange`隐式视为
`ReadWhole`或让read operation用于Staging。

- operation是闭集；`WriteStaging/CommitStaging`只适用于同一owner的Staging Artifact，`ReadWhole/ReadRange`只适用于
  通过state、classification与policy检查的可读Artifact；
- 签发前验证 current principal/workload、owner permission、state、classification、port、deadline 和 policy；
- 绑定单 Artifact/Intent、operation、audience/workload identity、purpose 和短 deadline；
- Sandbox、MCP、Context、remote Capability 获得的 grant 彼此不可替换；
- Model output staging grant只绑定exact `ModelArtifactProducer` identity和Model Attempt fence；Model Worker只持有
  `StageModelOutput`调用权，不取得object locator、S3/KMS credential或可转交的write bearer；
- download grant 默认单 audience、短期、可撤销，不作为分享链接；
- grant token 使用 opaque signed/encrypted form，数据库保存 receipt/digest 而非 bearer value；
- revoke、Run/Invocation terminal、Secret/network kill switch 可提升 generation；
- backend 不能扩大 range、续期、改变 classification 或转签给第三方。

## 13. Read 与 Download

每次 read/download 必须验证 tenant、principal/workload、Reference/owner permission、Ready state、classification、
retention/hold/suspension、grant generation 和 current policy。Object store redirect 只有在短期 exact-object grant、
安全 header 和客户端能力允许时使用；高敏数据通过 Artifact Gateway 流式代理。

响应规则：

- `Content-Type` 使用 verified media，设置 `X-Content-Type-Options: nosniff`；
- 风险 media 默认 `Content-Disposition: attachment`，display name 安全编码；
- HTML/SVG/script/executable 不在平台 origin inline 渲染；preview 必须是新的 sanitized derived Artifact；
- Range 仅对允许 media/operation，范围、并发和总 bytes 有上限；
- ETag/validator 使用 opaque artifact generation，不泄露跨 tenant blob identity；
- URL 不包含 raw object key、tenant ID、filename、Secret 或永久 token；
- download access 形成 body-free audit；正文不写 access log。

### 13.1 受信物化与对象定位机器合同

`artifact_blobs`中的`object_reference_ciphertext`是physical object locator的唯一durable
authority。它只能由对应audience的Artifact Broker读取；Sandbox Controller、Executor、Model/MCP/Capability
worker和公共API均不得取得明文locator、bucket credential或KMS plaintext。受信read authority在同一
PostgreSQL snapshot中重验caller冻结的tenant/owner/Job/lease/Worker generation/request、exact grant或
published package引用、Ready Artifact与Verified Blob后，返回以下非持久、不可Clone且Debug恒定脱敏的投影：

```rust
struct AuthorizedArtifactObjectRead {
    tenant_id: TenantId,
    blob_id: InternalBlobId,
    artifact: ArtifactRef,
    backend: StorageBackend,
    storage_binding_digest: Digest,
    encryption_domain_id: EncryptionDomainId,
    key_id: KmsKeyId,
    object_reference_ciphertext: SecretBytes,
    object_generation: ObjectGeneration,
    authorization_digest: Digest,
}
```

初始`StorageBackend`只注册`s3`。KMS decrypt必须同时提交canonical encryption context：
`schema_version=1`、tenant ID、Blob ID、storage binding digest、encryption domain ID和key ID；
返回plaintext固定为strict canonical JSON：

```json
{
  "schema_version": 1,
  "backend": "s3",
  "storage_binding_digest": "sha256:<64 lowercase hex>",
  "object_key": "<opaque key>",
  "object_generation": "<exact S3 version id>"
}
```

未知/重复字段、错误context/key/binding/backend/generation、空或不规范object key、KMS超时及解封后
任何字段漂移均fail closed。plaintext和ciphertext buffer在drop时清零，且不得进入错误、日志、trace、
metric label、Event、Receipt或Outbox。

Artifact Broker是共享协议/实现族，不是共享运行时。生产物理边界固定为两个audience-isolated服务：

- Model Artifact Broker是独立进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool和permit，只注册
  `ArtifactModelBrokerService.ReadModelRequest`，只接受exact Model Worker URI SAN；
- Sandbox Artifact Broker是另一组独立进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool和permit，只注册
  `ArtifactSandboxBrokerService.ReadWasiArtifact`与`ReadMicroVmArtifact`，只接受exact Sandbox Controller URI SAN；WASI与
  microVM允许共享Sandbox audience内的runtime、对象存储/KMS client和in-flight bulkhead。

两者不得共享Pod、ServiceAccount、数据库连接池或process-local semaphore，任一audience的队列、正文、连接或对象存储请求饱和不得
消耗另一audience的本地准入容量；不得通过同一listener动态选择audience，也不存在三方法通用服务或generic object read。实现可以复用
无状态library与相同machine schema，但每个进程只能安装自己的RPC surface、mTLS allowlist、storage-binding catalog、workload identity和
bounded resources。

本节两个 Broker 都是只读服务。它们的数据库角色只能执行授权读取，RPC surface、protobuf service 和listener均不得注册
upload、stage、complete、verify、finalize或generic object-write方法。Model output写入由§15.1的独立Model Artifact Producer承担；
“复用Broker library”不得被解释为复用Broker进程、ServiceAccount、数据库credential/pool、storage identity或permit。

每个Artifact Broker从CandidateManifest安装的closed storage-binding catalog按exact digest选择client；catalog
只含endpoint/region/bucket/path-style、timeout和hard byte limit，不含静态access key。生产S3/KMS client
只能使用该Pod的短期workload identity/default credential chain和private endpoint。读取必须对exact
`object_generation`执行HEAD及GET，禁止无version fallback；在流式聚合前核对长度上限，聚合后再次核对
ArtifactRef/Blob的exact length与SHA-256。object missing/version drift/oversize/digest mismatch归为integrity
failure，provider timeout/unavailable保持可重试但不得返回部分bytes。

Model逻辑输入的受信读取请求还必须冻结tenant、ModelTurn、当前Job ID/version、lease generation/token digest、
WorkerProcessGeneration、request digest、deadline、exact `model_request` RunValue、ModelTurn owner的active
ArtifactLink以及ArtifactRef/maximum bytes。PostgreSQL read authority在同一snapshot中逐项重验后才能返回上述
非持久投影；Model Artifact Broker完成object I/O后必须用同一closed请求再次授权。Model Worker取得bytes后仍须按Model请求的
closed JSON限制重新解析，要求输入已是canonical JCS并重算逻辑值content digest；任何link替换、lease/fence漂移、
非canonical正文或digest漂移都必须在Provider dispatch前fail closed。

## 14. Derived Artifact 与 Provenance

转换、预览、OCR、文本提取、转码、压缩、render、chunk 和 export 都创建新 Artifact：

```rust
struct ArtifactProvenanceEdge {
    source_artifact_id: ArtifactId,
    derived_artifact_id: ArtifactId,
    transformation_deployment_id: DeploymentId,
    producer: ArtifactOwner,
    parameters_digest: Digest,
    evidence_id: EvidenceId,
}
```

- source 不修改，derived 有独立 digest/media/classification/retention；
- classification 默认不低于所有 source，降级需要显式 declassification policy/approval；
- transform code/runtime exact digest 与参数 digest 可审计；
- derived failure/Quarantine 不影响 source 状态；
- preview 不能被误当原件，citation 指向实际使用的 source/derived content digest；
- provenance graph 有深度/edge limit，防止无界链；
- 删除 source/derived 按 retention/legal hold/lineage policy 独立判断。

## 15. Model、Context、MCP 与 Sandbox 使用规则

### 15.1 Model

Model adapter 只获得 Provider 支持且 data policy 允许的 Artifact grant/bytes。平台固定是否内联、上传 Provider file、
转码或提取文本；Provider file ID 是 encrypted backend handle，不是 ArtifactRef。Provider retention/region 进入 policy。

完整`CanonicalModelResponse`超过冻结Inline threshold时必须成为一个`purpose=RunOutput`、verified media为
`application/json`的Artifact-backed `RunValue`。正文必须先完成16的完整response/tool/schema/usage/safety验证，再编码为strict
canonical JCS；不能把ArtifactRef metadata当逻辑response做schema validation，不能截断、拆分成多个未建模值或把Provider原始
wire/hidden reasoning写入Artifact。

#### 15.1.1 Claim前 reservation

Model Worker领取每批Model Job前必须先预留独立的本地output-materialization RAII slot和有界ID bundle；实际claim少领、失败、
identity不匹配或slot drop时立即释放未使用容量与ID。这个本地slot不是durable authority，也不能借用Model request、只读Broker、
Sandbox或critical-control permit。

只有冻结合法response上限可能越过effective Inline threshold时，Model start transaction才为该物理Attempt原子冻结16的exact
`ModelOutputArtifactReservation`：tenant/Run/Node/ModelTurn与expected version、Job/version/attempt/lease token/Worker generation、
request/admission/Model Deployment/HardLimitProfile/output schema digest、classification、Artifact/RunValue/Output Link/grant/stage
Receipt IDs、候选Blob与duplicate-cleanup Job ID、Artifact-owned与candidate-Blob-owned两个quota bundle、最大materialized bytes、media、Retention/ArtifactIo Policy
revision、Blob security-domain digest、`staging_retain_until`、`ready_retention_seconds`、deadline与整个reservation digest。该事务创建同Attempt的Staging Artifact intent，
其当前`retain_until`只能是冻结的`staging_retain_until`；事务同时创建受限write grant，并按最坏合法response bytes/count预留quota；
Artifact bundle的owner从创建起是预留Artifact ID，candidate Blob bundle的owner是预留candidate Blob ID，dedupe/cleanup均不得转移；
此时Artifact允许尚未绑定Blob。两个bundle必须分别是04规定的count+logical与uploads+staging+physical exact line。合法response上限完全落在
Inline内时冻结16的`InlineOnly`分支，不创建虚假Artifact/Blob/quota预留。
Artifact-capable分支任一项不能完整预留时不得start该Attempt、不得调用Provider，也不得形成Provider usage。

预留ID本身不授权读取、写入或finalize。Retry/failover的新物理Attempt必须使用新Artifact、grant、Receipt、Link、RunValue和quota
identity；旧Attempt的任何ID或receipt都不能被新lease接管。结果最终可Inline时，Model terminal必须证明Artifact仍未绑定Blob/locator，
才能以零actual关闭两个bundle、撤销grant并把未使用intent标为可GC，而不是为了复用预留强制写对象存储；一旦candidate已绑定或可能
PUT，失败owner仍可关闭未Consume的Artifact bundle，但candidate Blob bundle必须保留到cleanup取得exact deletion/absence evidence。

#### 15.1.2 独立 Model Artifact Producer

Model Artifact Producer是独立进程、Deployment、ServiceAccount、mTLS server identity、restricted PostgreSQL write
credential/pool、S3/KMS workload identity、two-phase admission permit与transport backlog hard cap。它不与Model Worker、只读Model Artifact Broker、Sandbox Artifact
Broker、Artifact Gateway或Scanner共享Pod、ServiceAccount、DB pool、storage identity或process-local semaphore；其饱和、重启或
对象存储故障不能占用read Broker、Model Provider stream、API、Scheduler或其他WorkClass的准入容量。

Producer只注册versioned client-streaming `StageModelOutput`，只接受exact
`spiffe://insight.platform/workload/model-worker.artifact-output` URI SAN；Model read使用的`.../model-worker`身份必须被拒绝。首帧必须是一个closed header，
随后只能出现严格递增、非空且按16 canonical chunking的data frame，最后是唯一terminal frame；协议不定义`FenceRefresh`或任意metadata
frame，terminal只携带客户端最后观察到的fence lower bound。空首帧、重复header/terminal、sequence gap、短非末片、单片/总量越界、
terminal后数据、未知字段/enum、声明与实测length/digest/media/classification/schema/evidence不一致全部fail closed。它不注册
`ReadModelRequest`、WASI/microVM、generic upload/read/finalize或公共HTTP方法。Model Worker不能取得object locator、bucket credential、
KMS plaintext或Producer数据库credential。

`StageModelOutput`的closed header/receipt只使用16的同名machine contract，不在Artifact实现另建第二套DTO。header至少完整回绑
reservation、正文SHA-256/byte length、`application/json`、classification、output schema、validation evidence与stage request digest；
receipt只返回exact Artifact/candidate+resolved Blob/object generation的脱敏digest、Verified version、新增physical bytes、正文事实和receipt digest，不返回object key、URL、
grant token或业务Output Link。stream body、metadata或调用方header均不能覆盖tenant、owner、purpose、classification、retention、
storage binding、KMS context、deadline或预留ID。

容量准入分两阶段：exact TLS/service-role authorization后、读取bounded header前先取得18 ComponentCapacityManifest的global stream与
唯一per-stream wire-buffer weighted permit，后者weight exact为`effective model_output_chunk_bytes + 4096`并由Header/Data/Terminal复用；
所有后续DB/S3/KMS waiter都必须位于该global slot内。解析valid header并完成Receipt replay/pre-authorization、得到trusted tenant与declared
length后，在读取首个data frame前再原子取得声明总bytes与tenant stream permit，不另取第二份data buffer。全部permit持有到
terminal response、stream drop或absolute deadline。第一阶段不足返回固定body-free unavailable status；第二阶段不足走16 transient
`DependencyUnavailable + RetrySameAttempt`，若已claim Processing则缩短lease。两者都不能排入application queue、先读正文或借用
DB/S3/KMS/read Broker容量。

transport front必须在连接进入bounded accepted backlog时启动18冻结的monotonic accept deadline；TLS/service-role、backlog与第一阶段permit
等待、以及完整Header decode共享该deadline。silent/fragmented Header到期后body-free关闭连接、释放stream/wire-buffer permit且不创建
Receipt。只有valid Header完成current授权后才切换到冻结Attempt absolute deadline；不能以流仍存活、重试解析或重新取得permit延长任一期限。

Producer执行以下唯一写入协议：

```text
0. exact mTLS + closed key/digest lookup
   -> terminal same-key/same-digest Receipt: replay without I/O
   -> same-key/different-digest: idempotency conflict
   -> active Processing Receipt: bounded in-progress/defer
1. new/expired Processing claim
   -> pre-I/O current Job-fence authorization
   -> claim/reclaim same-attempt model_output.stage Receipt and increment claim_generation
   -> validate reserved Staging intent, grant, quota and Policy closure
2. exact security-domain dedupe lookup after authorization/quota
   -> existing Verified Blob: stream+validate all bytes without object write, then bind as resolved Blob
   -> no winner: create/load reserved candidate Blob and KMS-seal one exact opaque locator
3. candidate path streams exact bytes to one unique staging object
   -> conditional create; never overwrite an existing generation
   -> HEAD exact object generation/KMS context, then guarded Staging -> Uploaded checkpoint
4. guarded Uploaded -> Verifying checkpoint; perform bounded verification outside DB transaction
5. final PostgreSQL transaction under current claim_generation and dedupe advisory fence
   -> revalidate Attempt/reservation/grant/Policy and final Job authorization
   -> preexisting hit: bind resolved Blob and apply Artifact Staging -> Uploaded -> Verifying -> Verified in this transaction
   -> racing Verified winner: rebind Artifact to resolved winner; candidate -> Deleting if it has an object
   -> candidate winner: candidate Blob -> Verified and keep it as resolved Blob
   -> Artifact Verifying -> Verified + evidence + Processing -> Succeeded Receipt atomically
   -> return resolved Blob, new physical bytes, candidate-cleanup bytes/generation and optional preallocated cleanup Job identity after commit
6. no Ready, business Reference, RunValue, quota settlement or Model transition
```

两次授权都必须从PostgreSQL current authority读取完整Job fence：tenant、ModelTurn/Job、state、attempt、lease generation/token digest、
WorkerProcessGeneration、request/admission/binding、reservation、grant、Policy closure、deadline与cancel/terminal状态。header/terminal
提供的`expected_version`只是同generation当前事实的单调lower bound：final不得小于initial，且每次授权读取的current Job version必须
大于等于相应lower bound；Producer不以两者做Job CAS。合法heartbeat可在frame捕获前后继续推进current version，只要Running/InFlight
state、lease generation/token、Worker generation及全部immutable字段仍exact就不能误判stale。lease接管、Worker替换、
request/reservation漂移、cancel/timeout/terminal first-winner或deadline到期一律返回stale，不得提交Uploaded/Verified。对象I/O不在数据库
事务内。Processing claim、Blob bind、Uploaded/Verifying checkpoint与最终Verified事务必须按03锁序先锁stage Receipt并CAS
`claim_generation`，再按04 canonical顺序对Artifact与candidate Blob两个exact quota bundle header/line取得`FOR SHARE`，锁后重验冻结的
`UsageReservationId/generation`、Open state及全部line，随后对current ModelTurn/Job取得会与cancel/lease-takeover/terminal UPDATE冲突的
共享serialization guard，最后锁Artifact/Blob并提交。Quota Close/Expiry/settlement必须取得冲突锁并提升generation；Producer不更新Job，
heartbeat只会在这些短事务窗口等待。guard取得后仍要重验全部current事实；post-I/O CAS失败时对象仍是不可读staging orphan。

PUT只能对数据库中已绑定的exact opaque locator执行conditional create。Processing lease接管递增`claim_generation`；旧claimant即使
晚到完成PUT，也不能通过后续数据库CAS。新claimant先加载既有locator并HEAD exact generation：同一header digest/length/KMS context可继续
验证，不同generation、metadata或bytes进入integrity isolation，绝不生成第二locator、覆盖对象或把冲突对象提升Verified。

dedupe key固定为`tenant/backend/storage_binding/encryption_domain/security_domain/content_digest`。Producer只有在exact Attempt授权与
quota reservation成立后才能用完整key做常量shape查询，不能list/prefix search，也不能通过状态、时序或错误泄露跨tenant/安全域命中。
即使命中existing Verified Blob也必须读取并验证调用方本次完整stream；命中只跳过object write。并发candidate在final transaction按该key
取得与CR-119相同的transaction advisory fence：唯一candidate成为Verified winner；loser Artifact改绑winner，stage Receipt记录
`new_physical_bytes=0`。Receipt必须把结果闭合区分为`PreexistingHit | CandidateWinner | RacingCandidateLoser`；最后一种还记录candidate
exact bytes与object-generation digest。若loser candidate已经有object，则同事务把它推进Deleting并回绑预留
`duplicate_blob_cleanup_job_id`，但Producer不得创建Job/Event；随后Model owner terminal或bounded Artifact cleanup reconciler必须以该
same ID幂等创建exact `InternalBlob`-owned cleanup Job。stage Receipt/Deleting candidate是崩溃后的恢复authority，不能留下无定位的object。

Producer verifier固定为无脚本、无Provider扩展的closed Model response profile：UTF-8、strict JSON、拒绝duplicate key/尾随字节/NaN/
Infinity、输入bytes必须已经等于其JCS canonical serialization、反序列化为16当前`CanonicalModelResponse` nominal shape并重新执行
depth/object/array/string/total-byte limit及固定Secret/data-flow/classification规则。Agent/ModelLoop的exact output schema由Worker在stage前
和Model terminal repository在Ready前各自重验；Producer不能读取Artifact-backed request正文，也不能用调用方提供的schema digest冒充
语义验证。Producer同时重算digest/length，核对exact
S3 generation、storage-binding digest、tenant/security-domain KMS context和冻结ArtifactIo Policy。validator/profile/ruleset digest与
evidence schema version进入Verified evidence；Producer不能运行Skill/script/package manager或把任意content-type交给动态parser。

Producer最多写Artifact/Blob/Grant current fact与stage Receipt；数据库role必须在权限层拒绝修改Run、RunNode、Invocation/ModelTurn、
Job current state/lease、RunValue、业务Output Link、quota余额、Event或Outbox。`artifact.ready`和业务事件只能由owner terminal事务产生。
Producer不能将Artifact推进Ready、建立业务
Reference、settle Model/Artifact quota、推进Model/Node/Run，亦不能发布`model.completed`。只读Broker继续保持SELECT-only，不能通过
共享repository、stored procedure或数据库函数间接获得上述写权限。

#### 15.1.3 同Attempt幂等与 owner terminal

`StageModelOutput`使用03共享`JobCommit` Receipt，operation固定为`model_output.stage`；dedupe key严格为tenant、Job、lease generation与
commit request ID，其中commit request ID就是预留的`stage_receipt_id`。attempt必须与该Job generation的durable reservation一致，但
`stage_request_digest`不进入key，而是作为Receipt的独立`request_digest`保存。相同key/digest重放返回同一Verified receipt，不重复PUT、
Artifact/Blob、quota reservation或evidence；相同key不同digest稳定返回`idempotency_conflict`。Processing stage receipt使用短且不超过
Job lease/deadline的可恢复lease，可在同Attempt/current fence下有界续租；它不得跨对象I/O持有数据库事务或行锁。响应丢失或lease过期后的
恢复先按同一key/digest读取Receipt：`Succeeded | Failed | Rejected`直接返回既有safe terminal result，即使Job随后terminal或lease已被
接管也不重新授权、不做I/O；active Processing只返回bounded in-progress/defer。只有不存在Receipt或Processing lease过期的接管才重做
current pre/post fence授权；接管必须递增`claim_generation`，后续Blob bind、Artifact/Blob Verified及Receipt terminal commit全部CAS该值。

failure persistence是closed合同，不允许实现自行选择是否terminalize：

| 条件（按优先级） | Receipt持久化 | Artifact/Blob mutation | wire结果与后续 |
|---|---|---|---|
| existing terminal same key/digest | 不变 | 不变 | replay原Succeeded或terminal failure |
| same key/different digest | 不修改existing Receipt | 无 | transient envelope中的`IdempotencyConflict + Conflict`；该digest不得重试 |
| fresh stale fence / deadline | Producer不得新建或terminalize Receipt；已有Processing保持原状态 | 无 | transient `StaleFence/DeadlineExceeded + RejectStale`；Model owner cancel/timeout/cleanup事务可按同key/digest把已有Processing终结为Rejected |
| dependency/capacity unavailable且剩余deadline允许 | 保持Processing，lease缩短为`min(db_now + retry_after, Job deadline)` | 不推进 | transient `DependencyUnavailable + RetrySameAttempt`；lease后同Attempt递增claim_generation并按exact object HEAD恢复 |
| dependency失败且已无合法正retry窗口 | 按上一行deadline规则，不伪装transient retry | 无 | transient `DeadlineExceeded + RejectStale` |
| TooLarge / Invalid且final current guard仍成立 | `Processing -> Rejected`并保存bounded failure result | `Artifact -> Rejected`、撤销grant；candidate/object若存在只进入cleanup，不释放quota | terminal failure：`RejectResponse`，禁止重试该response |
| IntegrityFailure且final current guard仍成立 | `Processing -> Failed`并保存bounded evidence | candidate Artifact current `Staging | Uploaded | Verifying -> Quarantined`并撤销grant；仅在exact candidate generation证据充分时Blob -> Corrupt，否则由incident authority判定 | terminal failure：`IntegrityIncident`；只允许incident/cleanup |
| Success | `Processing -> Succeeded` | resolved Blob/Artifact -> Verified及evidence | success receipt；交给owner terminal |

最后三条terminal mutation必须与Receipt结果在同一事务CAS current `claim_generation`；取得final guard前若同时发生stale，stale优先且Producer
不得写Artifact/Receipt。`DependencyUnavailable`永远不能保存为terminal Failed，否则会永久阻断同Attempt恢复；Conflict也永远不能改写
original Receipt。Producer之外的Model owner/cleanup只能按该矩阵terminalize stale Processing，不能把transient dependency改写为业务成功。

Producer返回Verified receipt后，只有Model owner repository可在一个PostgreSQL terminal first-winner事务中：

1. 按ID排序锁定terminal与stage两个Receipt并claim/replay terminal Receipt，从已锁stage Receipt确定candidate disposition与可选预留
   cleanup Job ID；再按03顺序锁定Model quota、Artifact bundle与candidate Blob bundle以及current Run/Node/ModelTurn parent aggregate；
2. 在取得任何Job-rank锁之前，把current Model Job与可选`RacingCandidateLoser` cleanup Job组成canonical sorted-unique Job集合，并在同一个
   Job-rank阶段依ID顺序逐一lock existing或create-or-lock。cleanup Job必须是预留ID、exact `InternalBlob` owner且payload逐字段匹配stage
   receipt的candidate bytes/generation；随后重验current Job fence、Attempt、request/binding及全部identity。已经terminal的same cleanup
   Job/Receipt按原结果复验，different payload是invariant failure；不得先锁current Job再补锁排序更小的cleanup Job；
3. 锁定同tenant预留Artifact、resolved Verified Blob、可选Deleting/Deleted candidate、active write grant与冻结Policy，逐项比较digest、length、media、classification、schema、
   validation evidence、retention和object generation；
4. 将exact Artifact `Verified -> Ready`，以本事务单一PostgreSQL `db_now` checked-add冻结的`ready_retention_seconds`，把当前
   `retain_until`从`staging_retain_until`切换为计算出的`ready_retain_until`，并把该绝对值写入terminal Receipt；撤销write grant，创建预留且唯一的
   `owner=ModelTurn/reference_kind=Output/purpose=RunOutput/port=model_response` Artifact Link；
5. 用预留RunValue ID写immutable `model_response` `ValueRef::Artifact`，其schema/content digest与classification和Ready Artifact、已验证
   canonical response逐字段一致；
6. 提交ModelTurn/Job first-winner、Node/ModelLoop wake、Provider usage与Model quota settlement；Artifact bundle消费count=1与
   logical=canonical bytes并保持Open到该Artifact删除。candidate Blob bundle按04分支：PreexistingHit全line Close(0)；CandidateWinner把
   Uploads/StagingBytes以0终结并消费physical=new bytes，保持Open到resolved Blob最后alias物理删除；RacingCandidateLoser只终结Uploads，
   StagingBytes与未Consume Physical保留到同一cleanup Job exact deletion/absence后Close(0)；若cleanup Receipt已先提交则复验已Closed事实；
7. 追加Artifact Ready、Model terminal Event与Outbox；该Event回绑stage Receipt、candidate-cleanup/evidence digest。

锁序遵守03全局顺序，多Artifact时按tenant/Artifact ID排序；S3/KMS I/O不得进入该事务。任一CAS、policy、quota、Artifact或Receipt检查
失败必须回滚全部步骤；不存在Ready但无Output Link/RunValue、Model succeeded但Artifact未Ready、或已创建业务Reference却未settle quota的
可提交状态。重复terminal commit只返回既有receipt，不重复Ready、Link、RunValue、Event、Outbox或结算。

Producer的Blob bind及`Staging -> Uploaded -> Verifying -> Verified`是一个受限physical sub-protocol，不是业务aggregate command；为维持
restricted role，它不单独追加Event/Outbox。其durable审计由claim-generation-bound stage Receipt和Verified evidence承担；最终Ready事务
或orphan cleanup/incident事务必须把该Receipt/evidence digest写入各自Event。除此特例外仍遵守03的业务mutation+Event+Outbox规则，Producer
不得借此写任意silent业务状态。

#### 15.1.4 orphan、retention、错误与恢复

Model output的classification、exact Retention/ArtifactIo Policy、storage/KMS binding、`staging_retain_until`和
binding的`maximum_put_completion_uncertainty_milliseconds`、`ready_retention_seconds`来自Model Deployment/admission closure，Producer和Worker
都不能降低、延长或互换。未terminal的Staging/Uploaded/Verifying/Verified output
只使用冻结的短期`staging_retain_until`与GC grace；其deadline必须覆盖合法Provider/Producer/terminal恢复窗口，删除仍要重验active
grant、Job generation、hold和exact object generation。只有owner terminal进入Ready时才使用该事务database time计算并原子切换为
`RunOutput`的absolute `ready_retain_until`，随Artifact/terminal Receipt和业务Reference保存；重放不重新计算。04两个quota bundle在
claim/start时按最坏值预留。Inline或Producer bind前Reject可在同事务证明`blob_id=NULL`后零actual关闭两者；bind/PUT后的Reject/cancel/
timeout/first-winner loser可关闭未Consume Artifact bundle，但不得释放candidate Blob bundle，`cleanup_required`只是Artifact/Blob lifecycle
分类。所有conditional PUT都必须使用不晚于Attempt deadline的write deadline；candidate物理cleanup不得在`staging_retain_until`前执行或
采纳DELETE/absence，也不得Close/Expire其Blob bundle。到点后仍须对exact locator/generation重新执行DELETE/HEAD并取得18 binding保证的
stable evidence；更早的client timeout、连接关闭或HEAD absence都不能复用。只有该evidence成立时，GC事务才能Close其未Consume line。Ready Artifact删除Refund count/logical；只有
resolved Blob最后alias物理删除才Refund其original PhysicalBytes bundle。进程内Producer permit在RPC结束/失败时立即释放，不能代替durable quota。

稳定错误至少使用以下closed reason class；公共错误、Event和metric不得包含正文、object key、grant、digest、Policy内容或跨tenant存在性：

| reason class | 语义与重试 |
|---|---|
| `model_output_capacity_unavailable` | claim/start前无法预留本地future-stage slot、IDs或durable quota；不dispatch Provider，可按deadline安全重试 |
| `model_output_artifact_too_large` | 实测bytes超过冻结maximum；永久content rejection，不截断或提高本Attempt上限 |
| `model_output_artifact_invalid` | digest/media/JCS/schema/evidence/Policy不匹配；永久拒绝当前response |
| `model_output_artifact_stale_fence` | 任一pre/post authorization或terminal CAS已失效；旧Attempt永不重试提交或提升Ready |
| `model_output_artifact_deadline_exceeded` | stage或post-authorization已越过冻结deadline；当前Attempt永久RejectStale，后续只服从Model owner的总体deadline/retry policy |
| `model_output_artifact_conflict` | 同一stage identity携带不同request/content digest；映射`idempotency_conflict`且必须人工/代码缺陷调查 |
| `model_output_artifact_unavailable` | Producer、DB、S3或KMS短暂不可用；原Attempt仍current且exact bytes/digest可证明时只在同Attempt有界重试stage；Attempt失效后由冻结Model retry/budget policy决定是否创建新Attempt并记录可能重复费用，禁止在同Attempt仅为物化重放Provider |
| `model_output_artifact_integrity_failure` | exact object generation、KMS context、HEAD或存储正文漂移；隔离对象并触发Artifact incident |

恢复按事实窗口收敛：

- Staging intent已提交但没有object：deadline/active grant到期后进入Deleting/GC；
- PUT成功但Uploaded未提交：staging inventory只以opaque staging identity与exact generation创建cleanup候选，不推断Model成功；
- Uploaded/Verifying已提交但未Verified：Producer只按同Attempt stage Receipt、current Model Job fence与exact object generation重放
  受限同步verifier；它不创建或claim Artifact Job。无法证明exact bytes时Reject/GC；
- Verified receipt已提交但Model terminal响应丢失：先重放terminal Receipt；若terminal尚未提交且fence仍current，可重试同一owner事务；
- lease丢失、cancel、timeout、worker crash、terminal commit失败或另一first-winner已提交：对象保持非Ready且无业务Reference/RunValue，撤销/
  过期grant后按bounded orphan流程进入Deleting/GC；
- terminal事务已提交但响应丢失：重放同一terminal receipt，不重新stage、调用Provider或结算；
- 新Attempt不能adopt旧Attempt的Verified artifact、grant或receipt；GC/Producer/late Worker也不得在ModelTurn terminal后回写output。

### 15.2 Context

Context ingest 只接受 Ready Artifact，Dataset Generation 建立 durable Reference。ContextItem/Citation 记录使用的
content digest/locator；提取文本是 derived Artifact，不覆盖原件。

### 15.3 MCP

MCP embedded resource/resource link 经过 size/media/URI/auth policy后 ingest 或 transient observation。MCP server 不能
得到 tenant bucket或任意 Artifact 枚举；只得到当前 published Tool/Resource port 的 grant。

### 15.4 Sandbox

Sandbox input/output 使用 14 的 per-Job grant，并只经Sandbox Artifact Broker物化。Guest 只能读声明 input、写 staging output；不能指定 Artifact ID、
object key、classification 或 Ready 状态。`artifact_links`是撤销的唯一durable fact：只读Broker返回exact read receipt，Sandbox owner/
Controller authority在销毁证据形成前按Job/attempt/Worker generation/lease幂等推进`active -> released`，Job terminal事务释放遗漏项并
核对request冻结的完整grant集合。重复撤销不得形成第二状态或阻止terminal；未 finalize output 进入 staging GC。

Sandbox runtime bundle虽是普通Ready Artifact，其发布边界由18的HardLimitProfile version 4额外收紧：byte length必须非零且不超过
`capability_sandbox.runtime_bundle_bytes.hard_max=67108864`，Q1 effective limit为33554432，溢出稳定为`content_rejected`。
Artifact Ready不替代SandboxPackage发布校验，Broker读取上限也不能把非法Package延迟到Job执行时才发现。

## 16. Value 与 Inline Threshold

平台值合同使用：

```rust
enum ValueRef {
    Inline(BoundedClosedJson),
    Artifact(ArtifactRef),
}
```

Inline threshold 由protocol/data classification在不超过18唯一
`HardLimitProfile.run_scheduler.inline_value_bytes`的前提下固定，平台hard max不可提高。超过阈值或 binary 必须 Artifact；
不能把正文拆成大量数据库 rows/events 绕过限制。小 JSON 即使 inline 也有 depth/string/array/total byte limit。
ValueRef 的通用转换不会自动解析文件或执行内容。只有下游规范显式定义的trusted materializer
可以在exact grant下读取Artifact-backed逻辑值；它必须重新核对tenant/digest/length/media/classification，并在
使用前执行该消费者的exact schema validation。materializer不得直接接收object locator或storage credential；
所有物理读取必须经13.1中与调用方audience匹配的Artifact Broker authority，且读取成功不替代消费者对canonical正文和逻辑digest的复验。

## 17. Retention、Legal Hold 与删除

Artifact 可删除条件是：

```text
no live durable references
AND no active grant/upload/verification
AND retain_until elapsed
AND no legal hold/incident hold
AND no policy-required lineage retention
AND GC grace elapsed
```

这里的时钟输入来自 closed `ArtifactRetentionPolicy`：exact Retention revision必须提供
`minimum_retention_seconds`、`gc_grace_seconds`、`tombstone_retention_seconds`、`retain_provenance_sources`和
`delete_requires_approval`；Artifact prepare 拒绝短于 minimum 的`retain_until`。调用方不能提交或覆盖 GC grace。
`ArtifactRetentionPolicy.version`固定为1；它只允许出现在`PolicyKind::Retention`的`PolicyResourceSpec`，并要求
`rules_digest`等于整个closed policy document的canonical digest，不能只保存opaque digest而让repository无法执行规则。
Reference/Hold/Retention/GC/Delete 的当前事实写 Artifact/ArtifactLink；审批使用 Task，物理删除使用 Job，结果使用
Receipt/Event。provenance 或 approval 条件缺少完整证据时 fail closed。

每个tenant的首个内置Retention revision与其authoring Artifact可以形成自持有bootstrap root。两者必须由tenant onboarding
在一个事务内创建；数据库只将`Artifact -> retention ResourceVersion`的FK延迟到commit检查，使闭环可原子建立。不得用NULL、
虚构revision、禁用FK或提交后补写绕过该根合同。

删除流程：

```text
mark GC candidate with scan generation
 -> recheck all predicates under lock
 -> Staging/Uploaded/Verifying/Verified/Ready/Rejected/Quarantined/Corrupt -> Deleting
 -> lock all same-Blob Artifact aliases
 -> other live alias: preserve Blob + exact alias witness
 -> no other live alias: delete exact object generation/version + verify absence/deletion marker
 -> Deleted + exact Attempt-bound deletion receipt + audit/outbox
```

- 不使用最终一致的 cached refcount 单独决定删除；
- 非Ready candidate只有在没有业务Reference、write grant已撤销/过期、没有current Producer/scan claim且exact retention/hold允许时才能
  进入Deleting；stale/cancelled `Verifying`允许直接到Deleting，不必伪造Rejected/Quarantined或scan evidence；
- Model output candidate在冻结`staging_retain_until`前不得开始physical DELETE或采纳absence，即使Attempt已cancel、Receipt已terminal或
  client已报告PUT timeout；到点后cleanup必须丢弃任何早期观察，按exact locator/generation重新DELETE/HEAD。该时间已按04严格越过
  Candidate-qualified write-quiescence boundary，因此不会在quota Close后出现迟到PUT；
- `artifact_references` 是权威，引用数量可以是加速 projection；
- staging/abandoned multipart 使用更短独立 TTL，但仍验证 active grant/generation；
- Blob 只有在没有其他未删除Artifact alias时才物理删除；每个Artifact自己的Reference、Retention与Hold资格仍独立判定；
- 每个Ready Artifact删除只Refund以该Artifact ID为owner的Count/LogicalBytes bundle；即使它最初创建了Blob，只要仍有live alias就不得
  Refund PhysicalBytes；
- 最后一个alias删除并取得exact physical deletion/absence evidence时，Blob lifecycle按Blob ID查找最初winner的physical bundle并Refund；
  duplicate/race/orphan candidate从未Consume Physical时则Close其Staging/Physical line为0。resolved Blob的physical owner不会随alias转移；
- `artifact_only`回执必须绑定一个仍存活的same-Blob Artifact ID/version且不得包含backend deletion evidence；
- `blob_generation`回执必须证明无其他未删除alias并包含exact backend receipt与absence/deletion-marker evidence；
- legal hold 只能由授权 compliance principal 设/解并审计；
- deletion failure 重试且保留 Deleting，不把 object key 交给人工脚本任意删除；
- 物理删除后保留不含正文的 tombstone/digest/audit 达合规期限。

## 18. Quarantine、Rejection 与 Corruption

- Staging/Uploaded/Verifying/Verified 不可被普通 read；
- Quarantined 只对 Security/Compliance break-glass workflow 可见，普通 owner 只能看到安全状态；
- Rejected 保存 bounded reason class/evidence，不提供恶意正文直接下载；
- 已 Ready Artifact 后续命中恶意规则可原子 Quarantine，现有 grant 撤销，Reference 保留用于审计；
- digest/object generation mismatch、object missing 或 encryption failure进入 Corrupt 并触发 incident；
- Corrupt 不自动从客户端副本修复；恢复必须验证 backup/version 并创建 evidence，必要时创建新 Artifact；
- false positive release 需要双人/高权限 approval（按 policy）和新 scanner evidence；
- error/public event 不泄露 malware signature、object key 或其他 tenant existence。

## 19. 幂等、并发与恢复

- Prepare/Complete/Finalize/Delete使用command scope + idempotency key + request digest；
- 相同 upload completion/finalize 重复返回已有 receipt，不重复占 quota/reference；
- multipart completion、scanner、finalize、quarantine 和 delete 以 artifact generation CAS；
- scanner lease/epoch/fence 阻止 stale evidence 覆盖新状态；
- scan/rescan/delete/cleanup worker outcome使用无Principal的`ArtifactWorkerAudit`，worker identity必须等于Job fence；
- upload 与 deadline、finalize 与 quarantine、read 与 revoke、GC 与新 Reference 竞态由 PostgreSQL first-winner；
- object PUT 成功/DB 失败由 staging inventory GC；DB Ready/object 缺失由 finalize 前 HEAD/digest 阻止；
- callback/outbox/NATS 丢失由 Uploaded/Verifying/Deleting safety scan 恢复；
- object store transient failure bounded retry，不在 DB transaction 内等待网络；
- expired Artifact Attempt按bounded UUID低位shard/keyset扫描；deadline内仍有pinned generation时，Recovery pool以数据库时钟
  写Lost、一次关闭permit并保留Operation/Artifact/Blob exact父版本供retry；deadline/limit耗尽或物理effect不确定时必须进入
  Artifact reconciliation/人工处置，不能自动恢复Blob integrity或伪造删除结果；
- restore 后按 PostgreSQL metadata 与 object inventory/digest 做 reconciliation，不把 bucket listing 当业务权威。

## 20. 配额与背压

层级 quota：

```text
tenant stored logical bytes/artifact count
tenant physical blob bytes
staging bytes/uploads/multipart parts
per principal/workload upload/download rate
per Run/Invocation/port input/output bytes and count
scan/transform concurrent resource units
egress/download bytes
```

- Prepare 时 reservation，Verified/Ready 时结算，Reject/GC 时释放；
- Model output在Provider dispatch前按最大合法canonical response预留Artifact bytes/count和exact IDs；Verified不结算，只有Model
  terminal owner transaction按actual bytes结算，Inline/失败/取消/loser由terminal或GC释放；
- reservation 有 deadline，不能无限占额度；
- dedupe 不允许绕过 logical byte/Artifact count quota；
- Artifact Gateway、scanner、transformer、GC、download 使用独立队列、permit、连接池和 autoscaling；
- scan backlog 超过安全门槛时拒绝/延迟新高风险 upload，不把未扫描内容当 Ready；
- large transfer 使用 streaming backpressure，不在内存缓冲全文件；
- control/quarantine/revoke/delete 使用保留 capacity；
- Artifact/S3 饱和不能耗尽 API/Scheduler/Model/MCP/Sandbox control DB pool；Sandbox Artifact Broker的permit/DB pool耗尽也不能
  占用Model Artifact Broker或Model Artifact Producer的permit/DB pool，三者反向同理。Worker在claim前必须持有本地future-stage slot和
  durable reservation；独立Producer仍在真正stage时以自己的服务端permit准入。dispatch后发生的Producer瞬时饱和只能在同Attempt、
  同bytes/digest内有界重试stage，不能借用其他lane、形成无界客户端buffer或仅为物化重放Provider。

## 21. 安全、租户与加密

- 所有 Artifact/Blob/Grant/Reference/Evidence/Cache query 同时限定 tenant；
- object store policy 只允许 Artifact service workload identity，禁止 tenant 直接 list bucket；
- at-rest encryption 使用 tenant/security-domain scoped key；key ref 在 KMS，数据库不保存 key value；
- in-transit 使用 TLS/mTLS，direct upload/download grant 绑定 exact operation/object/deadline；
- object key、URL、grant、KMS context 和 backend error 视为敏感 metadata；
- no cross-tenant dedupe/cache/preview/scan result reuse；
- filenames、archive paths、document links、media metadata 和 active content 均不受信任；
- high-risk content 只在 Scan Sandbox 解析，不在 Gateway/API；
- public/share/export 需要独立 Policy/Approval/expiry/audit，不改变 bucket ACL 为 public；
- data residency、Provider transfer、retention 和 legal hold 是 binding/policy，不由调用者字段覆盖。

## 22. 所有权接口

```rust
trait ArtifactRepository {
    async fn prepare(&self, command: PrepareArtifact) -> PrepareArtifactReceipt;
    async fn commit_upload(&self, command: CommitUpload) -> UploadReceipt;
    async fn commit_evidence(&self, command: CommitContentEvidence) -> EvidenceReceipt;
    async fn finalize_and_reference(&self, command: FinalizeArtifact) -> FinalizeReceipt;
    async fn issue_grant(&self, command: IssueArtifactGrant) -> GrantReceipt;
    async fn mark_for_deletion(&self, command: MarkArtifactDeletion) -> DeletionReceipt;
}

trait BlobStore {
    async fn create_upload(&self, request: BlobUploadRequest) -> BlobUploadHandle;
    async fn head(&self, request: BlobHeadRequest) -> BlobHead;
    async fn read(&self, request: BlobReadRequest) -> ByteStream;
    async fn delete_generation(&self, request: DeleteBlobGeneration) -> DeleteBlobReceipt;
}

trait ModelArtifactProducer {
    async fn stage_model_output(
        &self,
        stream: ClientStream<StageModelOutputFrame>,
    ) -> Result<StageModelOutputReceipt, StageModelOutputFailure>;
}
```

`ModelArtifactProducer`只表达§15.1的内部client-streaming port；frame/header/receipt必须是closed nominal type，不能退化为
`Stream<Bytes>`加任意metadata map，也不能由只读Artifact Broker实现该trait。failure必须使用16的closed reason/disposition DTO，
并与§15.1错误表一一映射；自由错误文本、backend状态、locator、grant或正文不得进入wire。

Artifact Worker只调用两个typed backend port：`ArtifactScanner::scan(exact request)`与
`BlobStore::delete_generation(exact request)`。网络/对象读取/扫描发生在数据库事务外；backend返回bounded evidence后，worker用原
Job fence提交。duplicate-Blob候选回收使用owner为`InternalBlob`的cleanup Job，必须验证exact object generation、backend receipt与
absence evidence后才把Blob推进Deleted；不能由bucket inventory或人工脚本直接写数据库状态。

Domain contract 不依赖具体 S3 SDK。BlobStore 不接收 principal/authorization decision；Artifact application service 先
验证并只传 exact opaque object operation。Artifact repository 与 owner repository 必须能在同一 PostgreSQL 事务
完成 finalize/reference。

## 23. API 与事件合同

外部 API 至少包含：

```text
POST /v1/artifacts:prepare
POST /v1/artifacts/{artifact_id}:complete-upload
GET  /v1/artifacts/{artifact_id}
POST /v1/artifacts/{artifact_id}:issue-download
POST /v1/artifacts/{artifact_id}:rescan
POST /v1/artifacts/{artifact_id}:delete
```

Finalize 通常由 owner domain/Capability callback 内部调用，不允许客户端仅凭 Artifact ID 直接置 Ready。API 使用
idempotency、etag/generation、closed schema 和 stable error。直接 transfer credential 标记 `no-store`，不进入响应
缓存或事件。异步verify/rescan/delete使用03/17统一`/v1/operations/{operation_id}`，不定义Artifact专用Operation。

事件最小集合：

```text
artifact.uploaded
artifact.ready
artifact.quarantined
artifact.rejected
artifact.corrupt
artifact.deleted
```

事件只携带 Artifact ID、purpose、state、bounded media/classification/size bucket 和 reason class；不携带 filename、
digest（除内部受限投影）、URL、object key、grant、正文或 scanner raw report。

这些是Artifact aggregate的durable internal/integration事件，不属于17的PublicRunEventType。Run只通过owner
Node/Capability/Model的公开事件和授权ArtifactRef观察结果；平台首个合同不提供Artifact全局事件订阅API。

## 24. Persistence 映射

Artifact 领域只映射为 `artifacts`、`artifact_blobs` 与 `artifact_links`：Artifact 保存当前 lifecycle/metadata，Blob 保存
content-addressed backend fact，Link 以 closed kind 表达 reference、grant、hold、provenance 与 operation target。upload、
scan、rescan、delete 与 GC 使用共享 Job；command/use/backend callback 使用 Receipt；历史与安全结果使用 Event；需要人工授权
使用 Task。大正文始终在 object store，数据库只保存 bounded typed metadata。

`WorkClass::Artifact` 的 Job owner 是 closed union：面向调用方的 upload/scan/rescan/delete Operation 由 exact
`ManagementOperation` 拥有；不再对应可继续执行的调用方 Operation、且只负责回收候选 object generation 的内部 cleanup Job
由 exact `InternalBlob` 拥有。两者都必须通过 machine owner-pair registry，禁止 `artifact`、`artifact_blob` 等任意字符串 owner。

所有Artifact Job使用一个closed tagged payload union：`scan | rescan | delete | blob_cleanup`。scan/rescan由exact
ManagementOperation拥有，delete由exact ArtifactDelete ManagementOperation拥有，blob cleanup由exact InternalBlob拥有；每个variant
分别校验owner、object generation、policy/contract/evidence字段，generic Job decoder不得只按JSON形状猜测variant。

物理事实按以下规则唯一归属：Artifact 保存 prepare admission（expected size、optional expected digest、optional declared media）、
verified media、retention、creator 与 exact Blob reference；Blob 单独保存 verified content digest/byte length、object generation、
storage binding 与由classification、exact retention revision、encryption domain组成的closed security-domain digest。Staging
阶段允许未知 digest/media/generation，不能写 sentinel 冒充验证结果；构造 Ready
ArtifactRef 时 join exact tenant-scoped Blob，不在 Artifact 重复保存 Blob digest/size。该修正不增加表，baseline 仍为23张表。
Retention bootstrap FK与closed policy payload进入schema contract version 5；Blob安全域进入version 6。

Model Artifact Producer不增加领域表：预留/Staging/Verified事实仍写上述三张Artifact表与04两个共享Quota bundle，stage幂等写共享
`Receipt`；需要持久化的历史/安全证据只能由Model terminal或既有Artifact cleanup/incident authority写共享`Event/Outbox`，Producer
本身不得写二者。Model terminal事务复用既有Invocation/Job/RunValue/ArtifactLink/Receipt/Event/Outbox。
不得为Producer另建attempt、upload session、transition、evidence、orphan或terminal表，也不得把object store tag/queue消息当current authority。

### 24.1 已撤销 persistence 记录（非规范性）

旧 migration 24～28、Artifact 专用 Operation/Attempt/transition/evidence 表族及其 repository checkpoint 已撤销，
不属于当前 baseline、实现状态或资格证据；详细记录只保留在 Git 历史。

当前目标只承认本节定义的 `artifacts`、`artifact_blobs`、`artifact_links` 与 03 的共享
`Job`、`Task`、`Receipt`、`Event` 聚合。Phase 3 的 prepare、CompleteUpload、Begin/CompleteVerification与
FinalizeAndReference已由closed Rust state machine和caller-owned PostgreSQL transaction实现；fresh fixture覆盖grant/object
generation/size/digest fence、stale CAS、Ready-only ArtifactRef、reference与staging quota settle。ArtifactLink hold/provenance、
tenant内shared Blob dedupe也已按完整security-domain key通过顺序与双事务并发fixture，候选对象以cleanup Job收敛且跨安全域
不复用。hold/provenance/reference release与shared Blob两阶段删除的closed domain/repository transaction已经实现；fresh PostgreSQL 16
fixture覆盖GC grace、exact approval、live link阻塞、same-Blob alias witness、Job fence、exact object generation、backend/absence evidence、
replay及Event/Receipt/Outbox原子闭合。CR-130要求的Artifact Job union、worker audit/current scan evidence、rescan与cleanup
completion也已通过23项domain/worker fixture和fresh PostgreSQL 16 transaction fixture：rescan排队先进入Quarantined，只有exact
WorkerProcessGeneration/Job fence可提交新证据，delete/blob cleanup必须匹配exact object generation、backend receipt与absence evidence。
既有开发期实现已分别证明Model、WASI与microVM closed read authority、exact Model/Sandbox URI SAN、bounded stream、只读repeatable-read authority以及Job/Secret
数据库越权拒绝；但此前把Model与Sandbox组合进同一process-wide runtime/in-flight bulkhead的部署证据已被本次architecture revision取代，
不能证明新目标。当前实施批把它拆为Model与Sandbox两个Broker进程/Deployment/ServiceAccount/DB pool/permit；Model进程只注册一个Model
read RPC，Sandbox进程只注册WASI与microVM两个RPC。双向mTLS/NetworkPolicy、不同DB credential/config/TLS identity、独立饱和和rolling
restart门禁全部通过前，这一新拓扑仍只是target/implementation slice，不登记Gate或Phase完成。Sandbox Controller仍不得持有
object-store/KMS credential或对应直出网络。该切片没有新增表或migration。Artifact-backed output、真实
object-store/KMS负向资格、scanner/GC provider、公开 `/v1` 和对应qualification尚未交付，不能由当前
开发期fixture或旧候选记录推断为当前行为。尤其是本次新增的Model Artifact Producer进程、`StageModelOutput`、restricted DB write
role、独立S3/KMS identity/two-phase permit、expected-version lower-bound+generation authorization、canonical JSON verifier、dedupe/
failure/双quota-bundle closure以及Model terminal Artifact first-winner事务均为
Accepted目标合同但当前不存在交付证据。既有Model Artifact Broker仍只证明read authority/SELECT-only边界；不得扩展其RPC或数据库权限来冒充Producer。

## 25. 可观测性与隐私

```text
artifact_operations_total{operation,outcome,purpose}
artifact_bytes_total{operation,size_bucket}
artifact_state_total{state,purpose}
artifact_scan_duration_seconds{profile,outcome}
artifact_scan_backlog_total{profile}
artifact_grant_total{operation,outcome}
artifact_gc_total{state,outcome}
artifact_integrity_incident_total{class}
```

tenant/Artifact/digest/filename/media detail/object key/owner 不进入 metric label。Trace 记录 operation、state、bytes、
storage binding/validator revision的受控hash、latency和reason class，不记录正文/URL/grant。审计覆盖prepare、read/download、
share/export、quarantine release、hold、delete 和 break-glass。

## 26. 配置与部署

- PostgreSQL 是 metadata/reference/grant/lifecycle 权威；S3-compatible store 是 blob 权威；
- Model Artifact Broker与Sandbox Artifact Broker分别使用独立Deployment、ServiceAccount、restricted DB pool、storage identity和permit；
  Artifact Gateway、Upload Finalizer、Scanner、Transformer、Download Gateway、GC/Reconciler也各自使用独立Deployment/permit；
- Model Artifact Producer使用第三个独立Deployment、ServiceAccount、restricted DB write pool、S3/KMS workload identity、mTLS endpoint、
  two-phase admission permit与transport backlog hard cap，只允许exact Model Worker调用`StageModelOutput`；它与两个只读Broker及Model Worker
  均不得同Pod或共享credential/pool；
- scanner/transformer 使用 14 Sandbox node pool，不在 Gateway/API 解析复杂文件；
- bucket 默认 private、versioning/object-lock/replication 按环境 policy，禁止静态 public website；
- Artifact service 使用最小 bucket/KMS identity，不与 Sandbox/Model/MCP 共享 credential；
- storage backend、bucket/region和encryption/KMS binding由18 CandidateManifest固定为installation-scoped digest，
  不存在tenant ArtifactBackend Entity或公共active head；scanner/retention规则使用immutable Policy Revision；
- rolling deploy 不改变已签发 grant 语义，protocol generation 不兼容时先停止签发并 drain；
- readiness 区分 metadata、upload、scan、download 和 GC，单 scanner backlog 不使 Runtime API 全局 unready。

## 27. 测试矩阵

- direct、streaming、multipart、resume、duplicate complete/finalize 和 grant expiry；
- PUT 成功/DB 失败、DB response 丢失、scanner crash、outbox 丢失、object store timeout；
- digest/size/media mismatch、truncated object、multipart swap、object generation overwrite；
- path traversal、symlink、archive bomb、zip slip、malware、macro、SVG/HTML active content、parser crash；
- cross-tenant ID/digest/object key/grant/cache/dedupe timing isolation；
- finalize/quarantine、read/revoke、reference/GC、legal hold/delete、restore/delete竞态；
- Sandbox/MCP/Context/Model grant audience/port/purpose swap；
- Model/Sandbox Broker endpoint、URI SAN、ServiceAccount、DB credential和config互换，以及单audience饱和/重启；
- Model output claim前slot+weighted bytes/ID/双bundle quota预留失败不会claim/start或调用Provider；少领、claim失败、Inline/取消和crash
  按no-object/candidate cleanup事实释放，不泄漏或提前释放reservation；
- `StageModelOutput` exact `model-worker.artifact-output` URI SAN、client-stream header/chunk/terminal顺序、chunk/total limit，以及read Broker/Producer
  endpoint、ServiceAccount、DB role、S3/KMS identity和permit互换的负向门禁；
- Model output duplicate JSON key、尾随字节、非canonical JCS、schema/evidence/media/classification/digest/length/KMS context/object
  generation漂移全部fail closed；secret/content canary不进入日志、错误、Event或receipt；
- Producer pre-I/O授权后cancel/heartbeat/lease takeover/Worker restart/terminal first-winner的竞态，以及post-I/O fresh fence规则；
- PUT成功/DB失败、Uploaded后crash、Verified receipt响应丢失、Model terminal commit冲突/响应丢失、stale Verified orphan与exact-generation GC；
- preexisting Blob hit、并发candidate new/race winner、candidate cleanup先于/晚于Model terminal、Artifact先删但shared Blob仍有alias、最后alias
  physical refund分别验证两个quota owner与settlement identity，不双退、不泄漏；
- InProgress/Dependency transient、fresh Stale/Deadline、TooLarge/Invalid Rejected、Integrity Failed+Quarantined及Conflict不改existing Receipt
  的每个failure persistence分支均覆盖response loss与claim-generation takeover；Integrity分别从candidate current Staging、Uploaded、
  Verifying进入Quarantined，且都不能绕过incident/rescan guard晋升Ready；
- 同Attempt相同stage digest重放得到同一receipt，不同digest冲突；新Attempt不能adopt旧Attempt Artifact/grant/receipt；
- Producer饱和、DB/S3/KMS timeout与rolling restart不耗尽Model read Broker、Provider stream、Sandbox、API或Scheduler容量；
- derived Artifact classification/provenance/citation 和 source deletion；
- quota reservation leak、staging TTL、abandoned multipart、Artifact logical vs Blob physical quota lifecycle；
- S3 version/object missing/corrupt、KMS revoke/rotation、backup restore reconciliation；
- scan/download saturation时 API/Scheduler/control capacity 不受影响；
- filename、Secret、URL、object key、grant/content canary 不进入 public event/metric/log/error。

## 28. 验收标准

- 业务表无法引用非 Ready、跨 tenant、digest/media/size 不一致的 Artifact；
- Prepare/upload/verify/finalize 在每个崩溃窗口都不会产生可读半成品；
- 同一 mutation/finalize 并发只建立一个逻辑 Artifact/Reference；
- ArtifactRef 无 URL/key/path/grant，持有 Ref 不能绕过授权读取；
- S3 object 永不 public，direct grant 短期且 exact operation/object/audience；
- 跨 tenant 相同 digest 不 dedupe、不共享 scan/cache/preview 并不泄露存在性；
- scanner 未通过、失败或超时的内容不能进入 Ready；
- scan/rescan/delete/cleanup全部由exact Artifact Job lease fence提交，stale worker不能覆盖current evidence或对象generation；
- rescan排队即撤销普通可读状态；通过后才恢复Ready，失败/超时保持Quarantined；
- Quarantine/revoke 可以阻止已有 Artifact 新读取且不破坏审计 Reference；
- Reference/retention/hold/grant 任一存在时 GC 无法删除；
- Sandbox 输出只能写 staging，不能自置 Ready/classification/object key；
- Model与Sandbox Artifact Broker不能共享进程、Pod、ServiceAccount、DB pool或permit，任一audience饱和不阻断另一方；
- Model Artifact Producer只接受exact Model Worker的`StageModelOutput`并最多推进到Verified；只读Broker保持SELECT-only且不能注册写RPC；
- 每个Model Attempt在Provider dispatch前已有exact output IDs、Artifact+candidate Blob两个quota bundle与本地materialization slot+weighted
  bytes；预留失败时Provider调用数为零；
- Model Artifact只有在单一terminal PostgreSQL事务同时完成Ready、Output Link、RunValue、Model/Job first-winner、quota与outbox后才可读；
- stale/cancel/timeout/commit失败的Model Staging/Uploaded/Verifying/Verified对象无业务Reference且只能进入bounded orphan GC；
- derived content 产生新 Artifact/Provenance，不覆盖 source；
- object missing/corrupt 被检测、隔离并有恢复/incident runbook；
- 大文件 streaming 不形成无界内存/DB/event，饱和不拖垮控制面。

## 29. 明确推迟的工作

- 公共匿名分享和 CDN public origin；
- 跨 tenant/region blob dedupe；
- active-active multi-region metadata/object writes；
- POSIX mount、可变文件、在线协作编辑和 append；
- 用户自定义 KMS key 与 storage bucket；
- 任意 client-side encrypted opaque file 的内容检索；
- 通用数字签名/电子证据产品；
- 对所有格式进行语义级无害化重写。

## 30. 未决问题

没有阻止 API 或 Qualification 设计的未决问题。具体 S3-compatible 产品、scanner 和 transformer 可替换，但
PostgreSQL lifecycle/reference 权威、tenant 隔离、prepare/finalize、Ready-only 引用和两阶段 GC 不得弱化。
