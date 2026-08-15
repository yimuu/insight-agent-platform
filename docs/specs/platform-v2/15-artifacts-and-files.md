# Platform v2 Artifact 与 File 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
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
    blob_id: InternalBlobId,
    encryption_domain_id: EncryptionDomainId,
}
```

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
    content_digest: Digest,
    byte_length: u64,
    storage_binding_digest: Digest,
    opaque_object_key: EncryptedObjectKey,
    object_generation: ObjectGeneration,
    encryption_domain_id: EncryptionDomainId,
    integrity_state: BlobIntegrityState,
}
```

规则：

- digest 使用 02 定义的 `sha256:<lowercase-hex>`，由平台验证，不仅信任客户端 header；
- object key 由服务生成，使用 tenant-keyed opaque partition，不直接包含公开 tenant ID、filename 或裸 digest；
- 同一 tenant、encryption/classification/retention domain 内可以复用 verified Blob；
- 跨 tenant 永不 dedupe；同 digest 也产生独立 blob/encryption/object identity；
- dedupe lookup 在完成授权和 quota reservation 后进行，响应时序/错误不泄露其他内容是否存在；
- Artifact 与 Blob 分离：不同 purpose/retention/provenance 的 Artifact 可以在允许域内引用同一 Blob；
- Object version/generation 固定，禁止 overwrite existing key；
- object lock/versioning 是防御层，PostgreSQL 仍是 Artifact lifecycle 权威。
- `storage_binding_digest`引用18 CandidateManifest中的installation-scoped storage/region/KMS binding，不是tenant
  Revision或可运行时选择的backend ID；

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
Staging -> Uploaded | Rejected | Deleting
Uploaded -> Verifying | Rejected | Deleting
Verifying -> Verified | Quarantined | Rejected
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

`CompleteUpload`只提交Uploaded事实。随后durable scheduler command把Uploaded原子推进Verifying并创建
`WorkClass::Artifact` scan Job；Job payload冻结exact Artifact/Blob/object generation、scan Policy、scanner contract与ruleset。
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

### 15.2 Context

Context ingest 只接受 Ready Artifact，Dataset Generation 建立 durable Reference。ContextItem/Citation 记录使用的
content digest/locator；提取文本是 derived Artifact，不覆盖原件。

### 15.3 MCP

MCP embedded resource/resource link 经过 size/media/URI/auth policy后 ingest 或 transient observation。MCP server 不能
得到 tenant bucket或任意 Artifact 枚举；只得到当前 published Tool/Resource port 的 grant。

### 15.4 Sandbox

Sandbox input/output 使用 14 的 per-Job grant，并只经Sandbox Artifact Broker物化。Guest 只能读声明 input、写 staging output；不能指定 Artifact ID、
object key、classification 或 Ready 状态。`artifact_links`是撤销的唯一durable fact：Broker在Sandbox销毁证据形成前按exact
Job/attempt/Worker generation/lease幂等推进`active -> released`，Job terminal事务释放遗漏项并核对request冻结的完整grant集合。
重复撤销不得形成第二状态或阻止terminal；未 finalize output 进入 staging GC。

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
 -> Ready/Rejected/Quarantined/Corrupt -> Deleting
 -> lock all same-Blob Artifact aliases
 -> other live alias: preserve Blob + exact alias witness
 -> no other live alias: delete exact object generation/version + verify absence/deletion marker
 -> Deleted + exact Attempt-bound deletion receipt + audit/outbox
```

- 不使用最终一致的 cached refcount 单独决定删除；
- `artifact_references` 是权威，引用数量可以是加速 projection；
- staging/abandoned multipart 使用更短独立 TTL，但仍验证 active grant/generation；
- Blob 只有在没有其他未删除Artifact alias时才物理删除；每个Artifact自己的Reference、Retention与Hold资格仍独立判定；
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
- reservation 有 deadline，不能无限占额度；
- dedupe 不允许绕过 logical byte/Artifact count quota；
- Artifact Gateway、scanner、transformer、GC、download 使用独立队列、permit、连接池和 autoscaling；
- scan backlog 超过安全门槛时拒绝/延迟新高风险 upload，不把未扫描内容当 Ready；
- large transfer 使用 streaming backpressure，不在内存缓冲全文件；
- control/quarantine/revoke/delete 使用保留 capacity；
- Artifact/S3 饱和不能耗尽 API/Scheduler/Model/MCP/Sandbox control DB pool；Sandbox Artifact Broker的permit/DB pool耗尽也不能
  占用Model Artifact Broker的permit/DB pool，反向同理。

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
```

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
开发期fixture或旧候选记录推断为当前行为。

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
- derived Artifact classification/provenance/citation 和 source deletion；
- quota reservation leak、staging TTL、abandoned multipart、logical vs physical quota；
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
