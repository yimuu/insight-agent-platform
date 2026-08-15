# Platform v2 Artifact 与 File 规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md)、[`12-context-and-retrieval.md`](12-context-and-retrieval.md) |
| 直接下游 | 14、16、17、18 |

> Persistence ruling：Artifact 只保留 Artifact、Blob 与 Link 三类领域事实；upload/scan/rescan/delete/GC 使用共享 Job、
> Receipt、Event 与 Task。历史专用 transition/evidence/operation 持久化族已废止；物理映射只由ADR拥有。

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
    declared_media_type: Option<MediaType>,
    blob_id: Option<InternalBlobId>,
    verified_media_type: Option<MediaType>,
    content_evidence: Option<AcceptedArtifactContentEvidenceV1>,
    retention_policy_revision_id: ResourceVersionId,
    retain_until: DateTime<Utc>,
    created_by: PrincipalId,
    created_at: DateTime<Utc>,
    projection_version: u64,
}
```

`blob_id`、verified media与current content evidence是不同阶段的事实：Staging Artifact可在PUT前绑定`Some(blob_id)`，但media/evidence仍为空；
未开始物化时三者都为空。`Verified | Ready`必须同时具有exact Blob ref、verified media和§7 closed tagged content evidence；Blob是verified
content digest/byte length、object generation与encryption domain的唯一owner，Artifact不重复保存这些事实。`expected_digest`只是不受信upload admission
约束，即使最终相等也不能成为verified fact。所有非法state/field组合及sentinel都拒绝。

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
metadata 中不保存 Secret、raw filename path、prompt 或业务正文。`verified_media_type/content_evidence`在
`Staging | Uploaded | Verifying | Rejected | Quarantined | Deleting | Deleted`必须为空，进入`Verified | Ready`必须非空；离开这两个状态时先把
需要保留的历史证据写入bounded Event再清空current字段，同一CAS推进`projection_version`。授权owner可读取所有未删除状态的
safe `ArtifactSnapshot`，但只有Ready snapshot通过exact Blob join投影不可空`ArtifactRef`和current verified media/evidence；其他状态只公开
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
    kms_binding_digest: Digest,
    encryption_domain_id: EncryptionDomainId,
    encryption_domain_generation: u64,
    encryption_domain_binding_digest: Digest,
    key_id: KmsKeyId,
    security_domain_digest: Digest,
    object_reference_ciphertext: SecretBytes,
    object_reference_ciphertext_digest: Digest,
    object_generation: Option<ObjectGeneration>,
    current_job_id: Option<JobId>, // only the exact InternalBlob-owned cleanup Job
    integrity_state: BlobIntegrityState,
    verified_at: Option<DateTime<Utc>>,
}

struct ObjectGeneration(String); // ogv1_<base64url-no-pad of exact backend generation bytes>

struct ObjectGenerationDigestPreimageV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    blob_id: InternalBlobId,
    backend: StorageBackend,
    storage_binding_digest: Digest,
    object_reference_ciphertext_digest: Digest,
    object_generation: ObjectGeneration,
}
```

Staging Blob在唯一opaque locator封印后即可存在，因此`content_digest`、`byte_length`、`object_generation`和`verified_at`在
`Staging`可以为空；Uploaded/Verifying按已观察事实逐步填充。进入`Verified`后四者必须全部非空并与exact object generation及HEAD evidence
一致；引用它的`Verified | Ready` Artifact只保存exact Blob ref、verified media和current content evidence，digest/length仍从Blob解析。数据库CHECK与
domain validator共同强制该closed state/field invariant。不得用空digest、零长度
sentinel或虚构generation绕过未知状态；真实零字节对象仍以`Some(0)`表达并受purpose/content policy决定是否合法。
`current_job_id`只允许指向03 mapping中owner为本Blob的exact cleanup Job；Job创建、terminal等待merge及清除/替换遵守03同事务pointer规则，
scan/rescan/delete或其他Job不得写入该字段。

`ObjectGeneration`是15唯一nominal：adapter把backend返回的exact non-empty generation bytes（首版S3 VersionId的UTF-8 bytes）编码为无padding
base64url并加`ogv1_`前缀；decoded bytes为1～512 bytes，非canonical/空/oversize、padding、别名编码或客户端提交值都拒绝。跨进程使用的
`object_generation_digest`唯一计算式为
`SHA-256(UTF8("insight.artifact.object-generation.v1") || 0x00 || JCS(ObjectGenerationDigestPreimageV1))`，并按02 `Digest` wire编码。
preimage使用actual same-tenant Blob、process-installed storage binding、从sealed locator actual bytes重算的ciphertext digest及exact generation；
不得只hash backend generation、公开Artifact ID、调用方摘要或解封后的object key。`object_generation=None`时不存在preimage且digest也必须缺失；
任何要求generation的Job/Receipt/projection都必须携带并重算同一digest。16 Model Producer与candidate cleanup只引用本公式，不得拥有第二种编码或domain separator。

规则：

- digest 使用 02 定义的 `sha256:<lowercase-hex>`，由平台验证，不仅信任客户端 header；
- object key 由服务生成，使用 tenant-keyed opaque partition，不直接包含公开 tenant ID、filename 或裸 digest；
- 同一 tenant、encryption/classification/retention domain 内可以复用 verified Blob；
- 跨 tenant 永不 dedupe；同 digest 也产生独立 blob/encryption/object identity；
- dedupe lookup 在完成授权和 quota reservation 后进行，响应时序/错误不泄露其他内容是否存在；
- Artifact 与 Blob 分离：不同 purpose/retention/provenance 的 Artifact 可以在允许域内引用同一 Blob；
- Object version/generation 固定，禁止 overwrite existing key；
- object lock/versioning 是防御层，PostgreSQL 仍是 Artifact lifecycle 权威。
- storage/KMS/encryption-domain generation/binding digest与key ID都在首次封印locator前从04 current Active binding冻结；Rebind/Revoke不改写
  existing Blob，后续新读取由current fence拒绝；`security_domain_digest`覆盖tenant、classification、Retention、storage/KMS、domain generation/
  binding与key identity，排除自身及secret ciphertext；
- `storage_binding_digest`引用本规范的installation-scoped storage/region/KMS binding，不是tenant
  Revision或可运行时选择的backend ID；本规范是该catalog与1～64 hard max的owner，Model output是否Inline-only
  不影响Package、request Artifact及其他Artifact路径对该catalog的需求；

### 6.1 Installation storage binding机器合同

```rust
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

struct ResolvedArtifactStorageBindingV1 {
    manifest: ArtifactStorageBindingManifestV1,
    manifest_digest: Digest,
}

struct StorageBindingTimingLimitsV1 {
    effective_artifact_staging_seconds: u64,
    artifact_io_staging_grace_seconds: u64,
}

struct ValidatedStorageBindingTimingV1 {
    required_write_quiescence_seconds: u64,
}
```

schema路径固定为`contracts/platform-v1/schemas/artifact-storage-binding-manifest.schema.json`，所有object closed。
`StorageBackend::S3`、`S3AddressingMode`、`ObjectWriteMode`与`ExactKeyObservationContract`的wire分别exact为`s3`、
`virtual_hosted | path_style`、`conditional_create_versioned`与`strong_after_write_quiescence`。`region`只复用02
`CanonicalRegion` nominal/common schema，15不再拥有第二个region validator。uncertainty必须为正且不超过
`9007199254740991`，request timeout与maximum object bytes也必须为正，后者不超过JSON safe integer；所有digest必须使用02 exact wire。
endpoint/bucket/KMS字段引用Candidate安装的exact private endpoint、opaque bucket/prefix与workload-identity/KMS binding，不携带hostname、
bucket name、access key或Secret正文；runtime按digest解析后仍须逐字段复验region/addressing/timeout/byte limit。

`MAX_INSTALLATION_ARTIFACT_STORAGE_BINDINGS=64`由本规范唯一拥有。每个生产Candidate必须安装1～64份canonical manifest；digest按raw
bytes严格升序且唯一。未被某一时刻动态Deployment引用的binding不是orphan，因为同一catalog还服务Package、request Artifact及其他
Artifact路径。

`ResolvedArtifactStorageBindingV1`必须重算完整manifest canonical digest并逐值相等，不能把调用方提供的digest和另一份manifest拼接。
纯`ArtifactStorageBindingManifestV1::validate_timing(StorageBindingTimingLimitsV1)`返回上述validated value，并使用checked arithmetic计算
`required_write_quiescence_seconds = ceil(uncertainty_milliseconds / 1000) + 1`；实现按`ms / 1000 + (ms % 1000 != 0)`再checked add margin，
任何add或换算溢出都拒绝。两个input必须为正且不超过JSON safe integer；installation effective
`artifact.staging_seconds`必须严格大于结果，引用该binding的ArtifactIo Policy `staging_grace_seconds`必须大于等于结果且严格小于
effective staging。后一条是可满足性约束：Attempt deadline严格晚于admission DB time，若grace大于等于整个staging window，则任何请求都
不可能通过04的`deadline + grace`上界。

Candidate builder只以同一算法验证binding uncertainty与installation effective staging的第一条关系；它不得读取动态Model Deployment或
tenant Policy catalog。16 compatibility port在创建/激活/Release scan/Run admission时对每个exact ArtifactIo Policy调用完整validator。
04 admission timing helper必须复用本规范的返回值，不得复制第二份ceil/margin语义。所有conditional PUT都
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

Artifact metadata的current security projection使用一个closed tagged evidence，不允许把Worker或调用方提供的opaque digest冒充scanner/
Producer evidence：

```rust
#[serde(tag = "validator", rename_all = "snake_case", deny_unknown_fields)]
enum AcceptedArtifactContentEvidenceV1 {
    GenericScan {
        schema_version: u32, // const 1
        scan_policy_revision: ExactVersionRef,
        scanner_contract_digest: Digest,
        ruleset_digest: Digest,
        object_generation: ObjectGeneration,
        evidence_digest: Digest,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    },
    ModelOutputProducer {
        schema_version: u32, // const 1
        content_validation_profile_digest: Digest,
        producer_runtime_manifest_digest: Digest,
        artifact_io_rules_digest: Digest,
        storage_binding_manifest_digest: Digest,
        encryption_domain_binding_digest: Digest,
        object_generation: ObjectGeneration,
        evidence_digest: Digest,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    },
}
```

两种variant都只表示accepted result；拒绝/失败进入Artifact state和Receipt，不能伪造accepted evidence。`evidence_digest`由实际执行validator
的受信组件对exact Artifact/Blob identity、object generation、verified digest/length/media/classification、除`evidence_digest`自身外的variant
全部字段、accepted result和时间做closed canonical digest，不由上传者提供。它与Artifact version一起CAS，是当前可读性/证据新鲜度的唯一current authority；generic
完整scanner report仍保存为受限Artifact，Event只保存bounded摘要。
`ArtifactRecord.content_evidence`是唯一current evidence落点，并与`projection_version`在同一Artifact CAS内变化；`Verified | Ready`恰有一个
`Some`，其他状态只能是`None`。evidence preimage中的verified digest/length、object generation及encryption domain只从同tenant exact Blob读取，
不能从Artifact或请求中的expected fields复制；read projection也用同一次Blob join取得这些事实。过期不会静默替换evidence：新read fail closed，
rescan排队把Ready推进Quarantined并清空current evidence，只有新validator成功才能写新evidence并恢复Ready。

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

对于公共独立上传，prepare事务同时创建kind exact为`ArtifactVerify`的durable ManagementOperation作为初始owner、Staging Artifact和UploadGrant；
Operation此时Queued且`current_job_id=None`，其binding冻结Artifact/Policy/预分配scan Job，complete前不创建Job。abandon/reject/deadline由同一
Operation authority终结并清理；verify成功后该Operation finalize/reference并返回ArtifactRef，之后Agent Run/Revision创建新Reference，不复制正文。
对于Capability/Sandbox输出，owner transaction是Invocation output commit。首版不存在UploadObject、WorkspaceAttachment
或其他未注册owner kind。

Model output是同一状态机的受限内部路径：claim/start预留Staging intent与全部identity，独立Model Artifact Producer只推进到
Verified，随后由Model terminal owner transaction执行Ready/reference/RunValue/terminal commit。该路径不开放公共prepare/finalize，
也不允许Producer成为第二个Model current-state authority。

## 9. Upload 协议

```rust
struct ArtifactGatewayAddress(String); // canonical https origin, no path/query/fragment

struct PrepareArtifactRequestV1 {
    schema_version: u32, // const 1
    purpose: ArtifactPurpose,
    expected_size: BoundedSize,
    expected_digest: Option<Digest>,
    declared_media_type: Option<MediaType>,
    classification: DataClassification,
    retention_policy_revision_id: ResourceVersionId,
}

struct UploadGrantViewV1 {
    schema_version: u32, // const 1
    artifact_grant_id: ArtifactGrantId,
    artifact_id: ArtifactId,
    gateway_address: ArtifactGatewayAddress,
    opaque_upload_token: SecretBytes,
    max_bytes: u64,
    expires_at: DateTime<Utc>,
    generation: u64,
}

struct ArtifactPreparedViewV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    projection_version: u64,
    artifact_etag: Etag,
    state: ArtifactState, // exact Staging
    purpose: ArtifactPurpose,
    expected_size: BoundedSize,
    expected_digest: Option<Digest>,
    declared_media_type: Option<MediaType>,
    classification: DataClassification,
    retention_policy_revision_id: ResourceVersionId,
}

struct ArtifactEtagPreimageV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    projection_version: u64,
    state: ArtifactState,
    classification: DataClassification,
    purpose: ArtifactPurpose,
    expected_size: BoundedSize,
    expected_digest: Option<Digest>,
    declared_media_type: Option<MediaType>,
    verified_media_type: Option<MediaType>,
    content_evidence_digest: Option<Digest>,
    retention_policy_revision_id: ResourceVersionId,
    retain_until: DateTime<Utc>,
}

enum ArtifactOperationKindV1 {
    Verify,
    Rescan,
    Delete,
}

struct ArtifactOperationReferenceV1 {
    schema_version: u32, // const 1
    operation_id: OperationId,
    kind: ArtifactOperationKindV1,
    artifact_id: ArtifactId,
}

enum ArtifactCommandProblemCodeV1 {
    InvalidRequest,
    PermissionDenied,
    ResourceNotFound,
    EtagMismatch,
    IdempotencyConflict,
    InvalidStateTransition,
    PolicyDenied,
    QuotaExceeded,
    RateLimited,
    ContentRejected,
    DeadlineExceeded,
    TemporarilyUnavailable,
    InternalError,
}

struct ArtifactCommandProblemV1 {
    schema_version: u32, // const 1
    status: u16,
    code: ArtifactCommandProblemCodeV1,
    retryable: bool,
    retry_after_milliseconds: Option<u64>,
    current_artifact_etag: Option<Etag>,
}

struct ArtifactUploadGrantReplayProjectionV1 {
    schema_version: u32, // const 1
    artifact_grant_id: ArtifactGrantId,
    artifact_id: ArtifactId,
    gateway_address: ArtifactGatewayAddress,
    claims: OpaqueArtifactGrantTokenClaimsV1,
    token_digest: Digest,
    max_bytes: u64,
    expires_at: DateTime<Utc>,
    generation: u64,
}

struct PrepareArtifactResponsePreimageV1 {
    schema_version: u32, // const 1
    artifact: ArtifactPreparedViewV1,
    upload_grant: ArtifactUploadGrantReplayProjectionV1,
    verify_operation: ArtifactOperationReferenceV1,
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactPrepareTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        response: PrepareArtifactResponsePreimageV1,
    },
    Rejected {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
    Failed {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
}

struct CompleteArtifactUploadRequestV1 {
    schema_version: u32, // const 1; no optional fields
}

struct CompleteArtifactUploadReceiptRequestV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    if_match: Etag,
    body: CompleteArtifactUploadRequestV1,
}

struct CompleteArtifactUploadAcceptedV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    uploaded_artifact_version: u64,
    artifact_etag: Etag,
    verify_operation: ArtifactOperationReferenceV1,
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactCompleteUploadTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        response: CompleteArtifactUploadAcceptedV1,
    },
    Rejected {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
    Failed {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
}

#[serde(tag = "download_scope", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactDownloadScopeV1 {
    Whole { schema_version: u32 }, // const 1
    Range { schema_version: u32, range: BoundedRange },
}

struct IssueArtifactDownloadRequestV1 {
    schema_version: u32, // const 1
    scope: ArtifactDownloadScopeV1,
}

struct IssueArtifactDownloadReceiptRequestV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    body: IssueArtifactDownloadRequestV1,
}

struct ArtifactDownloadGrantReplayProjectionV1 {
    schema_version: u32, // const 1
    artifact_grant_id: ArtifactGrantId,
    artifact_id: ArtifactId,
    gateway_address: ArtifactGatewayAddress,
    claims: OpaqueArtifactGrantTokenClaimsV1,
    token_digest: Digest,
    scope: ArtifactDownloadScopeV1,
    expires_at: DateTime<Utc>,
    generation: u64,
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactIssueDownloadTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        grant: ArtifactDownloadGrantReplayProjectionV1,
    },
    Rejected {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
    Failed {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
}

struct ArtifactRescanRequestV1 {
    schema_version: u32, // const 1; no optional fields
}

struct ArtifactRescanReceiptRequestV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    if_match: Etag,
    body: ArtifactRescanRequestV1,
}

struct DeleteArtifactRequestV1 {
    schema_version: u32, // const 1; no optional fields
}

struct DeleteArtifactReceiptRequestV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    if_match: Etag,
    body: DeleteArtifactRequestV1,
}

struct ArtifactAsyncMutationAcceptedV1 {
    schema_version: u32, // const 1
    artifact_id: ArtifactId,
    artifact_projection_version: u64,
    artifact_etag: Etag,
    operation: ArtifactOperationReferenceV1,
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactRescanTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        response: ArtifactAsyncMutationAcceptedV1,
    },
    Rejected {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
    Failed {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactDeleteTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        response: ArtifactAsyncMutationAcceptedV1,
    },
    Rejected {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
    Failed {
        schema_version: u32, // const 1
        problem: ArtifactCommandProblemV1,
    },
}
```

Artifact strong ETag唯一计算为02 `Etag("etg1_" + BASE64URL_NO_PAD(SHA-256(UTF8("insight.artifact.etag.v1") || 0x00 ||
JCS(ArtifactEtagPreimageV1))))`。preimage逐值复制同一locked Artifact row的完整safe current projection；evidence存在时只复制其closed
`evidence_digest`，并排除ETag自身、Secret、Grant token、Blob ID/object locator与backend metadata。`projection_version`为唯一Artifact optimistic version
且为正；`staging_artifact_version`/`uploaded_artifact_version`等字段只是某次transition捕获的该值，不是第二column/authority。prepare/complete/rescan/delete
winner把用于response/Receipt的ETag与aggregate mutation在同一事务计算并保存到terminal result；replay不得按later state重算。

`PrepareArtifactRequestV1`是唯一public body；Idempotency-Key只在header，authenticated principal/owner binding只来自server context，二者都不得
复制进body。平台为其创建上述ArtifactVerify ManagementOperation owner。任何客户端提交owner type/ID、Run ID、Revision ID、idempotency key或object key
覆盖字段都按unknown/forbidden field拒绝。`ArtifactGatewayAddress`必须是Candidate安装的exact canonical HTTPS origin：1～2048 ASCII bytes、无userinfo、
query、fragment或path（除空/`/`），host/port按URI canonical规则唯一编码；不能存object-store endpoint。`UploadGrantViewV1`只是§12
`StagingWrite` grant的一次性public projection；
其subject/delivery必须是`Principal + OpaqueBearer`。`opaque_upload_token`只在成功prepare/replay的`no-store`响应返回，不进入持久化、日志或
Event，authority仍是§12唯一`ArtifactGrant`。

五个public command的Receipt保存上列closed response preimage而不是Secret token bytes。prepare/download replay只从result内claims、token key version、
token digest及safe projection，以§12 deterministic seal重建byte-identical token；不得读取current Grant/Artifact/Operation或当前gateway config。
HTTP adapter只可从terminal result和固定`/v1` route grammar确定status、body、Location、ETag与problem title/type，并为本次HTTP exchange另加新的
`Request-Id`；该correlation header不属于“same logical response”。`Succeeded | Rejected | Failed` result variant必须与03 ReceiptState逐值对应，
problem status/code组合必须存在17唯一error registry，unknown/null/free text均拒绝。`ArtifactOperationKindV1` wire exact为
`verify | rescan | delete`并分别映射17 `ArtifactVerify | ArtifactRescan | ArtifactDelete`，不得直接依赖17 transport enum。
`ArtifactCommandProblemCodeV1` wire使用上列variant的snake_case，并只允许逐值映射17同名`ApiProblemCode`；15不导入HTTP transport type，17也不得
把另一个ApiProblem code映射成同一Artifact result。

03 Receipt registry的五个public Artifact entry固定如下；schema ID与path逐值匹配且所有request均为`CompleteAtClaim`，canonical request/result
maximum分别为16384/65536 bytes：

| ClosedOperation | request schema ID / exact path | result schema ID / exact path |
|---|---|---|
| `artifact.upload.prepare.v1` | `artifact.upload.prepare.request.v1` / `contracts/platform-v1/schemas/artifacts/artifact-upload-prepare-request.schema.json` (`PrepareArtifactRequestV1`) | `artifact.upload.prepare.result.v1` / `contracts/platform-v1/schemas/artifacts/artifact-upload-prepare-result.schema.json` (`ArtifactPrepareTerminalResultV1`) |
| `artifact.upload.complete.v1` | `artifact.upload.complete.request.v1` / `contracts/platform-v1/schemas/artifacts/artifact-upload-complete-request.schema.json` (`CompleteArtifactUploadReceiptRequestV1`) | `artifact.upload.complete.result.v1` / `contracts/platform-v1/schemas/artifacts/artifact-upload-complete-result.schema.json` (`ArtifactCompleteUploadTerminalResultV1`) |
| `artifact.download_grant.issue.v1` | `artifact.download_grant.issue.request.v1` / `contracts/platform-v1/schemas/artifacts/artifact-download-grant-issue-request.schema.json` (`IssueArtifactDownloadReceiptRequestV1`) | `artifact.download_grant.issue.result.v1` / `contracts/platform-v1/schemas/artifacts/artifact-download-grant-issue-result.schema.json` (`ArtifactIssueDownloadTerminalResultV1`) |
| `artifact.rescan.v1` | `artifact.rescan.request.v1` / `contracts/platform-v1/schemas/artifacts/artifact-rescan-request.schema.json` (`ArtifactRescanReceiptRequestV1`) | `artifact.rescan.result.v1` / `contracts/platform-v1/schemas/artifacts/artifact-rescan-result.schema.json` (`ArtifactRescanTerminalResultV1`) |
| `artifact.delete.v1` | `artifact.delete.request.v1` / `contracts/platform-v1/schemas/artifacts/artifact-delete-request.schema.json` (`DeleteArtifactReceiptRequestV1`) | `artifact.delete.result.v1` / `contracts/platform-v1/schemas/artifacts/artifact-delete-result.schema.json` (`ArtifactDeleteTerminalResultV1`) |

五项`receipt_kind=Command`、`authority_scope_kind=Tenant`；dedupe owner统一为04完整
`security.principal-snapshot.v1`/version 1/`contracts/platform-v1/schemas/security/principal-snapshot.schema.json`/32768-byte maximum。
scope aggregate分别为新prepare command scope或path Artifact typed ref，且必须与snapshot tenant一致。request/result schema version均exact 1，
不得用Principal ID、session、Grant或Operation作为另一dedupe owner。

上述路径及03 operations registry当前均为CR-165目标、尚未checked in；缺任一entry/schema、operation/type错配、max漂移或public adapter没有逐值消费
registered result都使Candidate/server启动失败，不能回退到generic JSON receipt。

- 对外只有一个Artifact Gateway hostname/HTTP合同；upload route只进入独立Artifact Upload Gateway Deployment，download route只进入独立
  Artifact Download Gateway Deployment。两者不共享进程、ServiceAccount、数据库pool、storage/KMS identity、permit或HPA；public upload也只经
  Gateway有界流式代理，不返回presigned/object-store URL、redirect或storage credential；
- multipart part number/count/size、并发、总时长和 abandoned upload 有硬上限；
- Gateway 不接受客户端 bucket/key、ACL、SSE key、public flag 或 storage class；
- expected digest 缺失时允许上传，但 Verify 必须计算；有值时不匹配直接 Rejected；
- streaming upload 在超过 quota/size 时立即终止，不先落完整超限文件；
- resume 绑定同一 grant generation、principal/workload 和 staging object；
- browser filename 和 Content-Type 只作为 hint/display，不能决定 verified media。

普通internal producer不调用public Upload Gateway，也不接收OpaqueBearer。Registry、Capability、Context、MCP或Sandbox owner的admission只冻结
逻辑output slot、port/purpose、classification、media policy、byte/digest ceiling与retention约束；每次`NewPhysicalAttempt`的start事务取得exact
Attempt/lease/Worker fence后，才为该attempt原子创建一组全新的Staging Artifact intent、candidate Blob、
`JobAttempt + WorkloadBound + StagingWrite` Grant，预分配exact `ArtifactVerify` Operation、scan Job与stage Receipt identity，并把Operation binding
preimage嵌入完整`ArtifactStageAttemptBindingV1`。Registry/Capability/Context/MCP Job把它直接作为03唯一`current_attempt_snapshot`；Sandbox Job
则按14把它嵌入`SandboxJobAttemptBindingV1::CapabilityOutput`后保存该outer snapshot。此时不创建ManagementOperation或scan Job row。
该snapshot与Grant只冻结稳定attempt identity/digest，不保存本次lease或Worker fence；具体stage Header另携current fence。`ResumePhysicalAttempt`只能复用
同一组identity；retry/lost lease产生的新物理attempt不得adopt旧attempt的Artifact、Blob、Grant、Operation、scan Job或Receipt，旧组按本节failure
matrix进入bounded cleanup。随后调用方以17
对应exact method流式调用Artifact Workload Producer。Producer重验subject/delivery/audience、owner、JobAttempt、port/purpose、byte/digest/multipart/
deadline与current encryption fence，最多提交`Staging -> Uploaded`及其Receipt/scan Job/Event；Verified/Ready、Reference、业务result与terminal仍由
scanner和owner唯一事务完成。public Principal upload、ordinary workload staging与Model output staging三条写路径不可互换。

### 9.1 Internal Workload Stage 机器合同

```rust
enum ArtifactWorkloadStageKindV1 {
    RegistryArtifact,
    CapabilityOutput,
    ContextOutput,
    McpOutput,
    SandboxOutput,
}

struct ArtifactVerifyOperationBindingV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    operation_id: OperationId,
    artifact_id: ArtifactId,
    staging_artifact_version: u64,
    candidate_blob_id: InternalBlobId,
    scan_job_id: JobId,
    scan_policy_revision_id: ResourceVersionId,
    scan_policy_digest: Digest,
    scanner_contract_digest: Digest,
    ruleset_digest: Digest,
    maximum_scan_bytes: u64,
    deadline: DateTime<Utc>,
}

struct ArtifactVerifyJobBindingV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    scan_job_id: JobId,
    operation_id: OperationId,
    operation_binding_digest: Digest,
    artifact_id: ArtifactId,
    uploaded_artifact_version: u64,
    blob_id: InternalBlobId,
    object_generation: ObjectGeneration,
    object_generation_digest: Digest,
    uploaded_body_length: u64,
    uploaded_body_digest: Digest,
    scan_policy_revision_id: ResourceVersionId,
    scan_policy_digest: Digest,
    scanner_contract_digest: Digest,
    ruleset_digest: Digest,
    maximum_scan_bytes: u64,
    deadline: DateTime<Utc>,
}

struct ArtifactStageAttemptBindingV1 {
    schema_version: u32, // const 1
    stage_kind: ArtifactWorkloadStageKindV1,
    tenant_id: TenantId,
    workload_role_identity_digest: Digest,
    owner: ArtifactOwner,
    job_owner: TypedOwnerRef,
    job_kind: JobKind,
    work_class: WorkClass,
    job_id: JobId,
    attempt_no: u32,
    artifact_id: ArtifactId,
    staging_artifact_version: u64,
    candidate_blob_id: InternalBlobId,
    scan_operation_binding: ArtifactVerifyOperationBindingV1,
    scan_operation_binding_digest: Digest,
    artifact_grant_id: ArtifactGrantId,
    initial_grant_generation: u64, // exact 1
    exact_staging_identity_digest: Digest,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    classification: DataClassification,
    media_policy_digest: Digest,
    maximum_bytes: u64,
    expected_digest: Option<Digest>,
    multipart_contract_digest: Digest,
    encryption_domain_fence_digest: Digest,
    stage_receipt_id: ReceiptId,
    deadline: DateTime<Utc>,
}

struct StageWorkloadArtifactRequestCoreV1 {
    schema_version: u32, // const 1
    stage_kind: ArtifactWorkloadStageKindV1,
    tenant_id: TenantId,
    workload_role_identity_digest: Digest,
    owner: ArtifactOwner,
    job_owner: TypedOwnerRef,
    job_kind: JobKind,
    work_class: WorkClass,
    job_id: JobId,
    job_version: u64,
    attempt_no: u32,
    source_attempt_binding_digest: Digest,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    artifact_id: ArtifactId,
    staging_artifact_version: u64,
    candidate_blob_id: InternalBlobId,
    scan_operation_binding: ArtifactVerifyOperationBindingV1,
    scan_operation_binding_digest: Digest,
    artifact_grant_id: ArtifactGrantId,
    grant_generation: u64,
    grant_authorization_binding_digest: Digest,
    grant_request_binding_digest: Digest,
    exact_staging_identity_digest: Digest,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    classification: DataClassification,
    media_policy_digest: Digest,
    declared_media_type: MediaType,
    maximum_bytes: u64,
    expected_digest: Option<Digest>,
    multipart_contract_digest: Digest,
    encryption_domain_fence_digest: Digest,
    stage_receipt_id: ReceiptId,
    deadline: DateTime<Utc>,
}

struct StageWorkloadArtifactDedupeOwnerV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    job_id: JobId,
    attempt_no: u32,
    source_attempt_binding_digest: Digest,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    stage_receipt_id: ReceiptId,
}

struct StageWorkloadArtifactHeaderV1 {
    schema_version: u32, // const 1
    core: StageWorkloadArtifactRequestCoreV1,
    stage_request_core_digest: Digest,
}

#[serde(tag = "frame_kind", rename_all = "snake_case", deny_unknown_fields)]
enum StageWorkloadArtifactFrameV1 {
    Header { header: StageWorkloadArtifactHeaderV1 },
    Data { sequence: u32, bytes: BoundedBytes },
    Terminal {
        last_sequence: u32,
        body_length: u64,
        body_digest: Digest,
        stage_request_core_digest: Digest,
        stage_request_digest: Digest,
    },
}

struct StageWorkloadArtifactRequestCommitmentV1 {
    schema_version: u32, // const 1
    stage_request_core_digest: Digest,
    body_length: u64,
    body_digest: Digest,
}

enum StageWorkloadArtifactFailureReasonV1 {
    StaleFence,
    DeadlineExceeded,
    InvalidStream,
    ContentMismatch,
    TooLarge,
    IntegrityFailure,
    IdempotencyConflict,
}

enum StageWorkloadArtifactRetryDispositionV1 {
    DoNotRetry,
    ReconcileCandidate,
}

enum StageWorkloadArtifactDeferredReasonV1 {
    InProgress,
    DependencyUnavailable,
}

struct StageWorkloadArtifactReceiptV1 {
    schema_version: u32, // const 1
    stage_receipt_id: ReceiptId,
    tenant_id: TenantId,
    stage_kind: ArtifactWorkloadStageKindV1,
    owner: ArtifactOwner,
    job_owner: TypedOwnerRef,
    job_kind: JobKind,
    work_class: WorkClass,
    job_id: JobId,
    attempt_no: u32,
    source_attempt_binding_digest: Digest,
    artifact_id: ArtifactId,
    uploaded_artifact_version: u64,
    candidate_blob_id: InternalBlobId,
    object_generation_digest: Digest,
    observed_body_length: u64,
    observed_body_digest: Digest,
    artifact_state: ArtifactState, // exact Uploaded
    scan_operation_id: OperationId,
    scan_operation_binding_digest: Digest,
    scan_job_id: JobId,
    scan_job_binding_digest: Digest,
    grant_authorization_binding_digest: Digest,
    stage_request_core_digest: Digest,
    stage_request_digest: Digest,
    receipt_digest: Digest,
}

#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum StageWorkloadArtifactTerminalResultV1 {
    Succeeded {
        schema_version: u32, // const 1
        receipt: StageWorkloadArtifactReceiptV1,
    },
    Failed {
        schema_version: u32, // const 1
        stage_receipt_id: ReceiptId,
        stage_request_core_digest: Digest,
        terminal_stage_request_digest: Option<Digest>,
        reason: StageWorkloadArtifactFailureReasonV1,
        disposition: StageWorkloadArtifactRetryDispositionV1,
        safe_error_digest: Digest,
    },
}

#[serde(tag = "result_kind", rename_all = "snake_case", deny_unknown_fields)]
enum StageWorkloadArtifactResultV1 {
    Terminal {
        schema_version: u32, // const 1
        result: StageWorkloadArtifactTerminalResultV1,
    },
    Deferred {
        schema_version: u32, // const 1
        stage_receipt_id: ReceiptId,
        stage_request_core_digest: Digest,
        reason: StageWorkloadArtifactDeferredReasonV1,
        retry_after_milliseconds: u32,
    },
    RejectedObservation {
        schema_version: u32, // const 1
        stage_receipt_id: ReceiptId,
        stage_request_core_digest: Digest,
        terminal_stage_request_digest: Option<Digest>,
        reason: StageWorkloadArtifactFailureReasonV1,
        disposition: StageWorkloadArtifactRetryDispositionV1,
    },
}
```

`RejectedObservation`不是Receipt result，只允许`StaleFence | DeadlineExceeded | IdempotencyConflict`加`DoNotRetry`；它不得携带safe error、写入
Receipt或触发mutation。其他reason只能出现在current-fence winner持久化的terminal `Failed`；Deferred只允许其两个dedicated reason。

五个service method与`stage_kind`/audience/exact URI SAN一一映射，映射由17唯一注册；method、kind、SAN、Grant audience任一交换都在读取Data前拒绝。

| stage kind | exact method / URI SAN | Artifact owner | Job `TypedOwnerRef` / JobKind / WorkClass | exact port / purpose |
|---|---|---|---|---|
| `RegistryArtifact` | `StageRegistryArtifact` / `.../registry-validation-worker` | `Revision` | source Validation `ManagementOperation` / `RegistryValidation` / `RegistryValidation` | frozen Registry artifact slot / `Package \| Sbom \| BackendBinding` |
| `CapabilityOutput` | `StageCapabilityOutput` / `.../capability-worker` | `CapabilityInvocation` | same `CapabilityInvocation` / `Capability` / `CapabilityNative \| CapabilityRemote` | frozen Interface output port / `CapabilityOutput` |
| `ContextOutput` | `StageContextOutput` / `.../context-worker` | `ContextObservation` | source `ContextQuery` / `Context` / `Context` | frozen Context output port / `ContextDerived \| McpResource` |
| `McpOutput` | `StageMcpOutput` / `.../mcp-host` | `CapabilityInvocation` | same `CapabilityInvocation` / `Capability` / `CapabilityRemote` | frozen MCP-backed Interface output port / `CapabilityOutput` |
| `SandboxOutput` | `StageSandboxOutput` / `.../sandbox-controller` | `CapabilityInvocation` | same `CapabilityInvocation` / `Sandbox` / `Sandbox` | frozen Sandbox output port / `SandboxOutput` |

03 Receipt `ClosedOperation`按同一行逐值固定为：

| stage kind | ReceiptKind / ClosedOperation |
|---|---|
| `RegistryArtifact` | `JobCommit` / `artifact.workload_stage.registry.v1` |
| `CapabilityOutput` | `JobCommit` / `artifact.workload_stage.capability.v1` |
| `ContextOutput` | `JobCommit` / `artifact.workload_stage.context.v1` |
| `McpOutput` | `JobCommit` / `artifact.workload_stage.mcp.v1` |
| `SandboxOutput` | `JobCommit` / `artifact.workload_stage.sandbox.v1` |

五项authority scope均为Tenant，dedupe owner schema exact为`artifact.workload-stage.dedupe-owner.v1`/version 1/path
`contracts/platform-v1/schemas/artifacts/workload-stage-dedupe-owner.schema.json`/65536-byte maximum，对应完整
`StageWorkloadArtifactDedupeOwnerV1`。request schema exact为`artifact.workload-stage.request-core.v1`/version 1/path
`contracts/platform-v1/schemas/artifacts/workload-stage-request-core.schema.json`/196608-byte maximum；result schema exact为
`artifact.workload-stage.terminal-result.v1`/version 1/path
`contracts/platform-v1/schemas/artifacts/workload-stage-terminal-result.schema.json`/196608-byte maximum。
commitment mode必须为`StreamingCoreThenTerminal`，terminal schema exact为`artifact.workload-stage.request-commitment.v1`/version 1/path
`contracts/platform-v1/schemas/artifacts/workload-stage-request-commitment.schema.json`/4096-byte canonical maximum，registry的静态
`stream_bytes_hard_maximum`固定为平台`artifact.single_bytes.hard_max = 1073741824`。Candidate effective single/staging limit、tenant quota、Grant与
Attempt maximum只能在运行时进一步收紧该上限，不能改写root Receipt registry。operation与stage kind/method/SAN一一对应，不能五项合并成generic
operation或互换dedupe owner。

ellipsis只缩写共同的`spiffe://insight.platform/workload`前缀，不是pattern匹配。`Mcp` WorkClass的discovery/subscription不能借
`StageMcpOutput`伪装成Capability output；MCP Resource正文只有形成exact `ContextObservation`后才由Context Worker走`ContextOutput`。
`StageSandboxOutput`只服务14 `Capability | ManagedMcpTool` source且其physical Job owner/Artifact owner都为same CapabilityInvocation；
Managed MCP subscription child Job不得调用该method，其session/resource通过13 parent MCP protocol与Context observation flow提交。
每个row的Artifact owner、Job immutable back-reference、registered JobKind/owner/WorkClass triple、port source与purpose都必须逐值匹配，不能只检查合法
enum值；binding/core/receipt中的`job_owner`、`job_kind`与`work_class`从current Job读取并纳入digest，调用方不能只给一个`job_id`隐去owner关系。
`ArtifactStageAttemptBindingV1`在start时嵌入完整`ArtifactVerifyOperationBindingV1`；非Sandbox row以03 schema ID exact
`artifact.workload-stage.attempt-binding.v1`保存为source Job的`current_attempt_snapshot`，Sandbox row则从14 registered outer attempt snapshot取出
exact nested binding；stage success创建ManagementOperation时，只抽取并重新验证
nested `scan_operation_binding`的exact canonical bytes作为其03/17 `binding_snapshot`，schema ID为`artifact.verify.operation-binding.v1`，不得把整个
stage-attempt payload写入Operation。`scan_operation_binding_digest =
SHA-256(JCS(ArtifactVerifyOperationBindingV1))`，并逐值等于该VersionedSnapshot的payload digest。所有version/maximum为正，operation target必须是
同一Artifact，candidate/scan Job ID与stage binding一致；Operation、Artifact、Policy、scanner/ruleset、deadline任一交换都拒绝。只保存裸digest、
创建target-only Operation、在success时换preimage或创建后修改binding均非法。
`ArtifactVerifyJobBindingV1`是03 Job `binding_snapshot`中schema ID exact`artifact.verify.job-binding.v1`的完整payload，digest exact为
`artifact_verify_job_binding_digest = SHA-256(JCS(ArtifactVerifyJobBindingV1))`。它只由public CompleteUpload或Workload stage success winner在actual
object generation/length/digest已知后构造；必须复制immutable Operation binding中的tenant/operation/Artifact/candidate Blob/scan Job、Policy/
scanner/rules/max/deadline并回绑其digest，再追加actual Artifact version/object facts。Job owner exact为该ManagementOperation、Kind/WorkClass exact为
Artifact；recovery只能重放同一snapshot，不能修改Operation binding或从bucket重新发现字段。
每个stream恰有一个Header、从0严格递增的非空Data和一个Terminal；unknown/null/重复字段、空中间chunk、sequence gap、terminal后数据、单chunk/
总bytes/总时长越界或Terminal commitment与body实测不一致全部fail closed。首帧、transport backlog、in-flight stream/bytes、DB/S3/KMS waiter均受18
独立Workload Producer hard limit和permit约束，正文只做bounded streaming，不进入数据库、Receipt、Event、日志或unbounded buffer。
Header中的lease/token/Worker字段必须逐值等于接收时current Running Job fence；它们不是snapshot或Grant binding的一部分。只有尚未claim
`stage_receipt_id`、未发送任何Header且没有可能发生object I/O时，03 `ResumePhysicalAttempt`才可在新lease下复用同一stable attempt snapshot与Grant，
并以新current fence构造Header。一旦Producer提交该Receipt的Processing claim或接收Header，就禁止把Job转入可Resume的Waiting continuation；连接丢失、
lease loss或Worker loss必须先由Receipt takeover与exact staging cleanup收敛，再进入全新`NewPhysicalAttempt`，不能以旋转后的fence复用旧Receipt、stream或object session。

stage digest按无环时序构造。`NewPhysicalAttempt` start事务创建本attempt专属Artifact/Blob/Grant并预分配Operation/scan Job/Receipt ID，只用当时已知的
maximum/optional expected约束构造`ArtifactStageAttemptBindingV1`；其中完整scan Operation binding及其digest必须逐值回绑同一start事务冻结的
same-tenant preimage；Operation与scan Job row都尚未创建，其ID由nested binding预分配，完整stage binding以03 `JobAttempt` registry验证并保存。
`artifact_stage_attempt_binding_digest = SHA-256(JCS(binding))`。`source_attempt_binding_digest`对前四种stage逐值等于该digest；Sandbox逐值等于14
outer `SandboxJobAttemptBindingV1` snapshot payload digest，同时nested stage digest仍按本段公式单独重算。15 `JobAttempt.attempt_binding_digest`与
`WorkloadBound.request_binding_digest`都逐值等于`source_attempt_binding_digest`，随后计算Grant authorization并原子激活Grant。actual media/body此时不进入binding，也不要求提前
知道。`ResumePhysicalAttempt`必须从current Job payload/Receipt claim恢复exact binding和同一组identity，不重新分配；任何新的
`NewPhysicalAttempt`必须使用不同Artifact/Blob/Grant/Operation/scan Job/Receipt ID及新的binding digest。执行产生输出且同一Attempt fence仍current后，
调用方可立即构造Header：逐值复制binding与Grant authorization、current source attempt snapshot digest，追加已知的declared media，并计算
`stage_request_core_digest = SHA-256(JCS(StageWorkloadArtifactRequestCoreV1))`，Header wrapper只能携带该完整core与digest。Header/core不含最终body length/digest；
03共享Receipt的immutable`request_digest`使用该core digest。Producer随Data增量计算actual facts；Terminal构造
`StageWorkloadArtifactRequestCommitmentV1`并计算`stage_request_digest = SHA-256(JCS(commitment))`，两层digest及实测length/digest必须逐值相等。
因而remote/Sandbox大输出可以直接有界转发，不要求Worker预先完整buffer/spool，也禁止把
最终stage/body digest反向放进Grant。same-stage resume只有在执行adapter已有受本WorkClass byte/disk permit、deadline和secure-delete约束的exact
replayable source时才允许；否则current owner终结该attempt并在cleanup后启动全新physical attempt，不能以unbounded内存、临时目录或stale Attempt新Grant
模拟resume。

Producer在任何object I/O前后都重验current Job/version/attempt/lease/Worker、Grant generation/binding、Staging Artifact/version、quota、deadline与
encryption fence。same Receipt ID/different core digest可在Header后立即返回`IdempotencyConflict`；same core的已有terminal Receipt不能立即重放，
Producer必须有界消费Data/Terminal并重算final stage request digest，只有final digest也相同才重放，正文不同则Conflict且existing Receipt不变。
Processing Receipt按03
claim generation有界takeover并只返回正数bounded retry-after的`Deferred`；只有Header中的Job lease/Worker fence仍current且调用方拥有exact
replayable source时，依赖恢复后才可用same Attempt/request继续。fence已旋转必须走上述cleanup + NewPhysicalAttempt，不能把Receipt takeover误作
`ResumePhysicalAttempt`。Deferred不是terminal Receipt result。成功事务最多写actual candidate Blob/object generation、Artifact `Staging -> Uploaded`、terminal Receipt，并原子创建预分配的
`ArtifactVerify` ManagementOperation及其immutable binding snapshot、由该Operation拥有的预分配scan Job与actual Job binding、设置唯一
`current_job_id`、追加bounded `artifact.uploaded` Event；需要
跨进程交付时同一事务的Outbox只能引用该committed Event，不能承载无Event的独立wake。Scheduler随后只claim/replay这一个scan Job，不再创建第二个；
scan Job保存上述exact `ArtifactVerifyJobBindingV1` snapshot，Receipt的`scan_job_binding_digest`逐值等于其payload digest；
`receipt_digest = SHA-256(JCS(StageWorkloadArtifactReceiptV1 without receipt_digest))`。timeout/uncertain PUT使用
`ReconcileCandidate`并由exact-generation Maintenance cleanup收敛，不能伪造Uploaded/Verified/Ready；自由错误文本、locator、credential或正文不进入wire。

failure persistence按以下优先级唯一裁定：core conflict → terminal final-digest replay/conflict → transport identity与current fence/deadline → stream/content
limits → dependency/object I/O → post-I/O fence/integrity → success CAS。后置检查不得覆盖更早的terminal winner；`IdempotencyConflict`只作为
stable response返回，不修改已存在Receipt。

| observation | durable Receipt/result | Artifact / object / scan effect |
|---|---|---|
| valid Header前的transport、identity、backlog或capacity拒绝 | 不创建或修改Receipt | 不创建对象、不改变Artifact、不创建scan Job |
| same Receipt + different core digest | Header后返回`RejectedObservation(IdempotencyConflict)`；existing Receipt不变 | 不读取Data，不改变existing state/object |
| same Receipt + same core且已有terminal | 有界读取到Terminal并重算final digest；same才重放，different final返回`RejectedObservation(IdempotencyConflict)`；existing Receipt不变 | 不重复object/state side effect，且不同正文不能借same Header重放旧Succeeded |
| fresh `Processing` lease仍有效 | `Deferred(InProgress)`；Processing不terminalize | 不改变Artifact/object/scan Job |
| object I/O前依赖不可用 | 保持可接管Processing，返回`Deferred(DependencyUnavailable)` | 不创建对象、不改变Artifact/scan Job |
| stale fence或deadline observation | 返回但不持久化`RejectedObservation(StaleFence \| DeadlineExceeded, DoNotRetry)`；旧claimant无terminalize权限 | Producer不修改Receipt/Grant/Artifact/Job；current owner/recovery用自己的fresh fence终结Processing、revokeGrant，并为可能存在的attempt object创建exact-generation cleanup |
| invalid stream | terminal `Failed(InvalidStream, DoNotRetry)` | revokeGrant；无scan Job；无对象则保持Staging，有partial/uncertain对象则进入cleanup |
| content mismatch或too large | terminal `Failed(ContentMismatch \| TooLarge, DoNotRetry)` | Artifact推进Rejected、revokeGrant、无scan Job；任何candidate按exact generation cleanup |
| post-I/O fresh fence失败 | 与stale observation相同，不持久化Producer结果 | 丢弃物理结果；仅current owner/recovery创建attempt-scoped orphan cleanup，不标记内容integrity incident |
| current fence仍有效时的storage/digest integrity failure | terminal `Failed(IntegrityFailure, ReconcileCandidate)` | candidate/Artifact推进Quarantined或Deleting并创建bounded incident/cleanup事实；不得创建scan Job |
| success CAS | terminal `Succeeded(receipt)` | exact candidate facts、Uploaded、唯一ArtifactVerify scan Job、Event/Outbox原子提交 |

## 10. Content Verification

Model output Producer使用的profile不是裸digest。15拥有以下closed descriptor及唯一registry/schema：

```rust
struct ModelOutputContentValidationProfileV1 {
    schema_version: u32, // const 1
    validator_contract_digest: Digest,
    validator_implementation_digest: Digest,
    ruleset_digest: Digest,
    canonical_response_contract_digest: Digest,
    accepted_media_type: MediaType, // exact application/json
    evidence_schema_version: u32, // const 1
    evidence_validity_seconds: u64,
}
```

registry与schema路径固定为`contracts/platform-v1/artifacts/model-output-content-validation-profiles.json`和
`contracts/platform-v1/schemas/model-output-content-validation-profiles.schema.json`并进入root contract digest。registry为1～64项，按完整
profile digest raw bytes严格升序且唯一；profile digest是整个descriptor strict JCS的SHA-256，descriptor自身不含digest字段且canonical bytes
不超过4096。所有digest required，media exact为`application/json`，evidence schema version exact为1，validity必须为正且不超过18 Candidate
effective `artifact.model_output_content_evidence_validity_seconds`。unknown/null/重复字段、未注册digest或Producer image未安装exact
implementation均fail closed。

`canonical_response_contract_digest`必须逐值等于16唯一machine authority
`contracts/platform-v1/schemas/model/canonical-model-response.schema.json`解析后的JCS SHA-256。Candidate从同一root manifest先验证该entry的raw
SHA/length，再解析self-contained closed schema并重算digest；它不能是调用方提交值、文件原始字节hash、root `contract_digest`或任意Rust type
摘要。缺失/重复entry、external `$ref`或digest漂移均使profile/Candidate加载失败。

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

公共上传的`CompleteUpload` winner事务提交Uploaded事实，同时在prepare已创建的exact `ArtifactVerify` ManagementOperation上创建其唯一
`WorkClass::Artifact` scan Job、设置`current_job_id`并追加`artifact.uploaded` Event/Outbox；它是public path的唯一scan-Job creation authority。
Artifact Workload Producer path已在§9.1 success事务创建/重放同一预分配scan Job，后续scheduler只claim该Job。scan Worker取得claim后才按Job fence把
Uploaded推进Verifying；两条路径都不得由scheduler、scanner或recovery创建第二个Job。Job payload冻结exact Artifact/Blob/object generation、scan
Policy、scanner contract与ruleset。

exact `purpose=RunOutput + owner=ModelTurn + ModelOutputArtifactReservation + ModelArtifactProducer`组合是唯一首版例外：它不调用generic
`CompleteUpload`、不创建或claim Artifact scan Job，而由§15.1/16的同Attempt Producer执行固定canonical Model-response同步verifier并以
stage Receipt `claim_generation`推进到Verified。generic scheduler/scanner必须拒绝该route，Producer也必须拒绝所有其他purpose/owner/
audience；routing predicate与negative fixture进入machine contract，防止两个verifier对同一Artifact竞推进。
Producer必须从实际stream/object及其安装的selected profile计算`ModelOutputProducer` evidence；Worker header中的Model语义校验摘要只参与
stage请求幂等绑定，不能填充或替代该content evidence。
Producer提交Verified的最终短事务用唯一PostgreSQL `db_now`写`observed_at`并checked-add selected profile
`evidence_validity_seconds`得到`expires_at`；owner terminal与每次Ready read都要求current evidence未过期。调用方、Worker、环境变量或ArtifactIo
Policy不能覆盖该期限，overflow或`expires_at <= observed_at`拒绝提交。
rescan创建独立`ArtifactRescan` ManagementOperation和同结构Job；Ready Artifact在rescan排队事务先进入Quarantined，避免旧证据
过期期间继续读取。scan/rescan物理结果必须由exact WorkerProcessGeneration lease fence和`JobCommit` Receipt提交。

## 11. Reference 与所有权

```rust
enum ArtifactLinkState { Active, Released }

#[serde(tag = "owner_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactLinkOwnerFenceV1 {
    Run { run_id: RunId, expected_projection_version: u64 },
    Revision { version: ExactVersionRef },
    CapabilityInvocation { invocation_id: InvocationId, expected_projection_version: u64 },
    ContextObservation { observation_id: ContextObservationId },
    ModelTurn { model_turn_id: ModelTurnId, expected_projection_version: u64 },
    ManagementOperation { operation_id: OperationId, expected_projection_version: u64 },
}

struct ArtifactLink {
    schema_version: u32, // const 1
    artifact_link_id: ArtifactLinkId,
    tenant_id: TenantId,
    artifact_id: ArtifactId,
    owner_fence: ArtifactLinkOwnerFenceV1,
    port_or_purpose: ArtifactPortName,
    reference_kind: ArtifactReferenceKind,
    state: ArtifactLinkState,
    link_digest: Digest,
    projection_version: u64,
    created_at: DateTime<Utc>,
    released_at: Option<DateTime<Utc>>,
}

struct ArtifactLinkBindingPreimageV1 {
    schema_version: u32, // const 1
    artifact_link_id: ArtifactLinkId,
    tenant_id: TenantId,
    artifact_id: ArtifactId,
    owner_fence: ArtifactLinkOwnerFenceV1,
    port_or_purpose: ArtifactPortName,
    reference_kind: ArtifactReferenceKind,
}

enum ArtifactOwner {
    Run(RunId),
    Revision(ResourceVersionId),
    CapabilityInvocation(InvocationId),
    ContextObservation(ContextObservationId),
    ModelTurn(ModelTurnId),
    ManagementOperation(OperationId),
}
```

`ArtifactReference`是业务关系名称；唯一durable aggregate/type是上述`ArtifactLink`，不存在第二个Reference row或无ID边。
`link_digest = SHA-256(UTF8("insight.artifact-link.binding.v1") || 0x00 || JCS(ArtifactLinkBindingPreimageV1))`，排除自身、mutable
state/projection version与时间。所有ID/version为正且preimage逐值来自同一create transaction；16等下游的`artifact_link_digest`必须等于这里的
`link_digest`并同时检查current `state=Active`、Link projection version、owner identity/tenant与Ready Artifact。`owner_fence`只保存create-time
CAS evidence：mutable owner variant在创建事务逐值匹配当时的current aggregate projection version；owner随后合法推进projection version不会使
Active Link失效，read/replay也不得要求stored expected version等于owner current version。Revision使用02 `ExactVersionRef`，ContextObservation为一次提交后immutable并只匹配same-tenant ID。variant必须与
业务期望的`ArtifactOwner` ID逐值相等，不能把immutable owner伪造成generation 0或复用另一个owner kind。新Link固定
`Active/projection_version=1/released_at=None`；唯一迁移`Active -> Released`原子增加projection version并写database time，Released终态不可离开。
schema固定为`contracts/platform-v1/schemas/artifacts/artifact-link.schema.json`，unknown/null/cross-owner、digest漂移或同owner/port的重复active Link
均fail closed。

- Link 必须由 owner domain 在同一 PostgreSQL 事务创建/释放；
- owner是closed tagged union，不能接受任意表名/string或不匹配variant的ID prefix；异步scan/transform/delete产物由
  03的统一ManagementOperation aggregate拥有并由17投影API，不创建第二种Artifact operation资源；
- create mutation必须在同一事务验证typed owner存在、tenant相同且command的expected owner generation/version逐值匹配，并把该create-time
  evidence冻结进`owner_fence`；release command必须另携owner current expected projection version并在release事务执行CAS，但不得拿stored
  create-time version代替它。任一identity/tenant/current command fence缺失或漂移都整体回滚；
  owner完整性只有这一项observable authority，不在本规范选择foreign key、service或其他物理机制，具体映射只由ADR决定；
- RunBinding、Revision、Invocation、ContextObservation、SkillPackage、SandboxPackage、Model input/output 和 user
  attachment 都使用 Reference；
- public/share/export 是单独授权 projection，不改变 Artifact 本体；
- 删除 owner 先按其 retention policy释放 reference，不能直接删 object；
- 不以缓存、消息或 object store tag 作为引用权威；
- reference graph 支持 legal hold、lineage、incident 和 GC 查询。

## 12. ArtifactGrant

```rust
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
}

enum ArtifactGrantState { Active, Revoked }

#[serde(tag = "subject_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactGrantSubjectV1 {
    Principal {
        principal_id: PrincipalId,
        principal_snapshot_digest: Digest,
    },
    JobRequest {
        workload_role_identity_digest: Digest,
        owner: ArtifactOwner,
        job_id: JobId,
        job_request_binding_digest: Digest,
    },
    JobAttempt {
        workload_role_identity_digest: Digest,
        owner: ArtifactOwner,
        job_id: JobId,
        attempt_no: u32,
        attempt_binding_digest: Digest,
    },
}

#[serde(tag = "grant_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactGrantCapabilityV1 {
    ReadWhole {
        max_uses: u32,
        uses_consumed: u32,
    },
    ReadRange {
        allowed_range: BoundedRange,
        max_uses: u32,
        uses_consumed: u32,
    },
    StagingWrite {
        exact_staging_identity: OpaqueStagingIdentity,
        max_bytes: u64,
        expected_digest: Option<Digest>,
        multipart_contract_digest: Digest,
    },
}

#[serde(tag = "delivery_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactGrantDeliveryV1 {
    OpaqueBearer {
        token_key_version: TokenKeyVersion,
        token_digest: Digest,
    },
    WorkloadBound {
        request_binding_digest: Digest,
    },
}

struct OpaqueArtifactGrantTokenClaimsV1 {
    schema_version: u32, // const 1
    tenant_id: TenantId,
    artifact_grant_id: ArtifactGrantId,
    authorization_binding_digest: Digest,
    issuance_generation: u64,
    expires_at: DateTime<Utc>,
    issuance_receipt_id: ReceiptId,
    token_key_version: TokenKeyVersion,
}

#[serde(tag = "delivery_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactGrantIssuanceResultV1 {
    OpaqueBearer {
        schema_version: u32, // const 1
        claims: OpaqueArtifactGrantTokenClaimsV1,
        token_digest: Digest,
    },
    WorkloadBound {
        schema_version: u32, // const 1
        artifact_grant_id: ArtifactGrantId,
        authorization_binding_digest: Digest,
        generation: u64,
        request_binding_digest: Digest,
    },
}

struct ArtifactGrant {
    artifact_grant_id: ArtifactGrantId,
    tenant_id: TenantId,
    subject: ArtifactGrantSubjectV1,
    artifact_id: ArtifactId,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    audience: ArtifactWorkloadAudience,
    capability: ArtifactGrantCapabilityV1,
    delivery: ArtifactGrantDeliveryV1,
    issuance_receipt_id: ReceiptId,
    authorization_binding_digest: Digest,
    state: ArtifactGrantState,
    expires_at: DateTime<Utc>,
    generation: u64,
    projection_version: u64,
}
```

Grant 是 capability-style authorization，不是 ArtifactRef：

`ArtifactWorkloadAudience`、三个`grant_kind`和两个`delivery_kind`均进入machine registry；audience必须与签发时的subject identity、port、
purpose、capability和delivery一起进入`authorization_binding_digest`。该digest是排除自身、`OpaqueBearer.token_digest`、mutable
state/use/version字段后的完整immutable grant JCS SHA-256；opaque token digest由repository从实际一次性bearer bytes计算。不得用任意
service-account字符串扩展audience，也不得把一个variant解释成另一个能力。

subject也是closed且不存在未定义的“Job generation”：`Principal`用于authenticated public upload/download；`JobRequest`用于在claim前随Ready
Job冻结、可跨bounded retry重新claim的read grant，只绑定exact workload role、typed owner、Job ID与无环request-core digest；`JobAttempt`用于claim/start
后才可激活的staging write，只绑定exact workload role、typed owner、Job ID、稳定`attempt_no`与03 current-attempt snapshot digest。每次claim都会旋转的
lease generation/token digest与WorkerProcessGeneration只进入具体Broker/Producer request并从current Job复验，不得写入Grant或attempt snapshot。
`JobRequest + StagingWrite`、
`JobAttempt + ReadWhole/ReadRange`、Model Producer使用`JobRequest`或普通read使用`JobAttempt`全部是非法组合。Sandbox output等内部staging路径可在
admission预分配grant/staging identity，但只能在start事务取得完整Attempt fence后创建/激活`JobAttempt + StagingWrite`。
`Principal`必须且只能使用`OpaqueBearer`；`JobRequest | JobAttempt`必须且只能使用`WorkloadBound`，其`request_binding_digest`分别逐值等于subject的
`job_request_binding_digest | attempt_binding_digest`。任一subject/delivery跨组合、internal bearer或public workload-bound credential都拒绝。

- `ReadWhole`没有range字段；`ReadRange`必须有一个非空bounded range；两者的max/use均为正/非负且`uses_consumed <= max_uses`。
  `StagingWrite`没有read range/use counter，只绑定同一owner的Staging Artifact、exact opaque staging identity、正数byte ceiling、optional
  expected digest与安装的multipart contract；unknown/null/cross-variant字段及read/write混合全部拒绝；
- 签发前验证 current principal/workload、owner permission、state、classification、port、deadline、policy，并从current Tenant aggregate构造04
  `ValidatedCurrentTenantEncryptionDomainFenceV1`；read/download要求Active binding与Artifact/Blob冻结binding逐字段相等；
- 绑定单 Artifact/Intent、closed capability、audience/subject identity、purpose 和短 deadline；
- Sandbox、MCP、Context、remote Capability 获得的 grant 彼此不可替换；
- Model output staging grant只绑定exact `ModelArtifactProducer` identity、Model Attempt fence与`WorkloadBound` delivery；Model Worker只持有
  `StageModelOutput`调用权，不取得object locator、S3/KMS credential或可转交的write bearer；
- download grant 默认单 audience、短期、可撤销，不作为分享链接；
- 只有`OpaqueBearer`使用opaque deterministic misuse-resistant sealed form；token preimage唯一为strict JCS
  `OpaqueArtifactGrantTokenClaimsV1`，显式排除`OpaqueBearer.token_digest`、bearer bytes、mutable state/use/projection version及claims自身不存在的
  digest。签发顺序固定为：先计算grant authorization binding；再以初始generation/expiry/Receipt/key version构造claims；使用该version的
  domain-separated deterministic misuse-resistant seal生成token bytes；最后从actual bytes计算`token_digest`，把delivery、Grant与Receipt原子提交。
  replay只从terminal Receipt result内嵌的完整claims与对应key version重建同一bytes，不读取current Grant、Artifact、Link、owner或current generation。
  Receipt retention必须覆盖API最大retry/replay窗口；轮换密钥版本必须保留到所有对应
  issuance Receipt的最大replay/retention边界结束，即使grant已经
  expiry/revoke；重放已失效token是安全的，因为Gateway仍会重验current grant state/generation/expiry/use。Grant delivery只保存key version与
  `token_digest`，terminal Receipt result保存bounded claims；任何位置都不保存bearer bytes；
  `issuance_receipt_id`必须逐值指向创建该grant的same-tenant terminal
  success Command/JobCommit Receipt，result exact为`ArtifactGrantIssuanceResultV1`。`OpaqueBearer`内嵌完整claims并回绑token digest，因此即使
  Grant/Artifact先过期、撤销或按retention删除，response loss仍只从terminal Receipt重放byte-identical token，不新发第二能力；普通随机AEAD重封、
  读取current aggregate补claims或在replay时替换token digest均非法。
  `WorkloadBound`只重放同一grant ID/binding/generation/request binding，不生成、保存或返回bearer；
- revoke、Run/Invocation terminal、Secret/network kill switch 可提升 generation；
- backend 不能扩大 range、续期、改变 classification 或转签给第三方。

`generation/projection_version`都为正。新Grant为`Active/generation=1`；read capability从`uses_consumed=0`开始。每个新的read authorization
attempt在返回locator projection前以current generation/version CAS把use加一并推进projection version；`uses_consumed == max_uses`只派生
exhausted eligibility并拒绝新consume，不是第三个持久状态。`Active -> Revoked`始终允许，即使use已耗尽；revoke推进generation/version且不重置
计数，`Revoked`不可离开。expiry由同一PostgreSQL `db_now >= expires_at`派生并拒绝，不伪造持久状态。已消费但在object I/O前崩溃的use不退款；
同一stream复用进程内sealed read ticket且不消费第二次，但在首个chunk、每个bounded chunk dispatch边界、固定最大授权间隔及terminal前逐值重验
ticket记录的consume后generation/version/use ordinal、subject/owner、Ready/evidence、Policy和current security fence。public download首版固定
`max_uses=1`；每个Range/重连须重新issue grant，避免bearer
并发重放。内部read的较大max必须受HardLimitProfile与exact Job/request约束；StagingWrite的resume/commit只按exact generation、staging identity、
multipart state与issuance Receipt幂等，不得复用read use语义。

## 13. Read 与 Download

每次业务 read/download 必须验证 tenant、principal/workload、Reference/owner permission、Ready state、classification、
retention/hold/suspension、grant generation、current policy、current Active encryption-domain fence和content-evidence freshness。公共下载与受信
workload read都只经对应Artifact Gateway/Broker有界流式代理；任何classification都不返回object-store presigned/signed URL或redirect，因为该bearer在
签发后无法逐请求重验generation、Rebind/Revoke与evidence expiry。每个Range/重连都是新read并重新执行完整授权，不能把旧连接或token当长期快照。
scan/head/delete不是业务read，必须走§13.1的exact maintenance authority、Job fence和lifecycle guard，不能借该例外向调用方返回正文。

响应规则：

- `Content-Type` 使用 verified media，设置 `X-Content-Type-Options: nosniff`；
- 风险 media 默认 `Content-Disposition: attachment`，display name 安全编码；
- HTML/SVG/script/executable 不在平台 origin inline 渲染；preview 必须是新的 sanitized derived Artifact；
- Range 仅对允许 media/operation，范围、并发和总 bytes 有上限；
- ETag/validator 使用 opaque artifact generation，不泄露跨 tenant blob identity；
- URL 不包含 raw object key、tenant ID、filename、Secret 或永久 token；
- download access 形成 body-free audit；正文不写 access log。

### 13.1 受信物化与对象定位机器合同

Blob aggregate中的`object_reference_ciphertext`是physical object locator的唯一durable authority。能解封或创建它的生产角色是closed set：
public Artifact Upload Gateway、public Artifact Download Gateway、Artifact Workload Broker、Model Artifact Broker、Sandbox Artifact Broker、
Artifact Workload Producer、Artifact Maintenance Authority，以及只限自身Model-output staging路径的Model Artifact Producer。每个角色只能使用下文列明的exact capability；Management/Runtime API、客户端、
Registry/Context/Capability/MCP/Model/Artifact Worker、Sandbox Controller及Executor均不得取得明文locator、bucket credential或KMS plaintext。受信read authority在同一
PostgreSQL authorization transaction/snapshot中重验对应closed source、current grant/security fence、Ready Artifact与Verified Blob后，返回以下
非持久、不可Clone且Debug恒定脱敏的投影：

```rust
struct AuthorizedArtifactObjectRead {
    audience: ArtifactObjectReadAudienceV1,
    scope: ArtifactObjectReadScopeV1,
    tenant_id: TenantId,
    artifact_grant_id: ArtifactGrantId,
    grant_generation: u64,
    grant_projection_version: u64,
    grant_use_ordinal: u32,
    blob_id: InternalBlobId,
    artifact: ArtifactRef,
    backend: StorageBackend,
    storage_binding_digest: Digest,
    kms_binding_digest: Digest,
    encryption_domain_id: EncryptionDomainId,
    encryption_domain_generation: u64,
    encryption_domain_binding_digest: Digest,
    key_id: KmsKeyId,
    security_domain_digest: Digest,
    current_encryption_domain_fence: ValidatedCurrentTenantEncryptionDomainFenceV1,
    object_reference_ciphertext: SecretBytes,
    object_reference_ciphertext_digest: Digest,
    object_generation: ObjectGeneration,
    authorization_source_digest: Digest,
    authorization_digest: Digest,
}

enum ArtifactObjectReadAudienceV1 {
    PublicDownload,
    GeneralWorkload,
    ModelRequest,
    SandboxExecution,
}

#[serde(tag = "scope_kind", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactObjectReadScopeV1 {
    Whole,
    Range { byte_range: BoundedRange },
}

enum ArtifactDownloadMethodV1 { Get, Head }

struct PublicArtifactDownloadReadRequestV1 {
    tenant_id: TenantId,
    principal_snapshot_digest: Digest,
    artifact_id: ArtifactId,
    artifact_grant_id: ArtifactGrantId,
    grant_generation: u64,
    download_token_digest: Digest,
    method: ArtifactDownloadMethodV1,
    byte_range: Option<BoundedRange>,
    deadline: DateTime<Utc>,
}

enum ArtifactGeneralWorkloadKindV1 {
    Runtime,
    RegistryValidation,
    Capability,
    Context,
    Mcp,
}

struct ArtifactWorkloadReadRequestV1 {
    tenant_id: TenantId,
    workload_kind: ArtifactGeneralWorkloadKindV1,
    workload_identity_digest: Digest,
    owner: ArtifactOwner,
    job_id: JobId,
    job_version: u64,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    artifact: ArtifactRef,
    artifact_link_id: ArtifactLinkId,
    artifact_link_digest: Digest,
    artifact_grant_id: ArtifactGrantId,
    grant_generation: u64,
    grant_authorization_binding_digest: Digest,
    scope: ArtifactObjectReadScopeV1,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    maximum_bytes: u64,
    job_request_binding_digest: Digest,
    request_digest: Digest,
    deadline: DateTime<Utc>,
}

struct ModelArtifactReadRequestV1 {
    tenant_id: TenantId,
    model_turn_id: ModelTurnId,
    job_id: JobId,
    job_version: u64,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    canonical_model_request_digest: Digest,
    artifact_input_ordinal: u32,
    model_request_value_id: RunValueId,
    artifact: ArtifactRef,
    artifact_link_id: ArtifactLinkId,
    artifact_link_digest: Digest,
    artifact_grant_id: ArtifactGrantId,
    grant_generation: u64,
    grant_authorization_binding_digest: Digest,
    port: ArtifactPortName,
    purpose: ArtifactPurpose,
    maximum_bytes: u64,
    artifact_input_binding_digest: Digest,
    deadline: DateTime<Utc>,
}

struct ArtifactMaintenanceFenceV1 {
    tenant_id: TenantId,
    job_id: JobId,
    job_version: u64,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    blob_id: InternalBlobId,
    object_generation: ObjectGeneration,
    policy_digest: Digest,
    result_receipt_id: ReceiptId,
    request_digest: Digest,
    deadline: DateTime<Utc>,
}

enum ArtifactMaintenanceOwnerV1 {
    Artifact(ArtifactRef),
    InternalBlob(InternalBlobId),
}

enum ArtifactMaintenanceOperationKindV1 {
    ScanRead,
    HeadExactGeneration,
    DeleteExactGeneration,
}

#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum ArtifactMaintenanceRequestV1 {
    ScanRead {
        fence: ArtifactMaintenanceFenceV1,
        artifact: ArtifactRef,
        scan_policy_digest: Digest,
        maximum_bytes: u64,
    },
    HeadExactGeneration {
        fence: ArtifactMaintenanceFenceV1,
        owner: ArtifactMaintenanceOwnerV1,
    },
    DeleteExactGeneration {
        fence: ArtifactMaintenanceFenceV1,
        owner: ArtifactMaintenanceOwnerV1,
        expected_lifecycle_generation: u64,
        deletion_policy_digest: Digest,
    },
}

struct AuthorizedArtifactMaintenanceOperationV1 {
    operation: ArtifactMaintenanceOperationKindV1,
    tenant_id: TenantId,
    owner: ArtifactMaintenanceOwnerV1,
    job_id: JobId,
    job_version: u64,
    lease_generation: u64,
    worker_process_generation_id: WorkerProcessGenerationId,
    blob_id: InternalBlobId,
    backend: StorageBackend,
    storage_binding_digest: Digest,
    kms_binding_digest: Digest,
    object_reference_ciphertext: SecretBytes,
    object_reference_ciphertext_digest: Digest,
    object_generation: ObjectGeneration,
    policy_digest: Digest,
    result_receipt_id: ReceiptId,
    authorization_source_digest: Digest,
    authorization_digest: Digest,
}
```

Public download request全部字段从authenticated opaque Gateway token、current principal binding与HTTP method/range派生，客户端不能在body/query覆盖。
Artifact Download Gateway repository在同一短事务逐字段要求：tenant/artifact命中request；stored token digest等于实际token digest；state为Active、generation相等、
未过期且仍有use；subject exact为当前principal ID/snapshot且delivery exact为`OpaqueBearer`；audience exact为`Principal`；port exact为
`public_download`；purpose等于Artifact purpose；
capability只能是`ReadWhole`或`ReadRange`。`byte_range`缺失要求前者，出现则要求后者且逐值落在`allowed_range`内，`null`非法；method、grant
capability与HTTP Range不匹配一律在locator/KMS前拒绝。`Get`无Range与`Head`只能构造`Whole`，`Get`带Range只能构造逐值相同的`Range`；
`Head`带Range拒绝。transaction还重验owner Reference permission、Ready Artifact/Verified Blob、current evidence/
Policy/security fence后才执行单次use CAS并构造带exact scope的`PublicDownload` projection；它不是generic API read port。三个internal read audience使用各自
closed Job/lease/Worker request和Broker，不接受public token。`authorization_source_digest`覆盖完整closed audience-specific request、
`authorization_binding_digest`及consume后的grant generation/version/use ordinal。

`ArtifactWorkloadReadRequestV1`是Runtime、Registry validation、Capability、Context与MCP五条普通内部读取路径的唯一共享envelope；
`workload_kind`逐值映射`Runtime | RegistryWorker | CapabilityWorker | ContextWorker | McpHost` audience，不能动态增加字符串role。
Artifact Workload Broker的method、exact client URI SAN、subject workload identity、typed owner、current Job/version/lease token/Worker generation、
active ArtifactLink ID/digest、exact grant ID/generation/authorization binding digest、scope、port/purpose、request digest与deadline必须全部一致；
tokenless internal grant必须是`JobRequest + WorkloadBound`，两处request binding digest与request字段逐值相等，调用方不能在RPC metadata/body替换；
`Whole`只能匹配`ReadWhole`，`Range`只能匹配
`ReadRange`且byte range逐值落在grant边界内。同一短事务
消费use并构造`GeneralWorkload` projection，随后按每个bounded chunk/授权间隔及terminal规则以sealed ticket完整重验。一个method的合法request不能改kind或SAN调用另一method。

General Workload的digest拓扑固定且无环：先预分配Link/Grant ID，以tenant、kind、workload role、typed owner、Job ID、Artifact/Link、Grant ID及初始
generation、scope、port/purpose、maximum bytes与deadline构造stable core；该projection排除尚不存在的current `job_version`/lease/Worker fence、
`grant_authorization_binding_digest`、`job_request_binding_digest`与`request_digest`。`job_request_binding_digest = SHA-256(JCS(core))`；再创建subject绑定该
digest的Grant并计算其authorization binding；claim后填充current fence，最后
`request_digest = SHA-256(JCS(ArtifactWorkloadReadRequestV1 without request_digest))`。repository必须按此顺序重算全部三层，不能让full request digest
反向进入Grant而形成digest环。

repository只有在同一transaction snapshot读取current Tenant security aggregate、Artifact/Reference/grant与Blob后才能构造该projection。
projection中的grant ID/generation/version/use ordinal必须逐值等于本次成功consume后的事实，sealed ticket及
`authorization_digest`同时绑定四者。current fence的tenant、
domain、storage/KMS digest、generation和binding digest必须与Artifact/Blob冻结security projection逐字段相等；Rebind/Revoked、缺项或Tenant version
读取失败均阻止新projection。`authorization_digest`是排除其自身与secret ciphertext bytes、但包含
`object_reference_ciphertext_digest`、audience、exact read scope、authorization source digest及current fence完整canonical字段后的JCS digest；
ciphertext digest必须由repository从实际sealed bytes计算。
issue-download grant的authorization binding digest同样绑定current fence generation/binding digest，grant使用时仍重新读取current fence，不能把
签发时快照当作长期权限。sealed ticket只证明本次consume与初始projection；每个chunk/授权间隔及terminal检查都重新执行除use increment外的
完整current校验，不能把“consume已成功”当作持续授权。

public Download Gateway只保留一个bounded chunk buffer：每个chunk发给客户端前先通过fresh校验，revoke/Rebind/evidence expiry立即停止后续
dispatch并触发bounded cancel。已经写入网络的bytes无法撤回，因此合同不承诺retroactive revocation；Whole响应只有在完整length/digest与terminal
fresh authorization均成功后才记success receipt/audit，Range响应必须对exact object generation、requested range与returned length逐值闭合。
若完整性错误在部分响应后才发现，Gateway立即终止stream、不得产生success terminal，并创建不含正文的corrupt/incident事实。

三个internal Broker采用terminal-use barrier：trusted consumer adapter把chunk写入受quota、request maximum、deadline和permit约束的bounded encrypted
ephemeral spool，只有收到Broker的唯一terminal success（完整length/digest及最终fresh authorization）后才能解析、传给Provider、注入guest或交给业务
handler。失败、取消、revoke或consumer drop必须清零并删除spool；spool不保存durable locator，也不能跨request复用。Broker与consumer都不需要把完整正文
留在内存。exact delete/GC/incident cleanup使用已冻结locator/generation，不要求binding仍Active，避免安全撤销阻塞清理。

`ArtifactMaintenanceRequestV1`不使用普通业务grant，也不能构造`AuthorizedArtifactObjectRead`。Artifact Maintenance Authority只接受17列明的exact
scanner/GC URI SAN，并从current Artifact Job payload与lease读取请求字段；调用方不能提交locator、storage/KMS binding或任意operation string。
`ScanRead`要求exact Artifact处于对应`Verifying`流程、scan policy/maximum bytes与Job冻结值一致；`HeadExactGeneration`与
`DeleteExactGeneration`要求typed owner、Deleting/cleanup lifecycle generation、retention/hold/reference资格及expected object generation一致。
authority在单一authorization snapshot中构造不可持久、不可Clone、Debug脱敏的`AuthorizedArtifactMaintenanceOperationV1`，在自身进程内解封并执行
exact-generation GET/HEAD/DELETE；scan只流出有界正文与evidence，head/delete只返回bounded evidence/receipt，三者都不返回locator、KMS plaintext、
bucket credential或generic object handle。object I/O后提交前再次重验Job fence、owner lifecycle与policy；stale结果只能丢弃或按原Job重试。

Maintenance digest不复用普通read公式。`authorization_source_digest`等于strict JCS SHA-256，输入恰为完整tagged
`ArtifactMaintenanceRequestV1`、authenticated service/method及leaf certificate exact URI SAN；因此覆盖tenant、Job/version、lease generation/token
digest、WorkerProcessGeneration、deadline、owner/lifecycle/policy、Blob/object generation、Receipt与request digest。
`authorization_digest = SHA-256(JCS(AuthorizedArtifactMaintenanceOperationV1 without authorization_digest and secret ciphertext bytes))`，但必须包含
`object_reference_ciphertext_digest`、operation、owner、Blob/object generation、storage/KMS binding、result Receipt与source digest。authority把该digest
封入不可导出的I/O ticket；I/O后逐值重验request/fence/lifecycle/policy，返回evidence与最终`JobCommit` Receipt都必须回绑同一authorization digest，
禁止跨method、SAN、Job、generation或Receipt复用结果。

初始`StorageBackend`只注册`s3`。KMS decrypt必须同时提交canonical encryption context：
`schema_version=1`、tenant ID、Blob ID、storage/KMS binding digest、encryption domain ID/generation/binding digest、security-domain digest和key ID；
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

Internal Artifact Broker是共享协议/实现族，不是共享运行时。生产物理边界固定为三个audience-isolated internal只读服务；public Artifact
Download Gateway是第四个独立受信read authority，但不是Broker gRPC service。public Artifact Upload Gateway只处理Principal staging，Artifact
Workload Producer只处理普通internal JobAttempt staging，Maintenance Authority只执行exact-generation维护，Model Artifact Producer只执行
Model-output staging；四者都不属于read Broker：

- Artifact Workload Broker是独立进程、Deployment、ServiceAccount、read-only PostgreSQL credential/pool和permit，只注册
  `ArtifactWorkloadBrokerService`的Runtime/Registry/Capability/Context/MCP五个exact read method，并对每个method安装17的closed URI SAN allowlist；
- Model Artifact Broker是独立进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool和permit，只注册
  `ArtifactModelBrokerService.ReadModelRequest`，只接受exact Model Worker URI SAN；
- Sandbox Artifact Broker是另一组独立进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool和permit，只注册
  `ArtifactSandboxBrokerService.ReadWasiArtifact`与`ReadMicroVmArtifact`，只接受exact Sandbox Controller URI SAN；WASI与
  microVM允许共享Sandbox audience内的runtime、对象存储/KMS client和in-flight bulkhead；
- Artifact Workload Producer是独立进程、Deployment、ServiceAccount、write-limited PostgreSQL credential/pool、staging storage identity和
  permit，只注册17的Registry/Capability/Context/MCP/Sandbox五个exact client-stream staging method；只接受`JobAttempt + WorkloadBound +
  StagingWrite`，最多推进`Staging -> Uploaded`并触发既有scan Job，不得读取、扫描、Verified/Ready、finalize或处理Model output；
- Artifact Maintenance Authority是独立进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool、storage identity和permit，
  只注册`ReadForScan`、`HeadExactGeneration`与`DeleteExactGeneration`，且每个method只接受17列明的scanner/GC identity；
- Model Artifact Producer保持§15.1的独立staging-write authority，只能访问其exact reservation/candidate path。

上述八个authority不得共享Pod、ServiceAccount、数据库连接池、storage identity或process-local semaphore；统一public hostname只能在
Ingress按closed route分发到Upload或Download Gateway，不能把两条lane装回同一进程。任一audience的队列、正文、连接或
对象存储请求饱和不得消耗另一audience的本地准入容量；不得通过同一listener动态选择audience，也不存在通用服务或generic object API。实现可以复用
无状态library与相同machine schema，但每个进程只能安装自己的RPC surface、mTLS allowlist、storage-binding catalog、workload identity和
bounded resources。

本节三个 Broker 都是只读服务。它们的数据库角色只能执行授权读取，RPC surface、protobuf service 和listener均不得注册
upload、stage、complete、verify、finalize或generic object-write方法。Model output写入由§15.1的独立Model Artifact Producer承担；
“复用Broker library”不得被解释为复用Broker进程、ServiceAccount、数据库credential/pool、storage identity或permit。

每个受信materializer从CandidateManifest安装的closed storage-binding catalog按exact digest选择client；catalog
只含endpoint/region/bucket/path-style、timeout和hard byte limit，不含静态access key。生产S3/KMS client
只能使用该Pod的短期workload identity/default credential chain和private endpoint。读取必须对exact
`object_generation`执行HEAD及GET，禁止无version fallback；`Whole`只能执行完整GET，`Range`只能把exact range传给backend并按range长度复验，
不得先full GET再截断或扩大range。首个backend read前核对长度上限，逐chunk累计实际长度和digest；Whole terminal必须再次核对
ArtifactRef/Blob的exact length与SHA-256，Range terminal核对exact generation/range/returned length及backend transport integrity evidence，不能声称已重算
whole-object digest。object missing/version drift/oversize/digest mismatch归为integrity failure；provider timeout/unavailable保持可重试，internal consumer
不得获得usable bytes，public stream即使已发送前缀也必须中止且不能产生success terminal。

Model逻辑输入只接受`ModelArtifactReadRequestV1`。它必须逐项来自16已冻结的`ModelArtifactInput`与current claim：tenant、ModelTurn、当前
Job ID/version、lease generation/token digest、WorkerProcessGeneration ID、canonical request digest、input ordinal/binding digest、deadline、
exact `model_request` RunValue、ModelTurn owner的active ArtifactLink ID/digest、ArtifactRef/maximum bytes，以及audience exact为`ModelWorker`的
`ReadWhole` grant ID/generation/authorization binding digest，且subject/delivery exact为`JobRequest + WorkloadBound`。Model canonical request必须整体物化，
不能用Range grant拼接或部分解析；grant subject必须绑定
16 §11定义的无环`model_request_core_binding_digest`，port/purpose也必须匹配；
tokenless internal RPC不能替换任一grant字段。PostgreSQL read authority在同一snapshot中消费exact grant use并逐项重验后才能返回上述
非持久投影；Model Artifact Broker按上述chunk/final规则持续授权并只在完整object校验通过后发terminal success。Model Worker的trusted adapter必须先
完成ephemeral spool terminal-use barrier，随后才按Model请求的
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
`ModelOutputArtifactReservation`并嵌入16 `ModelJobAttemptBindingV1::ArtifactCapable` snapshot：tenant/Run/Node/ModelTurn与expected version、
Job/attempt/start-version、
request/admission/Model Deployment/HardLimitProfile/output schema digest、classification、Artifact/RunValue/Output Link/grant/stage
Receipt IDs、候选Blob与duplicate-cleanup Job ID、Artifact-owned与candidate-Blob-owned两个quota bundle、最大materialized bytes、media、Retention/ArtifactIo Policy
revision、Blob security-domain digest、`staging_retain_until`、`ready_retention_seconds`、deadline与整个reservation digest。该事务创建同Attempt的Staging Artifact intent，
其当前`retain_until`只能是冻结的`staging_retain_until`；事务同时创建受限write grant，并按最坏合法response bytes/count预留quota；
Artifact bundle的owner从创建起是预留Artifact ID，candidate Blob bundle的owner是预留candidate Blob ID，dedupe/cleanup均不得转移；
此时Artifact允许尚未绑定Blob。两个bundle必须分别是04规定的count+logical与uploads+staging+physical exact line。合法response上限完全落在
Inline内时冻结16的`InlineOnly`分支，不创建虚假Artifact/Blob/quota预留。
两种分支的snapshot都不含lease token/generation或Worker process generation；这些volatile fence只在每次Model/Producer request携带并从current Job复验。
Artifact-capable分支任一项不能完整预留时不得start该Attempt、不得调用Provider，也不得形成Provider usage。

预留ID本身不授权读取、写入或finalize。Retry/failover的新物理Attempt必须使用新Artifact、grant、Receipt、Link、RunValue和quota
identity；旧Attempt的任何ID或receipt都不能被新lease接管。结果最终可Inline时，Model terminal必须证明Artifact仍未绑定Blob/locator，
才能以零actual关闭两个bundle、撤销grant并把未使用intent标为可GC，而不是为了复用预留强制写对象存储；一旦candidate已绑定或可能
PUT，失败owner仍可关闭未Consume的Artifact bundle，但candidate Blob bundle必须保留到cleanup取得exact deletion/absence evidence。

#### 15.1.2 独立 Model Artifact Producer

Model Artifact Producer是独立进程、Deployment、ServiceAccount、mTLS server identity、restricted PostgreSQL write
credential/pool、S3/KMS workload identity、two-phase admission permit与transport backlog hard cap。它不与Model Worker、Artifact Workload/Model/Sandbox
三个只读Broker、Artifact Workload Producer、Artifact Upload/Download Gateway、Artifact Maintenance Authority或Scanner共享Pod、ServiceAccount、DB pool、storage identity或process-local semaphore；其饱和、重启或
对象存储故障不能占用read Broker、Model Provider stream、API、Scheduler或其他WorkClass的准入容量。

Producer只注册versioned client-streaming `StageModelOutput`，只接受exact
`spiffe://insight.platform/workload/model-worker.artifact-output` URI SAN；Model read使用的`.../model-worker`身份必须被拒绝。首帧必须是一个closed header，
随后只能出现严格递增、非空且按16 canonical chunking的data frame，最后是唯一terminal frame；协议不定义`FenceRefresh`或任意metadata
frame，terminal只携带客户端最后观察到的fence lower bound。空首帧、重复header/terminal、sequence gap、短非末片、单片/总量越界、
terminal后数据、未知字段/enum、声明与实测length/digest/media/classification/schema/Worker semantic evidence不一致全部fail closed。它不注册
`ReadModelRequest`、WASI/microVM、generic upload/read/finalize或公共HTTP方法。Model Worker不能取得object locator、bucket credential、
KMS plaintext或Producer数据库credential。

`StageModelOutput`的closed header/receipt只使用16的同名machine contract，不在Artifact实现另建第二套DTO。header至少完整回绑
reservation、正文SHA-256/byte length、`application/json`、classification、output schema、Worker生成的Model-response semantic evidence与stage
request digest；receipt返回Producer从actual stream/object计算的tagged content evidence digest，以及exact Artifact/candidate+resolved Blob/
object generation的脱敏digest、Verified version、新增physical bytes、正文事实和receipt digest，不返回object key、URL、
grant token或业务Output Link。stream body、metadata或调用方header均不能覆盖tenant、owner、purpose、classification、retention、
storage binding、KMS context、deadline或预留ID。

容量准入分两阶段：exact TLS/service-role authorization后、读取bounded header前先取得18 `ComponentRuntimeManifest`中
`ModelArtifactProducerRuntimeManifestV1`的global stream与
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
   -> validate reserved Staging intent, grant, quota, Policy closure and current Active encryption-domain fence
2. exact security-domain dedupe lookup after authorization/quota
   -> existing Verified Blob: stream+validate all bytes without object write, then bind as resolved Blob
   -> no winner: create/load reserved candidate Blob and KMS-seal one exact opaque locator
3. candidate path streams exact bytes to one unique staging object
   -> conditional create; never overwrite an existing generation
   -> HEAD exact object generation/KMS context, then guarded Staging -> Uploaded checkpoint
4. guarded Uploaded -> Verifying checkpoint; perform bounded verification outside DB transaction
5. final PostgreSQL transaction under current claim_generation and dedupe advisory fence
   -> revalidate Attempt/reservation/grant/Policy, current encryption-domain fence and final Job authorization
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
request/reservation漂移、Tenant encryption binding revoke/rebind、cancel/timeout/terminal first-winner或deadline到期一律返回stale，不得
提交Uploaded/Verified。对象I/O不在数据库
事务内。Processing claim、Blob bind、Uploaded/Verifying checkpoint与最终Verified事务必须按03锁序先锁stage Receipt并CAS
`claim_generation`，再锁Tenant security aggregate并重验current Active binding与冻结projection逐字段相等，随后按04 canonical顺序对Artifact
与candidate Blob两个exact quota bundle header/line取得`FOR SHARE`，锁后重验冻结的
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
S3 generation、storage-binding digest、tenant/security-domain KMS context和冻结ArtifactIo Policy。selected closed profile digest以canonical
descriptor绑定validator contract/implementation、canonical response contract、ruleset、evidence schema与validity；该profile digest与实际
evidence schema version进入Verified evidence。Producer不能运行Skill/script/package manager或把任意content-type交给动态parser。

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
| IntegrityFailure且final current guard仍成立 | `Processing -> Failed`并保存bounded evidence | candidate Artifact current `Staging / Uploaded / Verifying -> Quarantined`并撤销grant；仅在exact candidate generation证据充分时Blob -> Corrupt，否则由incident authority判定 | terminal failure：`IntegrityIncident`；只允许incident/cleanup |
| Success | `Processing -> Succeeded` | resolved Blob/Artifact -> Verified及evidence | success receipt；交给owner terminal |

最后三条terminal mutation必须与Receipt结果在同一事务CAS current `claim_generation`；取得final guard前若同时发生stale，stale优先且Producer
不得写Artifact/Receipt。`DependencyUnavailable`永远不能保存为terminal Failed，否则会永久阻断同Attempt恢复；Conflict也永远不能改写
original Receipt。Producer之外的Model owner/cleanup只能按该矩阵terminalize stale Processing，不能把transient dependency改写为业务成功。

Producer返回Verified receipt后，只有Model owner repository可在一个PostgreSQL terminal first-winner事务中：

1. 按ID排序锁定terminal与stage两个Receipt并claim/replay terminal Receipt，从已锁stage Receipt确定candidate disposition与可选预留
   cleanup Job ID；再按03顺序锁定Tenant security aggregate并复验current encryption-domain fence，然后锁Model quota、Artifact bundle与
   candidate Blob bundle以及current Run/Node/ModelTurn parent aggregate；
2. 在取得任何Job-rank锁之前，把current Model Job与可选`RacingCandidateLoser` cleanup Job组成canonical sorted-unique Job集合，并在同一个
   Job-rank阶段依ID顺序逐一lock existing或create-or-lock。cleanup Job必须是预留ID、exact `InternalBlob` owner且payload逐字段匹配stage
   receipt的candidate bytes/generation；随后重验current Job fence、Attempt、request/binding及全部identity。已经terminal的same cleanup
   Job/Receipt按原结果复验，different payload是invariant failure；不得先锁current Job再补锁排序更小的cleanup Job；
3. 锁定同tenant预留Artifact、resolved Verified Blob、可选Deleting/Deleted candidate、active write grant与冻结Policy，逐项比较digest、length、
   media、classification、schema、Worker semantic evidence、Producer tagged content evidence、retention和object generation；
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

Sandbox input使用14的per-Job `ReadWhole` grant并只经只读Sandbox Artifact Broker完成terminal-use物化；Sandbox output使用attempt-bound
`StagingWrite` grant，由Controller调用Artifact Workload Producer的exact `StageSandboxOutput`。Guest只能读声明input、向受控output stream写bytes；
不能指定Artifact ID、object key、classification或Ready状态。ArtifactGrant aggregate是撤销的唯一durable fact：只读Broker返回exact read receipt，Sandbox owner/
Controller authority在销毁证据形成前按Job/attempt/Worker generation/lease幂等推进`Active -> Revoked`，Job terminal事务撤销遗漏项并
核对request冻结的完整grant集合。重复revoke不得形成第二状态或阻止terminal；未 finalize output 进入 staging GC。

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
- `ArtifactReference` link（映射进ADR的共享ArtifactLink aggregate）是权威，引用数量可以是加速 projection；
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
- Artifact Upload Gateway与Artifact Download Gateway两条物理lane、Artifact Workload/Model/Sandbox三个read Broker、Artifact Workload Producer、
  Artifact Maintenance Authority、Model Artifact Producer与transformer分别使用独立队列、permit、连接池和autoscaling；
- scan backlog 超过安全门槛时拒绝/延迟新高风险 upload，不把未扫描内容当 Ready；
- large transfer 使用 streaming backpressure，不在内存缓冲全文件；
- control/quarantine/revoke/delete 使用保留 capacity；
- Artifact/S3 饱和不能耗尽 API/Scheduler/Model/MCP/Sandbox control DB pool；public upload/download、Workload/Model/Sandbox read Broker、
  Workload Producer、Maintenance Authority与Model Artifact Producer任一permit/DB pool耗尽不能占用其他lane，反向同理。Worker在claim前必须持有本地future-stage slot和
  durable reservation；独立Producer仍在真正stage时以自己的服务端permit准入。dispatch后发生的Producer瞬时饱和只能在同Attempt、
  同bytes/digest内有界重试stage，不能借用其他lane、形成无界客户端buffer或仅为物化重放Provider。

## 21. 安全、租户与加密

- 所有 Artifact/Blob/Grant/Reference/Evidence/Cache query 同时限定 tenant；
- object store policy 只允许 Artifact service workload identity，禁止 tenant 直接 list bucket；
- at-rest encryption 使用 tenant/security-domain scoped key；key ref 在 KMS，数据库不保存 key value；
- in-transit 使用 TLS/mTLS；proxied public upload、Workload/Model staging及Gateway/Broker grant都绑定exact closed
  capability/object/audience/subject/deadline，客户端与普通worker永不直连object store；
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

16是`ModelArtifactProducer` trait、`StageModelOutput` method及其frame/header/receipt/failure wire的唯一owner；15只规定§15.1的Artifact侧
实现权限、state transition与evidence语义，不复制第二份port签名。该client-streaming contract不能退化为
`Stream<Bytes>`加任意metadata map，也不能由只读Artifact Broker实现。failure必须使用16的closed reason/disposition DTO，
并与§15.1错误表一一映射；自由错误文本、backend状态、locator、grant或正文不得进入wire。

Artifact Worker不持有BlobStore/S3/KMS credential，只调用Artifact Maintenance Authority的三个typed port：
`ReadForScan(ScanRead)`、`HeadExactGeneration(HeadExactGeneration)`与`DeleteExactGeneration(DeleteExactGeneration)`。网络/对象读取/扫描发生在
数据库事务外；authority返回bounded bytes/evidence后，worker用原Job fence提交。duplicate-Blob候选回收使用owner为`InternalBlob`的cleanup Job，
必须验证exact object generation、backend receipt与absence evidence后才把Blob推进Deleted；不能由bucket inventory或人工脚本直接写数据库状态。

Domain contract 不依赖具体 S3 SDK。`BlobStore`只是上述受信Gateway/Broker/Maintenance/Producer进程内部的private adapter port，不得暴露为
Worker service或generic RPC，也不接收 principal/authorization decision；各authority先验证并只传exact opaque object operation。
Artifact repository 与 owner repository 必须能在同一 PostgreSQL 事务
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
idempotency、etag/generation、closed schema 和 stable error。`issue-download`要求Idempotency-Key，Receipt operation exact为
`artifact.download_grant.issue.v1`，并只返回平台Artifact Gateway地址与opaque gateway token；credential标记`no-store`且不进入响应缓存或事件，
不返回object-store URL/credential。same key/digest按§12重建byte-identical sealed token，different digest不签发第二grant。异步verify/rescan/delete使用03/17统一
`/v1/operations/{operation_id}`，不定义Artifact专用Operation。

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

## 24. Persistence 边界

Artifact 领域只拥有Artifact、Blob与Artifact Link三个逻辑聚合：Artifact保存current lifecycle/metadata，Blob保存
content-addressed backend fact，Link以closed kind表达reference、grant、hold、provenance与operation target。具体物理名称、列、索引、表数与
schema contract version只由ADR和database contract决定，不能从本业务规范生成额外写authority。upload、
scan、rescan、delete 与 GC 使用共享 Job；command/use/backend callback 使用 Receipt；历史与安全结果使用 Event；需要人工授权
使用 Task。大正文始终在 object store，数据库只保存 bounded typed metadata。

`WorkClass::Artifact` 的 Job owner 是 closed union：面向调用方的 upload/scan/rescan/delete Operation 由 exact
`ManagementOperation` 拥有；不再对应可继续执行的调用方 Operation、且只负责回收候选 object generation 的内部 cleanup Job
由 exact `InternalBlob` 拥有。两者都必须通过 machine owner-pair registry，禁止 `artifact`、`artifact_blob` 等任意字符串 owner。

所有Artifact Job使用一个closed tagged payload union：`scan | rescan | delete | blob_cleanup`。scan/rescan由exact
ManagementOperation拥有，delete由exact ArtifactDelete ManagementOperation拥有，blob cleanup由exact InternalBlob拥有；每个variant
分别校验owner、object generation、policy/contract/evidence字段，generic Job decoder不得只按JSON形状猜测variant。

逻辑事实按以下规则唯一归属：Artifact 保存 prepare admission（expected size、optional expected digest、optional declared media）、
verified media、retention、creator 与 exact Blob reference；Blob 单独保存 verified content digest/byte length、object generation、
storage binding 与由classification、exact retention revision、encryption domain组成的closed security-domain digest。Staging
阶段允许未知 digest/media/generation，不能写 sentinel 冒充验证结果；构造 Ready
ArtifactRef 时解析exact tenant-scoped Blob，不在 Artifact 重复拥有 Blob digest/size。FK/constraint/index等物理实现必须服从ADR，不能改变这里的
唯一逻辑owner。

Model Artifact Producer不增加逻辑aggregate：预留/Staging/Verified事实仍由上述三类Artifact aggregate与04两个共享Quota bundle拥有，stage幂等写共享
`Receipt`；需要持久化的历史/安全证据只能由Model terminal或既有Artifact cleanup/incident authority写共享`Event/Outbox`，Producer
本身不得写二者。Model terminal事务复用既有Invocation/Job/RunValue/ArtifactLink/Receipt/Event/Outbox。
不得为Producer另建attempt、upload session、transition、evidence、orphan或terminal表，也不得把object store tag/queue消息当current authority。

### 24.1 已撤销 persistence 记录（非规范性）

旧Artifact专用Operation/Attempt/transition/evidence持久化族及其repository checkpoint已撤销，不属于当前baseline、实现状态或资格证据；
具体物理记录只保留在Git历史与ADR，不在本规范重复。

当前目标只承认本节定义的Artifact、Blob、Artifact Link三个逻辑aggregate与03的共享
`Job`、`Task`、`Receipt`、`Event` 聚合。Phase 3 的 prepare、CompleteUpload、Begin/CompleteVerification与
FinalizeAndReference已由closed Rust state machine和caller-owned PostgreSQL transaction实现；fresh fixture覆盖grant/object
generation/size/digest fence、stale CAS、Ready-only ArtifactRef、reference与staging quota settle。ArtifactLink hold/provenance、
tenant内shared Blob dedupe也已按完整security-domain key通过顺序与双事务并发fixture，候选对象以cleanup Job收敛且跨安全域
不复用。hold/provenance/reference release与shared Blob两阶段删除的closed domain/repository transaction已经实现；fresh PostgreSQL 16
fixture覆盖GC grace、exact approval、live link阻塞、same-Blob alias witness、Job fence、exact object generation、backend/absence evidence、
replay及Event/Receipt/Outbox原子闭合。CR-130要求的Artifact Job union、worker audit/current scan evidence、rescan与cleanup
completion也已通过23项domain/worker fixture和fresh PostgreSQL 16 transaction fixture：rescan排队先进入Quarantined，只有exact
WorkerProcessGeneration/Job fence可提交新证据，delete/blob cleanup必须匹配exact object generation、backend receipt与absence evidence。
既有开发期实现已分别证明Model、WASI与microVM closed read authority、exact Model/Sandbox URI SAN、bounded stream、只读repeatable-read authority以及
Job/Secret数据库越权拒绝；但旧实现/fixture仍使用旧Grant shape，且不能证明本次目标的Workload/Model/Sandbox三个read Broker、Workload Producer、
Maintenance Authority、Model Producer与public Upload/Download Gateway八个物理role隔离。新增`ArtifactWorkloadBrokerService`、
`ArtifactWorkloadProducerService`五个stage method及其`StageWorkloadArtifact*` wire、scanner/GC去S3/KMS credential、Maintenance exact-generation
RPC、public Gateway物理拆分、双向mTLS/NetworkPolicy、独立DB/storage identity/permit/HPA、逐lane饱和和rolling门禁都只是CR-165 Draft目标，
当前不存在交付证据。Sandbox Controller仍不得持有object-store/KMS credential或对应直出网络。

Artifact-backed output、真实object-store/KMS负向资格、公开 `/v1` 和对应qualification同样尚未交付，不能由当前开发期fixture或旧候选记录推断为
当前行为。尤其是Model Artifact Producer进程、`StageModelOutput`、restricted DB write role、独立S3/KMS identity/two-phase permit、
expected-version lower-bound+generation authorization、canonical JSON verifier、dedupe/failure/双quota-bundle closure以及Model terminal Artifact
first-winner事务均未实现。既有Model/Sandbox read authority不得扩展RPC或数据库权限来冒充Workload/Maintenance/Producer任一新role。

## 25. 可观测性与隐私

```text
artifact_operations_total{operation,outcome,purpose}
artifact_bytes_total{operation,size_bucket}
artifact_state_total{state,purpose}
artifact_scan_duration_seconds{profile,outcome}
artifact_scan_backlog_total{profile}
artifact_grant_total{grant_kind,outcome}
artifact_gc_total{state,outcome}
artifact_integrity_incident_total{class}
```

tenant/Artifact/digest/filename/media detail/object key/owner 不进入 metric label。Trace 记录 operation、state、bytes、
storage binding/validator revision的受控hash、latency和reason class，不记录正文/URL/grant。审计覆盖prepare、read/download、
share/export、quarantine release、hold、delete 和 break-glass。

## 26. 配置与部署

- PostgreSQL 是 metadata/reference/grant/lifecycle 权威；S3-compatible store 是 blob 权威；
- Artifact Workload/Model/Sandbox三个read Broker、Artifact Workload Producer、Artifact Maintenance Authority、public Artifact Upload Gateway与public Artifact Download
  Gateway分别使用独立Deployment、ServiceAccount、restricted DB pool、storage identity、permit与HPA；Upload只持有exact staging PUT/multipart
  权限，Download只持有exact-generation HEAD/GET权限。Ingress保留一个public Artifact Gateway hostname并按closed route分流，不合并物理角色；
  Scanner/Finalizer与GC/Reconciler Worker没有S3/KMS credential，只通过Maintenance Authority；Transformer使用独立Deployment/permit；
- Artifact Workload Producer使用独立write-limited DB pool、staging-only S3/KMS identity、mTLS endpoint、stream/byte/tenant permit与HPA；只允许
  17五个exact workload method，不能借public Upload Gateway或Model Producer的credential/pool；
- Model Artifact Producer使用另一独立Deployment、ServiceAccount、restricted DB write pool、S3/KMS workload identity、mTLS endpoint、
  two-phase admission permit与transport backlog hard cap，只允许exact Model Worker调用`StageModelOutput`；它与三个只读Broker、Maintenance
  Authority、Workload Producer及Model Worker均不得同Pod或共享credential/pool；
- scanner/transformer 使用 14 Sandbox node pool，不在 Gateway/API 解析复杂文件；
- bucket 默认 private、versioning/object-lock/replication 按环境 policy，禁止静态 public website；
- Artifact service 使用最小 bucket/KMS identity，不与 Sandbox/Model/MCP 共享 credential；
- storage backend、bucket/region和encryption/KMS binding由18 CandidateManifest固定为installation-scoped digest，
  不存在tenant ArtifactBackend Entity或公共active head；scanner/retention规则使用immutable Policy Revision；
- rolling deploy 不改变已签发 grant 语义，protocol generation 不兼容时先停止签发并 drain；
- readiness 区分 metadata、upload、scan、download 和 GC，单 scanner backlog 不使 Runtime API 全局 unready。

## 27. 测试矩阵

- proxied streaming upload/download、multipart、resume、duplicate complete/finalize 和 grant expiry；
- Grant variant/state fixture覆盖read-whole/read-range/staging-write的unknown/null/cross-variant及read/write混合拒绝、Active→Revoked、正数
  generation/version/max、derived exhaustion、atomic use counter边界与并发single-use public token；I/O前crash不退款，耗尽后仍可revoke并提升
  generation，同ticket不二次消费但在首chunk、每个bounded dispatch/授权间隔和terminal重验consume后version/use ordinal及完整current
  owner/evidence/Policy/security；consume-vs-revoke与I/O中revoke停止后续dispatch，Range/重连必须新grant，revoked/exhausted-derived/expired/旧
  generation/version均fail closed；
- PUT 成功/DB 失败、DB response 丢失、scanner crash、outbox 丢失、object store timeout；
- digest/size/media mismatch、truncated object、multipart swap、object generation overwrite；
- path traversal、symlink、archive bomb、zip slip、malware、macro、SVG/HTML active content、parser crash；
- cross-tenant ID/digest/object key/grant/cache/dedupe timing isolation；
- finalize/quarantine、read/revoke、reference/GC、legal hold/delete、restore/delete竞态；
- issue-download与每次proxied read都覆盖current Active encryption binding exact match、Rebind/Revoke、generation/digest漂移、content evidence
  到期及grant重放；签发token后再发生Rebind/Revoke/evidence expiry时，新Range/重连仍fail closed，当前stream停止后续chunk且明确不承诺撤回已发送
  前缀；完整性晚失败不产生success terminal/audit，且不存在可绕过Gateway/Broker的旧object-store URL；delete/GC/incident cleanup仍可按冻结object
  generation收敛；
- Sandbox/MCP/Context/Model grant audience/port/purpose swap；
- Workload/Model/Sandbox Broker endpoint、method-specific URI SAN、ServiceAccount、DB credential和config互换，以及单audience饱和/重启；
- public Download Gateway与三个internal Broker的token/mTLS/request互换全部在locator/KMS前拒绝；四者DB/storage identity、permit与饱和隔离，
  Gateway不能注册internal Broker RPC且Broker不能接受public grant token；
- public Upload/Download Gateway route、ServiceAccount、DB/storage/KMS credential、permit与HPA互换全部fail closed；任一进程故障、滚动或
  credential compromise不使另一物理role失效或取得相反方向object权限；
- Workload Producer五个method的JobAttempt/WorkloadBound/audience/SAN互换均在读取正文或对象I/O前拒绝；它最多形成Uploaded与scan wake，
  不能调用read Broker方法、处理Model output、Verified/Ready/finalize或取得其他lane credential；
- `StageWorkloadArtifact*` fixture逐项调用五个exact method并覆盖kind/SAN/Artifact owner/Job typed owner/JobKind/WorkClass映射、header/data/terminal顺序、
  0/N/N+1 chunk/bytes/deadline、Header不预知body facts的直接stream、Terminal actual length/digest、attempt binding→Grant authorization→request
  core→terminal commitment四层digest无环重算、body mismatch、Processing/dependency Deferred、same/different core与final digest Receipt replay、
  PUT/DB/response-loss、stale Attempt fence、integrity quarantine与
  exact-generation cleanup；任何失败窗口都不能形成Ready/Reference或泄露locator/content；
- internal read fixture证明encrypted ephemeral spool受request/tenant/bytes/deadline permit约束，terminal前不能被Provider/guest/handler使用，授权或
  integrity失败会secure-delete；public read fixture证明one-chunk buffer、逐chunk授权与partial-response failure语义，不以Broker全内存buffer实现；
- Maintenance的scan/head/delete request variant、scanner/GC URI SAN、Job/lease/Worker generation、owner/lifecycle/policy/object generation互换均
  fail closed；Artifact Worker没有BlobStore/S3/KMS路径，Maintenance Authority不能注册普通read、upload、stage或generic object API；
- Model output claim前slot+weighted bytes/ID/双bundle quota预留失败不会claim/start或调用Provider；少领、claim失败、Inline/取消和crash
  按no-object/candidate cleanup事实释放，不泄漏或提前释放reservation；
- `StageModelOutput` exact `model-worker.artifact-output` URI SAN、client-stream header/chunk/terminal顺序、chunk/total limit，以及read Broker/Producer
  endpoint、ServiceAccount、DB role、S3/KMS identity和permit互换的负向门禁；
- Model output duplicate JSON key、尾随字节、非canonical JCS、schema/evidence/media/classification/digest/length/KMS context/object
  generation漂移全部fail closed；secret/content canary不进入日志、错误、Event或receipt；
- Model output content-validation registry/profile覆盖1/64/N+1、排序/重复、unknown/null、descriptor digest、validator/rules/response-contract/
  evidence-version漂移、validity 0/上限/N+1与`observed_at/expires_at` checked arithmetic；过期证据不能Ready/read；
- Producer pre-I/O授权后cancel/heartbeat/lease takeover/Worker restart/terminal first-winner的竞态，以及post-I/O fresh fence规则；
- PUT成功/DB失败、Uploaded后crash、Verified receipt响应丢失、Model terminal commit冲突/响应丢失、stale Verified orphan与exact-generation GC；
- preexisting Blob hit、并发candidate new/race winner、candidate cleanup先于/晚于Model terminal、Artifact先删但shared Blob仍有alias、最后alias
  physical refund分别验证两个quota owner与settlement identity，不双退、不泄漏；
- InProgress/Dependency transient、fresh Stale/Deadline、TooLarge/Invalid Rejected、Integrity Failed+Quarantined及Conflict不改existing Receipt
  的每个failure persistence分支均覆盖response loss与claim-generation takeover；Integrity分别从candidate current Staging、Uploaded、
  Verifying进入Quarantined，且都不能绕过incident/rescan guard晋升Ready；
- 同Attempt相同core+final stage digest重放得到同一receipt，core相同但正文/final不同仍冲突；新Attempt不能adopt旧Attempt Artifact/grant/receipt；
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
- S3 object 永不 public；public upload grant短期且绑定exact closed capability/object/audience并只经Upload Gateway代理，download grant只能用于
  Download Gateway/Broker代理且不能换取S3 bearer；
- 跨 tenant 相同 digest 不 dedupe、不共享 scan/cache/preview 并不泄露存在性；
- scanner 未通过、失败或超时的内容不能进入 Ready；
- scan/rescan/delete/cleanup全部由exact Artifact Job lease fence提交，stale worker不能覆盖current evidence或对象generation；
- rescan排队即撤销普通可读状态；通过后才恢复Ready，失败/超时保持Quarantined；
- Quarantine/revoke 可以阻止已有 Artifact 新读取且不破坏审计 Reference；
- Reference/retention/hold/grant 任一存在时 GC 无法删除；
- Sandbox 输出只能写 staging，不能自置 Ready/classification/object key；
- Workload、Model与Sandbox Artifact Broker不能共享进程、Pod、ServiceAccount、DB pool或permit，任一audience饱和不阻断其他read audience；
- public Gateway upload/download、三个read Broker、Workload Producer、Maintenance Authority与Model Producer八个物理role彼此独立饱和时，
  其他Artifact/control lane仍可准入；
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

CR-165的current encryption fence、content-validation profile/evidence与Model output Producer合同仍需与04/16/17/18共同完成cross-review；关闭前
本规范保持Draft且不得作为实现输入。具体S3-compatible产品、scanner和transformer可替换，但PostgreSQL lifecycle/reference权威、tenant
隔离、prepare/finalize、Ready-only引用和两阶段GC不得弱化。
