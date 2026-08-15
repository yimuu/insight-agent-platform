# Platform v2 标识、资源版本与部署规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) |
| 直接下游 | 03、04、05、07、09、11、12、13、14、15、16、17、18 |

## 1. 决策摘要

Agent、Skill、Capability、Context、MCP、Model、Policy与Sandbox复用一个逻辑Resource生命周期，领域语义由
`resource_kind`对应的Rust nominal type与closed schema保证。作者配置发布为不可变ResourceVersion；只有需要endpoint、
credential、runtime、network或environment binding的版本才创建Deployment。Resource保存唯一当前管理状态、draft和active
pointer，不为每个领域复制Entity/Draft/Revision/Validation/Head/Suspension表族。

Run admission一次性冻结所用version/deployment及其digest；运行时不追随active、latest、selector或重新discovery。

## 2. 目标与非目标

### 2.1 目标

- stable、opaque、tenant-scoped identity；
- 作者语义与环境部署分离；
- draft CAS、validate、publish、deploy、activate、suspend、retire均有closed command；
- 历史Run可精确读取原version/deployment；
- 相同生命周期由共享repository实现，领域验证仍保持强类型；
- 规范化snapshot与digest检测内容一致性，但不代替授权；
- 不把相同事实复制到active-head、evidence和reference projection。

### 2.2 非目标

- 不支持可变published version；
- 不支持运行时`latest`、通配符或未冻结failover；
- 不把Artifact URL、Secret value、实时健康或延迟写入作者语义digest；
- 不用数据库自增值作为公开ID；
- 不通过物理删除历史版本实现撤销；
- 不使用无schema、无上限的JSON extension bag。

## 3. ID合同

公开ID采用`{prefix}_{uuid-v7}`，UUID符合RFC 9562、canonical lowercase。ID不编码tenant、region、名称、shard或
security classification。边界必须同时验证prefix、UUID形状和字段期望的resource kind；未知prefix fail closed。

完整且有序的prefix集合只由
[`contracts/platform-v1/registries.json`](../../../contracts/platform-v1/registries.json)中的`resource_kinds`
定义；规范正文不复制第二份可漂移的注册表。下表仅说明逻辑分类，不是prefix registry：

| 类别 | Prefix |
|---|---|
| Installation / Tenant / Principal / PrincipalSnapshot | `ins` / `ten` / `prn` / `psn` |
| Resource | `agt`、`skl`、`cap`、`cim`、`ctx`、`xim`、`mcp`、`mpr`、`mdl`、`pol`、`srt`、`spk`、`sxp` |
| ResourceVersion | `aif`、`arev`、`srev`、`cirev`、`cimp`、`xirev`、`ximp`、`mrev`、`mprev`、`mdrev`、`prev`、`srrev`、`sprev`、`sxrev` |
| Deployment | `adep`、`cdep`、`xdep`、`mcdep`、`mpdep`、`mdep` |
| Runtime | `run`、`nex`、`inv`、`job`、`evt`、`rcp` |
| Human Task | `apr` / `int` |
| Artifact / Blob | `art` / `blb` |
| Secret | `spr` / `sbd` |
| Tenant security | `enc` |
| Correlation | `req` |

新增或改变prefix是公共machine-contract变更。`PlanNodeId`、`SlotId`、`FieldId`和`ModelCallId`只在owner内稳定，
使用owner ID加bounded local key，不冒充全局资源。External task identity、provider ID、object generation、cursor、ETag、
idempotency key和digest也不是ResourceId。

平台不存在generic `RevisionId` alias。所有共享Resource不可变版本的裸nominal统一为`ResourceVersionId`，并在字段边界按expected
`ResourceKind`与prefix registry复验；凡授权、binding、publish或recovery还需要防止同ID正文漂移时，必须使用本规范`ExactVersionRef`
同时携带version ID与canonical digest。domain行文中的“revision”只是不可变ResourceVersion的业务名称，不能生成第二套ID/type/schema。

CR-165 clean-cut目标把`encryption_domain/enc` exposure从internal改为public，因为04/17会在tenant-authorized Management DTO中返回/接收该
opaque nominal；同时从目标registry删除没有shared Task owner的internal `task/tsk` kind。03共享human Task直接按kind使用public
`approval_task/apr`或`interaction/int`，不存在`tsk_` alias或第二ID。当前checked-in registry尚未应用这两个Draft变更，因此新增
encryption-domain route仍不是当前API；实施时必须在同一registry/schema/Rust exhaustive fixture切片原子替换，不能保留双ID兼容。

部署组件角色统一使用本规范拥有的nominal `ComponentRole`，不能由Worker、Candidate或Helm各自复制字符串validator：

```rust
#[serde(transparent)]
struct ComponentRole(String);
```

wire必须是1～128 ASCII bytes并匹配`^[a-z][a-z0-9_.-]{0,127}$`；构造时不做大小写、Unicode或别名归一化。公共machine
schema固定为`contracts/platform-v1/schemas/common/component-role.schema.json`并进入根contract digest，所有下游schema必须引用同一
定义。`ComponentRole`标识稳定的安装Deployment逻辑scope，同一scope的replica共享该值；它不等于Pod名、临时副本ID、
WorkerProcessGeneration的`worker_role`或component kind。

installation current-state owner和所有跨领域region字段同样只使用本规范拥有的公共nominal：

```rust
#[serde(transparent)]
struct InstallationId(ResourceId); // inner ID registry kind/prefix exact installation/ins；不是tenant Resource aggregate kind

#[serde(transparent)]
struct CanonicalRegion(String);

#[serde(transparent)]
struct Etag(String); // etg1_<43-char base64url-no-pad SHA-256>
```

`InstallationId`的wire使用`ins_` UUIDv7；它唯一标识一个安装，不是workload/service identity、tenant、Release或临时集群实例。
旧`installation_service/svc`没有独立生命周期且当前未被业务authority使用，clean-cut目标直接由`installation/ins`替换，不能同时保留
两个ID表示同一安装。`CanonicalRegion`必须为1～63 ASCII bytes并匹配
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`；不做lowercase、provider alias、Unicode normalization或`_`兼容。
公共schema固定为`contracts/platform-v1/schemas/common/installation-id.schema.json`和
`contracts/platform-v1/schemas/common/canonical-region.schema.json`并进入根contract digest。Capability、Context、Worker、Artifact、Model、
Candidate与startup document的region必须复用该nominal/schema，不得再定义字符集不同的`DataRegion`。

`Etag`是全平台唯一opaque strong-validator nominal，value exact为`etg1_`加32-byte SHA-256的43字符无padding base64url；domain owner定义
versioned closed preimage并使用domain separator，调用方不得解析。JSON/内部DTO只保存该value；HTTP `ETag`/`If-Match`必须使用一个RFC 9110 quoted
strong tag `"<value>"`，禁止weak tag、`*`、comma list、裸值、转义别名或多header合并。schema固定为
`contracts/platform-v1/schemas/common/etag.schema.json`并进入root contract digest。03/04/15/17/18都复用这一nominal，不得再以自由String定义ETag。

除installation-scoped singleton外，每个持久对象都有`tenant_id`。Repository predicate必须同时使用tenant和object ID；
公开404不区分不存在与跨租户不可见。

## 4. 共享Resource生命周期

```rust
enum ResourceKind {
    Agent,
    Skill,
    CapabilityInterface,
    CapabilityImplementation,
    ContextSourceInterface,
    ContextSourceImplementation,
    ContextDataset,
    McpServer,
    ModelProvider,
    ModelProfile,
    Policy,
    SandboxRuntime,
    SandboxPackage,
    SandboxProfile,
}

enum ResourceLifecycle { Active, Archived, Retired }
enum AdministrativeGate { Enabled, Suspended }

struct Resource {
    resource_id: ResourceId,
    tenant_id: TenantId,
    kind: ResourceKind,
    name: String,
    display_metadata: BoundedMetadata,
    lifecycle: ResourceLifecycle,
    gate: AdministrativeGate,
    draft: Option<VersionedDocument>,
    active_target: Option<ActiveTarget>,
    version: u64,
}
```

`ResourceKind`的wire名称与prefix只由`contracts/platform-v1/registries.json`拥有；上述Context variants精确编码为
`context_source_interface`、`context_source_implementation`与`context_dataset`。12为行文使用的`ContextInterface*`领域类型是前两种
Resource/Version payload，不得依赖serde默认生成另一个`context_interface` kind；Dataset Generation是`ContextDataset` root下的immutable Version。

唯一生命周期：

```text
Active -> Archived | Retired
Archived -> Active | Retired
Retired -> terminal

Enabled <-> Suspended
```

Lifecycle、gate和active target是Resource aggregate的三个独立字段，不能互相推断。Suspended立即阻止新admission和尚未
开始的leaf；已dispatch且可能产生Effect的工作进入cancel/reconciliation规则，不能假装从未发生。

## 5. Draft、Validation与Publish

每个Resource最多一个mutable draft。Draft是Resource当前authoring state，而不是独立业务aggregate：

```rust
struct VersionedDocument {
    schema_version: u32, // const 1; wrapper contract, ResourceSpec has its own registered version
    document: ResourceSpec,
    canonical_digest: Digest,
    size_bytes: u32,
}
```

- draft mutation携带expected Resource version/ETag；语义变化只推进一次version；
- `ResourceSpec`是按ResourceKind分派的closed Rust enum，不接受generic map；
- JSON拒绝重复key、未知字段、非有限数和越界集合；
- validation由独立`RegistryValidation` WorkClass/Worker role在事务外执行外部discovery/build；它不得借用Artifact、
  Interaction或Orchestration的queue、permit与worker identity，在完成事务中重新CAS draft digest；
- validation result是bounded typed value，包含validator版本、stable errors/warnings、dependency与security结果；
- publish在一个事务中确认draft未变，创建不可变ResourceVersion并记录Event/Receipt；
- validation不需要独立长期对象身份；发布所需证据嵌入ResourceVersion，长诊断正文存Artifact；
- publish exact replay返回同一version，不生成重复逻辑版本。

## 6. ResourceVersion

```rust
struct ResourceVersion {
    version_id: ResourceVersionId,
    tenant_id: TenantId,
    resource_id: ResourceId,
    resource_kind: ResourceKind,
    ordinal: u64,
    schema_version: u32,
    spec: ResourceSpec,
    semantic_digest: Digest,
    validation: ValidationSummary,
    created_by: PrincipalSnapshotRef,
    created_at: DateTime<Utc>,
}

struct ResourceSpecDigestPreimageV1 {
    schema_version: u32, // const 1
    resource_kind: ResourceKind,
    spec: ResourceSpec,
}

struct ExactVersionRef {
    version_id: ResourceVersionId,
    resource_kind: ResourceKind,
    semantic_digest: Digest,
}
```

- version发布后immutable；修正产生新version；
- ordinal在单Resource内严格递增但不是身份；
- version必须与owner tenant/kind一致；
- validation summary与spec/digest一起immutable；
- metadata-only display修改留在Resource，不改变运行语义；
- version被Run、Deployment或保留期内Event引用时不可hard delete。

`VersionedDocument.canonical_digest`与`ResourceVersion.semantic_digest`唯一使用同一公式：
`SHA-256(UTF8("insight.resource-spec.v1") || 0x00 || JCS(ResourceSpecDigestPreimageV1))`。preimage中的kind从owner Resource读取，
`spec`必须是该kind唯一registered `ResourceSpec` variant；两种digest逐值相等。`VersionedDocument.size_bytes` exact为`JCS(spec)` byte length，
不含preimage wrapper。formula排除digest自身、Resource/version ID、tenant、ordinal、validation、creator/time、display metadata、lifecycle/gate/active target；
这些wrapper/current facts不能改变payload semantic identity。`ExactVersionRef`必须逐值匹配same-tenant immutable row的ID、kind和重算digest；只匹配
ID或调用方digest都不足。schema固定为`contracts/platform-v1/schemas/common/exact-version-ref.schema.json`，preimage/所有Spec schema进入root digest。

## 7. Deployment

Deployment冻结环境相关绑定：

```rust
struct Deployment {
    deployment_id: DeploymentId,
    tenant_id: TenantId,
    resource_version_id: ResourceVersionId,
    resource_kind: ResourceKind,
    schema_version: u32,
    spec: DeploymentSpec,
    deployment_digest: Digest,
    gate: AdministrativeGate,
    version: u64,
}

struct DeploymentDigestPreimageV1 {
    schema_version: u32, // const 1
    resource_kind: ResourceKind,
    resource_version: ExactVersionRef,
    spec: DeploymentSpec,
}

struct ExactDeploymentRef {
    deployment_id: DeploymentId,
    resource_kind: ResourceKind,
    deployment_digest: Digest,
}
```

DeploymentSpec至少包含适用的exact dependency versions、implementation、adapter/protocol、endpoint identity hash、
04定义的`ExactSecretBindingRef`、runtime/image/module digest、Policy versions、network/isolation/resource limits和
conformance summary。
不保存Secret value、短期token或实时health。

`deployment_digest = SHA-256(UTF8("insight.deployment.v1") || 0x00 || JCS(DeploymentDigestPreimageV1))`；preimage使用同一row解析并
重验的exact ResourceVersion ref与该kind唯一closed DeploymentSpec。它排除deployment ID、tenant、digest自身、mutable gate/version、health、time与审计字段。
`ExactDeploymentRef`必须逐值匹配same-tenant Deployment ID、kind和重算digest；schema固定为
`contracts/platform-v1/schemas/common/exact-deployment-ref.schema.json`。DeploymentSpec或dependency中的每个exact ref必须先按其owner公式重验，
不得把嵌套caller digest当事实。unknown/null/cross-kind、self digest、field omission或canonical set排序漂移都fail closed。

需要Deployment的kind由closed matrix规定。Skill、Policy及纯作者资产通常直接引用ResourceVersion；Agent、Capability、Context、
MCP、Model以及带环境执行的Sandbox必须通过Deployment。运行时不能把失败自动路由到未冻结Deployment。

## 8. Active target与解析

Resource active target是同一aggregate内的CAS pointer：

```rust
enum ActiveTarget {
    Version { version_id: ResourceVersionId, digest: Digest },
    Deployment { deployment_id: DeploymentId, digest: Digest },
}
```

ResourceKind决定允许的target kind。Publish/deploy不自动activate；activate/rollback必须携带expected Resource version。
切换只影响未来resolution。active为空、Archived、Retired和Suspended是不同状态，并产生不同stable error/event。

## 9. Run绑定快照

Run admission把解析结果写入一个bounded immutable snapshot，而不是为slot、candidate、policy和reference分别复制projection：

```rust
struct InstallationReleaseBindingV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    release_id: ReleaseId,
    release_manifest_digest: Digest,
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
    compatibility_generation: u64,
    installation_state_digest: Digest,
}

struct RunBindingsSnapshot {
    schema_version: u32, // const 2
    installation_release: InstallationReleaseBindingV1,
    agent: ExactDeploymentRef,
    agent_interface: ExactVersionRef,
    plan: ExactVersionRef,
    principal: PrincipalSnapshot,
    slots: Vec<FrozenSlotBinding>,
    context_dataset_views: Vec<RunContextDatasetView>,
    policies: Vec<ExactVersionRef>,
    execution_profile: ExactVersionRef,
    canonical_size_bytes: u32,
    canonical_digest: Digest,
}
```

`FrozenSlotBinding`继续使用`contracts/platform-v1/schemas/frozen-slot-binding.schema.json`的closed discriminator。Model、
Capability、ChildAgent和Skill候选必须同时冻结Selection Policy；Context冻结exact binding。集合规范排序且受HardLimitProfile约束。

`installation_release`对每个Run都required且`null`非法。ID kind必须分别为`ins/rel/cand`，generation必须为正；ReleaseManifest中的
Candidate ID/digest、两个manifest canonical digest及18 current-state digest必须逐字段匹配。没有Active Release的installation不能创建
root Run。`RunBindingsSnapshot` clean-cut只接受version 2，不保留version 1 reader、optional field或fallback。

RunBindings canonical preimage包含`installation_release`、`context_dataset_views`及其余业务字段，但排除
`canonical_size_bytes/canonical_digest`；前者等于JCS preimage byte length，后者为其SHA-256。machine常量
`MAX_RUN_BINDINGS_CANONICAL_BYTES=1048576`和`MAX_RUN_MODEL_DEPLOYMENT_REFS=512`分别约束完整snapshot及跨全部Model slot按
Deployment ID去重后的候选集合。候选数组按Deployment ID严格升序且唯一；偏好、权重或随机语义只能来自exact Selection Policy，不能来自
偶然输入顺序。同一Deployment ID若在多个slot出现可以只验证一次，但digest必须一致。

公共machine schema固定为`contracts/platform-v1/schemas/common/installation-release-binding.schema.json`和
`contracts/platform-v1/schemas/run-bindings-snapshot.schema.json`并进入根contract digest。

Admission事务：

1. 在mutation transaction外先拒绝非version 2 shape，按exact ID/digest解析immutable Release/Candidate及其依赖正文并做bounded pure
   prevalidation；构造按Deployment ID排序去重的确定性Model candidate集合，先拒绝同ID/different digest、超过512项、canonical size超过
   1 MiB及算术溢出。root只捕获一个observed current binding，child只读取parent snapshot。该阶段不决定current bindability、不写Receipt，
   也不持有数据库锁；
2. 下游Run command进入03 caller-owned transaction并先claim/replay Receipt。root随后按全局rank先锁18唯一current
   InstallationReleaseState并逐字段匹配observed binding；child不锁current installation authority，而是准备在Tenant security rank之后锁parent Run；
3. 按03 rank锁定current Tenant security aggregate，再锁parent Run及按kind/ID排序的Resource gate/exact version/deployment。root验证current
   active target/bindability；child从已锁parent bindings继承同一exact `InstallationReleaseBindingV1`，复用parent frozen closure并重验current
   security fence，不要求这些exact ref仍是current head或对current Release bindable；
4. 对步骤1同一确定集逐项复验exact Deployment generation/digest；锁后重算canonical set/count/size必须byte-identical，否则回滚；
5. 对集合中每个Model Deployment的tagged output closure调用16同一installation compatibility port。任一候选不兼容时整个admission
   回滚；不得静默删候选、提前选择一个候选或改写Selection Policy；
6. 所有validation结果必须返回与步骤2/3逐字段相同的installation binding；构造typed snapshot并计算canonical size/digest；
7. root在提交前对已锁InstallationReleaseState复验generation/state digest；child对已锁parent binding复验一致，并确认对应immutable Candidate
   仍可由18 resolver读取且历史runtime/adapter仍在保留期；
8. 与Run、Receipt和Event原子提交。serializable/internal竞态按17规则bounded retry，任何路径都不得先锁tenant Resource再反向取得
   InstallationReleaseState。

Run之后只读取snapshot中的exact ref；active target变化、重新发布或discovery均不改变该Run。

## 10. 规范化与Digest

- JSON采用RFC 8785 JCS；
- digest格式`sha256:<lowercase-hex>`；
- 时间、随机ID和审计字段不进入semantic digest；
- schema明确哪些数组是有序序列、哪些是canonical set；
- snapshot保存`schema_version`、canonical byte length和digest；
- 大型document/diagnostic使用ArtifactRef，不能突破JSONB hard limit；
- digest证明内容一致，不授予读取、调用或跨租户复用权限。

## 11. Mutation与幂等

Mutation在principal与tenant scope解析后使用03统一Receipt：

```rust
struct MutationContext {
    idempotency_key: IdempotencyKey,
    request_id: RequestId,
    principal: PrincipalSnapshotRef,
    expected_version: Option<u64>,
    request_digest: Digest,
}
```

相同scope/principal/command/key与相同digest返回同一结果；不同digest返回`idempotency_conflict`。外部validation、discovery和build
先创建Job，不能在持有Resource锁时执行网络I/O。

## 12. 持久化边界

本规范定义逻辑aggregate，不规定专用表族。参考baseline可以用共享Resource、ResourceVersion和Deployment三类存储实现；
validation、active target、draft、suspension和binding不得再次成为同一事实的独立可写权威。为了热点查询提升出的generated
column或index是物理优化，不获得新的domain写入口。

## 13. 删除与保留

- 未发布draft可以显式清空；
- Published version与Deployment默认archive，不在线改写；
- Retired Resource不允许新draft/deployment，历史读取保留；
- 合规清除通过Artifact/Secret crypto-erasure与审计流程；
- GC只处理repository证明不可达且超过retention的对象；
- 删除Resource不级联删除仍被Run、Event、Artifact或Deployment引用的内容。

## 14. 验收标准

- 并发draft mutation只有一个CAS成功；
- publish/deploy/activate重放不产生重复逻辑对象；
- 每个ResourceKind的错误spec由typed validator拒绝；
- Run admission与active切换并发时只冻结一个完整snapshot；
- Run admission验证全部Model候选，任一非首选候选不兼容也不能创建Run；
- root Run与Release切换并发时只得到完整旧或完整新installation binding；child Run始终继承parent exact binding；
- InstallationId fixture接受`ins_<UUIDv7>`且kind为exact `installation`，拒绝旧`svc_`/`installation_service`、其他prefix/kind、非UUIDv7、
  大写或双解析alias；
- target registry exhaustive fixture接受public exact `encryption_domain/enc`、`approval_task/apr`与`interaction/int` kind/exposure，拒绝
  `task/tsk`、错误exposure、prefix/kind swap和兼容alias；checked-in JSON、Rust exhaustive enum/parser与公共SDK投影必须逐值一致；
- CanonicalRegion fixture覆盖1/63-byte合法边界与内部连字符，拒绝空/64-byte、首尾连字符、大写、下划线、Unicode、provider alias及任何
  lowercase/normalization后才“合法”的输入；两个common schema与Rust/SDK validator逐值一致；
- 没有Active Release、错误installation/release/candidate kind、零generation或manifest/state digest漂移均fail closed；
- RunBindings version 1、超过1 MiB或超过512个distinct Model Deployment refs在取得大批行锁前稳定拒绝；
- Run运行期间任何resource变化不改变bindings digest；
- 跨租户ID在resolve/activate/admit中返回不可区分404；
- published version/deployment不能更新；
- Suspension与active为空具有不同状态、错误和事件；
- Registry生命周期不随ResourceKind增加而复制新表族；
- canonical fixture在Rust及公共SDK语言产生相同digest。

## 15. 明确推迟

- 跨区域Deployment和active-active registry；
- UI协同编辑/CRDT；
- 在线法规hard-delete；
- 等价semantic digest跨Resource复用身份。

## 16. 未决问题

本次architecture revision要求03补齐installation-scoped aggregate/Receipt/Event/Outbox scope，05/06/08/17补齐root与child Run的
installation binding边界，07/09/12/15/16/18统一`CanonicalRegion`。这些下游完成cross-review前本规范不能恢复Accepted。
ResourceKind新增时仍必须先提供typed spec、schema、validation和allowed active target matrix，不得以generic JSON绕过本规范。
