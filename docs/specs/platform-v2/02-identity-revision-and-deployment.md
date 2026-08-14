# Platform v2 标识、资源版本与部署规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-09 |
| 依赖 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) |
| 直接下游 | 03、04、05、09、11、12、13、14、16、17、18 |

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
| Tenant / Principal / PrincipalSnapshot | `ten` / `prn` / `psn` |
| Resource | `agt`、`skl`、`cap`、`cim`、`ctx`、`xim`、`mcp`、`mpr`、`mdl`、`pol`、`srt`、`spk`、`sxp` |
| ResourceVersion | `aif`、`arev`、`srev`、`cirev`、`cimp`、`xirev`、`ximp`、`mrev`、`mprev`、`mdrev`、`prev`、`srrev`、`sprev`、`sxrev` |
| Deployment | `adep`、`cdep`、`xdep`、`mcdep`、`mpdep`、`mdep` |
| Runtime | `run`、`nex`、`inv`、`job`、`tsk`、`evt`、`rcp` |
| Artifact / Blob | `art` / `blb` |
| Secret | `spr` / `sbd` |
| Correlation | `req` |

新增或改变prefix是公共machine-contract变更。`PlanNodeId`、`SlotId`、`FieldId`和`ModelCallId`只在owner内稳定，
使用owner ID加bounded local key，不冒充全局资源。External task identity、provider ID、object generation、cursor、ETag、
idempotency key和digest也不是ResourceId。

除installation-scoped singleton外，每个持久对象都有`tenant_id`。Repository predicate必须同时使用tenant和object ID；
公开404不区分不存在与跨租户不可见。

## 4. 共享Resource生命周期

```rust
enum ResourceKind {
    Agent,
    Skill,
    CapabilityInterface,
    CapabilityImplementation,
    ContextInterface,
    ContextImplementation,
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
    schema_version: u32,
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
```

- version发布后immutable；修正产生新version；
- ordinal在单Resource内严格递增但不是身份；
- version必须与owner tenant/kind一致；
- validation summary与spec/digest一起immutable；
- metadata-only display修改留在Resource，不改变运行语义；
- version被Run、Deployment或保留期内Event引用时不可hard delete。

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
```

DeploymentSpec至少包含适用的exact dependency versions、implementation、adapter/protocol、endpoint identity hash、
04定义的`ExactSecretBindingRef`、runtime/image/module digest、Policy versions、network/isolation/resource limits和
conformance summary。
不保存Secret value、短期token或实时health。

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
struct RunBindingsSnapshot {
    schema_version: u32,
    agent: ExactDeploymentRef,
    agent_interface: ExactVersionRef,
    plan: ExactVersionRef,
    principal: PrincipalSnapshotRef,
    slots: Vec<FrozenSlotBinding>,
    policies: Vec<ExactVersionRef>,
    execution_profile: ExactVersionRef,
    canonical_digest: Digest,
}
```

`FrozenSlotBinding`继续使用`contracts/platform-v1/schemas/frozen-slot-binding.schema.json`的closed discriminator。Model、
Capability、ChildAgent和Skill候选必须同时冻结Selection Policy；Context冻结exact binding。集合规范排序且受HardLimitProfile约束。

Admission事务：

1. 锁定Resource active target/gate与exact version/deployment；
2. 验证tenant、kind、digest、dependency closure和当前bindability；
3. 构造typed snapshot并计算canonical digest；
4. 与Run、Receipt和Event原子提交。

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

没有阻止03一致性重写的问题。ResourceKind新增时必须先提供typed spec、schema、validation和allowed active target matrix，
不得以generic JSON绕过本规范。
