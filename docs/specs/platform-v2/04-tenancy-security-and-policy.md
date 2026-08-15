# Platform v2 多租户、安全与策略规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-15 |
| 依赖 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md)、[`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) |
| 直接下游 | 05、09、11、12、13、14、15、16、17、18 |

> Persistence ruling：本规范只拥有安全语义和逻辑聚合。历史专用表名、编号 migration、deferred trigger 与
> qualification checkpoint 不再具有规范效力；物理映射统一服从 02、03 与 persistence baseline ADR。

## 1. 决策摘要

Platform v2 将 tenant ownership、principal capability、Effect、Approval、Secret、Network、Isolation、Quota
和 emergency fence 建模为不可绕过的平台策略。模型、Skill、MCP metadata、脚本 manifest 和远程服务响应
全部是不受信任输入，不能授予权限。普通 active head 决定未来绑定；Suspension 是独立的紧急执行门。

## 2. 目标与非目标

### 2.1 目标

- 在数据库、API、事件、Artifact、Secret 和 callback 全链路强制 tenant isolation；
- 统一管理、运行、服务和人工审批 principal；
- 按 Capability Effect 和信任等级决定审批、重试、隔离、网络和预算；
- 使用 Secret reference 和短期 scoped credential，避免 value 进入平台状态；
- 对 Model、MCP、Skill、Context 和 Sandbox 输入执行零信任验证；
- 对每 tenant、Agent、Capability、backend 和 principal 设置有界配额；
- 提供可审计的 suspend、resume、revoke 和 kill switch；
- 让日志、指标、事件和错误默认不泄露正文或身份。

### 2.2 非目标

- 不允许 Operator token 自动成为 tenant user；
- 不允许 Agent 或 Skill 动态扩大权限；
- 不允许远程 Tool description 声明自己是 read-only 后被平台直接信任；
- 不允许 Secret value 通过 API 回读；
- 不把 Docker 容器本身视为充分的不可信代码隔离；
- 不通过“内部网络”假设跳过认证；
- 不提供匿名生产管理 API；
- 不承诺消除全部侧信道，资源隔离和节点池策略由风险等级决定。

## 3. 信任边界

```text
Untrusted
  user input / model output / Skill content / MCP metadata
  retrieved documents / uploaded files / script output / callback body
                            ↓ validation
Platform trusted domain
  identity / policy / bindings / state machine / approval / audit
                            ↓ scoped request
Execution backends
  provider / MCP server / remote service / sandbox
```

只有平台发布的 Revision、Policy 和 Deployment Binding 能成为授权证据。外部 metadata 只能作为 validation
evidence，不能直接修改 Effect、network、approval、secret scope 或 tenant ownership。

## 4. Principal 模型

```rust
enum PrincipalKind {
    InstallationOperator,
    TenantAdmin,
    AgentAuthor,
    AgentRunner,
    HumanApprover,
    ServiceIdentity,
}
```

认证结果产生不可变 `PrincipalContext`：

```rust
struct PrincipalContext {
    principal_id: PrincipalId,
    kind: PrincipalKind,
    tenant_id: Option<TenantId>,
    capabilities: BTreeSet<Permission>,
    authn_strength: AuthnStrength,
    expires_at: DateTime<Utc>,
}
```

- installation Operator 只能调用 installation/management API，不能通过 header 选择 tenant/user 执行；
- tenant impersonation 只能使用独立、显式、短期、body-free audited support session；默认关闭；
- ServiceIdentity 使用 workload identity/mTLS，不使用长期共享 API key。内部 workload identity 使用证书 URI SAN，
  规范形状为 `spiffe://insight.platform/workload/<closed-workload-role>`；授权只读取已由受信 CA 验证的 leaf
  certificate URI SAN，不读取 CN、DNS SAN、自报 header 或 bearer metadata。一个受保护端点必须要求恰好一个 URI SAN，
  且与其 closed allowlist exact match；同一身份的证书轮换不改变授权结果；
- HumanApprover 必须同时满足 task assignment、permission、tenant 和 approval policy；
- principal capability 是闭合集合，未知 permission 在启动和 token 解析时失败。

### 4.1 Principal authority

外部认证主体、tenant/installation 绑定与一次请求的 `PrincipalContext` 是三个不同事实：

```rust
struct PrincipalIdentity {
    principal_id: PrincipalId,
    authentication_authority_digest: Digest,
    subject_digest: Digest,
    state: PrincipalIdentityState,
    generation: u64,
    etag: Etag,
}

struct TenantPrincipalBinding {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    kind: PrincipalKind,
    capabilities: BTreeSet<Permission>,
    state: PrincipalBindingState,
    generation: u64,
    etag: Etag,
}

struct InstallationPrincipalBinding {
    principal_id: PrincipalId,
    kind: PrincipalKind,
    capabilities: BTreeSet<Permission>,
    state: PrincipalBindingState,
    generation: u64,
    etag: Etag,
}

enum PrincipalIdentityState { Active, Revoked }
enum PrincipalBindingState { Active, Revoked }
```

- `authentication_authority_digest + subject_digest` 是认证 adapter 生成的 domain-separated keyed digest，不保存
  issuer、email、subject 或证书正文；该二元组只能映射一个 `PrincipalId`；
- `PrincipalKind` 属于 binding，不属于外部身份。tenant binding 的唯一键是
  `(tenant_id, principal_id, kind)`，installation binding 的唯一键是 `(principal_id, kind)`；
- installation binding 只允许 `InstallationOperator`、`ServiceIdentity`；tenant binding 禁止
  `InstallationOperator`，允许其余 tenant 角色与 tenant-scoped `ServiceIdentity`；
- capability 数组必须按 permission wire value 规范排序、无重复且全部命中 closed registry；空集合法但不能通过任何受
  保护操作；
- 请求 token 必须选择一个 exact active binding。tenant-scoped `PrincipalContext.tenant_id` 只能来自该 binding，不能来自
  header、path 之外的客户端覆盖或另一个 membership；
- identity 或 binding 的 `Active -> Revoked` 都是单向终态。active binding 的 permission 变更使用 generation/ETag CAS；
  授权事务从锁定的current identity/binding行派生closed `PrincipalSnapshot`，并把完整快照与canonical digest嵌入消费
  聚合。PrincipalSnapshot没有独立ID、current row、generation history表或生命周期；current row后续permission CAS/
  revocation不改变已经提交的快照，但敏感读取/响应仍重查当前 binding；
- 普通 tenant command 不接受 installation binding；Operator support 路径仍按第4节和17的独立 audience/session 规则，
  不能创建伪 tenant binding。

### 4.2 首个 installation operator bootstrap

首个安装级操作员只能通过部署期的一次性bootstrap建立，解决“尚无Principal就无法授权创建首个Principal”的信任根
问题：

- 输入只接受外部认证authority digest、subject digest、预生成`PrincipalId`和`RequestId`，不接收或保存subject明文；
- bootstrap固定创建`InstallationOperator`，permission精确为`installation.manage`与`installation.support`，调用方不能
  自定义或扩大集合；后续变更全部走正常generation/ETag CAS与CommandReceipt；
- 创建前Principal identity、tenant binding、installation binding三个authority集合必须都为空；事务同时持有bootstrap
  advisory lock和三张authority表的写隔离锁，禁止与普通Principal创建竞态；
- identity、binding和append-only `installation.bootstrap` audit必须同事务提交。因为首个Principal创建前无法形成正常
  authenticated CommandReceipt，只有该部署入口按03定义豁免receipt；
- 只允许相同Principal、认证digest、固定permission、RequestId和audit evidence的精确重放。authority已被使用、对象被
  修改/撤销、证据缺失或输入不同都返回`bootstrap_conflict`；
- Gateway、控制面和运行时Worker不得暴露或调用该入口；bootstrap完成后再启用应用workload identity。

## 5. Tenant Isolation

### 5.1 数据库

- 所有 tenant-scoped 表含 `tenant_id NOT NULL`；
- primary/unique/foreign key 或 repository predicate 必须包含 tenant；
- connection 不保留跨请求 tenant session state；
- application repository 强制 tenant parameter，禁止无 tenant 的通用 `get_by_id`；
- PostgreSQL RLS 可作为 defense-in-depth，但不能替代 domain/repository 校验；
- installation-scoped 表与 tenant-scoped 表分 schema 或明确访问 port。

### 5.2 Artifact

- ArtifactRef 永远 tenant-scoped；
- object key 不使用用户文件名、email、Run ID 明文或可猜 tenant 名；
- 下载使用短期、单对象、受 audience 和 method 限制的 signed URL/token；
- cross-tenant dedup 即使 digest 相同也不能暴露对象存在性；
- filename 只作为经过规范化的 presentation metadata，不决定 object key。

### 5.3 事件与缓存

- 事件 envelope 包含 tenant scope，但外部 subject 不暴露 tenant 名；
- consumer identity 必须被授权读取对应 tenant partition/filter；
- cache key 包含 tenant 与 policy/binding digest；
- negative cache 不能形成跨租户存在性 oracle；
- live stream token 绑定单 tenant、principal、Run 和过期时间。

## 6. Permission 与 Policy

Policy由不可变Policy Revision表示，Deployment与RunBindings固定所用版本。首个公共合同的permission registry
是以下闭集：

```rust
struct PolicyRevision {
    policy_revision_id: RevisionId,
    policy_id: PolicyId,
    kind: PolicyKind,
    document: ClosedPolicyDocument,
    validation_id: ValidationId,
    semantic_digest: Digest,
}

enum PolicyKind {
    Authorization,
    Approval,
    DataFlow,
    Declassification,
    Network,
    Tls,
    Trust,
    Retry,
    Budget,
    Quota,
    Selection,
    Scheduling,
    Execution,
    Resource,
    Isolation,
    Parser,
    Chunker,
    Ranking,
    Retention,
    ArtifactIo,
    SecretResolution,
    PublicProjection,
    Protocol,
    McpAuth,
}
```

Policy ID必须是`pol`，Revision必须是`prev`。各domain定义`ClosedPolicyDocument`的typed variant/body，但不能自建第二个
Revision ID或mutable profile authority。字段名为`*_policy_revision_id`，以及protocol/execution/parser/chunker/ranking/
resource等非领域资源的`*_profile_revision_id`，都必须引用`prev`并验证expected `PolicyKind`。真正领域Profile例外只有
02注册的Model Profile `mdrev`与Sandbox Profile `sxrev`等明确resource kind。

| 引用字段/角色 | Expected PolicyKind |
|---|---|
| authorization/approval | `Authorization` / `Approval` |
| data/declassification | `DataFlow` / `Declassification` |
| network/tls/trust/isolation | `Network` / `Tls` / `Trust` / `Isolation` |
| retry/budget/quota/selection/scheduling | `Retry` / `Budget` / `Quota` / `Selection` / `Scheduling` |
| execution/resource | `Execution` / `Resource` |
| parser/chunker/ranking | `Parser` / `Chunker` / `Ranking` |
| retention/artifact_io | `Retention` / `ArtifactIo` |
| secret/rotation | `SecretResolution` |
| public_projection | `PublicProjection` |
| protocol/auth_profile | `Protocol` / `McpAuth` |

一个字段允许多个Policy Revision时，每个元素也必须是该role允许的kind，集合规范排序且不允许同kind冲突；合法的多层
policy组合使用发布时编译的deterministic intersection/join receipt，不能依赖运行时“最后一个覆盖”。

Artifact-backed Model response使用`ClosedPolicyDocument::ModelOutputArtifactIo`这一closed typed variant；它不能用generic
JSON、另一个Artifact purpose的Policy body或只含opaque digest的摘要替代：

```rust
struct ModelOutputArtifactIoPolicyDocument {
    schema_version: u32, // const 1
    staging_grace_seconds: u64,
    verified_media_type: MediaType, // exact application/json
    classification_ceiling: DataClassification,
    maximum_materialized_bytes: u64,
    storage_binding_digest: Digest,
    encryption_domain_id: EncryptionDomainId,
    content_validation_profile_digest: Digest,
}
```

`staging_grace_seconds`与`maximum_materialized_bytes`必须为正，media必须exact为`application/json`；storage、encryption与
validation profile必须能在18的Candidate closure中解析为一个exact、可用且同region的binding。storage binding还必须冻结正数
`maximum_put_completion_uncertainty_milliseconds`及strong-after-quiescence exact-key observation合同；checked计算
`required_write_quiescence_seconds = ceil(milliseconds / 1000) + 1`，并要求`staging_grace_seconds >= required_write_quiescence_seconds`。
Model output classification不得高于
ceiling，所有bytes/time值还必须被18的effective HardLimitProfile收紧。该document只允许出现在`PolicyKind::ArtifactIo`的
Policy Revision中，`rules_digest`必须等于完整closed document的canonical digest。Producer只能重验冻结revision/body/digest，不能
提交、选择或覆盖任一字段。

`Network`是MCP、Model、Capability与Sandbox共享的PolicyKind，而不是Sandbox私有资源。通用Network Policy Revision可以只以
closed AuthoringPackage和`rules_digest`承诺其领域正文，Resource合同不得强制它携带Sandbox专用typed body。只有当Sandbox
Profile引用某个Network Policy Revision时，该exact revision才必须同时携带完整`SandboxNetworkPolicyDocument`，且Sandbox
repository必须重新验证typed body与`rules_digest`一致；仅有通用Network摘要的Revision不能用于Sandbox执行。

```text
installation.manage/support
tenant.read/manage/emergency_stop
agent.read/write/publish/deploy/activate/run
skill.read/write/publish/bind/activate
capability.read/write/publish/deploy/activate/bind/invoke
context.read/write/publish/deploy/activate/query/build_dataset
mcp.read/write/discover/import/publish/deploy/activate/invoke
model.read/write/discover/import/publish/deploy/activate/invoke
sandbox.read/write/build/publish/activate/execute
artifact.read/write/delete/hold/rescan
approval.read/respond
interaction.read/respond
policy.read/write/publish/activate
operation.read/cancel
runtime.read/control/signal
secret.inspect/bind/rotate/revoke
```

斜杠表示逐项permission，不是wildcard；例如`runtime.read`与`runtime.control`是两个独立值。`secret.inspect`只能
读取provider/purpose/state/version policy等非敏感metadata，不读取value、provider path或resolver response。不存在
`secret.read`、`backend.pass_through`或任意`*` permission。每个17公开route、每个internal gRPC method和每个Worker
claim class必须在machine registry中精确映射permission/service authorization；启动时发现未映射或重复映射即失败。

授权判定输入至少包含 principal、tenant、resource owner、operation、bound policy revision、Effect、data
classification 和 execution profile。模型输出、Prompt 或 Skill 指令不能出现在“授予 permission”的路径。

## 7. Capability Effect

Effect 是平台发布时验证的闭合枚举：

```rust
enum Effect {
    Pure,
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
    Irreversible,
}
```

| Effect | 自动重试 | 默认审批 | 不确定结果 |
|---|---|---|---|
| Pure | 允许，有界 | 否 | safe retry |
| ReadOnly | 允许，有界 | 按数据等级 | safe retry 或 reconcile |
| IdempotentWrite | 仅有后端 idempotency contract | 按策略 | reconcile 后重试 |
| NonIdempotentWrite | 默认禁止 | 是 | manual review |
| Irreversible | 禁止 | 强审批/双人规则可选 | manual incident |

Implementation 不能声明比 Interface 更弱的 Effect。MCP/remote discovery 的 annotation 只能作为候选，
必须由 Operator/Policy 显式确认并发布。

Effect风险序为`Pure < ReadOnly < IdempotentWrite < NonIdempotentWrite < Irreversible`。Implementation验证和
runtime policy取Interface声明、Implementation实际行为与已知backend能力的最大风险；任何无法证明的写行为至少按
`NonIdempotentWrite`处理。风险序只用于收紧retry/approval/reconciliation，不能推导业务兼容性。

## 8. Approval 与 Human Input

Approval 是 durable task，不是一次同步弹窗：

```rust
struct ApprovalTask {
    approval_task_id: ApprovalTaskId,
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    state: ApprovalState,
    effect: Effect,
    input_digest: Digest,
    policy_revision_id: RevisionId,
    approver_rule: ApproverRule,
    deadline: DateTime<Utc>,
    generation: u64,
    projection_version: u64,
}

enum ApprovalState { Pending, Approved, Rejected, Expired, Cancelled }
```

```text
Pending -> Approved | Rejected | Expired | Cancelled
```

- task 固定 Run、Invocation、Effect、参数摘要、policy revision、approver rule 和 deadline；
- 参数摘要是平台生成的安全视图，Secret 和大正文使用受控 Artifact；
- 修改调用参数会使旧 approval 失效；
- response 使用 task generation 与 CAS，first-winner；
- self-approval、双人规则、工作时间和金额阈值由 Policy 表达；
- approval 只授权一次固定 Invocation，不产生永久 Capability permission；
- timeout 不自动视为批准。

InputRequired 与 ApprovalRequired 是不同状态：前者补充业务输入，后者授权已固定的高风险操作。
Run公开投影使用`approval.required/resolved`；resolved payload只含terminal class与safe summary，不含参数、policy表达式
或approver私密信息。

## 9. Secret 管理

Secret Provider是18 CandidateManifest安装的可信resolver，不是tenant可上传插件。tenant只创建opaque
SecretBinding：

```rust
struct SecretBinding {
    secret_binding_id: SecretBindingId,
    tenant_id: TenantId,
    provider_id: SecretProviderId,
    opaque_reference: OpaqueSecretReference,
    purpose: SecretPurpose,
    resolution_policy: SecretResolutionPolicy,
    state: SecretBindingState,
    binding_generation: u64,
    etag: Etag,
}

enum SecretResolutionPolicy {
    Pinned { opaque_version_identity: OpaqueSecretVersionIdentity },
    FollowProviderRotation { rotation_policy_revision_id: RevisionId },
}

enum SecretBindingState { Active, Revoked }

struct ExactSecretBindingRef {
    secret_binding_id: SecretBindingId,
    binding_generation: u64,
    provider_id: SecretProviderId,
    purpose: SecretPurpose,
    resolution_policy: SecretResolutionPolicy,
    resolution_policy_digest: Digest,
}
```

`Active -> Revoked`是唯一状态转换，Revoked不可恢复；需要重新授权时创建新Binding。Deployment保存Binding ID、
创建时观察到的binding generation、provider、purpose、完整resolution policy和其canonical digest，统一封装为
`ExactSecretBindingRef`。该引用必须进入Deployment canonical digest；公共Deployment request只提交Binding ID，
repository在同一个创建事务中从active SecretBinding派生其余字段，不接受调用方覆盖authority。Pinned永远解析exact opaque version；FollowProviderRotation
允许同一逻辑credential按已冻结Policy轮换，但每个Attempt必须保存实际opaque version identity/resolver generation
evidence，不保存value。该显式例外不允许endpoint、purpose、provider或policy静默漂移。

`ExactSecretBindingRef` canonical排序为`(purpose, secret_binding_id)`，同一Deployment内Binding ID和purpose均不得
重复。Deployment创建时必须拒绝generation、provider、purpose、policy或digest任一与当前active row不一致的
引用。leaf start始终重新检查current revoke/gate；Pinned resolver不得以当前generation代替冻结generation，
FollowProviderRotation只能按冻结policy跟随并记录实际解析evidence。无法解析exact generation/policy时fail closed。

强制要求：

- API只接受Secret reference/binding metadata，公共合同不接收、回读或代理写入Secret value；Secret value由外部
  Secret Provider的独立管理面创建；
- resolution 发生在最接近执行后端的受信任 adapter；
- Secret 使用后不进入 output、Artifact、exception、trace 或 audit；
- scoped credential 优先于长期 credential，并绑定 backend、operation、tenant 和 TTL；
- credential rotation产生新opaque version identity；是否影响历史Deployment完全由其固定resolution policy决定；
- rotate command只请求/采纳Provider原生rotation并递增binding generation，不接受客户端提交Secret value；
- revoke 是独立安全门，可以阻止历史 Deployment 的新 leaf start。

受信resolution固定为以下顺序，不能由Provider adapter自行省略或重排：

1. 从PostgreSQL按`(tenant_id, secret_binding_id)`读取current binding并重验`Active`、provider、purpose、policy与generation；
2. 只在Secret Broker内用绑定tenant、Binding、generation和key identity的KMS/AEAD解封opaque reference，并比对reference digest；
3. 只从CandidateManifest安装的closed Provider catalog按`provider_id`选择client，不接受tenant动态代码或调用方endpoint；
4. 在独立有界permit与总超时内按冻结的Pinned/Follow policy读取外部Secret Manager；
5. 重验实际opaque version identity，Pinned必须完全相等，Follow必须保存本Attempt实际generation/version evidence。

任一步不可用时需要Secret的新leaf fail closed；revoke、generation/policy漂移、reference digest或Provider evidence错误必须在返回
Secret material前拒绝。解封reference与material均为non-clone、redacted、drop时清零的短生命周期值。该流程复用现有
`secret_bindings`聚合，不建立Secret value、历史版本或broker session表。

## 10. 代码与 Sandbox 策略

代码分为：

```rust
enum CodeTrustClass {
    BuiltIn,
    ReviewedPublished,
    TenantPublished,
    ModelGenerated,
}
```

最低策略：

| Trust | 允许后端 | 默认网络 | Secret |
|---|---|---|---|
| BuiltIn | Native/Remote，或按14选择Sandbox | allowlist | purpose scoped |
| ReviewedPublished | SandboxedContainer；ReviewedShell最低MicroVm | deny | 默认无 |
| TenantPublished | MicroVm | deny | 显式policy且仍须MicroVm |
| ModelGenerated | MicroVm | deny | 禁止直接注入 |

- Shell 只允许作为 ReviewedPublished image/implementation 的固定 entrypoint；
- 公共 API 不接受任意 shell command string；
- code、args、environment、mount、network 和 output schema 分开建模；
- Sandbox Executor 不加入平台默认网络，不挂载 Docker socket、数据库、宿主目录或平台 Secret；
- 每次执行使用独立 UID、工作目录、deadline、cgroup/limit 和 output budget；
- 具体后端由 14 规范选择，但不得弱于本表。

## 11. 网络策略

默认 `deny_all`，Capability Deployment 显式声明：

```rust
enum NetworkPolicy {
    DenyAll,
    ProxyAllowlist(Vec<DestinationRule>),
    ServiceIdentityOnly(Vec<ServiceId>),
}
```

- 不允许自由 CIDR、任意 DNS 或由模型直接提供 allowlist；
- HTTP 出站经 egress proxy，执行 DNS pinning/rebinding 防护和 private range policy；
- remote service/MCP endpoint 在 Deployment 中固定 canonical identity；
- redirect、proxy CONNECT、IPv6、DNS CNAME 和 URL userinfo 必须纳入验证；
- callback endpoint 使用平台生成 token，不接受执行代码自选内部 URL；
- NetworkPolicy变化产生新Policy Revision；需要采用它的环境绑定创建新Deployment。紧急deny可以通过suspension fence
  立即生效。

## 12. 数据分类

最低分类：

```text
Public | Internal | Confidential | Restricted
```

分类全序为`Public < Internal < Confidential < Restricted`。复合值、Artifact派生物、Prompt assembly和多来源结果的
分类是所有输入的上确界；destination声明ceiling，只有`value_classification <= destination_ceiling`才允许流动。
任何降级必须有显式declassification Policy Revision、授权principal、转换Evidence与Audit，不能由模型摘要、编码、
文件名或Provider响应自动降级。未知分类按`Restricted`fail closed。

Context、Artifact、Prompt、Run input/output 和 Capability ports 都声明分类上限。信息流规则必须阻止高等级
数据发送到未获准 Provider、MCP Server、Remote Service 或 Sandbox network。模型不能通过生成 URL、
base64 或 Artifact 名称规避 egress 判定。

## 13. Quota、Budget 与并发隔舱

Quota Policy Revision使用machine-readable `QuotaDimension`闭集。每个dimension直接绑定18
`HardLimitProfile`中的唯一字段路径、计量单位和accounting mode，不能由调用方提交自由字符串或另建一份hard maximum。
首个合同至少覆盖：

- tenant active/waiting Runs；
- Agent concurrent Runs；
- work class concurrent operations；
- Capability/Implementation concurrent invocations；
- Sandbox executions、CPU time、memory、output bytes；
- Model tokens、cost、requests；
- Context query count/result bytes；
- MCP sessions/tasks/subscriptions；
- Artifact count、single size、tenant storage；
- HumanTask pending count 和 retention。

Artifact-backed Model output不能默认预留output quota。只有typed owner从exact output schema、Provider/adapter contract、
HardLimitProfile与剩余Run预算计算出的冻结legal maximum（16的`maximum_materialized_bytes`）严格大于effective Inline threshold时，
才允许在dispatch前原子创建下述Artifact与candidate Blob两个bundle及其ledger identity；小于或等于threshold时必须冻结`InlineOnly`，
不得创建任一output quota identity。调用方、Worker与materialization Producer均不能提交或放大这两个边界；预留量必须覆盖冻结合同允许的最坏合法正文，
不能按平均响应或Inline threshold少留额度。

同一Model Artifact-capable start必须冻结`staging_retain_until`与`ready_retention_seconds`，而不是预先计算Ready绝对时间。start事务
只使用自己的PostgreSQL `db_now`，按checked arithmetic计算
`staging_retain_until = attempt_deadline + ArtifactIo.staging_grace_seconds`；结果必须晚于attempt deadline，且
`staging_retain_until - db_now <= effective artifact.staging_seconds`，溢出或窗口不足均在Provider dispatch前fail closed。
同一事务还要冻结storage binding digest与其`maximum_put_completion_uncertainty_milliseconds`；上述grace下界保证
`staging_retain_until`严格晚于所有deadline前admitted PUT的write-quiescence boundary。candidate的physical delete、stable absence evidence、
Blob Closed/Expired或quota释放在candidate row/locator已绑定、Processing write已admit或PUT仍有可能时一律不得早于该绝对时间；到点后仍须
重新观察exact locator/generation，不能复用更早的absence结果。bind前同一事务能够证明没有candidate row、locator、admitted write且PUT不可能
时，仍按下文no-object分支立即Close Blob bundle，不制造无意义的grace占用。
`ready_retention_seconds`来自Model Deployment，必须不短于exact `RunOutput` Retention policy的
`minimum_retention_seconds`且不长于effective `artifact.ready_retention_seconds`。`Staging | Uploaded | Verifying | Verified`
candidate的current retention只能是冻结的`staging_retain_until`；Producer只能重验，不能修改、延长、缩短或把它换成Ready期限。
只有typed owner terminal把Artifact推进Ready的同一事务，才能以该事务自己的PostgreSQL `db_now`执行checked
`ready_retain_until = db_now + ready_retention_seconds`，持久化该绝对时间并建立业务Reference/RunValue；terminal响应丢失重放已保存的
绝对时间。Inline、失败、取消、超时或loser不执行该切换，继续按staging/orphan policy收敛。

其中single size、per-operation size和retention是HardLimit/Retention Policy约束，不创建虚假usage ledger；active、并发、
累计使用和可回收占用才进入共享Quota authority。dimension的accounting mode闭合为：

```rust
enum QuotaAccountingMode {
    Leased,      // 并发/等待：reservation存续期间占用，close/expiry释放
    Consumable,  // token、request、CPU time、egress：settle后永久计入该window
    Reclaimable, // Artifact count/bytes：commit后占用，删除/GC以受约束refund释放
}

enum QuotaReservationState { Open, Closed, Expired }

struct QuotaBudgetKey {
    tenant_id: TenantId,
    quota_policy_revision_id: PolicyRevisionId,
    dimension: QuotaDimension,
    scope: QuotaScope,
    window_key_digest: Digest,
}

struct UsageReservation {
    usage_reservation_id: UsageReservationId,
    tenant_id: TenantId,
    owner_resource_id: ResourceId,
    state: QuotaReservationState,
    deadline: UtcTimestamp,
    generation: u64,
}
```

CR-165的Model output quota closure在同一start事务原子创建两个、且只能两个共享Quota bundle，以匹配Artifact与Blob独立生命周期：

- Artifact bundle：`owner_resource_id=预留ArtifactId`，只含`ArtifactCount(Reclaimable, 1)`与
  `ArtifactLogicalBytes(Reclaimable, maximum_materialized_bytes)`；Ready时消费，exact Artifact删除时Refund；
- candidate Blob bundle：`owner_resource_id=预留candidate BlobId`，只含`ArtifactUploads(Leased, 1)`、
  `ArtifactStagingBytes(Leased, maximum_materialized_bytes)`与
  `ArtifactPhysicalBytes(Reclaimable, maximum_materialized_bytes)`；new Blob winner的physical占用跟随Blob直到最后一个alias删除，
  preexisting/race-loser candidate则由candidate cleanup关闭。

两个bundle owner从创建起固定，不是Model Worker、Job或Producer，也不能在dedupe时转给resolved Blob、另一个Artifact或新cleanup
reservation。existing Verified Blob沿用其最初创建时的physical bundle；复用它的新Artifact只消费自己的Artifact bundle，并关闭未创建
candidate的Blob bundle。candidate Blob ID是在同一output reservation中预分配的typed future owner：Blob row尚未创建期间只能由exact
Producer创建该same ID，或由owner按no-object evidence关闭bundle；不能把它当generic polymorphic owner或改绑另一Blob。缺bundle/line、
重复dimension、错误mode/unit/scope/window或调用方增加line均拒绝start。两个header分别保持
`Open`直到自己全部line终结；line settlement状态由现有reservation line/ledger表达，不增加新的`cleanup-required` quota state或表。

`QuotaScope`是closed tagged union，只允许Tenant、AgentDeployment、WorkClass、CapabilityDeployment、
ModelDeployment、ContextDeployment、McpDeployment、SandboxProfileRevision、Run或Principal；每个variant验证exact ID prefix，
WorkClass命中07的machine registry。Quota Policy固定每个dimension允许的scope和window；reservation不能把一个scope的额度
转给另一个scope。`QuotaWindowKind`闭合为`current`、`run`、`utc_day`、`utc_month`、`lifetime`：Leased只使用
`current`，Reclaimable只使用`lifetime`，Consumable只能使用`run/utc_day/utc_month`；window key与起止时间必须按kind
canonical生成，客户端不能提交任意bucket字符串。

tag/prefix只解决共享Quota schema的多domain键形状，不构成scope存在性或授权证明。Tenant/WorkClass由共享primitive直接
校验；其余scope只能由拥有对应Deployment/Run/Principal aggregate的application service在同一事务验证tenant与exact
authority row后传入。Gateway、Worker或客户端不得直接构造`QuotaScope`写repository；domain row应以复合tenant FK引用返回的
`UsageReservationId`，从另一方向关闭关联，不能创建无tenant的polymorphic弱引用。

一次业务准入可以同时命中platform/tenant/work-class/resource/Run多层budget，因此一个`UsageReservationId`是bundle header，
`quota_reservation_lines`按canonical BudgetKey排序保存各层amount。共享primitive在03 `TenantQuotaPolicy` lock rank一次锁定
全部budget，任一层不足则整包不写；禁止逐层提交、内存补偿或先入队后补reserve。

把reservation作为后续domain mutation前提的短事务，必须在Receipt rank之后、parent aggregate/Job rank之前，对请求冻结的exact
`(UsageReservationId, generation)` header按ID排序取得`FOR SHARE`，并按canonical BudgetKey对全部line取得同等级锁；锁后重新验证
tenant、owner、Open state、deadline、generation、dimension/mode/amount与未settle事实。Model Artifact Producer的Processing claim、
candidate Blob bind、Uploaded checkpoint、Verifying checkpoint与final Verified事务都必须以这种方式锁定exact两个bundle，不能只做
无锁snapshot读取。`Consume/Close/Refund/Expiry`及safety owner对同一header/line取得冲突的`FOR UPDATE`、CAS current generation并在成功
时递增generation，因此不能在Producer验证Open后先释放额度、再让Producer提交物理状态。外部对象/KMS I/O期间不持有这些锁。

settlement是append-only、以stable settlement key与canonical request digest去重的bundle操作：

- `Consume`允许Model等多Attempt工作从同一reservation envelope分次消耗；混合mode bundle的一次原子settlement可以把
  Reclaimable line的exact actual转成占用并释放余量，同时以`actual=0`终结已无物理/并发责任的Leased line；仍承担candidate cleanup的
  Leased line不写settlement，保持原reserved/Open事实，不产生actual、不过期也不释放；只要任一Reclaimable占用或Leased cleanup责任仍
  存活，bundle header就保持Open；
- `Close`只在该bundle所有Reclaimable line尚未`Consume`时以全line `actual=0`终结；Leased line的actual永远必须为零。未Ready
  Artifact bundle可由owner terminal直接Close；candidate Blob bundle只有在证明candidate row/locator/object均不存在，或cleanup已提交
  exact generation deletion/absence evidence时才可Close；已经形成Reclaimable占用后只能先走`Refund`；
- `Refund`只允许Reclaimable line，由拥有原资源lifecycle的domain在删除/GC事实同一事务调用，并可在净占用归零时
  同时把reservation置为Closed；
- actual超过reservation时仍必须记录已发生用量；超过budget limit的部分标为overage、阻止该window的新reservation并产生
  quota incident，不能因拒绝settlement而丢失计费事实；
- Expiry只释放尚未settle且已证明没有物理对象可能性的reserved amount，不撤销已发生Consumable/Reclaimable consumption。candidate Blob
  bundle若已绑定locator或存在PUT可能性，deadline只使Artifact/Blob lifecycle派生出`cleanup_required`分类，bundle仍为Open，不能释放或
  改为Expired；cleanup必须先用exact generation deletion/absence evidence闭合candidate，再Close未Consume line。new Blob winner因hold/
  live alias保留时，PhysicalBytes保持Consume占用，直到最后alias删除才Refund。
  safety scan与业务close竞争使用generation/ETag first-winner fence。

Materialization Producer不是quota owner。它只能为exact attempt授权读取预留identity、open state与上限，数据库role必须拒绝其
reserve、`Consume`、`Close`、`Refund`、`Expiry`或直接修改balance/ledger。typed owner terminal按stage disposition结算：

- 所有Artifact winner在Artifact bundle消费`Count=1`、`LogicalBytes=exact canonical bytes`并释放余量；该bundle保持Open到Artifact删除
  Refund，不负责physical bytes；
- `PreexistingHit`关闭candidate Blob bundle的三条line为0，resolved Blob原physical bundle不变；
- `CandidateWinner`在candidate Blob bundle把Uploads/StagingBytes以0终结，消费`PhysicalBytes=exact new bytes`并释放余量；该bundle随
  resolved Blob保持Open，最后一个live alias删除及physical deletion/absence后才Refund/Closed；
- `RacingCandidateLoser`只把Uploads以0终结，StagingBytes保持reserved/Open且PhysicalBytes不Consume；candidate exact cleanup后把剩余两条
  以0终结并Closed。race loser bytes由StagingBytes覆盖，不伪装为Ready physical占用。

Inline或失败owner可把未Ready Artifact bundle以0关闭；candidate row/locator/object完全不存在时也关闭Blob bundle。只要candidate已绑定或
可能PUT，Blob bundle就保留到exact cleanup，`cleanup_required`只是Artifact/Blob lifecycle分类。Ready Artifact删除只Refund自己的
Count/Logical bundle；shared Blob仍有alias时不得动Physical，最后alias删除才由Blob lifecycle Refund最初winner的Physical bundle。Producer
不得提前结算或释放任一bundle。

每个限制都有tenant value、platform hard maximum、window和overflow behavior。budget物化必须绑定exact Quota Policy
Revision与HardLimitProfile digest，并证明effective limit等于各层最小值；policy/head或capacity变化不改写既有reservation。
currency dimension额外绑定uppercase ISO-4217 `unit_qualifier`，非currency dimension的qualifier必须为空，禁止跨currency
合并microunit counter。
禁止无界排队；达到上限时返回稳定 `quota_exceeded`、进入有deadline的durable queued state或触发deadline，不能无限等待。

Sandbox、Model、MCP、Context 和 Remote Capability 使用独立 semaphore/queue/connection pool。Sandbox
饱和不得消耗 Model 或 API permit。本地semaphore只是减少无效claim；全局准入仍以PostgreSQL QuotaBudget projection、
reservation line和settlement ledger为唯一权威，metric不得回写额度。

## 14. Suspension、Revoke 与 Kill Switch

以下门必须独立存在：

| 门 | 影响 |
|---|---|
| Entity archived | 隐藏 authoring，不自动终止运行 |
| Active head changed/cleared | 影响未来 binding/admission |
| Agent suspended | 阻止新 Run，可按策略 cancel 活动 Run |
| Capability implementation suspended | 阻止尚未开始的 Invocation |
| Provider/MCP suspended | 阻止新外部 leaf/session |
| Secret revoked | 阻止需要该 credential 的新调用 |
| Tenant emergency stop | 阻止新 admission，并提交活动 Run cancel intent |
| Platform kill switch | 按 work class 停止 dispatch，不删除 durable work |

所有安全门使用数据库权威、generation 和 body-free audit。进程缓存必须有失效提示和短 safety poll；缓存
陈旧不能无限期绕过 suspension。

## 15. Supply Chain

- Native adapter、Sandbox runtime image、Skill package 和 script implementation 必须记录 digest；
- 生产环境只允许受信任 registry 和签名策略；
- mutable image tag 不能作为 Deployment binding；必须解析到 immutable digest；
- package 解压拒绝绝对路径、`..`、symlink escape、device、超限文件数和压缩炸弹；
- SBOM、漏洞扫描和签名 evidence 进入 publication validation；
- 远程 MCP/HTTP 服务升级不会自动改变已发布 Implementation Revision。

## 16. Persistence 聚合与所有权

- Policy 是 02 的 `ResourceKind::Policy`，Draft、Revision、active target 与 suspension 使用共享 Resource 模型；
- tenant 与 principal 当前权限事实使用 tenant/principal 聚合；需要冻结的 PrincipalSnapshot 嵌入 Run、Task、Receipt 或 Event；
- SecretBinding 是独立安全聚合，只保存 opaque reference、purpose、provider、generation 与 revoke 状态；
- Deployment closure 只冻结部署级凭据（例如 OAuth client credential、mTLS identity 或静态服务凭据）；
  principal-scoped OAuth access/refresh token 由 AuthorizationBinding 单独持有 exact `SecretBinding` 引用。两者属于不同
  authority scope，AuthorizationBinding 的 token binding 不得被要求预先出现在 immutable Deployment closure 中；
- AuthorizationBinding 消费的 Secret 必须冻结完整 `ExactSecretBindingRef`。OAuth grant 使用 `Pinned` resolution；token
  replacement 必须提升 SecretBinding generation 与 AuthorizationBinding generation，旧 session/未开始调用随旧 generation
  失效；
- exact `McpAuth` Revision必须分别冻结PKCE transient entry与principal token使用的`SecretProviderId`。该选择进入
  preparation digest，Broker只能从CandidateManifest安装的有界Provider catalog按exact ID选择，不能按purpose、环境变量或
  “当前默认Provider”动态漂移；两个角色可以显式选择同一Provider，但不能省略任一选择；
- 外部Secret Provider是prepared-write winner，PostgreSQL仍是SecretBinding current authority。Provider `prepare-or-load`
  成功后，Broker只把envelope-encrypted opaque reference、exact version identity与非敏感storage evidence通过受信
  ServiceIdentity登记到现有SecretBinding聚合；登记以preparation digest幂等并与Receipt/Event/Outbox原子提交。数据库响应丢失时
  必须load同一prepared winner后修复登记，不能重新生成逻辑secret或把raw material写入数据库；
- Egress Broker不能持有PostgreSQL credential。`secret_bindings`受信resolution projection与上述prepared winner登记只能通过
  独立Security Authority的versioned internal gRPC调用；该authority对每个method要求exact
  `spiffe://insight.platform/workload/egress-broker` URI SAN，逐请求重验tenant、closed command与大小上限。它拥有的数据库role只允许
  该projection读取及prepared registration所需的固定事务路径，不能调用其他业务command；同时不得拥有外网、DNS resolver、KMS或
  Secret Manager权限。Egress拥有KMS/Secret Manager和受控外网权限，但没有任何数据库连接。两者不能部署为同一进程、Pod或service account；
- Approval/HumanInput 使用 03 的共享 Task；命令、rotate、settle 与拒绝结果使用 Receipt/Event；
- quota current balance/policy 与 append-only reservation/settlement ledger 是全平台唯一配额权威；
- Scheduler 只引用 exact Policy ResourceVersion 和共享 scheduler state，不复制 policy lifecycle。

`opaque_reference`与opaque version evidence使用envelope encryption，普通query projection永不返回。Quota reserve/settle
必须与所属业务命令同事务；metric 不能作为准入权威。历史principal/security事实通过消费它的不可变 snapshot 保存，
不为每一种generation、rotation、approval或settlement新建专用表。

`PrincipalSnapshot`的首个closed contract为：

```rust
struct PrincipalSnapshot {
    schema_version: u32,
    tenant_id: TenantId,
    principal_id: PrincipalId,
    principal_kind: PrincipalKind,
    permissions: SortedUnique<Permission>,
    principal_version: u64,
    binding_generation: u64,
    binding_version: u64,
    permissions_digest: Digest,
    canonical_digest: Digest,
}
```

repository必须在执行permission判断的同一事务中从active Principal和调用请求选定的exact active tenant binding派生该
快照；调用方不能提交或覆盖其中任何authority字段。快照中的tenant、principal、kind、version、generation、permission正文与
digest必须相互一致，unknown字段和值fail closed。需要记录认证方法、session或token期限时写入同一Event的独立bounded认证
evidence，不把可续期的session状态变成PrincipalSnapshot的第二个current authority。

## 17. 审计、日志与隐私

Audit 记录：谁、何时、对哪个 opaque ID、执行什么操作、结果、policy revision、request digest 和来源
session。默认不记录正文。以下内容禁止出现：

- Secret value 和 authorization header；
- Prompt、代码、文档、模型输入输出全文；
- signed URL、callback token、OAuth code/token；
- PII 作为 metric label；
- endpoint query 和外部原始错误正文。

需要取证正文时，写入访问受控、tenant-scoped、短 retention 的 encrypted Artifact，并产生独立审计。

## 18. 稳定错误模型

安全相关错误至少包括：

```text
unauthenticated
permission_denied
resource_not_found
policy_denied
approval_required
quota_exceeded
resource_suspended
secret_unavailable
network_denied
isolation_unavailable
content_rejected
idempotency_conflict
```

跨租户访问统一返回 `resource_not_found`。客户端错误不回显内部 policy expression、Secret reference、节点
地址或安全产品细节；详细原因只进入受控审计 code。

本节列出的是17同步HTTP/gRPC安全command的`ApiProblem.code`语义子集；05拥有durable Run/Operation/leaf
`FailureCode`，04不创建第三套错误enum。已提交异步资源的安全失败按05投影，不能伪造成请求级500。

## 19. 威胁与强制缓解

| 威胁 | 强制缓解 |
|---|---|
| 模型诱导调用高风险 Tool | allowed binding + Effect + Approval |
| MCP 伪报 read-only | publication validation + platform Effect |
| 脚本逃逸影响服务 | 独立 Sandbox plane + isolation policy + no platform credentials |
| SSRF 访问内部服务 | deny network + egress proxy + destination policy |
| 跨租户 ID 枚举 | tenant predicate + opaque ID + indistinguishable 404 |
| Secret 经日志/输出泄露 | late resolution + redaction + output/content policy |
| 重放非幂等请求 | Invocation identity + approval binding + manual uncertainty handling |
| 迟到 Worker 覆盖结果 | epoch/fence commit |
| Supply-chain tag 漂移 | immutable digest + signature evidence |
| Artifact 恶意文件 | size/type/digest/content scan + scoped download |

## 20. 验收标准

- property tests 证明任何跨租户 ID 组合都无法读取、绑定、调用或推断对象；
- Operator token 不能直接调用 tenant Run API；
- Model/Skill/MCP metadata 无法改变 Effect、Permission、Network 或 Approval；
- NonIdempotentWrite timeout 进入 manual review，不自动重试；
- Secret canary 不出现在数据库 dump、日志、trace、metric、outbox、Artifact metadata 和 API response；
- Sandbox escape tests 无法访问平台网络、数据库、宿主目录或 Docker socket；
- Sandbox 饱和时 API、Model 和 Context 并发资格保持；
- suspension 在限定传播窗口内阻止未开始 leaf，旧缓存不能持续绕过；
- package/image signature 与 digest 不匹配时 publication 失败；
- approval 参数变化、过期、重复和错误 approver 全部被拒绝；
- egress 测试覆盖 redirect、DNS rebinding、IPv4/IPv6 private ranges 和 proxy bypass。
- quota fixture证明`maximum_materialized_bytes <= Inline threshold`不创建Artifact Reclaimable reservation，越过threshold才按最坏合法
  count/bytes预留；Producer credential的全部quota mutation和retention deadline修改被拒绝，只有owner terminal可把staging retention
  切换为ready retention，并对Artifact/Inline/失败竞态只结算或释放一次；Blob bind前失败可零actual关闭，bind/PUT后的cancel/loser及
  reservation expiry持续占worst-case staging额度，直到exact generation deletion/absence evidence与释放同事务提交。

当前实现证据已包含closed Rust `ModelOutputArtifactIoPolicyDocument`、生成并纳入根contract digest的独立JSON Schema、
`PolicyResourceSpec`的exact `ArtifactIo` variant/rules digest门禁，以及只接受显式effective limit与Candidate storage uncertainty输入的pure
checked timing helper。unit/checked-in schema fixture覆盖unknown field、错误media/ID kind、短write-quiescence grace、staging window、Ready
duration区间和admission/terminal数据库时间分离。该切片不解析Candidate storage binding、不升级HardLimitProfile v5、不创建Model
Deployment closure或reservation，也不启用Artifact-backed output；这些仍以implementation plan的后续依赖为准。

## 21. 明确推迟的工作

- 企业外部 IAM/SCIM 集成；
- Confidential Computing/TEE；
- 双人审批的具体 UI；
- 跨地域数据驻留；
- 自动 DLP 分类模型；
- microVM 后端产品选择。

## 22. 未决问题

没有阻止运行内核的安全未决问题。具体认证协议、默认数值、Sandbox 产品和 Secret Manager provider 由
17、18、14 的部署规范选择，但不得削弱本规范的信任边界与强制策略。
