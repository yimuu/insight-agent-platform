# Platform v2 Identity、Revision 与 Deployment 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-205 |
| 日期 | 2026-08-30 |
| 依赖 | 00、01 |
| 直接下游 | 03～18 |

> CR-205 impact：shared lifecycle的closed management noun覆盖全部可author的业务定义kind。Capability/Context
> Implementation与Sandbox Runtime/Package是definition-only：publish后以enabled exact Version被依赖，不伪造Deployment；Model Provider
> 保持自己的exact Deployment。ContextDataset仍只有build/read generation专用surface。

> CR-204 impact：Deployment create request只表达调用方可知的exact binding intent；`deployment_id`、嵌套identity和
> 派生digest由同一resolution transaction预留/计算。持久化Deployment及read response仍使用完整immutable exact closure。

> CR-200 impact：`ArtifactIo` closed document升级v3，新增exact write storage binding digest与tenant encryption domain ID；仍由同一immutable
> Policy ResourceVersion和TenantConfig exact slot拥有，process catalog只声明可支持的binding material。

> CR-199 impact：`PolicyKind::ArtifactIo`的closed document升级为v2，新增exact scanner contract digest、verification evidence TTL与retry
> backoff。它仍由同一Policy ResourceVersion拥有并通过TenantConfig exact Deployment slot选择；不新增PolicyKind、Deployment variant或active head。

## 1. 决策摘要

Agent、Skill、Capability、Context、Model、MCP、Policy和Sandbox共享一条动态管理生命周期：

```text
Resource -> immutable ResourceVersion -> Deployment -> tenant active binding
```

ResourceVersion表示可重现的逻辑定义，Deployment表示可运行的exact环境绑定。Run admission一次性冻结
所需Deployment和ResourceVersion闭包，active head与GitOps rollout变化不修改已存Run。

CR-189进一步明确“可运行”必须包含执行目标所需的完整静态解析事实：可执行Context backend冻结required Worker manifest；
RemoteSearch还冻结canonical endpoint和exact Network/TLS/Trust Policy。只保存endpoint digest、由进程配置反查URL或使用默认TLS信任
不构成exact Deployment。Secret仍只保存exact binding reference并由Egress最后一跳解析。

CR-195进一步要求首版MCP Streamable HTTP的process-installed endpoint entry把exact Trust Policy编译为bounded显式PEM trust bundle；entry与
startup config都必须content-addressed，并与exact MCP Deployment、endpoint、Network/TLS/Trust/Auth Policy refs一起校验。该运行时material
不回写Deployment、不成为第二active binding，也不能由RPC调用方覆盖。

CR-196把OAuth token endpoint纳入同一规则：OAuth process-installed verification binding除exact Auth Policy/profile/JWKS外，必须冻结
Deployment closure的exact Trust Policy及其bounded PEM roots。token endpoint变化、Trust Policy漂移或bundle变化都产生新的startup config
digest；Callback RPC不携带或覆盖trust正文。

这里的“可运行”包含可被未来Run选择/解析的定义绑定，不等于启动独立进程。Skill Deployment冻结一个exact Skill Revision及其
requirement resolution；Policy Deployment冻结一个exact Policy Revision及其适用环境/资格闭包；Sandbox Deployment冻结exact
Profile、Runtime/Package compatibility与隔离policy闭包。它们不得绕过Deployment而直接把ResourceVersion设为tenant active binding。
ContextDataset是唯一例外：它不是可调用definition，root的`active_version_id`只表示未来query使用的immutable Dataset Generation data head。

首版没有Installation业务identity、Release/Candidate binding、compatibility generation或installation-scoped current row。

## 2. Nominal ID registry

每个ID是UUIDv7-backed nominal type，wire使用stable prefix；完整closed machine registry由
`contracts/platform-v1/registries.json`拥有。只有owner registry可创建/解析ID，不通过裸UUID、
字符串前缀或同UUID alias推断类型。

| 对象 | 示例prefix | 约束 |
|---|---|---|
| Tenant | `ten_` | 所有业务current row的scope |
| Resource | `res_` | generic lifecycle root |
| ResourceVersion | kind-specific，如`agtv_`/`skv_`/`capv_` | immutable version |
| Deployment | kind-specific，如`adep_`/`skdep_`/`cdep_`/`xdep_`/`mcdep_`/`mdep_`/`pdep_`/`sxdep_` | exact binding |
| Run / NodeExecution | `run_` / `nod_` | orchestration identity |
| Invocation / Job / Task | `inv_` / `job_` / `tsk_` | 彼此独立 |
| RunValue | `val_` | 不与Job/Artifact共享UUID |
| Artifact / Blob / Link / Grant | `art_` / `blb_` / `lnk_` / `grt_` | Artifact domain identity |
| Event / Receipt / Outbox | `evt_` / `rcp_` / `obx_` | shared consistency identity |

不定义`InstallationId`、`SandboxJobId`或独立`OperationId`。public Operation ID就是JobId的API field projection。

解析必须验证prefix、UUIDv7、canonical lowercase encoding、nil/variant和expected nominal kind。跨类型比较必须显式转换
为internal UUID only after both nominal types are proven，不暴露为public API。

## 3. Canonical shared nominal types

`ComponentRole`和`CanonicalRegion`由本规范唯一拥有，Worker/startup/Deployment/Artifact/Model等下游只复用。

`CanonicalRegion`长度1～63，只允许小写ASCII字母、数字和单中划线，首尾必须字母数字。空值、
大写、下划线、Unicode、连续/首尾中划线和超长值fail closed。

`ComponentRole`是closed enum。CR-202 的当前闭包包含16个实际部署的role：既有Management/Runtime API、
Scheduler/Recovery、Model、Native/Remote Capability、Context、MCP、Sandbox Controller/WASI/gVisor、
Artifact Gateway/DataWorker/Maintenance、Egress/Secret Broker，另加`RegistryValidationWorker`。Artifact仍只有
Gateway/DataWorker/Maintenance，Sandbox仍只有Controller/WasiExecutor/GvisorExecutor；无MicroVM、ManagedStdio或
ModelArtifact role。

## 4. Resource

```rust
struct Resource {
    resource_id: ResourceId,
    tenant_id: TenantId,
    kind: ResourceKind,
    name: ResourceName,
    draft_generation: u64,
    draft: TypedDraftPayload,
    validation: Option<ValidationSummary>,
    active_deployment_id: Option<DeploymentId>,
    active_data_version_id: Option<ResourceVersionId>, // ContextDataset only
    lifecycle_state: ResourceLifecycleState,
    projection_version: u64,
}
```

ResourceKind是closed registry，至少包含Agent、Skill、CapabilityInterface、CapabilityImplementation、ContextSource、ContextDataset、
ModelProvider、ModelProfile、McpServer、Policy、SandboxRuntime和SandboxProfile。shared table不消除domain payload的nominal type。

Resource拥有name、唯一current editable Draft、validation fence、current binding/data head和lifecycle；Draft不是一条尚未发布的
ResourceVersion，也不在`resource_versions`中创建mutable row。ResourceVersion只拥有publication产生的immutable definition，
不复制active state。Draft update/validation/publication与head mutation使用expected Resource projection version，validation还绑定
exact draft generation + document digest；tenant + kind + normalized name唯一。

除ContextDataset外，`active_data_version_id`必须为空且active command只接受属于该Resource的exact Deployment。ContextDataset不创建
可调用Deployment，`active_deployment_id`必须为空，Dataset build成功事务以generation CAS更新`active_data_version_id`。物理列名可以沿用
baseline的`active_version_id`，但规范与Rust owner type不得把它解释为普通definition active head。

## 5. ResourceVersion

```rust
struct ResourceVersion {
    version_id: ResourceVersionId,
    resource_id: ResourceId,
    tenant_id: TenantId,
    kind: ResourceKind,
    ordinal: u64,
    payload: TypedVersionPayload,
    schema_version: u16,
    payload_digest: Digest,
    created_at: Timestamp,
    published_at: Timestamp,
}
```

Draft通过Resource上的generation继续编辑；每次更新使旧validation失效。只有publication才创建ResourceVersion，所有
ResourceVersion从出生起immutable。publication必须以Resource projection version + draft generation + document digest CAS，验证closed
payload、引用图、schema/digest、policy/security/hard limits、Artifact/Secret requirements和domain-specific validator，并可为Agent等
owner原子创建一个bounded typed revision batch。不修改已发布row来“修补”历史，也不为Draft建立第二current-state投影。

payload是有size limit的typed JSONB，带`schema_version`、closed validation、canonical serialization和digest。每个kind的Rust
nominal payload是语义authority，boundary schema从它生成或做conformance对照。

## 6. Deployment

```rust
struct Deployment {
    deployment_id: DeploymentId,
    tenant_id: TenantId,
    resource_id: ResourceId,
    version_id: ResourceVersionId,
    environment: EnvironmentName,
    closure: DeploymentClosure,
    closure_digest: Digest,
    created_by: PrincipalId,
    created_at: Timestamp,
}
```

Deployment是一经创建就不可变的exact runnable closure：它不复制Version definition，只冻结环境相关backend、
credential reference、region（若该typed closure需要）、runtime/protocol和exact dependency。Deployment不拥有可变state、
projection version或另一个current head；可绑定性由它引用的immutable closure与Secret/policy安全门禁共同决定。

create request不是持久化closure的逐字段镜像。凡字段依赖本次command才产生的identity（例如Agent Context slot的
`adep` owner与`xcb`）或可由typed intent规范派生（例如binding digest），都必须由服务端在同一事务物化；public caller不得
预选或覆盖。Receipt重放返回第一次提交的相同Deployment及嵌套identity，不能重新分配后产生第二个closure。

closed Deployment closure matrix至少包含Agent、Skill、Capability、Context、MCP、Model Provider/Profile、Policy与Sandbox Profile；
每个variant必须携带其owner exact Revision。definition-only variant仍必须冻结selection/requirement/applicability或qualification evidence，
不能用空closure、裸Version ID或通用JSON代替。
Agent Deployment还必须冻结由exact Plan Revision验证得到的entry node ID/kind；该入口进入closure digest，root Run admission
不得从untrusted请求接收内部node kind，也不得在事务中临时读取Artifact来猜入口。

Resource是未来Run绑定的唯一current authority：`active_deployment_id`指向当前exact Deployment，`AdministrativeGate`
决定该绑定是否接受新admission。同一tenant/resource只有一个active binding。activate事务锁定Resource与目标
Deployment，校验expected Resource version和exact closure digest，原子设置active binding及`Enabled` gate，并写Event/Outbox/Receipt。
suspend只在path Deployment仍是Resource active binding时，以Resource CAS将gate设为`Suspended`；它不改写Deployment。
再次activate同一或其他exact Deployment可原子恢复`Enabled`。上述命令都不扫描或改写已存Run。

## 7. 引用闭包与发布

ResourceVersion/Deployment引用必须使用exact typed ID + digest + allowed-kind edge，不许可“当前最新”或自由字符串查找。
发布验证有界DAG、无非法循环、tenant/region/classification一致、全部dependency已发布并未被suspended。

validation/discovery/build等异步工作使用shared Job。成功Job事务创建immutable evidence/derived version并推进owner；
public Operation只是Job projection。

`RegistryValidation`由独立`RegistryValidationWorker`执行，而非Management API、CLI、Scheduler或通用后台代理。它只
消费`RegistryValidation` WorkClass、当前Job payload固定的validator/profile digest与当前Draft的exact version/digest；不执行用户代码、
不调用任意网络端点，也不把process configuration当作租户业务输入。成功必须在一个fenced owner transaction中同时重验Job lease、
Resource version/draft digest、tenant-scoped validator ServiceIdentity与闭合依赖/安全证据，写入`ValidationSummary`、终结Job并写
Job/Resource Event、Outbox及JobCommit Receipt。Draft在执行期间变更、lease失效、validator/profile漂移或依赖闭包不成立时，不得写入
ValidationSummary；只按shared Job failure/recovery合同终结或重试。该闭环不增加表、ResourceVersion、Operation aggregate或public route。

## 8. RunBindingsSnapshot

root Run admission从request所选Agent Resource的tenant-scoped enabled active Deployment出发，解析完整exact closure并一次性保存：

```rust
struct RunBindingsSnapshotV2 {
    schema_version: ConstU16<2>,
    tenant_id: TenantId,
    agent_deployment_id: DeploymentId,
    agent_version_id: ResourceVersionId,
    agent_interface_version_id: ResourceVersionId,
    plan_version_id: ResourceVersionId,
    principal: FrozenPrincipal,
    skill_bindings: Vec<ExactDeploymentBinding>,
    capability_bindings: Vec<ExactDeploymentBinding>,
    context_bindings: Vec<ExactDeploymentBinding>,
    model_bindings: Vec<ExactDeploymentBinding>,
    policy_bindings: Vec<ExactDeploymentBinding>,
    sandbox_bindings: Vec<ExactDeploymentBinding>,
    mcp_bindings: Vec<ExactDeploymentBinding>,
    execution_profile: FrozenExecutionProfile,
    closure_digest: Digest,
}
```

列表使用canonical ID排序并受hard limit。snapshot不包含Installation、Release/Candidate、compatibility generation、CI manifest或
active-head pointer。Run执行只使用snapshot内exact IDs/digests，不重新解析当前active binding。

child Run使用父Run允许的exact binding子集及child Agent exact Deployment。父子冻结与parent link同事务形成，
不在child创建时重读active head猜测“更新”binding。

## 9. 动态管理与GitOps边界

业务定义的动态发布使用Resource -> Version -> Deployment -> Binding。平台运行时不动态安装arbitrary
binary、Secret Provider、Sandbox runtime、database migration或Kubernetes topology。

binary/image/config/schema/HardLimit/Capacity profile的release、promotion和rollback由GitOps/CI/CD/Kubernetes拥有，不是
Resource/Deployment API或业务current state。启动时通过18的startup manifest对照exact digests。

## 10. 事务、Event 与Receipt

- create/draft-update/validate/publish/deploy/activate/suspend全部tenant-scoped；
- mutation使用Receipt + expected projection version；
- aggregate mutation、Event、Outbox和Receipt result同事务；
- active switch与Run admission并发时，Run只能冻结完整旧或完整新closure，不得混合；
- suspended Deployment阻止新admission，已存Run按04/06的emergency policy处理，不改写snapshot。

## 11. 验收标准

- ID registry对prefix/UUIDv7/kind正负矩阵通过，不存在SandboxJob/Operation/Installation独立ID；
- Published ResourceVersion与Ready/Active Deployment immutable字段不能原地改写；
- unknown payload field/version/kind、wrong tenant/region/digest/reference edge fail closed；
- active switch/admission并发只产生完整旧或完整新RunBindingsSnapshot；
- child Run只使用parent允许的exact closure；
- GitOps rollout不需要Installation Release DB row，不改写历史Run；
- JSONB schema/digest/size/canonicalization与boundary codegen/conformance通过。

## 12. 明确推迟

- cross-tenant/public marketplace、federated identity和cross-region active binding；
- runtime binary/plugin/provider installer；
- Installation Release/Gate API和兼容代际。

## 13. 未决问题

首版identity/resource/deployment/run binding合同无未决设计问题。
