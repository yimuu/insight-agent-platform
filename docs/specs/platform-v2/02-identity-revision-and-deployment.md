# Platform v2 Identity、Revision 与 Deployment 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-20 |
| 依赖 | 00、01 |
| 直接下游 | 03～18 |

## 1. 决策摘要

Agent、Skill、Capability、Context、Model、MCP、Policy和Sandbox共享一条动态管理生命周期：

```text
Resource -> immutable ResourceVersion -> Deployment -> tenant active binding
```

ResourceVersion表示可重现的逻辑定义，Deployment表示可运行的exact环境绑定。Run admission一次性冻结
所需Deployment和ResourceVersion闭包，active head与GitOps rollout变化不修改已存Run。

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
| Deployment | `dep_` | exact runnable binding |
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

`ComponentRole`是closed enum，只包含18实际部署的role。Artifact只有Gateway/DataWorker/Maintenance，Sandbox只有
Controller/WasiExecutor/GvisorExecutor；无MicroVM、ManagedStdio或ModelArtifact role。

## 4. Resource

```rust
struct Resource {
    resource_id: ResourceId,
    tenant_id: TenantId,
    kind: ResourceKind,
    name: ResourceName,
    active_version_id: Option<ResourceVersionId>,
    lifecycle_state: ResourceLifecycleState,
    projection_version: u64,
}
```

ResourceKind是closed registry，至少包含Agent、Skill、CapabilityInterface、CapabilityImplementation、ContextSource、ContextDataset、
ModelProvider、ModelProfile、McpServer、Policy、SandboxRuntime和SandboxProfile。shared table不消除domain payload的nominal type。

Resource拥有name/current head/lifecycle。ResourceVersion拥有immutable definition，不复制active state。并发head mutation使用
expected projection version，tenant + kind + normalized name唯一。

## 5. ResourceVersion

```rust
struct ResourceVersion {
    version_id: ResourceVersionId,
    resource_id: ResourceId,
    tenant_id: TenantId,
    kind: ResourceKind,
    ordinal: u64,
    state: VersionState,
    payload: TypedVersionPayload,
    schema_version: u16,
    payload_digest: Digest,
    created_at: Timestamp,
    published_at: Option<Timestamp>,
}
```

Draft可以通过创建新version继续编辑，Published/Retired version immutable。publication必须验证closed payload、引用图、
schema/digest、policy/security/hard limits、Artifact/Secret requirements和domain-specific validator。不修改已发布row来“修补”历史。

payload是有size limit的typed JSONB，带`schema_version`、closed validation、canonical serialization和digest。每个kind的Rust
nominal payload是语义authority，boundary schema从它生成或做conformance对照。

## 6. Deployment

```rust
struct Deployment {
    deployment_id: DeploymentId,
    tenant_id: TenantId,
    resource_id: ResourceId,
    version_id: ResourceVersionId,
    state: DeploymentState,
    environment: EnvironmentName,
    region: CanonicalRegion,
    backend: TypedBackendBinding,
    secret_bindings: Vec<SecretBindingRef>,
    policy_bindings: Vec<ResourceVersionId>,
    dependency_closure: Vec<ExactDependency>,
    runtime_digest: Digest,
    closure_digest: Digest,
    projection_version: u64,
}
```

Deployment不复制Version definition，只冻结环境相关backend、credential reference、region、runtime/protocol和exact dependency closure。
合法state为`Draft -> Validating -> Ready -> Active | Suspended -> Retired`，具体合法边由domain owner定义。

同一tenant/resource/environment/region只有一个active Deployment。activate事务锁定Resource、old/new Deployment和expected
versions，更新active binding、Event/Outbox和Receipt。它不扫描或改写已存Run。

## 7. 引用闭包与发布

ResourceVersion/Deployment引用必须使用exact typed ID + digest + allowed-kind edge，不许可“当前最新”或自由字符串查找。
发布验证有界DAG、无非法循环、tenant/region/classification一致、全部dependency已发布并未被suspended。

validation/discovery/build等异步工作使用shared Job。成功Job事务创建immutable evidence/derived version并推进owner；
public Operation只是Job projection。

## 8. RunBindingsSnapshot

root Run admission从tenant active Agent Deployment出发，解析完整exact closure并一次性保存：

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

- create/version/publish/deploy/activate/suspend全部tenant-scoped；
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
