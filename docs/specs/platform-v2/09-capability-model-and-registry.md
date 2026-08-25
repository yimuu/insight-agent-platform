# Platform v2 Capability 模型与 Registry 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-188 |
| 日期 | 2026-08-20 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) |
| 直接下游 | 10、11、13、14、15、17、18 |

> CR-181 impact：05 Plan v4的CapabilityCall冻结slot、input/output、candidate route与retry limit；Capability Interface/
> Deployment必须为publication和owner transaction提供exact input/output schema及04 selector closure，不能由Invocation caller补充。

> CR-182：Capability/Model/Skill candidate binding的Selection Policy Revision必须解码为04 schema v1 document；空Policy document、
> unsupported mode或route schema不兼容使Deployment publication失败。

> Persistence ruling：Capability Interface、Implementation 与 Deployment 使用 02 的共享 Resource 模型；validation、
> conformance、selection 与 suspension 保存为 typed snapshot/event，不建立专用 lifecycle/evidence/head 表族。

## 1. 决策摘要

Capability Interface 是唯一通用可调用业务合同；Implementation 是原生 Rust、HTTP、gRPC、MCP Tool 或
Sandboxed Script 的不可变后端实现。Agent/Skill 依赖 Interface slot，Deployment 固定 Interface Revision、
Implementation Revision、Policy 和运行证据。Registry 不执行调用，也不让 discovery 自动发布或授权。

## 2. 目标与非目标

### 2.1 目标

- 用同一 Interface 表达本地、远程、MCP 和脚本能力；
- 把类型、Effect、幂等、取消、进度、Artifact 和数据策略纳入机器合同；
- 分离业务 Interface 与后端 endpoint/image/module；
- 显式 discovery、validation、publish、deploy、activate 和 suspend；
- 在 Agent Deployment 时验证实现兼容性并固定 exact binding；
- 为 Model tool projection 生成稳定、安全、无冲突的工具视图；
- 提供 backend conformance suite 和健康/安全门。

### 2.2 非目标

- 不保留顶层 Action 概念或 `ActionRegistry`；
- 不把 ContextSource、Model、Subagent 或 Skill 伪装成 Capability；
- 不允许 runtime 通配符、`latest`、自动 import 或 discovery 后直接授权；
- 不允许 Interface 直接含 credential、endpoint、container image 或 MCP session；
- 不允许客户端上传原生 Rust 动态库；
- 不提供任意 shell command Capability；
- 不承诺所有实现的进度、取消和流式能力完全相同，差异必须在实现验证中显式。

## 3. Capability Interface Revision

```rust
struct CapabilityInterfaceRevision {
    interface_revision_id: ResourceVersionId,
    capability_id: CapabilityId,
    qualified_name: CapabilityName,
    input_schema: ClosedJsonSchema,
    output_schema: ClosedJsonSchema,
    error_schema: ErrorSchema,
    effect: Effect,
    idempotency: IdempotencyContract,
    cancellation: CancellationContract,
    progress: ProgressContract,
    artifacts: ArtifactContract,
    data_policy: DataFlowPolicy,
    execution_limits: InterfaceLimits,
    semantic_digest: Digest,
}
```

上述占位类型的首个closed machine合同固定为：

```rust
enum CapabilityArtifactDirection { Input, Output }

struct CapabilityArtifactPort {
    name: PortName,
    direction: CapabilityArtifactDirection,
    media_types: Vec<MediaTypePattern>,
    maximum_count: u16,
    maximum_single_bytes: u64,
    maximum_total_bytes: u64,
    maximum_classification: DataClassification,
}

struct CapabilityArtifactContract {
    ports: Vec<CapabilityArtifactPort>,
}

struct CapabilityDataFlowPolicy {
    maximum_input_classification: DataClassification,
    maximum_output_classification: DataClassification,
    allowed_regions: Vec<CanonicalRegion>,
    declassification_policy: Option<ExactVersionRef>,
}

struct CapabilityInterfaceLimits {
    maximum_input_bytes: u32,
    maximum_output_bytes: u32,
    maximum_artifacts: u16,
    maximum_execution_milliseconds: u64,
}
```

Artifact direction machine wire固定为`input | output`。port按`(direction,name)`严格排序且唯一，最多64个；每个port最多
64个严格排序唯一的lowercase media pattern，只允许exact `type/subtype`或`type/*`，不能包含parameter。count、single/total
bytes均为正，`single <= total <= single * count`；所有port count之和必须精确等于Interface limits的
`maximum_artifacts`，避免两个容量authority。Interface input/output分别不超过16/64 MiB，execution不超过1小时，且仍受18
deployment HardLimitProfile进一步收紧。

`allowed_regions`只使用02 `CanonicalRegion`，非空、按canonical bytes严格排序唯一且最多32个；旧的32-byte且允许下划线的
`DataRegion` validator在clean-cut目标中删除。实际input不能高于input ceiling；实际output不能高于output ceiling，也不能
低于实际input classification，除非冻结exact Declassification Policy Revision并由04授权、记录转换Evidence。Implementation和
Deployment可以进一步收紧这些合同，不能扩大。Invocation admission完整冻结Artifact/DataFlow/Interface limits和三个schema
digest；exact Interface ResourceVersion保存三个完整`ClosedJsonSchema` validation snapshot。

`qualified_name` 仅用于 authoring/discovery，格式为小写 dot-separated name，例如 `presentation.render`。
执行、权限和绑定使用 opaque ID/revision，不使用名称重新路由。

## 4. Schema 合同

- 必须使用05唯一权威的`insight.closed-json-schema/1`，不得增加backend/provider专用keyword；
- input/output 顶层必须是 object，空输入使用闭合空 object；
- 每个字段声明 description、classification、required/optional、size bounds；
- Interface内显式的Artifact字段使用 nominal ArtifactRef 和 media type policy。整个逻辑JSON Value因超过inline
  threshold而由`ValueRef::Artifact`承载时，input/output schema仍描述物化后的逻辑值，不描述外层
  ArtifactRef metadata；
- Secret 不作为普通字段；使用已绑定 SecretPurpose，由 adapter late resolve；
- schema validation 在 dispatch 前和 result commit 前都执行；
- 实现返回额外字段、非有限数字、错误 Artifact scope 或超限正文时调用失败；
- schema digest 进入 Interface semantic digest。

发布的 Interface ResourceVersion 必须同时保存 input/output/error schema digest 与完整 validation snapshot；缺少任一项就
不能发布或被 Invocation 引用。同步成功结果的 Value schema 必须等于 Invocation 冻结的 exact output schema；Declared
Failure 必须引用同一 Interface ResourceVersion。backend 或调用方不能在提交结果时替换 schema/error Interface。

## 5. Effect 与幂等

Interface Effect 使用 04 的闭合枚举。幂等合同：

```rust
enum IdempotencyContract {
    Intrinsic,
    CallerKey { field_or_header: IdempotencyLocation },
    ReconcileBeforeRetry { reconcile_operation: ReconcileContract },
    None,
}
```

machine wire registry固定为`intrinsic | caller_key | reconcile_before_retry | none`；variant payload仍由闭合
Interface文档承载，不能把未识别字符串降级为`none`。

- Pure 必须是 `Intrinsic`；
- ReadOnly 应为 Intrinsic，若外部服务有观察性副作用必须明确；
- IdempotentWrite 必须声明后端接受并持久化 caller key 的证据；
- NonIdempotentWrite/Irreversible 通常为 None；
- Implementation 不能弱化 Interface Effect/Idempotency；
- retry policy 由 Interface、Implementation、NodePolicy 与安全 Policy 的交集决定。

## 6. Cancellation 与 Progress

```rust
enum CancellationContract {
    Unsupported,
    BestEffort,
    Confirmed,
}

struct ProgressContract {
    mode: ProgressMode,
    schema_digest: Option<Digest>,
    max_events: u32,
    max_bytes_per_event: u32,
    minimum_interval_milliseconds: u64,
    durability: ProgressDurability,
}
```

Cancellation machine wire registry固定为`unsupported | best_effort | confirmed`。Progress `mode`固定为
`none | events`，`durability`固定为`none | live_only | coarse_durable`：`mode=none`时durability必须为`none`、schema
缺省且所有event limit为零；`mode=events`时durability不能为`none`，schema和正数硬限制必须完整。上述值与backend
kind一样由同一machine registry供Revision、Deployment、dispatcher和conformance消费。

Progress durability 只有 `LiveOnly` 或 `CoarseDurable`。高频 token/log/progress 不进入 durable ledger；
CoarseDurable 只保存有界 milestone。取消确认只表示后端确认任务停止，不证明此前副作用未发生。

`ProgressContract`是Capability Interface ResourceVersion的closed machine字段，不得只存在于authoring Artifact或validation
evidence。`max_events`、`max_bytes_per_event`和`minimum_interval_milliseconds`还必须分别受18的versioned HardLimitProfile
约束；Invocation admission冻结整份合同，progress command不能提交新限制。

## 7. Artifact Contract

Artifact port 声明：

```rust
struct ArtifactPort {
    name: PortName,
    direction: InputOrOutput,
    media_types: BTreeSet<MediaTypePattern>,
    max_count: u16,
    max_single_bytes: u64,
    max_total_bytes: u64,
    classification: DataClassification,
}
```

Implementation 只能通过 scoped Artifact protocol 读取/写入声明端口，不能获得 tenant bucket 通用凭据。

## 8. Implementation Revision

```rust
struct CapabilityImplementationRevision {
    implementation_revision_id: ResourceVersionId,
    implementation_id: CapabilityImplementationId,
    interface_revision_id: ResourceVersionId,
    backend_contract: BackendContractDescriptor,
    credential_requirements: Vec<SecretPurpose>,
    backend_limits: BackendLimits,
    implementation_digest: Digest,
}
```

Revision中的后端合同代数：

```rust
enum BackendContractDescriptor {
    Native(NativeAdapterContract),
    Http(HttpProtocolContract),
    Grpc(GrpcProtocolContract),
    Mcp(McpToolContract),
    Sandbox(SandboxExecutionContract),
}
```

其machine wire registry固定为`native | http | grpc | mcp | sandbox`；Revision、Deployment binding、dispatcher和
conformance suite必须消费同一registry，不能以自由字符串或远端discovery值扩展。

Revision只固定adapter/protocol/schema mapping、credential requirement、取消/异步语义和backend limits，不固定
endpoint、SecretBinding、network/TLS、具体MCP Deployment或Sandbox runtime/profile。未知backend fail closed；增加
backend需要协议版本、dispatcher、policy、metrics和conformance更新。

### 8.1 Installed protocol codec authority

HTTP、gRPC与MCP的mapping digest不是可执行程序。首版把mapping authoring输入在publication/镜像构建阶段验证并编译为受信
Capability Worker镜像内的静态codec；runtime不解释模板、不读取源码Artifact、不下载模块。每个已安装codec必须由startup报告以下
closed manifest，按`(backend_kind, codec_id, codec_version, descriptor_digest)`严格排序且唯一，最多1024项：

```rust
struct InstalledCapabilityCodecManifest {
    schema_version: u32,                 // 固定1
    backend_kind: CapabilityBackendKind, // 仅http | grpc | mcp
    codec_id: StableCodecId,
    codec_version: StableVersion,
    module_digest: Digest,
    worker_protocol_version: u32,
    descriptor_digest: Digest,
}
```

`descriptor_digest`是backend closed contract中全部protocol/schema/mapping字段的domain-separated canonical digest；HTTP包含
method、protocol与request/response/error mapping及idempotency header，gRPC包含protobuf/service/method及三类mapping与
idempotency metadata，MCP包含tool/schema/output mapping/protocol profile/discovery evidence与task/progress feature。增加或减少任一
字段必须改变digest。Implementation publication必须找到exact installed manifest并把
`codec_id/version/module/descriptor/worker_manifest_digest`冻结进Deployment backend binding；不能只比较mapping digest。

Native沿用installed adapter manifest；Sandbox沿用package/runtime binding。Remote Capability Deployment的HTTP、gRPC、MCP binding
均必须新增`worker_manifest_digest`和上述exact codec identity。claim与dispatch同时比较进程Worker manifest、codec manifest及backend
descriptor；错lane、错镜像、缺codec或digest漂移在外部I/O前fail closed。

## 9. Native Backend

Native 实现是随受信任 Worker 构建/安装的静态 adapter：

```text
adapter_id
adapter_version
module_digest
entrypoint_id
worker_protocol_version
```

- Registry API 只能选择已安装 manifest，不能上传 `.so`/Rust code；
- Worker startup 报告签名 manifest；
- claim 必须匹配 exact adapter/module digest；
- Native 仍在 Capability Worker 运行，不进入 Scheduler/API 进程；
- panic 被 Worker containment 捕获为 platform failure。

## 10. HTTP/gRPC Backend

Revision固定：

- protocol/OpenAPI/protobuf contract digest；
- authentication purpose/credential requirements；
- request/response mapping；
- sync/deferred/cancel/callback capabilities；
- timeout、body和protocol limits。

所有backend共享以下closed feature contract，并作为Implementation ResourceVersion的machine字段持久化：

```rust
struct CapabilityBackendFeatures {
    deferred: bool,
    input_required: bool,
    callback: bool,
    poll: bool,
    progress: bool,
    cancellation: bool,
    max_remote_state_bytes: u32,
    max_poll_count: u32,
}
```

`callback|poll`要求`deferred=true`；`poll=false`时`max_poll_count=0`，反之必须为正；不支持deferred时
`max_remote_state_bytes=0`。Implementation feature只能收紧Interface合同，dispatcher在每个outcome/wake/progress/cancel
事务重新校验frozen feature，不能相信backend自报能力。

Capability Deployment固定canonical endpoint、SecretBinding、TLS/mTLS、network、redirect/proxy和connection policy。

映射authoring格式是受验证的declarative input；只有publication/build生成并由上述exact installed manifest标识的静态codec可执行。
runtime不执行任意模板代码。HTTP redirect默认禁止；错误正文不直接成为public Failure。

## 11. MCP Backend

MCP Tool Implementation Revision固定tool name/schema mapping、required protocol profile、OAuth/credential
requirements、task/cancellation/interaction语义和发现时的semantic evidence digest；Capability Deployment再固定13的
exact MCP Deployment、discovery snapshot和authorization binding policy。MCP annotation不覆盖平台Effect、approval、
schema和data policy。详细协议由13定义。

## 12. Sandbox Backend

Sandbox Implementation Revision固定：

- code/package Artifact digest；
- fixed entrypoint；
- dependency lock digest；
- input/output mapping。

Capability Deployment再固定Sandbox Package/Runtime/Profile Revision、isolation/network/resource policy和部署证据。

Shell 只能是 ReviewedPublished implementation 的固定 entrypoint。参数作为结构化 argv/JSON 传递，不拼接
`sh -c`。ModelGenerated code 由专用动态执行 Capability 和更强 isolation policy 承载。

## 13. Registry 生命周期

```text
Capability Entity
  -> Interface Draft
  -> Interface Validation
  -> Interface Revision

Implementation Entity
  -> Implementation Draft/Discovery Snapshot
  -> Schema + Semantic Validation
  -> Implementation Revision
  -> Deployment Draft/Resolution
  -> Connectivity + Conformance Validation
  -> Capability Deployment
  -> Active Head / Suspension
```

Interface publish 与 Implementation publish 是独立操作。一个 Interface 可以有多个 Implementation；一个
Implementation 只实现一个精确 Interface Revision。升级 Interface 必须重新验证/发布 Implementation。

## 14. Discovery 与发布

- MCP/OpenAPI/protobuf/installed adapter/image metadata 只能生成 candidate/evidence；
- discovery 不持有 DB transaction；
- candidate 有来源、digest、observed_at、expires_at 和 size limits；
- Operator 显式选择、确认 Effect/Policy、validation 后才能 publish；
- publish 不 activate；
- implementation health 不自动移动 active head；
- suspension 是独立运行时门。

## 15. Capability Deployment

```rust
struct CapabilityDeployment {
    capability_deployment_id: DeploymentId,
    interface_revision_id: ResourceVersionId,
    implementation_revision_id: ResourceVersionId,
    backend_binding: CapabilityBackendBinding,
    secret_bindings: Vec<ExactSecretBindingRef>,
    network_policy_revision_id: Option<ResourceVersionId>,
    isolation_policy_revision_id: Option<ResourceVersionId>,
    resource_profile_revision_id: Option<ResourceVersionId>,
    policy_revision_ids: Vec<ResourceVersionId>,
    conformance_evidence_id: EvidenceId,
    deployment_digest: Digest,
}

enum CapabilityBackendBinding {
    Native(InstalledNativeBinding),
    Http(ExactHttpEndpointBinding),
    Grpc(ExactGrpcEndpointBinding),
    Mcp(ExactMcpToolBinding),
    Sandbox(ExactSandboxBinding),
}
```

Deployment固定Interface、Implementation、实际backend、SecretBinding、network/isolation/resource Policy、runtime
identity与绑定exact环境的conformance evidence。backend binding与Revision的backend contract variant必须一致；其
credential requirements必须被SecretBinding完整且仅按purpose满足。Agent Deployment可以引用一个或多个已批准
Capability Deployment候选，但RunBindings必须同时固定集合、exact Selection Policy和模型tool name mapping。Selection
Policy只决定一次Invocation的首次candidate并产生typed evidence，不授予自动failover；运行时不自动跨Deployment重试。
如果未来支持failover，必须新增独立failover policy、冻结全部候选并把每次切换作为新的可审计选择，不能复用首次选择证据。

`secret_bindings`由创建Deployment的repository从active Binding派生04的exact reference；它是Deployment
closure和digest的一部分，不是仅含ID的可变查找列表。

## 16. Model Tool Projection

Model 可见工具从已绑定 Interface 生成：

```rust
struct ModelToolProjection {
    tool_name: ModelToolName,
    capability_interface_revision_id: ResourceVersionId,
    description: String,
    input_schema: ClosedJsonSchema,
    safe_output_summary: Option<ClosedJsonSchema>,
}
```

- tool name 在单 ModelLoop 内唯一，长度/字符受限；
- name mapping 在 Deployment 固定，不从远程 description 推断；
- description 是平台审核的 Interface 文本，不直接使用 MCP/remote 未信任文案；
- output 是否回给模型与是否公开给用户是两种策略；
- tool projection 不能包含 endpoint、credential、tenant ID 或 backend 类型。

## 17. Compatibility 与 Conformance

实现发布前必须通过：

- schema positive/negative fixtures；
- effect/idempotency declared behavior tests；
- timeout、retry、cancel、duplicate request；
- oversized/malformed input/output；
- Artifact scope/media/digest；
- network/TLS/auth failure；
- deferred callback/poll；
- Secret redaction；
- worker crash/late outcome；
- implementation-specific protocol tests。

Conformance evidence 有版本和过期策略。过期不改写历史 Deployment，但可以阻止新绑定或触发 suspension。

## 18. 健康与安全门

Registry 分离：active head、observed health、circuit state、administrative suspension、credential revoke。
健康变化不生成新Revision；adapter/protocol/schema mapping改变必须新Implementation Revision；endpoint、Secret、
network/TLS、runtime/profile或其他环境绑定改变必须新Capability Deployment。紧急suspension可以阻止历史
Deployment尚未开始的调用。

## 19. Persistence 映射

Capability Interface 与 Implementation 是不同 `ResourceKind`，分别保留 identity、schema、effect 和 backend 类型；它们
共享 Resource/ResourceVersion 的 lifecycle 机制。Capability Deployment 使用共享 Deployment，bindings payload 冻结 exact
Interface/Implementation/Policy/Secret references。Discovery、validation 与 conformance 是 Job 结果和 ResourceVersion 的
typed validation snapshot；历史进入 Event/Artifact，不创建独立 evidence/head/suspension 表。

## 20. 可观测性与审计

Registry 指标使用 backend class、state、outcome 等低基数标签。endpoint、tool name、tenant、Capability ID、
SecretBinding ID/opaque reference不进入label。publish/activate/suspend产生body-free audit与outbox。

## 21. 验收标准

- 同一 Interface 分别由 Native、HTTP、MCP、Sandbox fixture 实现并通过同一 conformance suite；
- Implementation 无法弱化 Effect、Schema、Approval、Network 或 Data Policy；
- discovery 不会自动 publish/activate；
- active head 切换不改变既有 Run binding；
- implementation suspension 阻止尚未开始的历史 leaf；
- Model tool intent 不能越过固定 name mapping；
- malformed/oversized output 在进入 Plan Value 前失败；
- 任意 Shell 字符串、动态库上传和 mutable image tag publication 被拒绝；
- Secret canary 不进入 revision digest、API、日志、event 或错误；
- `allowed_regions`只接受02 CanonicalRegion common schema，覆盖非空、1/32项、canonical bytes排序唯一及33项/重复/乱序；大写、下划线、
  Unicode、provider alias和旧DataRegion合法但CanonicalRegion非法的输入均拒绝，不做归一化；
- backend 增加时 exhaustive protocol/conformance gate 生效。
- HTTP/gRPC/MCP Deployment缺失exact installed codec identity/module/descriptor或required Worker manifest时不能publish/claim；空registry、
  错lane、错镜像与descriptor漂移在外部I/O计数仍为零时fail closed。

### 21.1 当前实施证据边界（非规范性）

Capability Worker已能从PostgreSQL claim的exact ExecutionContract/Input与Job fence构造credential-free adapter request，按完整
Native descriptor和WorkerManifest digest选择process-installed adapter，并只经fenced PostgreSQL authority提交outcome。durable control
winner与原claim必须在tenant、Invocation/Job、attempt、lease/token、Worker generation及Deployment/Input上exact一致，只允许旋转Job
optimistic version；Native/HTTP/gRPC随后取消同一物理执行。transport cancel observation不等于no-effect proof，write Effect因此进入
ReconciliationRequired；deadline后只允许在由frozen backend timeout派生且受平台hard limit封顶的cleanup window提交。

16项adapter/worker unit、8项Invocation unit与fresh PostgreSQL 16端到端fixture覆盖exact selection、malformed input、timeout uncertainty、
unsafe write retry降级、attempt exhaustion、stale Worker identity、terminal/cancellation commit、cancel/completed first-winner和replay。
Egress另有29项unit，其中8项覆盖Capability HTTP/gRPC exact catalog、DNS public-IP/connection pinning、late Secret、bounded framing/response、
Effect/idempotency failure与stale exact cancel；不增加表或migration。

该证据证明当前Native执行/取消组合及HTTP/gRPC Egress候选代码边界，不证明所有backend已完成真实进程conformance。
真实远端服务、Secret Manager/TLS/mTLS composition、callback、Sandbox组合和18的L4～L6资格尚未完成。
这只是当前实施证据边界；本规范已Accepted，不能用候选代码或fixture绕过Implementation/Verified门禁。

## 22. 明确推迟的工作

- Marketplace 和第三方签名信任联盟；
- 自动 backend failover；
- streaming output 作为 Plan Value；
- billing/price catalog；
- compatibility range 自动求解；
- capability composition DSL。

## 23. 未决问题

CR-181要求Capability publication证明Plan node input/output schema compatibility、candidate route Policy schema及所有candidate的Effect/
backend/retry约束交集；任一candidate不兼容时Deployment失败，不能在runtime删减集合。CR-181 cross-review已确认该合同并恢复Accepted。

CR-166已将region nominal统一到02，并删除Model/release installation compatibility合同。本规范已Accepted；
Capability registry、backend resolution和publication fixture仍待实现。具体HTTP/gRPC wire envelope与Sandbox protocol
分别由10、14冻结，但必须实现本规范的统一Interface和安全合同。

CR-188已关闭remote codec可执行权威：runtime只执行Deployment冻结且由Worker startup manifest证明的静态installed codec；
mapping digest不再被误当成可实例化程序。

2026-08-25 implementation evidence：Rust owner已加入closed installed codec reference与domain-separated完整backend descriptor digest；
HTTP/gRPC/MCP Deployment binding冻结required Worker manifest，三个dispatcher在transport前同时重验manifest、codec identity/module与
descriptor。16项adapter test包含manifest/module/descriptor漂移且transport调用计数为零；fresh PostgreSQL 16 r200完整Invocation
admission/claim/outcome/Task/reconcile/Receipt/quota fixture通过。production binary/startup manifest publication与L3/L4仍待完成。

后续fresh PostgreSQL 16 r201进一步把required Worker manifest放入claim command并由owner transaction在attempt/quota reservation前
对照exact Deployment；错误镜像返回空claim，正确manifest仍通过完整Invocation回归。dispatcher二次校验继续保留，形成claim与I/O双闸。
