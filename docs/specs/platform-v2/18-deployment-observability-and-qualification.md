# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | [`00-overview.md`](00-overview.md)～[`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) |
| 直接下游 | 实现计划、迁移记录、资格报告与 `docs/current` |

> Persistence ruling：旧 migration 1～35、177 表 catalog、schema checksum 与其资格证据均已撤销。Qualification 只对新的
> 单一 baseline 和后续真实行为生效；旧记录不能证明当前实现。

## 1. 决策摘要

Platform v2 的生产基线是单 region、多 availability zone 的 Kubernetes 部署，使用 PostgreSQL 16 HA 作为唯一
事务/运行权威、S3-compatible private object store 作为 Artifact blob 权威、NATS 作为 wake/live/outbox 投影
transport、外部 Secret Manager 作为 Secret value 权威。Control（其中RegistryValidation使用独立Worker role、pool与permit）、Runtime、Model、Capability、Context、MCP、
Sandbox、Artifact 与 Recovery 使用独立 Deployment、service account、DB/connection pool、queue、permit 和
autoscaling policy。

Artifact受信读取进一步按调用audience物理切分：Model Artifact Broker与Sandbox Artifact Broker必须是不同进程、
Deployment、ServiceAccount、restricted PostgreSQL credential/pool和process-local permit。Model Broker保持read-only且只暴露
`ReadModelRequest`；Sandbox Broker只暴露WASI与microVM read RPC，后两者可以共享Sandbox audience自己的bulkhead。两个Broker不得
共Pod、共pool或通过单listener动态选择audience；一侧饱和、重启或泄露不能消耗另一侧准入容量。

Artifact-backed Model output由第三个独立组件Model Artifact Producer承载，不扩展上述read Broker。Producer使用独立进程、
Deployment、ServiceAccount、write-limited PostgreSQL credential/pool、S3/KMS write workload identity、client-stream endpoint和
write permit；它不得与Model read Broker或Sandbox Broker共享Pod、ServiceAccount、数据库credential/pool、storage identity、
connection pool或semaphore。Model Worker使用与Model read client分离的exact
`spiffe://insight.platform/workload/model-worker.artifact-output` mTLS client、连接池和有界stream调用Producer；Producer
饱和、滚动或失败不得耗尽Model request读取、Sandbox读取、Model Worker control/cancel或控制面容量。

普通 workload 遵守 Kubernetes Restricted security baseline。Sandbox controller 与 KVM Executor 位于独立 namespace/
node pool；只有最小 Executor Agent 能访问 `/dev/kvm`，user code 只在14定义的WASM/gVisor/microVM边界执行。
PodDisruptionBudget、topology spread和rolling strategy共同保护自愿中断，但durable PostgreSQL state仍是实例丢失后的
恢复基础。

Observability 统一使用OpenTelemetry语义的traces/metrics/log correlation，另设不可变Audit管道。任何指标、日志、
trace、profile和alert都不能包含Secret、Prompt/代码/文档/模型正文、signed URL、token或高基数tenant/Run ID label。

Release只有通过Contract、Functional、Security、Recovery、Capacity、Soak与DR七类资格门后才能声明Verified。
初始生产资格profile是 `Q1-50`：50 active Runs、隔舱混合负载、Sandbox饱和、组件kill、消息丢失和24小时soak。

## 2. 目标与非目标

### 2.1 目标

- 给所有逻辑组件明确Kubernetes、identity、network、storage、pool、scaling和readiness边界；
- 冻结单region生产HA、备份、恢复、迁移、发布、回滚和drain合同；
- 定义低基数metrics、trace/log correlation、audit、dashboard、alert与runbook最低集合；
- 给API、Scheduler、Outbox、SSE、Worker、Sandbox和Artifact可测量SLI/SLO；
- 提供real-process、故障注入、容量、soak、DR与安全资格矩阵；
- 让每次资格报告绑定commit、image/schema/config/dataset/命令和原始证据；
- 证明Sandbox/MCP/Provider/Artifact任一隔舱饱和或失败不会拖垮控制面；
- 证明Model Artifact Producer、Model read Broker与Sandbox Broker任一单独饱和、失败或滚动不会消耗另外两条Artifact lane；
- 明确何时00～18可从Draft进入Verified并同步 `docs/current`。

### 2.2 非目标

- 不提供multi-region active-active、global scheduler或跨region Run迁移；
- 不部署自托管PostgreSQL/NATS/S3/Secret Manager的具体产品教程；
- 不提供GPU/HPC、大型batch计算或模型训练节点；
- 不以Kubernetes Job/CronJob作为Run/Sandbox durable authority；
- 不用日志搜索、Prometheus、NATS或Kubernetes object代替业务数据库；
- 不支持新旧 `/v1` 双栈、dual-write、在线数据兼容迁移或自动资源翻译；
- 不承诺未经过Q1及正式证据证明的云厂商、规模或SLO；
- 不定义组织排班、商业支持套餐或合规认证本身。

## 3. 外部平台基线

本规范使用Kubernetes稳定能力和OpenTelemetry标准信号，但不把业务协议绑定到某个发行版/后端。非规范性参考：

- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/)
- [Kubernetes disruptions/PDB](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/)
- [OpenTelemetry concepts](https://opentelemetry.io/docs/concepts/)
- [OpenTelemetry metrics/cardinality](https://opentelemetry.io/docs/concepts/signals/metrics/)

生产环境必须固定受支持的Kubernetes、PostgreSQL、NATS、object store、runtime、OTel collector和Secret Manager版本，
记录到 CandidateManifest/ReleaseManifest。升级先通过相同 conformance/chaos，不运行时追随“latest”。

## 4. 环境与发布单位

环境至少分为：

| 环境 | 用途 | 是否可声明生产资格 |
|---|---|---|
| Local Authoring | compiler/schema/unit、本地PostgreSQL/NATS/S3 | 否 |
| Integration | real-process contract和故障窗口 | 否 |
| Qualification | production-equivalent topology/load/chaos/soak | 是，产生候选证据 |
| Production | approved ReleaseManifest | 是 |

Local环境不得用SQLite、in-memory durable authority、plain runc user code或跳过Policy模拟生产语义。没有Linux KVM时，
microVM workload必须调用受控remote Qualification Sandbox或明确不可运行；测试stub不能生成security qualification。

资格测试首先固定不可变 CandidateManifest；通过资格并批准后，再生成引用证据的 ReleaseManifest，避免 manifest
digest 与 qualification bundle 形成循环引用：

```rust
struct CandidateManifest {
    candidate_id: ReleaseCandidateId,
    git_commit: GitCommit,
    contract_digest: Digest,
    database_schema_version: SchemaVersion,
    component_images: BTreeMap<ComponentRole, ImageDigest>,
    worker_manifests: Vec<WorkerManifestDigest>,
    model_output_materialization_mode: ModelOutputMaterializationMode,
    component_capacity_manifests: Vec<ComponentCapacityManifestDigest>,
    artifact_storage_binding_manifests: Vec<ArtifactStorageBindingManifestDigest>,
    component_startup_manifests: Vec<ComponentStartupManifestDigest>,
    deployment_config_digest: Digest,
    hard_limit_profile_digest: Digest,
    policy_baseline_digest: Digest,
    qualification_profile: QualificationProfileId,
    created_at: DateTime<Utc>,
}

struct ReleaseManifest {
    release_id: ReleaseId,
    candidate_manifest_digest: Digest,
    qualification_bundle_id: ArtifactId,
    approval_receipt_id: ApprovalReceiptId,
    created_at: DateTime<Utc>,
}

enum ModelOutputMaterializationMode {
    InlineOnly,
    ArtifactCapable,
}

struct CapacityPrimitiveIdentityV1 {
    primitive_name: String,
    identity_digest: Digest,
}

struct CapacityIsolationIdentitySetV1 {
    schema_version: u32, // const 1
    pool_identities: Vec<CapacityPrimitiveIdentityV1>,
    semaphore_identities: Vec<CapacityPrimitiveIdentityV1>,
}

struct ComponentStartupManifestV1 {
    manifest_version: u32, // const 1
    component_role: ComponentRole,
    startup_profile_id: ComponentStartupProfileId,
    startup_schema_digest: Digest,
    startup_config_digest: Digest,
    capacity_isolation: Option<CapacityIsolationIdentitySetV1>,
}

#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum StartupCapacityRequirementV1 {
    CapacityFree,
    Isolated {
        pool_primitive_names: Vec<String>,
        semaphore_primitive_names: Vec<String>,
    },
}

struct ComponentStartupProfileV1 {
    profile_id: ComponentStartupProfileId,
    startup_schema_digest: Digest,
    capacity_requirement: StartupCapacityRequirementV1,
}

struct ComponentStartupProfileRegistryV1 {
    schema_version: u32, // const 1
    profiles: Vec<ComponentStartupProfileV1>,
}
```

`GitCommit` wire值必须是带算法标签的完整小写object ID：`sha1:<40-hex>`或`sha256:<64-hex>`；分支、tag、缩写SHA和
`latest`均非法。`ComponentRole`只使用02的共享nominal；`component_images`的key就是Candidate计划安装的完整Deployment logical scope集合，
不得用临时Pod名或副本名。
`database_schema_version`精确表示`insight-platform-postgres`导出的schema contract version（当前候选基线为`6`），不是
migration文件数量、数据库产品版本或payload schema version。Candidate创建器必须从实际安装的closed `WorkerManifest`集合和
`HardLimitProfile`计算canonical digest closure；worker、component-capacity、artifact-storage-binding与component-startup digest各自按字节
升序且唯一。每个component image/Deployment role必须恰有一份`ComponentStartupManifestV1`；每个由WorkerManifest或当前已注册
ComponentCapacityManifest variant管理的role还必须恰有一份对应manifest。manifest role必须与image、Deployment及startup config逐字段匹配；重复role、
缺失或额外manifest、limit digest漂移均拒绝。Candidate schema/builder/runtime readiness必须执行同一closure，不能只在文档或Helm lint检查。

四个digest数组始终是required字段。machine constants固定
`MAX_CANDIDATE_WORKER_MANIFESTS=512`、`MAX_CANDIDATE_COMPONENT_CAPACITY_MANIFESTS=256`、
`MAX_CANDIDATE_ARTIFACT_STORAGE_BINDING_MANIFESTS=MAX_INSTALLATION_ARTIFACT_STORAGE_BINDINGS`、
`MAX_CANDIDATE_COMPONENT_STARTUP_MANIFESTS=256`；`worker_manifests`固定
1～512项，`component_capacity_manifests`固定0～256项，`artifact_storage_binding_manifests`固定1～64项，
`component_startup_manifests`固定1～256项且与`component_images` role集合exact相等。storage manifest wire与64项hard max只由15拥有；18
只验证其digest closure。catalog独立服务Package、request Artifact与Model output，不随Model output mode清空，也不要求每个binding已被某个动态Deployment引用。
JSON Schema执行对应`minItems/maxItems/uniqueItems`，Rust additionally执行raw digest bytes严格升序；component-capacity空数组是显式状态，
不是unknown或unconfigured。

`model_output_materialization_mode` wire只允许`inline_only | artifact_capable`，并且只能由Candidate builder从release installation closure派生，
不能由调用者布尔值、Policy baseline digest、动态Model Deployment catalog或opaque `deployment_config_digest`声称：

- `inline_only`当且仅当不存在任何`ModelArtifactProducer` ComponentCapacityManifest或使用
  `model_artifact_producer/v1` startup profile的component scope；storage catalog仍必须有1～64项；
- `artifact_capable`当且仅当存在一至多个完整Producer logical scope；每个scope在`component_images`、一个
  `ModelArtifactProducer` capacity manifest和一个`model_artifact_producer/v1` startup manifest三处exact出现，并且至少存在一份匹配
  Model Worker v2 manifest。即使当前没有任何Model Deployment也合法；
- 每个Producer scope的storage binding集合必须非空、是15 Candidate catalog子集且全部region匹配；不同scope的binding集合不得重叠。
  未分配给Producer的binding仍可服务其他Artifact路径。任一scope partial/orphan、同binding路由到多个scope或capacity/startup role错配都拒绝；
  runtime readiness还必须证明实际Deployment、image与startup document逐值匹配。

同一Producer logical scope的replica使用同一`ComponentRole`、byte-identical startup config和logical capacity identities；“2 per storage
region/boundary”表示该scope内副本数，不是两个manifest。不同region/boundary使用不同opaque `ComponentRole`和不同manifest/identity，
component kind仍由`kind=model_artifact_producer`表达，不能把所有boundary挤进一个固定role。

`ComponentStartupManifestV1`是capacity identity的共同machine carrier，closed schema路径固定为
`contracts/platform-v1/schemas/component-startup-manifest.schema.json`。唯一role/profile registry document与schema固定为
`contracts/platform-v1/deployment/component-startup-profiles.json`及
`contracts/platform-v1/schemas/deployment/component-startup-profiles.schema.json`，两者进入根contract digest。registry最多256个profile，
按`profile_id` UTF-8 bytes严格升序且唯一；ID为1～128 ASCII bytes并匹配`^[a-z][a-z0-9_.\/-]{0,127}$`。每个entry冻结exact
`startup_schema_digest`与closed tagged capacity requirement；`isolated`的pool/semaphore name数组各0～16、严格升序且唯一且不能同时为空，
`capacity_free`不得携带数组。unknown profile、schema digest漂移或unknown registry字段fail closed。

`startup_config_digest`必须是该role实际closed startup document的canonical digest，`startup_profile_id`必须在registry中且
`startup_schema_digest`逐值相等。registry要求`isolated`时，startup document与manifest都必须携带非空、非null
`capacity_isolation`，且pool/semaphore `primitive_name`集合分别exact等于profile；`capacity_free`时必须完全省略property。每个identity entry
是closed object；`primitive_name`是1～64 bytes的ASCII stable key，pattern固定`^[a-z][a-z0-9_.-]{0,63}$`。两数组按name严格升序；identity
digest在单项、单role和全Candidate范围都必须唯一。

contracts crate不导入各binary的startup config。它只定义sealed `ValidatedComponentStartupV1`值、registry validator及
`CandidateStartupProjectionV1` port；每个deployable component crate负责用registry锁定的closed schema把typed startup config验证并投影为
`(component_role, canonical_startup_bytes, ComponentStartupManifestV1)`。Candidate builder只接收这些validated projection，自行复算
`startup_config_digest`和manifest digest；不能接收预计算digest、未验证JSON或调用者预投影的identity集合。readiness由同一component adapter
对进程实际启动document重新投影并与Candidate exact compare，projection逻辑不得在builder与binary各复制一份。每个Candidate image的
signed build provenance还必须声明其compile-time `startup_profile_id/startup_schema_digest`，并与对应manifest exact相等；错误image/profile
组合不能只等到进程启动才发现。

`CapacityPrimitiveFactoryV1`是production composition创建本地pool、semaphore和weighted permit registry的唯一port：每次构造必须按
`component_role + kind + primitive_name`恰消费manifest中的一个identity，重复、缺失、kind/name不匹配或启动结束后存在未消费entry都使
readiness失败；architecture gate禁止production component绕过该port直接构造未登记primitive。因此startup manifest是运行时capacity
identity的输入authority，不是从任意binary内部状态事后猜测的摘要；checked-in profile registry是唯一role/schema/capacity注册authority，
不得再由binary或Helm维护第二份列表。

把全部startup manifest的pool与semaphore identity digests合并后，每个identity在整个Candidate中必须恰出现一次，包括同role的
pool↔semaphore；任何alias都fail closed。identity表示role-scoped logical allocation family，不是容量值、整组config digest或Pod/进程实例；
不同logical primitive必须有不同identity，同一role的replica各自实例化physical member但共享该role family identity。`model_artifact_producer/v1`
固定pool names `{database,kms,object_store}`及semaphore names
`{declared_bytes,global_stream,per_tenant_stream_registry,wire_buffer}`；其他profile的exact集合也只来自registry并由上述factory逐项消费，
不能以generic runtime map、未消费entry或直接构造primitive绕过。

只要Candidate包含MCP OAuth绑定，`deployment_config_digest`覆盖的closed Egress配置还必须包含exact Auth Policy revision、完整
Auth Profile、允许的非对称JWT算法、public JWKS及其canonical digest，以及OAuth写入所用ServiceIdentity Principal。运行时不得从
issuer动态补齐、刷新或替换该信任根；key rotation通过新Candidate发布并重新资格，而不是在旧Candidate内静默漂移。

`WorkerManifestDigest`必须是`contracts/platform-v1/schemas/worker-manifest.schema.json` closed document的canonical digest。
Accepted目标使用07的`manifest_version=2`：每份document只允许一个exact WorkClass，并冻结`component_role`、`worker_role`、
`adapter_runtime_digest`、协议版本、业务最大并发和正数`critical_control_reserved_slots`；Model role还必须且只有它可以携带closed
`model_output_materialization { slots, aggregate_bytes }`。CandidateManifest中的不同digest不能在运行时合并成共享semaphore。当前
wire对非Model必须完全省略该property，`null`非法；Model必须提供closed object，两个值为正、`slots <= max_concurrency`、
`aggregate_bytes <= 9007199254740991`，并分别不超过Candidate profile的effective worker slots/aggregate bytes。`component_role`必须存在于
`component_images`且在WorkerManifest集合中唯一，并与对应startup manifest逐值相等；`worker_role`独立标识claim role且仍须唯一。任一
capacity变化都必须改变canonical WorkerManifest digest。
当前
checked-in v1 contract不具备该字段，不能证明Artifact-backed output；本地pool primitive的unit evidence也不是CandidateManifest，
不能替代Gate E/Q1负载证据。

非Job-claim服务的本地容量使用独立closed `ComponentCapacityManifest`，不能伪装成WorkerManifest或只藏在opaque
`deployment_config_digest`中。首个variant固定为：

```rust
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ComponentCapacityManifest {
    ModelArtifactProducer(ModelArtifactProducerCapacityV1),
}

enum AdmissionQueueMode { RejectWhenSaturated }

struct ModelArtifactProducerCapacityV1 {
    manifest_version: u32, // const 1
    component_role: ComponentRole,
    region: CanonicalRegion,
    storage_binding_digests: Vec<Digest>,
    admission_queue_mode: AdmissionQueueMode, // exact RejectWhenSaturated
    transport_accept_backlog: u32,
    transport_accept_timeout_milliseconds: u32,
    in_flight_streams: u32,
    in_flight_declared_bytes: u64,
    in_flight_buffer_bytes: u64,
    streams_per_tenant: u32,
    database_connections: u16,
    object_store_connections: u16,
    kms_connections: u16,
}
```

`ComponentCapacityManifest`使用flat internally-tagged JSON；首个variant的discriminator exact为
`"kind":"model_artifact_producer"`，其`manifest_version=1`与`admission_queue_mode="reject_when_saturated"`为const，不能用外层
`spec`、untagged shape或`null`代替；`component_role`使用02 nominal并与对应startup/image scope相等，不再固定为单一literal。
`region`使用15 `CanonicalRegion`并与16 `DataRegion`按exact bytes比较。`storage_binding_digests` required 1～64项，按raw bytes严格升序且
唯一；每项必须存在于Candidate的15 storage catalog且manifest region逐值相等，不同Producer scope不得重复认领。所有object
`additionalProperties=false`，所有JSON u64数值还必须不超过`9007199254740991`。

除`admission_queue_mode`外所有容量值必须为正；Producer不允许application admission queue，饱和必须立即返回16的typed transient
failure。`transport_accept_backlog <= in_flight_streams`、`streams_per_tenant <= in_flight_streams`；backlog、accept timeout、streams、
declared bytes、buffer bytes与per-tenant streams六项分别不超过HardLimitProfile v5同名Candidate effective字段。checked
`in_flight_streams * (effective model_output_chunk_bytes + protobuf_envelope_hard_overhead)`不得超过`in_flight_buffer_bytes`；
`in_flight_declared_bytes`是按每个header声明总量加权的并发准入，不代表把完整response聚合进内存。DB pool必须同时不超过该组件stream
上限、effective `control_data.database_connections`及installation DB总连接预算；object-store/KMS pool各不超过stream上限。所有角色的
DB pool总和加migration/incident reserve仍须低于DB max connections。manifest digest、上述canonical
`component_startup_manifests`与exact startup config一并进入Candidate closure；Candidate必须拒绝任一局部pool/semaphore identity重复，单个
整组摘要或opaque `deployment_config_digest`不能证明该不变量。目标schema路径为
`contracts/platform-v1/schemas/component-capacity-manifest.schema.json`；当前尚未checked in，故现有
Candidate证据不适用于Model Artifact Producer。

`transport_accept_timeout_milliseconds`不是只用于配置检查的数字。transport front在连接进入bounded accepted backlog时立即记录
`accepted_monotonic_at`，并以effective manifest/HardLimit较小值建立唯一monotonic deadline；TLS handshake、service-role authorization、
backlog/global-stream/wire-buffer permit等待以及完整首个Header的bounded decode都必须在该deadline内完成。kernel/listener backlog和ingress
handshake/idle timeout不得大于同一effective值。到期必须取消transport、释放已取得的全部permit且在尚无valid Header identity时只返回
body-free status，不创建/修改Receipt或其他数据库事实。valid Header完成current授权并取得冻结Attempt absolute deadline后，后续Data、
Terminal、DB/S3/KMS等待改由该Attempt deadline封顶，不能用重新计时延长它。

`ArtifactStorageBindingManifestDigest`是15 closed manifest canonical bytes的digest，也就是04/15中的`storage_binding_digest`；不能用
opaque deployment config或endpoint字符串代替。Candidate使用15的pure timing validator，以effective `artifact.staging_seconds`和所有引用
Policy重验strict quiescence/grace关系；不满足生产等价backend/proxy合同的binding不得进入Candidate。目标schema尚未checked in，因此既有
Candidate不能作为该write-quiescence合同的实现证据。

其中`protobuf_envelope_hard_overhead`不是自由配置：16 RPC machine contract固定
`MODEL_OUTPUT_PROTOBUF_ENVELOPE_OVERHEAD_BYTES=4096`。其当前machine carrier是closed
`contracts/platform-v1/protocol/model-output-rpc.json`与对应schema，两者必须进入root contract digest；公开Rust const与后续protobuf逐值
复验同一document。Candidate builder只能用该const做checked add/multiplication，环境变量、Helm与HardLimitProfile均不能覆盖。
Candidate的installation mode为`artifact_capable`时，必须同时存在exact Model Worker v2 manifest及至少一个完整Producer scope；未部署
scope时不得把孤立capacity/startup manifest计作可用容量。18把Candidate投影为16 `InstalledModelOutputCapabilitiesV1`实现；每个
ArtifactCapable Model Deployment在创建/激活/Run admission时通过该port证明其Policy storage digest路由到exact Producer scope，并证明至少一个
匹配adapter/region的Model Worker满足`slots >= 1 && aggregate_bytes >= maximum_materialized_bytes`，且Producer满足
`in_flight_declared_bytes >= maximum_materialized_bytes`与
`in_flight_buffer_bytes >= effective model_output_chunk_bytes + 4096`，15 binding还必须满足
`maximum_object_bytes >= maximum_materialized_bytes`；否则拒绝Deployment激活、Release切换或Run admission，不能让Job永久Ready到deadline。

Release切换必须实现16的共享compatibility generation协议：在03既有installation-scoped Aggregate row的同一`FOR UPDATE`事务内，把incoming
Candidate投影为port、重验全部active Model Deployment、CAS exact旧generation并提交新Candidate digest/generation；与并发Deployment
activation互斥。Runtime API在创建Run的事务中按同一row做共享锁或等价CAS，重新调用port并冻结Candidate digest/generation。Kubernetes
rollout/readiness只能在该durable fence之后推进流量，不能用先扫描后异步写pointer的两阶段窗口代替；不新增release compatibility表。

### 4.1 当前证据边界（非规范性）

旧177表设计的migration 1～35、catalog、fixture和CR-090/092/093/094候选记录已全部撤销，不计入任何当前Gate、
CandidateManifest或ReleaseManifest，也不得作为新`0001`的历史前置条件。

当前可复现的数据库foundation只有23表、单一`0001_platform_baseline.sql`与对应schema verifier。Phase 1/2真实PostgreSQL 16
integration fixture已覆盖generic Resource/Security、Run/Job/Task/Subagent/controller、并发claim、fence/retry/wait/recovery、
bounded safety scan、独立business/critical-control pool以及lease-fenced executor start/heartbeat/handoff。这些记录只属于开发期
Contract/Functional子证据：尚未绑定immutable CandidateManifest、production-equivalent images/config/topology和完整Q1 dataset，
因此不能声明Gate D/E、Q1或Release资格。Artifact/Invocation、外部backend、50 active Runs、跨WorkClass饱和、24小时soak与DR
证据必须在对应实现阶段重新产生，不能沿用已撤销设计的数字或报告。

此前CandidateManifest基础的closed Rust type、checked-in JSON Schema、canonical digest与WorkerManifest v1/HardLimitProfile v4
exact-closure validator已经交付并进入`insight.platform/v1`根合同digest；它们尚无本节新增的materialization mode、component-capacity、
storage-binding、component-startup digest集合或全Candidate capacity alias验证，不能证明CR-165目标合同。当前也未生成绑定
production-equivalent images/config/topology的Candidate实例，没有任何Gate A～G结果或ReleaseManifest；因此旧foundation本身不构成新合同
或资格证据。

Sandbox expired-lease runtime现也有独立`WorkClass::Sandbox` business/critical-control permit、分片scan、backend evidence与fenced
commit driver；unit fixture证明Sandbox业务permit耗尽时critical-control scan仍运行。Core NATS control adapter也已实现exact
WorkerProcessGeneration subject、bounded closed request/reply和signal-digest binding。Helm已把WASI与microVM拆为独立DaemonSet/node
selector；microVM Pod内又把非root Executor和唯一持有KVM/cgroup/jail/state权限的Provider按volume、credential与capability拆开。CR-164
进一步要求Executor、Provider和shared control-plane使用不同immutable image，冻结builder/runtime base digest并按target platform构建；每个
microVM target只复制自己的平台可执行文件。执行pool固定Linux与单一`amd64 | arm64`架构及NodeRestriction保护的exact selector；KVM pool另有
exact taint/toleration，attestor以独立selector覆盖两个pool。Provider独占彼此不重叠的exact hostPath及预安装、root-owned、Kubernetes 1.33+
递归只读runtime-assets。部署声明只接受Firecracker `1.16.1`且version与Firecracker/jailer路径segment必须
一致；该静态规则不替代per-arch asset bytes/digest证明。全部workload/NATS Secret名称互异，ValidatingAdmissionPolicy覆盖Pod与ephemeral
container子资源并锁定exact volume source、mount、image、command、env/probe、CPU/memory resource和security context；`runtimeClassName`、
DRA claim、Pod-level resource、extended-resource device request、secondary-CNI annotation及其他非允许metadata均关闭；全部Binding使用
Kubernetes维护的exact namespace-name label，受限子资源policy恒拒绝Executor namespace的exec/attach/port-forward/resize。Pod固定default
scheduler；binding只接受Candidate配置中经cluster audit确认的exact scheduler identity、Node target、空annotation及region/zone topology
label。Pod CREATE只接受同样经audit确认的exact DaemonSet controller identity和唯一role ownerReference，UPDATE保持该ownerReference逐字段不变，
阻止复制或孤立合法spec产生额外root authority。Dockerfile instruction closure、Pod security-projection mutation、Helm负向门禁及
Kubernetes 1.35.6 server-side CEL编译检测缺失Provider、共享/mutable image、build-host binary、credential/hostPath alias、非递归只读asset mount、
node/version/path漂移。该证据只证明静态部署与启动依赖合同；真实Admission Deny与cluster audit identity fixture、asset实物与ancestor/TOCTOU、node image、签名/SBOM/provenance、authenticated NATS、真实KVM
node、Linux capability充分性、PostgreSQL故障窗口和Q1饱和仍未绑定同一Candidate资格，因此只属于开发期Contract/Functional子证据。

Model执行面现有OpenAI Responses与Anthropic Messages两个wire adapter的共同开发期fixture，覆盖固定protocol request、text/tool/schema/
usage normalization与未知Provider字段拒绝；credential-free brokered connector还覆盖incremental SSE、总量边界、重复JSON key、closed
content-type/status与`[DONE]`终止，且不暴露Secret value或任意endpoint。Provider、MCP、Capability和Sandbox连接边界
现传递冻结generation/provider/purpose/policy digest的`ExactSecretBindingRef`，不以当前Binding ID查找替代Deployment闭包。独立
`insight-platform-egress`生产HTTPS slice已覆盖Model、MCP OAuth、同步及experimental Task-aware MCP Streamable HTTP operation及Capability HTTP/HTTP2-gRPC的exact catalog、DNS全量public-IP验证与
连接pinning、SSRF、no-proxy/no-redirect、Pinned/Follow Secret evidence、fixed auth header/metadata、bounded stream/framing/response与
exact generation cancel；MCP operation还执行`initialize -> initialized -> method POST`，重验冻结protocol/capability、敏感session header及
strict JSON/SSE/JSON-RPC bounds。Task-aware路径只在冻结profile/Discovery/tool contract共同允许时附加`task.ttl`，以AES-256-GCM密文保存
exact-attempt-bound task/session，按原session执行`tasks/get`/`tasks/result`并强制related-task；poll上限使未决write进入reconciliation且保留密文handle。
同一密文handle也支持协商后的`tasks/cancel`，只确认同一task ID的`cancelled`结果，且取消接受不构成此前Effect未发生的证明。
当前40项Egress unit中8项专门覆盖Capability闭包漂移、private DNS、late Secret、Effect/idempotency failure及
stale cancel。但该证据尚未经过真实Secret Manager provider、真实Provider/MCP/Capability进程、
故障注入、独立Pod/NetworkPolicy或同一CandidateManifest，因此不能登记为Gate B、C、D或E。

Model Worker现在已有独立候选binary和静态Kubernetes拓扑：进程启动复验config/WorkerManifest/两个adapter descriptor，使用独立bounded
PostgreSQL pool、Model Worker mTLS Egress客户端和独立Model Artifact Broker read mTLS客户端；chart提供双副本rolling Deployment、PDB、HPA、topology spread、Restricted Pod、
无入站的default-deny NetworkPolicy及只到DNS/Egress/PostgreSQL和配置allowlist NATS TLS端口的出口。CI同时拒绝mutable image、单副本、
空PostgreSQL/NATS allowlist、缺失NATS TLS Secret key和非法
HPA。durable cancel driver现使用reserved critical-control permit，把当前generation的bounded PostgreSQL safety scan、Egress exact cancel和
旋转fence下的保守terminal结算组合起来；unit fixture证明业务permit饱和不阻止取消，数据库fixture覆盖取消/完成first-winner。Artifact-backed
request已经由生产进程通过对应audience的Artifact Broker RPC物化；既有双副本、Restricted Pod、只读PostgreSQL与loopback mTLS证据证明最小authority和
错误workload role拒绝，但旧单Broker拓扑已被本次architecture revision取代，不能作为新目标证据。
当前实施批将Model与Sandbox Broker拆成不同进程/Deployment/ServiceAccount/DB credential/pool/permit，并分别收窄为Model-only read与
WASI+microVM-only read RPC surface；Sandbox Controller仍移除provider catalog、AWS workload token、S3/KMS client与对应直出网络。双向
Helm/mTLS/DB credential互换、独立饱和和rolling restart门禁未全部形成Candidate evidence前，该切分只是target/implementation slice，
不登记Gate或Phase完成。独立Model Artifact Producer尚无domain/RPC实现、binary、Model Worker client-stream组合、Deployment、ServiceAccount、
write-limited数据库role/pool、S3/KMS write identity、NetworkPolicy、autoscaling或故障/容量fixture；现有Model Worker也没有独立Producer
mTLS client、连接池或permit。该组合未绑定真实CandidateManifest，Artifact output仍为Inline，read Broker不得被临时扩权为output writer。
Model text delta内部publisher已有exact fence、canonical credential-free envelope、将容量permit
保留到有界批次flush结束的双重有界non-blocking queue和TLS/mTLS NATS组合；它不发布tool argument/Provider metadata，NATS故障不阻断durable执行。但Artifact-backed
output、Model Artifact Producer三lane隔舱、真实S3/KMS、公开SSE消费、真实NATS/Provider/process-kill/cross-workclass saturation资格证据仍缺失，
因此只属于Contract/Functional输入，不能关闭Phase 4/6或登记Gate A～E通过。

Capability Worker的开发期Functional证据现把fresh PostgreSQL 16 claim、exact Native adapter dispatch/cancel和fenced terminal/
cancellation commit连成同一可复现fixture，并覆盖durable control后的Job version fence旋转、完整物理身份重验、write reconciliation、
RunValue、Receipt、Event、Outbox、quota settle/replay、reserve/settle ledger identity隔离及cancel/completed first-winner。deadline后cancel
只在frozen backend timeout派生、平台hard limit封顶的cleanup window保留同一fence authority。该fixture没有绑定CandidateManifest，
也未执行进程终止、真实远端HTTP/gRPC、Secret Manager/TLS、跨Pod取消或饱和故障窗口，因此只属于Gate A/B的候选输入，
不能登记任何Gate通过。

## 5. Kubernetes Namespace 与节点池

规范逻辑隔离：

```text
platform-control       Management API, Registry validators
platform-runtime       Runtime API, SSE, Scheduler, Recovery, regular Workers
platform-integrations  Model Workers, MCP Hosts, remote adapters
platform-artifacts     Model/Sandbox Artifact read Brokers, Model Artifact Producer, Artifact Gateway, scanner controller, GC
platform-sandbox       Sandbox Gateway/Controller, Secret/Egress proxies
platform-sandbox-exec  gVisor/microVM/WASM Executors on dedicated nodes
platform-observability OTel Collectors and telemetry agents
```

- namespace使用独立service account、Role、NetworkPolicy、ResourceQuota和Pod Security Admission；
- 普通 namespace 执行 Restricted profile 并锁定版本；
- `platform-sandbox-exec` 使用单独admission policy，只给特定Executor Agent精确KVM/runtime权限；
- microVM node pool有taint/toleration、node selector、无普通业务Pod；
- management/control workload不调度到sandbox nodes；
- telemetry agent不能读取tenant volume/Secret/guest memory；
- node/runtime attestor以DaemonSet或等价node agent运行，只有它可以观察Executor Pod UID、node UID、runtime sandbox/cgroup与
  process-start identity并签发generation登记/absence证明；Controller和Executor不得拥有该node/runtime读取权限；
- cluster-system/operator identity与平台application identity分离；
- namespace不是tenant security boundary，tenant isolation仍由04/DB/Artifact/Grant强制。

## 6. 组件部署矩阵

Q1生产最小topology：

| 角色 | 最小副本 | 模式 | 关键依赖 | 是否可scale-to-zero |
|---|---:|---|---|---|
| Management API | 3 | stateless Deployment | PostgreSQL、Policy、SecretBinding metadata | 否 |
| Registry/Management Operation Worker | 2 | queue worker | PostgreSQL、Artifact、target validator | 否 |
| Runtime API | 3 | stateless Deployment | PostgreSQL、Policy | 否 |
| SSE Gateway | 3 | stateless Deployment | PostgreSQL、NATS hint | 否 |
| Scheduler/Coordinator | 3 | active-active shard lease | PostgreSQL、NATS hint | 否 |
| Outbox Dispatcher | 2 | active-active claim | PostgreSQL、NATS | 否 |
| Recovery/Deadline Worker | 2 | active-active shard lease | PostgreSQL | 否 |
| Model Worker | 2 per required adapter/region | queue worker | PostgreSQL、Provider、Secret | 否，生产绑定存在时 |
| Model Artifact Broker | 2 per storage region/boundary | stateless Model-only read gRPC | exact Model Worker read mTLS、Model专用read-only PostgreSQL pool、S3/KMS read identity | 否，存在Artifact-backed Model request时 |
| Model Artifact Producer | 2 per storage region/boundary | stateless Model-output client-stream gRPC | exact Model Worker output mTLS、独立write-limited PostgreSQL pool、S3/KMS write identity | 否，允许Artifact-backed Model output时 |
| Sandbox Artifact Broker | 2 per storage region/boundary | stateless WASI+microVM internal gRPC | exact Sandbox Controller mTLS、Sandbox专用restricted PostgreSQL pool、S3、KMS | 否，存在Sandbox Package/Artifact绑定时 |
| Egress Broker | 2 per external region/boundary | stateless internal gRPC | Security Authority RPC、private DNS、KMS/Secret Manager、exact remote endpoints | 否，生产外部绑定存在时 |
| Security Authority | 2 | stateless internal gRPC | PostgreSQL restricted role、Policy | 否 |
| Capability Worker | 2 per required manifest | queue worker | PostgreSQL、remote backend | 否，生产绑定存在时 |
| Context Worker | 2 | queue worker | PostgreSQL、index/Artifact | 否 |
| Interaction/Approval Worker | 2 | queue/deadline worker | PostgreSQL、Policy | 否 |
| Dataset Builder | 1+ | separate queue | PostgreSQL、Artifact/index | 是 |
| MCP Host | 2 | session/queue worker | PostgreSQL、Secret、remote MCP | 否，生产绑定存在时 |
| Artifact Gateway | 3 | stateless stream | PostgreSQL、S3、KMS | 否 |
| Artifact Scanner/Finalizer | 2 | queue worker | PostgreSQL、S3、Sandbox | 否 |
| Artifact GC/Reconciler | 2 | shard lease | PostgreSQL、S3 | 否 |
| Sandbox Gateway/Controller | 2 | durable queue controller | PostgreSQL、Artifact/Secret Broker | 否 |
| WASM/gVisor/microVM Executor | capacity-based | dedicated nodes | Sandbox Controller | 按已验证cold-start policy |

副本不是一致性来源；所有multi-active角色使用03/07的claim/lease/epoch/fence。增加副本不能创建per-pod全表scanner、
per-SSE DB listener或per-worker NATS data connection。

## 7. Pod 安全与资源基线

普通Pod必须：

```text
runAsNonRoot
readOnlyRootFilesystem
allowPrivilegeEscalation=false
capabilities.drop=ALL
seccompProfile=RuntimeDefault or stricter
automountServiceAccountToken=false unless exact API use required
no hostPID/hostIPC/hostNetwork/hostPath/privileged
requests and hard limits set
ephemeral storage limit set
```

需要Kubernetes API的controller使用专用projected short-lived token与最小Role；业务Worker默认无API access。Sandbox
Executor例外必须逐字段admission allowlist，不能给整个namespace unrestricted privileged。gVisor Pod必须指定已验证
RuntimeClass；admission拒绝RuntimeClass缺失/变更为runc。microVM Executor访问KVM但guest不见host device/credential。

所有Pod定义startup/readiness/liveness probe：startup保护冷启动；readiness控制新traffic/claim；liveness只检测无法
自愈deadlock，不因远端Provider/MCP/S3暂时失败反复重启。

## 8. Network 拓扑

默认deny ingress/egress。允许边：

```text
Ingress -> Management/Runtime/SSE/Artifact Gateway
Platform services -> PostgreSQL/NATS/Artifact/Secret endpoints
Workers -> credential-free closed request -> Egress Broker
Egress Broker -> Security Authority RPC -> PostgreSQL restricted Secret authority
Egress Broker -> KMS/Secret Manager/private DNS/exact Provider/MCP/remote capability
Model Worker -> exact mTLS -> Model Artifact Broker
Model Worker -> separate exact mTLS client-stream -> Model Artifact Producer
Sandbox Controller -> exact mTLS -> Sandbox Artifact Broker
Model Artifact Broker -> its own read-only PostgreSQL pool / private S3/KMS read identity
Model Artifact Producer -> its own write-limited PostgreSQL pool / private S3/KMS write identity
Sandbox Artifact Broker -> its own restricted PostgreSQL pool / private S3/KMS read identity
Sandbox Provider -> private guest materialization channel
OTel SDK -> local/central Collector
```

- API/Runtime/Scheduler不能直接访问untrusted internet；
- Egress Broker是唯一同时接触resolved Secret和untrusted Internet的普通执行角色；它必须使用独立Pod、workload identity、
  connection pool、并发/字节bulkhead和default-deny NetworkPolicy，且不能拥有任何数据库credential或直连PostgreSQL；
- Security Authority使用独立Pod、service account、mTLS listener和restricted PostgreSQL role；它只向exact Egress workload identity提供
  SecretBinding受信读取和prepared winner登记两个closed method，不能访问公网、private DNS resolver、KMS、Secret Manager或远端backend。
  resolution调用不改变数据库；prepared registration只能复用04冻结的Receipt/Event/Outbox原子事务，不能成为通用业务mutation API；
- Provider/MCP/remote/Sandbox egress按Revision/tenant policy经过proxy、DNS/TLS/allowlist；
- Sandbox Controller不得直连S3、KMS或workload-identity endpoint；只有Sandbox Artifact Broker持有相应物理读取identity；
- Model read Broker保持read-only且只注册`ReadModelRequest`；其数据库role不得写Artifact或共享聚合，storage identity不得PUT、
  删除或枚举bucket，进程不得注册output upload/finalize或generic object RPC；
- Model Artifact Producer只注册closed Model-output client-stream RPC，只接受exact `model-worker.artifact-output` URI SAN并拒绝read
  client的`model-worker` URI SAN；stream
  header、chunk、总bytes、deadline、attempt/lease和idempotency均有硬界，Provider正文、object locator和storage credential不回传Worker；
- Model read Broker、Model Artifact Producer与Sandbox Broker使用三个不同Service、ServiceAccount、数据库credential/pool、mTLS server
  identity、storage workload identity、NetworkPolicy、connection pool和in-flight permit；Model read Broker拒绝Producer/Sandbox调用，
  Producer拒绝Sandbox Controller及read RPC，Sandbox Broker拒绝Model Worker与Producer；
- Producer的write-limited数据库role只能读取16的closed `ModelOutputStageAuthorizationProjection`（覆盖exact Model admission/Job fence、
  policy、quota、Artifact/Blob/grant与stage Receipt safe state），并通过closed repository command写预留Artifact/Blob/grant与
  stage Receipt；它不得写ManagementOperation/Artifact Job、修改quota余额、Event/Outbox、Run、NodeExecution、ModelTurn、Model Job、
  RunValue、业务Output Link或CapabilityInvocation authority；`artifact.ready`和业务事件只由Model terminal事务产生；
- PostgreSQL/NATS/S3/Secret Manager只接受workload identity/private endpoint；
- cloud metadata、Kubernetes API、node/kubelet、container runtime和cluster DNS敏感域默认阻断；
- Executor只能通过同节点Unix socket调用attestor的generation登记端点；该listener同时要求mTLS exact Executor role，并从内核
  Unix peer credentials取得宿主PID后直接观察runtime/cgroup，不能接受payload/header/env自报PID。Controller只能通过独立集群内
  mTLS listener调用verify/absence端点。两个listener使用不同exact workload role allowlist；registration socket不得发布为Service、
  跨节点volume或host-wide writable socket；
- attestor Controller listener不得置于普通负载均衡Service之后。登记回执封印exact private node-IP route与fixed host port，Controller再以
  CandidateManifest冻结的Sandbox node CIDR/port allowlist和exact mTLS server identity双重校验；该route随Sandbox Job现有JSON payload
  持久化，不建立中心route registry，也不给Controller Kubernetes API读取权限；
- ingress/internal gRPC使用TLS/mTLS，证书自动轮换且有expiry alert；内部角色授权使用04冻结的exact workload URI SAN，
  服务端必须同时验证client CA与endpoint-specific closed role allowlist，CN、DNS SAN和客户端自报metadata不参与授权；
- NetworkPolicy只是防御层，adapter/proxy仍执行destination policy；
- egress failure隔离到backend circuit，不使服务全局unready。

## 9. PostgreSQL

- 生产最低 PostgreSQL 16，单 writer HA、多 AZ 同步/准同步复制与自动 failover；
- authority query、claim、CAS、Run snapshot和mutation receipt只读writer，不从可能陈旧replica做授权/状态决定；
- read replica只允许离线analytics/qualification且不回写业务；
- 使用fresh `platform` database/schema ownership；旧实现不与新schema dual-write；
- connection pool按role硬隔离；Model read Broker与Sandbox Broker即使具有相同只读表集合也必须使用不同credential和pool，Model Artifact
  Producer还必须使用第三套write-limited credential/pool，不能借用任一read Broker或Model Worker pool；所有pool最大值总和 +
  migration/admin reserve必须低于DB max connections；
- Producer role对Model/Run/Policy/Quota/Artifact/Blob/grant/Receipt只有构造16 closed row-scoped projection所需的column-level SELECT或
  security-barrier view权限；同安全域dedupe只允许在Attempt+quota授权后按完整tenant/backend/storage/encryption/security-domain/content-digest
  key执行constant-shape exact Verified Blob lookup，不得list/prefix/filter。对Artifact/Blob/stage Receipt只有closed command所需的最小INSERT/UPDATE；不得授予正文/Secret列、generic list/
  filter query、ManagementOperation/Artifact Job/Quota/Event/Outbox mutation、schema ownership、DDL、table-wide
  DELETE/TRUNCATE或ModelTurn/Model Job mutation，实际grant集合进入Candidate证据；
- Q1至少保留20% connections给critical control、failover、migration check和incident；
- statement/lock/idle transaction timeout、batch、claim和transaction duration按role固定；
- PgBouncer/代理如使用必须验证transaction/session语义，不破坏advisory lock、prepared statement或RLS假设；
- PITR、base backup、WAL archive、restore和checksum/integrity定期演练；
- autovacuum、index bloat、dead tuple、long transaction、replication lag和connection saturation有dashboard/alert。

## 10. Migration

```text
build migration artifact
 -> static destructive/lock review
 -> restore production-like snapshot
 -> apply and measure
 -> schema compatibility check
 -> maintenance/online gate
 -> separate migration Job with advisory lock
 -> application rollout
 -> post-deploy invariant scan
```

- production应用启动不自动DDL；
- migration是forward-only、checksummed、单writer并有statement/lock timeout；
- destructive/drop/rewrite需要独立approved release，不与普通rolling deploy混合；
- expand/contract只服务新 `insight.platform/v1` 内部的rolling compatibility，不支持旧schema；
- rollback应用只能回到仍兼容current schema的image；数据库不自动down migration；
- migration Job不使用application runtime identity，完成后credential revoke；
- 失败保留 receipt/log 的 safe evidence，人工不能通过 ad-hoc SQL 伪造 version；
- schema version、contract digest和上述closed WorkerManifest canonical digest在readiness握手中验证。

首次installation operator由独立schema-admin Job在migration/schema verify成功后、任何Gateway/控制面/Worker启动前执行。
Job使用短期bootstrap数据库credential调用固定的`platform-bootstrap-operator`入口，只传04规定的opaque ID与认证digest；
不得传subject、token或证书正文。该入口要求空Principal authority、事务级并发隔离和mandatory audit，精确重放以外的
重复执行失败；成功后立即撤销credential。应用二进制不得在startup自动运行bootstrap，readiness也不得把失败降级成
anonymous/admin模式。

## 11. NATS

Q1使用3节点multi-AZ NATS cluster：

- Core NATS承担 `work.wakeup.*`、`run.live.*`、`registry.invalidate.*`和按exact WorkerProcessGeneration隔离的
  `insight.platform.v1.sandbox.control.*` request/reply；
- committed integration event可投影到受控JetStream/consumer，但PostgreSQL outbox/checkpoint仍是业务authority；
- subject ACL按workload identity/role，tenant/Run ID不允许任意wildcard订阅；
- wake/live payload无Secret/正文/授权结论；
- publisher/consumer使用bounded connection/channel/buffer，不为每Run/SSE创建连接；
- NATS断开时DB command继续，outbox重试/safety scan恢复；
- cluster不可用不使Runtime API写路径全局unready，除非outbox backlog达到安全硬门槛；
- 不把NATS backup作为DR前置，恢复后从PostgreSQL重新投影必要wake/event；
- 监控 connection、subscription、slow consumer、dropped message、reconnect、latency、cluster/quorum 和 JetStream backlog。

## 12. Artifact Store 与 KMS

- private S3-compatible backend，多AZdurability，versioning和按policy的object lock；
- bucket/object无public ACL/website，访问只给Artifact service workload identity；
- Model Artifact Producer使用独立write-limited identity执行exact staging PUT/HEAD、仅对同reservation exact staging generation执行
  verifier/recovery所需的GET，并按exact context执行KMS seal/unseal；它不能list tenant prefix、读取任意Ready object或复用Model/Sandbox
  read identity。Model与Sandbox read Broker只能对已授权exact generation执行HEAD/GET与KMS unseal，不能PUT；
- Producer write permit、S3/KMS client、连接池、byte budget和timeout与两个read Broker全部分离；partial upload、Producer crash或Model
  terminal first-winner失败必须由durable staging fact、同Attempt stage Receipt和bounded GC收敛；Producer不创建或claim Artifact Job，
  也不能留下无PostgreSQL locator的不可清理对象；
- Producer的每次conditional PUT必须使用Candidate冻结binding的write deadline/quiescence合同；candidate cleanup在冻结
  `staging_retain_until`之前不得执行或采纳DELETE/absence，也不得Close candidate Blob quota。到点后仍须对exact locator/generation执行
  DELETE/HEAD并取得稳定evidence；client timeout、连接关闭或一次早期HEAD absence都不是write quiescence；
- tenant/security-domain scoped encryption context，KMS key rotation不改Artifact digest；
- lifecycle rule不能早于PostgreSQL Reference/retention/hold/GC决定删除；
- staging、ready、quarantine、diagnostic可使用独立prefix/bucket policy但不泄露公开object key；
- S3 inventory只用于reconciliation，不作为业务reference authority；
- scanner/transformer使用Sandbox，不在Gateway解析复杂内容；
- object missing/digest/KMS/replication/version errors进入integrity incident；
- download/upload egress、latency、error、staging age、scan backlog和GC age有capacity/alert；
- 备份/恢复同时验证 PostgreSQL metadata 与 sample/full inventory digest。

## 13. Secret Manager

- 生产Secret value仅在外部Secret Manager，平台DB保存04定义的SecretBinding、opaque reference ciphertext、purpose、
  resolution policy与generation；
- workload使用短期identity读取其role/purpose允许的Secret，不共享installation master token；
- Secret rotation/revoke有generation/invalidation/propagation SLO；
- Kubernetes Secret只允许bootstrap到Secret Manager的最小identity/certificate，不存Provider/tenant业务Secret；
- Secret value 不进入 Helm values、Git、ConfigMap、env dump、Pod spec、log、trace、metric 或 qualification bundle；
- break-glass读取需独立approval/audit并默认禁止正文回显；
- canary Secret贯穿Provider/MCP/Sandbox/Artifact/错误/telemetry泄漏测试；
- Secret Manager不可用时需要Secret的新leaf fail closed，已有无Secret控制/取消/查询继续。

Egress role中的Secret Broker必须使用独立并发permit、总超时和role-scoped短期workload identity。它只能通过Security Authority internal
gRPC消费`secret_bindings.opaque_reference_ciphertext/key_id`受信projection，并独占普通执行角色中的KMS/Secret Manager调用权限；
Management、Runtime、Host与普通Worker只持有`ExactSecretBindingRef`。Security Authority是唯一可从PostgreSQL物理读取该projection和
提交prepared registration的进程，但它不能解封reference、读取Secret value或访问外部网络。Provider catalog由immutable
CandidateManifest安装，空catalog、重复ID、未知Provider或运行时动态加载均使readiness/leaf start失败。Broker和Authority都不增加
数据库表、broker session或Secret cache authority。

Egress启动必须在开放internal listener前完成Secret Provider readiness与全部MCP OAuth verification binding校验；空OAuth key set、
Auth Profile digest漂移、重复/无序`kid`、非签名JWK、共享密钥算法或未知exact Auth Policy均fail startup/leaf request。授权码兑换与
PKCE删除只允许exact MCP Host workload URI SAN调用，其他Worker即使由同一CA签发也必须在request body解码前拒绝。
MCP Streamable HTTP operation与remote-task cancel同样只允许exact MCP Host URI SAN；其endpoint catalog、limits与AEAD key reference
必须在listener前安装。AEAD raw key不得进入Candidate ConfigMap或env，只能以精确32-byte文件存在于专用只读Kubernetes Secret投影目录；
Egress配置只引用该目录中的单层绝对路径并在打开后的文件句柄上复核regular file与精确长度。

OAuth callback使用独立双副本无状态进程，只暴露exact `/v1/mcp/oauth/callback` Ingress。该进程可访问PostgreSQL callback command
authority、cluster DNS与Egress Broker，不可直接访问公网、KMS或Secret Manager；authorization code只经MCP Host URI SAN的closed Egress
RPC发送。AEAD state raw key只从专用只读Kubernetes Secret投影读取，并按Candidate配置冻结的material digest校验；ConfigMap、env、日志、
Event和数据库均不得出现raw state key或authorization code。候选Ingress的存在不改变Phase 7前的`implementing-not-current`公共状态。

## 14. Deployment 配置

配置分三层：

```text
installation bootstrap: DB/NATS/S3/Secret/identity endpoints
immutable CandidateManifest: images/contracts/schema/hard-limit profile digest
versioned domain Policy/Revision: tenant/runtime behavior
```

- bootstrap使用closed versioned config schema，unknown/deprecated/duplicate key fail startup；
- environment variable只用于引用配置/Secret identity，不允许任意动态覆盖domain policy；
- hard security/size/queue/deadline 上限由 CandidateManifest 固定，tenant 只能收紧；
- config digest在startup、trace resource和qualification evidence记录，不记录Secret；
- ConfigMap更新不隐式热改变语义；需要新Revision或rolling release；
- 每 role 只读取自身配置 subset，Sandbox guest 无 installation config；
- dev/qualification/production使用同一schema，不存在低语义production flag；
- config validation在deploy前和startup重复执行。

CandidateManifest必须引用唯一`HardLimitProfile`，至少包含以下非optional字段族：

profile的machine contract目标路径为`contracts/platform-v1/limits/hard-limit-profile.schema.json`；Q1基线实例为
`contracts/platform-v1/limits/q1-50.json`。二者在Gate A前必须存在并进入`contract_digest`，文档中的散落示例数字不能
覆盖该实例。

### 14.1 已撤销 persistence 记录（非规范性）

旧 migration 34～35、Deferred poll/callback 专用 evidence 及其 adapter checkpoint 已撤销，不属于当前 baseline、
部署状态或 Gate 证据；详细记录只保留在 Git 历史。

HardLimitProfile machine contract仍以checked-in schema和CandidateManifest精确引用的实例为唯一输入。当前revision固定
`profile_version=4`并新增必填
`capability_sandbox.runtime_bundle_bytes={unit:"bytes",hard_max:67108864,q1_default:33554432,
overflow_outcome:"content_rejected"}`。SandboxPackage发布必须从Ready `runtime_bundle_artifact`取得可信byte length，并拒绝长度为零或
大于67108864；Q1 effective limit为33554432且只能被deployment/tenant进一步收紧。WASI ABI的16 MiB module限制仍是backend-specific
更严格上限。缺失该字段、旧profile version、错误单位/outcome或越界Package都必须在Candidate/发布阶段fail closed，不能形成永远无法执行的Job。
schema/Q1实例、Rust exact validator、Package publication fixture和Candidate closure门禁已经通过；这只构成该合同切片的实现证据，不单独构成Phase或Gate完成证据。
Deferred execution、callback ingress、timer/wake Worker 与 Q1 资格必须在对应 Phase 3～6 重新生成证据，不得沿用旧候选记录。

CR-165的Accepted目标合同要求下一revision固定为`profile_version=5`并新增以下全部必填字段；数字是目标machine contract，不是当前
运行行为：

| 字段路径 | unit | hard_max | Q1 default | overflow outcome |
|---|---:|---:|---:|---|
| `artifact.model_output_chunk_bytes` | bytes | 262144 | 65536 | `content_rejected` |
| `artifact.model_output_worker_slots` | count | 1024 | 16 | `temporarily_unavailable` |
| `artifact.model_output_worker_aggregate_bytes` | bytes | 17179869184 | 67108864 | `temporarily_unavailable` |
| `artifact.model_output_producer_transport_backlog` | count | 4096 | 64 | `temporarily_unavailable` |
| `artifact.model_output_producer_transport_timeout_milliseconds` | milliseconds | 60000 | 1000 | `temporarily_unavailable` |
| `artifact.model_output_producer_in_flight_streams` | count | 4096 | 64 | `temporarily_unavailable` |
| `artifact.model_output_producer_in_flight_declared_bytes` | bytes | 68719476736 | 268435456 | `temporarily_unavailable` |
| `artifact.model_output_producer_in_flight_buffer_bytes` | bytes | 4294967296 | 16777216 | `temporarily_unavailable` |
| `artifact.model_output_producer_streams_per_tenant` | count | 1024 | 8 | `temporarily_unavailable` |
| `artifact.ready_retention_seconds` | seconds | 315576000 | 2592000 | `invalid_request` |

上述十个tuple在profile v5中逐字段exact，不能只验证正数或`q1_default <= hard_max`；所有Limit的`hard_max/q1_default`还必须不超过
JSON safe integer。Candidate qualification的installation effective值就是其exact profile的`q1_default`；Deployment/tenant/Attempt只能在
typed closure中进一步收紧，不能扩大或静默改写Candidate manifest。Worker/Component manifest因此与Candidate effective值比较，而不是直接
借用`hard_max`。profile validator还必须以checked arithmetic同时证明hard/Q1两组关系：

```text
model_output_chunk_bytes <= model_context_mcp.response_bytes
model_output_worker_aggregate_bytes
  == model_output_worker_slots * model_context_mcp.response_bytes
model_output_producer_in_flight_declared_bytes
  == model_output_producer_in_flight_streams * model_context_mcp.response_bytes
model_output_producer_transport_backlog <= model_output_producer_in_flight_streams
model_output_producer_streams_per_tenant <= model_output_producer_in_flight_streams
model_output_producer_in_flight_buffer_bytes
  >= model_output_producer_in_flight_streams
     * (model_output_chunk_bytes + MODEL_OUTPUT_PROTOBUF_ENVELOPE_OVERHEAD_BYTES)
```

任一add/multiply溢出都拒绝profile/Candidate，不能saturate。Q1对应关系固定为
`16*4194304=67108864`、`64*4194304=268435456`和`64*(65536+4096)=4456448 <= 16777216`；hard关系固定为
`1024*16777216=17179869184`、`4096*16777216=68719476736`和
`4096*(262144+4096)=1090519040 <= 4294967296`。这些等式是machine invariant，不是解释性示例。

chunk字段冻结`StageModelOutput` canonical data frame大小；有效值只能进一步收紧且不能超过该Attempt的
`maximum_materialized_bytes`。Worker两字段封顶07 manifest的slot+weighted bytes；Producer transport与四项in-flight字段封顶上述ComponentCapacityManifest和
每个RPC的双层weighted admission；Ready retention封顶16 Model Deployment的exact duration。当前checked-in `profile_version=4`没有这些
字段，现有schema/Q1/Candidate证据不能证明Artifact-backed Model output；实现该路径时必须原子升级schema、Q1实例、Rust exact
validator、WorkerManifest v2、15 ArtifactStorageBindingManifest、ComponentCapacityManifest、ComponentStartupManifest/profile registry/
projection/factory、16 installation compatibility port/generation、RPC protocol carrier、Candidate digest和正负向fixture，不能以环境变量或Helm自由值补字段。

### 14.2 Capacity contract

| 字段族 | 必须冻结的上限 |
|---|---|
| API | header/URL/compressed与decoded body、JSON depth/properties/items、list page、SSE event/buffer/connection |
| Registry/Plan | Draft/package/schema bytes、definitions/nodes/edges、branch/map/loop/model round、dependency closure |
| Run/Scheduler | active/waiting Run、descendants、ready rows、inline Value bytes、ValueRef count、claim batch、attempts、lease/heartbeat、deferred poll base/max、wake contracts |
| Model/Context/MCP | request/response/delta、tokens、tool calls、candidates/items/pages、sessions/tasks/subscriptions |
| Capability/Sandbox | input/runtime bundle/output/progress、queue、CPU/memory/pids/files/IO/network、wall time、cleanup deadline |
| Artifact | single/total bytes、parts、references/grants、Model output canonical chunk/Worker slot+bytes/Producer streams+declared+buffer bytes/per-tenant streams、scan expansion/page/object、staging/Ready retention/batch |
| Durable Quota | Agent/work-class/Capability/Sandbox并发、CPU/memory/output、Model token/cost/request、Context usage、Artifact占用、HumanTask |
| Control/Data | DB connections/transactions、outbox/callback/recovery batch、NATS payload、telemetry buffer/cardinality |

每个字段必须有单位、正整数hard maximum、Q1 default和overflow stable outcome。domain Revision、tenant Policy和请求值
只能取`min(platform hard max, deployment limit, tenant limit, run remaining budget)`；缺失、零值歧义、单位溢出或未知字段
使配置/发布失败。Q1 evidence保存完整effective profile，不能只保存profile名称。

## 15. Autoscaling 与容量隔舱

HPA/cluster autoscaler信号：

| WorkClass | 主信号 |
|---|---|
| API | in-flight、latency、CPU辅助 |
| Scheduler/Recovery | ready age、drive duration、DB pressure |
| Model | ready age、active streams、token throughput、connections |
| Capability | ready age、backend permit、latency |
| Context | query ready age、candidate throughput、index latency |
| MCP | operation ready age、sessions、remote tasks、connections |
| Sandbox | weighted queued resource units、slots、startup latency |
| Artifact | Model read Broker、Model Artifact Producer、Sandbox Broker三者各自的in-flight/bytes/DB pool；Producer staging/upload/verifier backlog；独立scanner/download/GC backlog与bytes/IO |

- autoscaling不能超过DB connection、Provider quota、node/KVM、NATS/S3和tenant hard capacity；
- scale-up前保留control/cancel/cleanup slots；
- scale-down先readiness false/drain，不能中止lease而伪造failure；
- mandatory control/SSE/API/Scheduler/Recovery不scale-to-zero；
- Sandbox warm pool有独立memory ceiling且不挤占running capacity；
- node pressure/eviction/spot只允许经过failure qualification的worker pool；
- 单workclass的HPA不能使用全平台共享queue长度导致连锁扩容；
- Model Artifact Producer按自己的active stream、weighted declared/buffer bytes、durable staging/cleanup backlog、oldest production age、write permit、DB pool和S3/KMS latency扩缩；
  它不得消费Model read或Sandbox read指标、semaphore与扩缩容预算，任一lane达到hard capacity不得触发另外两条lane连锁扩容或拒绝；
- Producer在TLS/service-role authorization后、读取bounded header前先取得global stream与weight exact为
  `effective model_output_chunk_bytes + 4096`的唯一per-stream wire-buffer permit，所有frame复用该buffer；解析并授权valid header后、
  读取首个data frame前再取得declared bytes与tenant stream permit，不重复取得data buffer。全部持有到唯一terminal、stream drop或absolute deadline；
  第一阶段饱和返回body-free unavailable，第二阶段返回`DependencyUnavailable + RetrySameAttempt`，不得进入application queue。DB/S3/KMS
  pool waiters受global stream permit封顶，client library不得再建立无界内部队列；transport accept开始的同一monotonic timeout必须同时覆盖
  TLS、bounded backlog、第一阶段permit等待和完整Header decode，silent/fragmented pre-header流到期释放全部资源；
- capacity configuration和实际limit进入Q1 evidence。

## 16. Availability 与滚动发布

- API/SSE/Scheduler/Controller跨至少3个failure domain做topology spread/anti-affinity；
- replicated critical Deployment配置PDB与rolling `maxUnavailable=0`、bounded `maxSurge`；
- PDB不替代rolling strategy，也不能阻止involuntary node failure；
- readiness false后停止新traffic/claim，继续heartbeat/commit/drain到grace；
- termination grace小于lease hard max但足够提交handoff，超时让lease自然expire；
- old worker adapter/runtime digest在历史binding仍有ready/in-flight work时保留兼容pool；
- canary按role和workclass逐步放量，不把所有Scheduler/Recovery同时替换；
- release失败先停promotion，应用rollback只使用schema-compatibleimage；
- Provider/MCP/Sandbox/Artifact单pool rollout不触发无关服务rollout；Model read Broker、Model Artifact Producer与Sandbox Broker必须可以
  分别独立rollout，不通过共享Pod、ServiceAccount、pool或readiness联动；
- deployment controller/API不通过active head自动改变RunBindings。

## 17. `/v1` Clean Replacement

新架构直接成为唯一的 `insight.platform/v1`，不与当前 `insight.agent/v1` 共存，也不承诺wire、ID、数据或行为兼容。
`Platform v2` 只表示第二代架构。Cutover流程：

1. 在隔离Qualification环境完成同一CandidateManifest的Gate A～G；
2. 进入maintenance，旧入口停止admission和所有mutation；
3. 旧Run在maintenance前完成，剩余Run由Operator显式cancel/close；不让旧Worker跨cutover继续执行；
4. 对旧PostgreSQL与Artifact做最终backup/只读归档，然后停止旧API、Scheduler和Worker；
5. 创建fresh `platform` database/schema，部署新data/control/runtime plane；
6. 使用新合同显式创建并发布Agent、Skill、Capability、Context、Model等资源；
7. 原子地把 `/v1` ingress、audience和discovery document指向新平台，执行smoke与rollback gate。

同一时刻只能有一个 `/v1` 合同对外提供mutation。旧client必须根据新OpenAPI重新生成并部署；旧shape、token audience、
ID和cursor全部fail closed。任何旧数据import都是离线、显式、产生全新Draft并重新validation/publish，不属于完成条件。
新Runtime永不fallback到旧Worker、schema或event。

切换前失败可以abort并恢复旧入口；新 `/v1` 接受第一条mutation后，禁止回退旧实现。此后的rollback只允许在
新平台内部切换到与fresh schema兼容、已经资格化的Candidate image；灾难恢复从新平台backup恢复，不重新启用旧DB。

## 18. Observability 架构

```text
Application OTel SDK
 -> node/local Collector (bounded buffer, redaction)
 -> central Collector gateway
 -> Metrics backend / Trace backend / Log backend

Domain Audit + committed outbox
 -> append-only audit sink / retention archive
```

- telemetry是观察面，不拥有Run/Operation/Artifact状态；
- Collector不可用时应用使用bounded buffer/drop policy，不阻塞critical DB commit；
- audit和业务outbox使用durable path，不依赖普通log exporter；
- tracecontext只允许标准bounded字段，清理外部baggage，禁止tenant/Secret/Prompt注入；
- 每 component 声明 service name/version/release/config/region/role resource attributes；
- clock通过可靠timesync监控，业务ordering仍使用DB sequence/generation而非wall clock；
- telemetry backend访问独立permission，不给平台runtime反向控制能力。

## 19. Metrics 规范

所有00～18列出的最低metrics必须实现，并遵守：

- counter单调，histogram使用统一latency/size buckets，gauge有明确owner/scrape语义；
- label是closed allowlist；tenant、principal、Run、Node、Invocation、Artifact、tool/model/server name、endpoint、error正文
  和raw URL禁止；
- high-cardinality诊断使用trace/受控query，不转移到resource attribute逃避cardinality limit；
- exporter/SDK设置cardinality和memory ceiling，overflow计数告警；
- status/error/work class/backend class使用closed low-cardinality enum；
- dashboard query/version 与 CandidateManifest 一起保存；
- SLI从server-side committed/observed facts计算，不用client log估算成功；
- missing telemetry本身产生pipeline health metric/alert。

## 20. Trace 规范

最小span关系：

```text
HTTP command/query
 -> DB transaction/outbox
 -> Scheduler drive/claim
 -> ModelTurn | CapabilityInvocation | ContextQuery | ChildRun
 -> Provider/MCP/Sandbox/remote/Artifact operation
 -> outcome commit
```

- async边界使用span link连接committed event/work identity，不伪造单一长驻parent span；
- trace可以携带opaque resource ID作为受控attribute，但必须sampling/redaction且不进入metric；
- baggage禁止Secret、tenant display、Prompt、filename、tool args、endpoint和policy；
- 100%采样security denial、reconciliation、corruption、fence rejection、platform invariant和qualification traffic；
- 普通成功 Q1 基线 head sampling 5%，可用 tail sampling 保留错误/高延迟；
- body/message/delta/code/document/stdout/stderr不作为span event；
- Provider/MCP 外部 trace header 仅在 trust policy 允许时传播，清理 untrusted tracestate/baggage。

## 21. Log 与 Audit

应用log使用stable structured schema：timestamp、level、service/release、event code、request/trace/span ID、safe opaque
aggregate ID、outcome和bounded fields。JSON本身不等于结构化；field类型/语义必须版本化。

禁止日志：Secret/token/auth header/cookie、Prompt/代码/文档/模型正文、tool args/output、signed URL/object key、OAuth
code、MCP task/session handle、SQL text/params、stack中的用户数据、stdout/stderr原文。Panic/error先redact映射；需要正文
取证写短retention encrypted Artifact并audit。

Audit最低字段：tenant、principal/workload、action、opaque target、request digest、policy/release revision、outcome、source
session/request ID和timestamp。Audit append-only、tamper-evident、访问独立授权、retention/hold可配置。普通log丢失不
允许丢失mandatory audit。

### 21.1 Q1 公开观察保留合同

- nonterminal Run 的当前 `RunSnapshot` 在Run存续期间始终可读；terminal snapshot和result默认至少保留30天；
- Durable PublicRunEvent 从事务提交时起至少可回放7天，其opaque cursor在同一窗口内有效；
- 连接使用已过期cursor时返回410 `cursor_expired`和当前snapshot URI，不从最早可用event静默续接；
- `run.snapshot` 是按需合成的观察投影，不作为event重复存储；LiveOnly delta/progress不回放、不延长cursor窗口；
- tenant/compliance policy可以延长snapshot、result和public replay期限，不能把Q1最低期限缩短；legal hold优先；
- internal transition、Audit、Artifact和qualification evidence分别遵守其domain retention，不因公开event GC一并删除；
- retention/GC按tenant分批、可恢复且可审计，不能因单tenant积压阻塞Runtime API或SSE；
- Q1资格测试覆盖窗口内重连、边界时刻、过期410、snapshot恢复和跨tenant不可枚举。

## 22. SLI 与 SLO

以下是Q1资格门，不在Verified前对外承诺：

| SLI | Q1 SLO |
|---|---|
| Runtime API monthly availability | ≥ 99.9% |
| Management API monthly availability | ≥ 99.5% |
| `POST /v1/runs` committed latency | p95 ≤ 250 ms，p99 ≤ 750 ms |
| `GET /v1/runs/{id}` latency | p95 ≤ 150 ms，p99 ≤ 500 ms |
| cancel command committed latency | p95 ≤ 250 ms，p99 ≤ 750 ms |
| cancel intent到未开始leaf停止dispatch | p99 ≤ 5 s |
| SSE初始snapshot | p95 ≤ 500 ms，p99 ≤ 1.5 s |
| committed Run event到SSE可观察 | p95 ≤ 2 s，p99 ≤ 5 s |
| Ready work到首次claim | p95 ≤ 500 ms，p99 ≤ 2 s |
| expired lease/deadline safety recovery | p99 ≤ 30 s |
| outbox oldest unpublished age（健康依赖下） | p99 ≤ 10 s，硬告警30 s |
| suspension/revoke阻止新leaf | p99 ≤ 5 s |
| durable terminal correctness | 100%，不允许双终态/丢终态 |
| cross-tenant/Secret isolation | 100%，零容忍 |

APIavailability只统计平台可控制的edge/auth/policy/DBcommand path；Provider/MCP业务失败不算API unavailable，但错误
映射、timeout和bulkhead失败算正确性。SLO按rolling 30天和qualification window分别计算，低流量窗口不以缺失数据
伪造100%。

## 23. Error Budget 与告警

- SLO各有burn-rate fast/slow window alert，单次请求不直接page；
- correctness/security/DB corruption/dual terminal/cross-tenant/Secret泄漏为零容忍立即incident，不用error budget抵扣；
- release在error budget快速燃烧、outbox/reconciliation/security incident时自动停止promotion；
- alert必须指向owner、dashboard、runbook、severity和safe labels；
- page级最低集合：DB unavailable/failover stuck、API SLO burn、Scheduler no progress、outbox hard age、lease storm、
  reconciliation growth、Secret leak canary、Artifact corruption、Sandbox escape/cleanup failure、audit pipeline loss；
- ticket级：capacity trend、scan backlog、cache miss、single backend circuit、evidence expiry；
- alert不包含tenant/Prompt/filename/endpoint/token；
- 每个 alert 有测试或 synthetic signal 证明可触发/恢复。

## 24. Runbook 最低集合

```text
postgres-failover-and-restore
schema-migration-failure
nats-outage-and-reprojection
outbox-backlog
scheduler-stall-and-lease-storm
provider-or-mcp-circuit
secret-manager-outage-or-revoke
sandbox-node-escape-or-capacity
artifact-corruption-quarantine-and-gc
sse-backlog-or-live-gap
cross-tenant-or-secret-incident
release-rollback-and-worker-compatibility
disaster-recovery
```

Runbook包含症状、safe诊断query、权限、止损、恢复、验证、回滚、审计和何时升级。禁止建议直接编辑Run/Invocation/
Artifact terminal row、清空NATS当修复、删S3 prefix或执行无scope的ad-hoc脚本。

## 25. Backup 与灾难恢复

Q1单region目标：

| 数据 | RPO | RTO |
|---|---:|---:|
| PostgreSQL authority | ≤ 5 min | ≤ 60 min |
| Ready Artifact blobs/metadata一致恢复 | ≤ 15 min | ≤ 120 min |
| Secret references/config | provider SLA且≤ 60 min恢复可用 | ≤ 60 min |
| NATS wake/live | 无业务RPO要求 | DB恢复后≤ 15 min重建 |
| Observability | ≤ 15 min（Audit另按合规） | ≤ 120 min |

恢复顺序：PostgreSQL PITR -> schema/invariant check -> Artifact inventory/digest/KMS -> Secret resolver -> control/runtime ->
execution planes -> outbox/recovery scan -> NATS projection -> ingress。所有旧lease视为过期并提升epoch。Callback/outbox
可能重复，consumer/inbox按ID去重。

每季度至少一次production-like restore drill；每个major schema/storage change前后执行。DR报告记录实际RPO/RTO、缺失/
corrupt Artifact、重复event/callback和修复，不用“备份任务成功”代替restore证据。

## 26. Qualification Profile Q1-50

Q1基线manifest：

```text
50 concurrent active Runs
10 Run admissions/second sustained for 30 minutes
25 admissions/second burst for 60 seconds
200 concurrent SSE clients with reconnect churn
mixed durable waits, Model, Capability, Context, Subagent and Artifact work
10 concurrent Sandbox jobs, then 100% Sandbox slot saturation for 30 minutes
100% Sandbox Artifact Broker permit/DB-pool saturation while Model Artifact reads and output production remain admissible
100% Model Artifact Broker read permit/DB-pool saturation while Model output production and Sandbox reads remain admissible
100% Model Artifact Producer write permit/DB-pool saturation while Model/Sandbox reads and Model control/cancel remain admissible
20 concurrent Artifact transfers, typical 10 MiB, boundary fixture 100 MiB
two or more Scheduler/Runtime instances claiming concurrently
```

混合Run fixture固定随机seed、Agent/Plan/Binding digests和backend latency/error distribution：

| 路径 | 比例 |
|---|---:|
| ModelLoop含tool intent | 30% |
| Native/remote Capability fast/deferred | 25% |
| Context query/fan-out | 15% |
| ChildRun | 10% |
| Human/signal/timer wait | 10% |
| Sandbox Capability | 10% |

Qualification使用协议真实fake Provider/MCP/remote service确保可重复，并另有真实外部provider smoke；不能用纯内存mock
证明DB/NATS/S3/HTTP/gRPC/failure窗口。Sandbox security/capacity在Linux KVM production-equivalent nodes运行。

## 27. Test 层级

1. Unit：domain pure transition、schema、policy、canonicalization；
2. Property/model-based：状态机、first-winner、tenant、budget、cursor、expression；
3. Contract：OpenAPI/JSON Schema/protobuf/backend adapter positive/negative；
4. Real-process integration：PostgreSQL 16、NATS、S3-compatible、HTTP/gRPC、Secret test provider；
5. End-to-end：Management publish -> Deployment -> Run -> leaf -> Artifact -> SSE/result；
6. Security：tenant、SSRF、OAuth、Secret、Artifact、Prompt injection、Sandbox escape；
7. Fault injection：kill/partition/timeout/duplicate/late/stale/corrupt；
8. Load/capacity：Q1 SLI、fairness、bulkhead、DB/queue/connection；
9. Soak：24小时持续混合负载和周期故障；
10. DR：backup restore、epoch接管、outbox/callback replay和Artifact integrity。

fake服务也必须实现bounded、versioned协议和错误，不在测试中给平台额外内部hook绕过生产路径。

Gate A的CR-165 machine fixture还必须覆盖：v4/v6 profile与十个tuple任一unit/value/outcome漂移；Worker v1、缺/错component role、Model缺capacity、非Model
额外/null capacity、slot/aggregate越界；Component unknown kind/field、错误const role/queue mode、零值、backlog/per-tenant/pool越界、
wire-buffer checked add/multiply溢出；Storage空catalog、错误backend/region/addressing/write/observation、零timeout/object limit、超safe
uncertainty/object limit及quiescence边界；Candidate两个mode、
四个digest数组缺字段/无序/重复/超限/missing/extra、Producer三处partial/orphan closure、worker/startup/component role与image不匹配、
unknown/duplicate startup profile、schema/provenance drift、capacity property缺失/null/多余、primitive name set偏差、inner set无序/重复/空/超限、
未消费entry/绕过factory及任意跨role或pool↔semaphore alias；Producer多scope role/region/binding重叠与错误路由；协议document/schema/Rust常量与
4096值漂移。正向fixture必须分别证明纯Inline Candidate、零Model Deployment的Artifact-capable Candidate及后续Inline/ArtifactCapable
Deployment反向激活验证稳定，readiness对实际manifest/startup closure exact复验；并发Deployment activation/Candidate switch/Run admission
必须由同一compatibility generation产生单一winner且每个admitted Run冻结兼容digest。

### 27.1 共享 Conformance Fixture Manifest

所有规范共享`contracts/platform-v1/fixtures/manifest.json`，不允许各crate复制一套稍有不同的expected value。
每个fixture记录稳定ID、owner spec、关联spec、profile/version、seed、input Artifact/digest、expected output/digest或
stable rejection、privacy classification和positive/negative标签。首批suite闭集：

```text
F-ID       UUIDv7 prefix、tenant scope、非法/错kind ID
F-CANON    RFC 8785、semantic digest、set/path/package canonicalization
F-SCHEMA   insight.closed-json-schema/1 与 nominal types
F-STATE    Resource/Run/Node/Invocation/Job/Task/Artifact states and first-winner transitions
F-FENCE    idempotency、CAS、lease、epoch、callback、first-winner crash windows
F-EVENT    Event/Receipt/Outbox、public sequence、cursor、live gap、error/failure projection
F-POLICY   principal、permission、Effect、approval、Secret、classification、tenant denial
F-BACKEND  Native/HTTP/gRPC/MCP/Model/Context/Sandbox/Artifact adapter conformance
F-E2E      publish/deploy/admit/execute/wait/cancel/result/recovery
F-Q1       Q1-50 load、bulkhead、chaos、soak和DR dataset
```

Rust、SQL constraints、OpenAPI/JSON Schema/protobuf和需要支持的SDK语言必须消费同一manifest或由其生成fixture。
新增状态、prefix、permission、event、error或schema keyword时，Gate A要求manifest与所有exhaustive consumer同一变更。

## 28. Fault Injection 矩阵

每个场景覆盖故障前/中/后invariant：

```text
kill -9 Management API / Runtime API / SSE Gateway
kill Scheduler / Outbox / Recovery owner during claim/commit
kill Model / Capability / Context / MCP Worker before and after external dispatch
kill Sandbox Controller / Executor / microVM during start/run/result/cleanup
kill or saturate Model Artifact read Broker while Model Artifact Producer and Sandbox Broker remain admissible
kill or saturate Model Artifact Producer before/during/after staging PUT while both read Brokers remain admissible and cleanup converges
kill or saturate Sandbox Artifact Broker while Model read Broker and Model Artifact Producer remain admissible
PostgreSQL primary failover and connection reset
NATS total outage, duplicate and reordered hints/events
S3 timeout, partial upload, missing object, digest/KMS failure
PUT在Attempt deadline前发出、client timeout/取消后迟到成功；barrier前HEAD absence不得删除/Close，write-quiescence后exact delete/absence收敛
TLS完成后不发送Header、逐字节Header、bounded accept backlog饱和；monotonic timeout释放stream/wire buffer且不创建Receipt
Secret Manager timeout, rotate and revoke
Provider/MCP/remote 429/5xx/hang/late callback/protocol drift
network partition and DNS rebinding
clock skew within monitored bounds
node drain/eviction/zone loss
```

判定必须证明：无双终态/越权重放、stale epoch拒绝、等待释放permit、uncertain Effect reconciliation、NATS非authority、
Artifact Ready-only、Secret不泄漏、control reserved capacity和最终收敛。

## 29. Security Qualification

- SAST/dependency/container/IaC/SBOM/signature/provenance gate；
- API fuzz：parser、request smuggling、auth、ID/cursor/etag/idempotency、cross-tenant；
- SSRF：IPv4/IPv6 private、metadata、DNS rebinding、redirect、proxy/tunnel；
- OAuth/MCP：audience/resource/token passthrough/PKCE/state/elicitation phishing；
- Prompt/Context/Skill：role injection、tool/permission override、malicious document；
- Artifact：zip slip/bomb/malware/active content/grant/object key/dedupe timing；
- Sandbox：escape、host/Kubernetes/KVM/device/network/Secret/resource/warm residue；
- Supply chain：mutable tag、unsigned image/package、manifest/runtime mismatch；
- Telemetry：Secret/Prompt/code/document/token/URL canary acrossDB dump/log/trace/metric/event/report；
- independent red-team review for Sandbox boundary and tenant isolation before Verified。

任何cross-tenant、Secret泄漏、sandbox escape、unsigned code执行或Effect绕过都是release blocker，不接受风险豁免直接
进入Verified。

## 30. Soak

24小时Q1 soak要求：

- 不中断产生混合 Run，维持 50 active target 并记录 arrival/completion；
- 周期执行 Worker kill、NATS outage、backend 429、Sandbox saturation、Model Artifact Producer/read Broker独立饱和和 SSE reconnect；
- 无 unbounded memory/connection/task/queue/Artifact staging/outbox/reconciliation 增长；
- 无 stuck nonterminal 超过其 deadline + recovery SLO；
- 无双 terminal、lost committed result、stale outcome commit 或 cross-tenant error；
- SLO窗口达标，telemetry无采集中断掩盖；
- 开始/结束保存 DB invariant query、resource usage、queue age、Artifact inventory 和 canary scan；
- 报告包含完整时间轴和失败，不允许拼接多个短 run 冒充连续 24 小时。

## 31. Qualification Gate

```text
Gate A Contract
  -> Gate B Functional
  -> Gate C Security
  -> Gate D Recovery/Chaos
  -> Gate E Capacity/SLO
  -> Gate F 24h Soak
  -> Gate G DR Restore
  -> Release Approval
```

- Gate A：00～18 Reviewed/Accepted、machine contracts无diff、migration review；
- Gate B：全部real-process E2E/adapter/conformance通过；
- Gate C：security matrix/red-team无open blocker；
- Gate D：fault matrix全部收敛且invariant通过；
- Gate E：Q1容量、fairness、bulkhead和SLO达标，包括Model read Broker、Model Artifact Producer与Sandbox Broker三lane逐一独立饱和，
  每次故障时另外两lane及Model control/cancel仍满足其准入与延迟门槛；
- Gate F：连续24小时soak达标；
- Gate G：从正式backup恢复并达到RPO/RTO；
- 任何Gate失败使后续证据无效或需要从受影响Gate重跑；
- approval不允许覆盖correctness/security failure。

当前独立Model Artifact Producer尚未实现，既有Inline output、Model request read Broker、Sandbox Broker或静态NetworkPolicy证据均不能
替代Producer的RPC、write-limited数据库role、S3/KMS write identity、durable cleanup、三lane故障和Q1容量证据。在这些证据绑定同一
CandidateManifest并通过对应Gate前，不得关闭Phase 4/6，也不得登记Gate A、B、C、D或E通过。

## 32. Qualification Evidence

```rust
struct QualificationBundle {
    qualification_id: QualificationId,
    candidate_manifest_digest: Digest,
    profile_digest: Digest,
    environment_manifest_digest: Digest,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    gate_results: Vec<GateResult>,
    reports: Vec<ArtifactRef>,
    raw_evidence_manifest: ArtifactRef,
    signer: PrincipalId,
}
```

Evidence至少包含commands/test binary digests、seed、load profile、component/image/schema/config digests、metrics query和
raw export、trace/log/audit sample、fault timeline、DB invariant output、Artifact inventory、security/DR报告。Secret/
token/raw tenant 正文必须 redact；redaction 本身版本化。

只保存截图/摘要/人工结论不足。Bundle是immutable Artifact closure并有签名/retention。过期、环境不等价、image/config
变化或证据缺失会使qualification无效。

## 33. Release、Promotion 与 Rollback

```text
Built -> Staged -> Qualifying -> Qualified -> Approved -> Deploying -> Active -> Draining -> Retired
```

- CI构建一次immutable image，环境间只promotion digest，不重建；
- image/package有signature、SBOM、provenance，admission按digest验证；
- promotion检查 CandidateManifest 与 qualification bundle 中的 digest 完全匹配，再生成 ReleaseManifest；
- immutable ReleaseManifest不充当mutable active pointer；唯一current release/Candidate digest与16
  `installation_compatibility_generation`保存在03既有installation-scoped Release aggregate payload中，promotion、rollback、Model Deployment
  activation与Run admission都锁定/复验同一row，禁止ConfigMap、Helm value或进程cache形成第二current authority；
- canary只接synthetic/allowlistedtenant，验证后逐步扩大；
- rollout期间持续检查SLO/error budget/invariant/outbox/reconciliation；
- rollback停新版本traffic并恢复schema-compatible旧image，不能down migrate或改写Run；
- 历史 binding 需要的 worker/runtime 保留到 work/retention 安全结束；
- security kill switch可以停止特定work class/backend，不删除durablestate；
- 每次 promotion/rollback 产生 audit 和 Release receipt。

## 34. 完成定义

Platform v2只有同时满足以下条件才算全部完成：

1. 00～18全部Accepted、Implemented、Verified；
2. OpenAPI/JSON Schema/protobuf/DB constraints/Rust types一致；
3. production manifests、migrations、dashboards、alerts、runbooks已提交；
4. Gate A～G 对同一 CandidateManifest 全部有效，ReleaseManifest 精确引用其 qualification bundle；
5. Q1 SLO、24h soak、DR、security和bulkhead证据完整；
6. 新 `/v1` ingress/audience正式启用，旧实现已下线且无fallback/dual-write；
7. `docs/current`只描述已验证的 `insight.platform/v1` 行为并包含Operator/Developer/API文档；
8. 活动spec归档且qualification bundle可复现/审计；
9. 未决correctness/security blocker为零；
10. 发布决策由授权principal显式批准，不由测试自动修改production head。

## 35. 明确推迟的工作

- multi-region active-active与global traffic/Run migration；
- GPU/HPC、训练、长期batch和spot优化；
- v1数据/Conversation/Run在线迁移；
- Confidential Computing/TEE和hardware side-channel guarantee；
- service mesh产品强绑定；
- 自动capacity ML预测和跨云调度；
- 外部合规认证、billing/chargeback 和商业 SLA；
- 超过 Q1-50 的规模承诺，需新增 Q2+ profile 与证据。

## 36. 未决问题

没有阻止实现计划的架构未决问题。具体云、Kubernetes发行版、managed PostgreSQL/NATS/S3/Secret/telemetry产品和
microVM VMM可以在满足本规范conformance后选择；任何产品选择都不能削弱PostgreSQL authority、隔舱、tenant/Secret
边界、durable recovery、SLO与Gate A～G。
