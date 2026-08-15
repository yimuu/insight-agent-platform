# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
| 日期 | 2026-08-15 |
| 依赖 | deployment/release合同依赖00～16；qualification合同再依赖17的API/Event surface |
| 直接下游 | deployment/release合同直接供17消费；完整规范再供实现计划、迁移记录、资格报告与`docs/current`消费 |

> Persistence ruling：旧专用表族、catalog/checksum与其资格证据均已撤销。Qualification只对ADR定义的单一baseline与后续真实行为生效；
> 本业务规范不拥有物理表名、表数或migration序列，旧记录不能证明当前实现。

## 1. 决策摘要

Platform v2 的生产基线是单 region、多 availability zone 的 Kubernetes 部署，使用 PostgreSQL 16 HA 作为唯一
事务/运行权威、S3-compatible private object store 作为 Artifact blob 权威、NATS 作为 wake/live/outbox 投影
transport、外部 Secret Manager 作为 Secret value 权威。Control（其中RegistryValidation使用独立Worker role、pool与permit）、Runtime、Model、Capability、Context、MCP、
Sandbox、Artifact 与 Recovery 使用独立 Deployment、service account、DB/connection pool、queue、permit 和
autoscaling policy。

Artifact受信业务读取按调用audience物理切分为三个Broker：Artifact Workload Broker、Model Artifact Broker与Sandbox Artifact Broker。
三者必须使用不同进程、Deployment、ServiceAccount、restricted PostgreSQL credential/pool、S3/KMS read identity和process-local permit。
Workload Broker只暴露Runtime、Registry Validation、Capability、Context与MCP五个exact read method，并为每个method冻结17列明的exact
mTLS URI SAN allowlist；Model Broker只暴露`ReadModelRequest`；Sandbox Broker只暴露WASI与microVM read RPC。三者不得共Pod、共pool或
通过单listener动态选择audience；任一侧饱和、重启或泄露不能消耗另外两侧准入容量。public `Artifact Gateway`只是统一hostname/HTTP
contract，物理上拆为Artifact Upload Gateway与Artifact Download Gateway两个独立Deployment；两者都不是internal gRPC Broker，也不得转发这些method。
Artifact Upload Gateway只接受15 `Principal + OpaqueBearer`，不得把workload mTLS或`JobAttempt + WorkloadBound`投影成public upload。

Artifact scan/head/delete由第四个独立组件Artifact Maintenance Authority执行，只暴露`ReadForScan`、`HeadExactGeneration`与
`DeleteExactGeneration`，并按17的method-specific scanner/GC URI SAN allowlist授权。Artifact Scanner/Finalizer与GC/Reconciler只持有
durable Job/fence和typed RPC client，不持有S3/KMS identity、明文locator或bucket credential。Maintenance Authority使用独立restricted
PostgreSQL credential/pool、S3/KMS maintenance identity和permit，只返回bounded bytes或typed head/delete evidence，不提供generic object API。

普通Registry/Capability/Context/MCP/Sandbox Job Attempt输出由第五个独立internal service Artifact Workload Producer承载。它只注册17五个
method-specific client-stream RPC，只接受`JobAttempt + WorkloadBound + StagingWrite`，使用独立Deployment、ServiceAccount、write-limited
PostgreSQL credential/pool、S3/KMS staging identity和permit；最多推进`Staging -> Uploaded`并创建或重放15既有scan Job，不得read、scan、
Verified/Ready、finalize/reference、处理Model output或提供generic object API。它与public Upload Gateway的Principal/bearer路径不可互换。

Artifact-backed Model output由第六个独立internal service Model Artifact Producer承载，不扩展上述read Broker、Workload Producer或Maintenance Authority。Producer使用独立进程、
Deployment、ServiceAccount、write-limited PostgreSQL credential/pool、S3/KMS write workload identity、client-stream endpoint和
write permit；它不得与Workload/Model/Sandbox read Broker、Workload Producer或Maintenance Authority共享Pod、ServiceAccount、数据库credential/pool、
storage identity、connection pool或semaphore。Model Worker使用与Model read client分离的exact
`spiffe://insight.platform/workload/model-worker.artifact-output` mTLS client、连接池和有界stream调用Producer；Producer
饱和、滚动或失败不得耗尽Workload/Model/Sandbox读取、Workload Producer、Maintenance、Artifact Upload/Download Gateway、Model Worker control/cancel或控制面容量。

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
- 证明六个internal Artifact service以及Artifact Upload/Download Gateway八个物理角色/lane任一单独饱和、失败或滚动不会消耗另外七条lane或控制面；
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
    installation_id: InstallationId,
    candidate_id: ReleaseCandidateId,
    git_commit: GitCommit,
    contract_digest: Digest,
    database_schema_version: SchemaVersion,
    component_images: BTreeMap<ComponentRole, ImageDigest>,
    worker_manifests: Vec<WorkerManifestDigest>,
    model_output_materialization_mode: ModelOutputMaterializationMode,
    component_runtime_manifests: Vec<ComponentRuntimeManifestDigest>,
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
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
    qualification_bundle_digest: Digest,
    release_approval_digest: Digest,
    created_at: DateTime<Utc>,
}

enum InstallationReleaseStatusV1 { Uninitialized, Active }

struct ExactInstalledReleaseRefV1 {
    release_id: ReleaseId,
    release_manifest_digest: Digest,
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
}

struct InstallationReleaseStateV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    status: InstallationReleaseStatusV1,
    active_release: Option<ExactInstalledReleaseRefV1>,
    active_model_deployment_count: u32,
    compatibility_generation: u64,
    state_digest: Digest,
}

enum ModelOutputMaterializationMode {
    InlineOnly,
    ArtifactCapable,
}

struct CapacityPrimitiveIdentityV1 {
    primitive_name: String,
    identity_digest: Digest,
}

struct CapacityPrimitiveIdentityPreimageV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    candidate_id: ReleaseCandidateId,
    component_role: ComponentRole,
    region: CanonicalRegion,
    startup_profile_id: ComponentStartupProfileId,
    startup_schema_digest: Digest,
    kind: CapacityPrimitiveKindV1,
    primitive_name: String,
}

struct CapacityIsolationIdentitySetV1 {
    schema_version: u32, // const 1
    pool_identities: Vec<CapacityPrimitiveIdentityV1>,
    semaphore_identities: Vec<CapacityPrimitiveIdentityV1>,
}

struct ComponentStartupManifestV1 {
    manifest_version: u32, // const 1
    component_role: ComponentRole,
    region: CanonicalRegion,
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
    projection_requirement: ComponentProjectionRequirementV1,
    capacity_requirement: StartupCapacityRequirementV1,
}

enum ComponentProjectionRequirementV1 {
    StartupOnly,
    Worker { work_class: WorkClass },
    SandboxController,
    ArtifactWorkloadProducer,
    ModelArtifactProducer,
}

struct ComponentStartupProfileRegistryV1 {
    schema_version: u32, // const 1
    profiles: Vec<ComponentStartupProfileV1>,
}
```

`InstallationReleaseStateV1`是全平台唯一current Release/Candidate pointer和compatibility fence；本规范只定义逻辑aggregate，物理映射
只由ADR决定。Provisioning为每个02 `InstallationId`创建恰好一个`Uninitialized` state：`active_release=None`、active Model count为0、
generation为1。`Active`必须有完整exact release ref；ReleaseManifest的Candidate ID/digest和加载到的Candidate body ID/digest必须逐字段相等。
任一ID、digest、status、count或generation改变都重算排除`state_digest`自身的closed JCS digest；canonical document不超过4096 bytes。
`active_model_deployment_count <= MAX_ACTIVE_MODEL_DEPLOYMENTS_PER_INSTALLATION=4096`，且必须等于02 exact active predicate实际集合大小。
所有改变Model bindable active set的mutation更新count并推进compatibility generation；04任一Tenant encryption-domain add/rebind/revoke保持
count不变但也推进generation/state digest，因为它改变Artifact-capable compatibility input。
没有Active release时禁止Model Deployment activation和root Run admission。不存在`latest`、nullable局部字段、fake tenant、ConfigMap/Helm
pointer或第二compatibility summary。

CandidateManifest与ReleaseManifest是immutable signed machine documents，由exact ID+digest的installation manifest resolver读取；resolver只能
按完整key返回canonical bytes并重验签名/digest，不能读取mutable latest。active release及仍被Run binding引用的历史Candidate必须持续可解析，
相关runtime/image/adapter也保留到全部绑定work安全结束；process cache只是按digest验证的可丢失副本，不是authority。

`GitCommit` wire值必须是带算法标签的完整小写object ID：`sha1:<40-hex>`或`sha256:<64-hex>`；分支、tag、缩写SHA和
`latest`均非法。`ComponentRole`只使用02的共享nominal；`component_images`的key就是Candidate计划安装的完整Deployment logical scope集合，
不得用临时Pod名或副本名。
`database_schema_version`精确表示`insight-platform-postgres`导出的schema contract version；CR-165 Draft目标为`7`，当前已实现基线仍为
`6`直到migration/schema gate实际通过。它不是
migration文件数量、数据库产品版本或payload schema version。Candidate创建器必须从下述sealed component projections、实际安装的closed
storage manifests和`HardLimitProfile`计算canonical digest closure；worker、component-runtime、artifact-storage-binding与component-startup digest各自按字节
升序且唯一。每个component image/Deployment role必须恰有一份`ComponentStartupManifestV1`；每个由WorkerManifest或当前已注册
ComponentRuntimeManifest variant管理的role还必须恰有一份对应manifest。manifest role/region必须与image、Deployment及startup config逐字段匹配；重复role、
缺失或额外manifest、limit digest漂移均拒绝。Candidate schema/builder/runtime readiness必须执行同一closure，不能只在文档或Helm lint检查。

PostgreSQL startup verifier成功后才能构造sealed `ValidatedInstalledDatabaseSchemaVersion`，其唯一值来自
`insight-platform-postgres`导出的当前schema contract version，不接受请求、Release、Helm或环境变量覆盖。`promote`与`rollback`都要求incoming
Candidate的`database_schema_version`与该值逐值相等；首版没有range、向前/向后兼容标志或down-migration。因而旧Release只有在仍使用当前已安装
schema contract version时才可rollback；不相等是确定性不兼容并在改变current pointer前拒绝。

四个digest数组始终是required字段。machine constants固定
`MAX_CANDIDATE_WORKER_MANIFESTS=512`、`MAX_CANDIDATE_COMPONENT_RUNTIME_MANIFESTS=256`、
`MAX_CANDIDATE_ARTIFACT_STORAGE_BINDING_MANIFESTS=MAX_INSTALLATION_ARTIFACT_STORAGE_BINDINGS`、
`MAX_CANDIDATE_COMPONENT_STARTUP_MANIFESTS=256`；`worker_manifests`固定
1～512项，`component_runtime_manifests`固定1～256项，`artifact_storage_binding_manifests`固定1～64项，
`component_startup_manifests`固定1～256项且与`component_images` role集合exact相等。storage manifest wire与64项hard max只由15拥有；18
只验证其digest closure。catalog独立服务Package、request Artifact与Model output，不随Model output mode清空，也不要求每个binding已被某个动态Deployment引用。
JSON Schema执行对应`minItems/maxItems/uniqueItems`，Rust additionally执行raw digest bytes严格升序；component-runtime空数组非法，因为每个Candidate
至少包含一个Artifact Workload Producer runtime scope；`inline_only`只省略Model Producer，不省略Workload Producer。

`model_output_materialization_mode` wire只允许`inline_only | artifact_capable`，并且只能由Candidate builder从release installation closure派生，
不能由调用者布尔值、Policy baseline digest、动态Model Deployment catalog或opaque `deployment_config_digest`声称：

- `inline_only`当且仅当不存在任何`ModelArtifactProducer` ComponentRuntimeManifest或使用
  `model_artifact_producer/v1` startup profile的component scope；storage catalog仍必须有1～64项；
- `artifact_capable`当且仅当存在一至多个完整Model Producer logical scope；每个scope在`component_images`、一个
  `ModelArtifactProducer` runtime manifest和一个`model_artifact_producer/v1` startup manifest三处exact出现，并且至少存在一份匹配
  Model Worker v2 manifest。即使当前没有任何Model Deployment也合法；
- 每个Model Producer scope的storage binding集合必须非空、是15 Candidate catalog子集且全部region匹配；不同scope的binding集合不得重叠。
  未分配给Producer的binding仍可服务其他Artifact路径。任一scope partial/orphan、同binding路由到多个scope或runtime/startup role错配都拒绝；
  runtime readiness还必须证明实际Deployment、image与startup document逐值匹配。

Artifact Workload Producer不改变`model_output_materialization_mode`，但每个Candidate必须安装至少一个完整Workload Producer logical scope；每个scope
必须在`component_images`、一个`ArtifactWorkloadProducer` runtime manifest和一个`artifact_workload_producer/v1` startup manifest三处exact出现。
每个scope的storage binding集合必须非空、属于15 Candidate catalog且region匹配；不同Workload Producer scope不得重复认领同一binding，且所有scope的
集合并集必须逐值等于Candidate完整1～64项storage catalog，不能留下ordinary-output不可路由的binding。

Candidate builder只从17 exact `artifact-workload-stage-routes` registry与本Candidate的client startup manifests派生按17 ordinal排序且唯一的非空
enabled stage-kind set；调用者、Helm、环境变量或Producer runtime不得提交第二份enabled列表。每个enabled stage kind与每个Candidate
storage binding digest的笛卡尔积都必须恰路由到一个Workload Producer scope：stage kind按17 registry唯一选择method/profile/SAN/audience/
Artifact owner/Job typed owner/
JobKind/WorkClass/port/purpose，binding digest按上述不相交全集分区唯一选择scope。零scope、空enabled集合、任一pair漏路由/多路由、额外kind/binding、
partial/orphan scope、把Model Producer manifest冒充ordinary workload writer或让public Upload Gateway引用该runtime variant都使Candidate fail closed。
Q1 client startup closure必须启用17全部五个stage kind；其他profile只能减少未部署client对应的kind，不能留下已部署client但disabled的route。
client与Producer startup readiness必须调用Candidate builder同一pure/versioned route-closure函数，从实际startup manifests、17 registry、runtime
manifests和storage catalog重算完整pair set并与Candidate projection byte-identical比较；任一profile、descriptor、routing或binding漂移都保持listener
closed，而不是等首个请求才发现。

同一Producer logical scope的replica使用同一`ComponentRole`、byte-identical startup config和logical capacity identities；“2 per storage
region/boundary”表示该scope内副本数，不是两个manifest。不同region/boundary使用不同opaque `ComponentRole`和不同manifest/identity；component
kind分别由exact `kind=artifact_workload_producer | model_artifact_producer`表达，不能把所有boundary挤进一个固定role或互换variant。

`ComponentStartupManifestV1`是capacity identity的共同machine carrier，closed schema路径固定为
`contracts/platform-v1/schemas/component-startup-manifest.schema.json`。唯一role/profile registry document与schema固定为
`contracts/platform-v1/deployment/component-startup-profiles.json`及
`contracts/platform-v1/schemas/deployment/component-startup-profiles.schema.json`，两者进入根contract digest。registry最多256个profile，
按`profile_id` UTF-8 bytes严格升序且唯一；ID为1～128 ASCII bytes并匹配`^[a-z][a-z0-9_.\/-]{0,127}$`。每个entry冻结exact
`startup_schema_digest`、closed projection requirement与closed tagged capacity requirement；`isolated`的pool/semaphore name数组各0～16、严格升序且唯一且不能同时为空，
`capacity_free`不得携带数组。unknown profile、schema digest漂移或unknown registry字段fail closed。

17 `artifact-workload-stage-routes` registry列明的六个client profile必须在本registry以以下exact entry出现；`I(P;S)`表示
`capacity_requirement=Isolated { pool_primitive_names=P, semaphore_primitive_names=S }`：

| profile ID | projection requirement | capacity requirement |
|---|---|---|
| `registry_validation_worker/v1` | `Worker { work_class=RegistryValidation }` | `I(P;S)` |
| `capability_native_worker/v1` | `Worker { work_class=CapabilityNative }` | `I(P;S)` |
| `capability_remote_worker/v1` | `Worker { work_class=CapabilityRemote }` | `I(P;S)` |
| `context_worker/v1` | `Worker { work_class=Context }` | `I(P;S)` |
| `mcp_host/v1` | `Worker { work_class=Mcp }` | `I(P;S)` |
| `sandbox_controller/v1` | `SandboxController` | `I(P;S)` |

```text
P = {artifact_read_client, artifact_stage_client, database}
S = {artifact_stage_bytes, artifact_stage_streams, business_slots, critical_control_slots}
```

集合按上示UTF-8 bytes顺序exact编码。前三个pool ticket只允许`Fixed + Connections`；四个semaphore依次只允许`Fixed + Bytes`、
`Fixed + Count`、`Fixed + Count`、`Fixed + Count`。Worker profile的`business_slots`与`critical_control_slots`分别逐值等于07
WorkerManifest `max_concurrency`与`critical_control_reserved_slots`；`artifact_stage_streams <= business_slots`，且checked
`artifact_stage_streams * effective artifact.single_bytes`必须大于等于`artifact_stage_bytes`。Sandbox Controller的closed startup schema冻结同名
正数值并受Candidate hard limits收紧；其business/control slots仍是互不借位的普通准入与取消/cleanup reserve。三个connection值受各role startup
schema和installation连接预算共同封顶。所有值只经projection ticket进入factory，不得由route registry、Helm或环境变量另给默认值。

六个profile ID各自只能解析到上述一个entry且不能被两个stage kind复用；被Candidate实际startup manifest引用时，该profile就启用对应kind。
profile缺失/额外、projection或capacity shape/names漂移、role使用相邻client profile、同profile映射两个kind，或client image provenance/profile/schema
与route registry不一致都在Candidate/startup阶段fail closed。`artifact_workload_producer/v1`与`model_artifact_producer/v1`自身不是stage client，
不能出现在该六项映射中。

`startup_config_digest`必须是该role实际closed startup document的canonical digest，`startup_profile_id`必须在registry中且
`startup_schema_digest`逐值相等。registry要求`isolated`时，startup document与manifest都必须携带非空、非null
`capacity_isolation`，且pool/semaphore `primitive_name`集合分别exact等于profile；`capacity_free`时必须完全省略property。每个identity entry
是closed object；`primitive_name`是1～64 bytes的ASCII stable key，pattern固定`^[a-z][a-z0-9_.-]{0,63}$`。两数组按name严格升序；identity
digest在单项、单role和全Candidate范围都必须唯一。

`identity_digest = SHA-256(UTF8("insight.capacity-primitive.identity.v1") || 0x00 ||
JCS(CapacityPrimitiveIdentityPreimageV1))`。Candidate builder先为本installation预分配candidate ID，再把exact installation/candidate context交给sealed
component adapter；preimage其余字段逐值来自同一startup manifest和pool/semaphore列表位置，禁止调用方digest、Pod/replica ID、环境变量或limit值替代。
manifest entry与construction ticket必须按kind+name一一对应，ticket保存完整preimage并重算digest；preimage的role/region/profile/schema与
`ValidatedComponentProjectionV1.startup_manifest`逐值相等。CandidateManifest的`installation_id`必须等于18 current state owner，不能把一个
Candidate或capacity ticket跨installation复用。缺context、错误kind/list、identity swap或digest漂移使projection/Candidate/readiness失败。
preimage故意不含`startup_config_digest`或capacity limit：startup document本身包含identity digest，纳入会形成hash cycle；limit仍由同一sealed
projection ticket和`projection_digest`绑定，不能据此把identity与limit从不同document拼接。

contracts crate不导入各binary的startup config。它定义sealed projection与construction ticket：

```rust
struct ValidatedComponentProjectionV1 {
    installation_id: InstallationId,
    candidate_id: ReleaseCandidateId,
    component_role: ComponentRole,
    region: CanonicalRegion,
    canonical_startup_bytes: BoundedBytes,
    startup_manifest: ComponentStartupManifestV1,
    worker_manifest: Option<WorkerManifestV2>,
    runtime_manifest: Option<ComponentRuntimeManifest>,
    capacity_tickets: Vec<CapacityPrimitiveConstructionTicketV1>,
    projection_digest: Digest,
}

enum CapacityPrimitiveKindV1 { Pool, Semaphore }
enum CapacityUnitV1 { Connections, Count, Bytes }

#[serde(tag = "limit_kind", rename_all = "snake_case", deny_unknown_fields)]
enum CapacityPrimitiveLimitV1 {
    Fixed { unit: CapacityUnitV1, maximum: u64 },
    PerKey { unit: CapacityUnitV1, total_maximum: u64, per_key_maximum: u64 },
}

struct CapacityPrimitiveConstructionTicketV1 {
    identity: CapacityPrimitiveIdentityPreimageV1,
    identity_digest: Digest,
    limit: CapacityPrimitiveLimitV1,
}

struct ComponentProjectionDigestPreimageV1 {
    schema_version: u32, // const 1
    installation_id: InstallationId,
    candidate_id: ReleaseCandidateId,
    component_role: ComponentRole,
    region: CanonicalRegion,
    canonical_startup_byte_length: u64,
    canonical_startup_bytes_digest: Digest,
    startup_manifest_digest: Digest,
    worker_manifest_digest: Option<Digest>,
    runtime_manifest_digest: Option<Digest>,
    capacity_tickets: Vec<CapacityPrimitiveConstructionTicketV1>,
}
```

`CapacityPrimitiveKindV1` wire exact为`pool | semaphore`，`CapacityUnitV1` exact为`connections | count | bytes`，limit discriminator exact为
`fixed | per_key`；所有object closed，unknown/null/cross-variant字段非法。所有maximum为正且不超过JSON safe integer；`PerKey`还要求
`per_key_maximum <= total_maximum`。`Pool`只能使用`Fixed + Connections`，`Semaphore`只能使用`Fixed | PerKey`加`Count | Bytes`；identity来自
startup manifest的哪个列表就必须使用对应kind。tickets按`identity.kind` ordinal后`identity.primitive_name` bytes严格升序且唯一，并与profile要求的identity集合
exact相等；capacity-free profile必须为空。由此Gate A可以逐值验证kind/unit/shape，而不是由factory猜测单位。

`projection_digest`的唯一算法是
`SHA-256(JCS(ComponentProjectionDigestPreimageV1))`；preimage object closed、`schema_version=1`且自身没有
`projection_digest`字段，因此不存在self-inclusion或“置零后hash”变体。`canonical_startup_byte_length`是actual canonical startup bytes的checked
正数长度，必须不超过该startup schema hard max与JSON safe integer；`canonical_startup_bytes_digest`逐值等于对同一bytes计算的02 Digest并等于startup manifest的`startup_config_digest`；
`startup_manifest_digest`、optional Worker/runtime digest都从对应完整closed manifest canonical bytes重算。optional字段按projection requirement
完全省略或出现，`null`非法；tickets使用上述exact排序、完整closed value而非ticket aggregate digest。role/region、任一byte/digest/ticket或preimage
version漂移都改变projection digest。adapter、Candidate builder与readiness复用同一pure function并拒绝调用方提交的预计算digest。

`projection_digest`是sealed adapter→builder/readiness校验值，不新增第五个Candidate digest数组或第二配置authority；Candidate manifest digest通过
同一projection产生的startup/Worker/runtime manifest closure、独立storage catalog、startup config digest与root contract/profile registry digest传递绑定其全部输入。
readiness比较完整projection输入及重算digest，不能只比较digest字符串。

每个deployable component crate的唯一versioned adapter必须从同一个registry-locked typed startup config一次性生成以上全部适用字段。
`projection_requirement`精确约束shape：`StartupOnly`要求两个optional manifest均省略；`Worker`要求exact WorkClass WorkerManifest且runtime
manifest省略；`SandboxController`同样要求Worker/runtime均省略，但只允许上述`sandbox_controller/v1` profile、对应compile-time image provenance
与closed controller startup schema，不能由generic StartupOnly冒充；`ArtifactWorkloadProducer`与`ModelArtifactProducer`都要求Worker省略且runtime manifest分别是exact同名variant；`null`、
错误variant和额外manifest非法。role、region、startup
digest、Worker/runtime values、capacity identity与limit必须来自同一document，不能从Helm/env/另一个builder输入拼接。

Candidate builder只接收sealed projection和signed image provenance，自行重算startup/Worker/runtime/projection digest及四个manifest数组；
不能接收预计算digest、未验证JSON、调用方投影的identity/容量/routing或独立mode。readiness由同一component adapter对进程实际启动document
重投影并与Candidate完整projection exact compare，逻辑不得在builder与binary复制。每个Candidate image的provenance还必须声明compile-time
`startup_profile_id/startup_schema_digest`并逐值相等；错误image/profile不能只等进程启动才发现。

`CapacityPrimitiveFactoryV1`是production composition创建本地pool、semaphore和weighted/per-key registry的唯一port。它只能消费上述
projection内的ticket，并用ticket中的unit和实际limit直接构造primitive；调用方不能另传capacity、env override或“期望值”。每张ticket
exactly once，重复、缺失、未消费、role/kind/name/identity/unit/value不匹配均使readiness失败。两个Producer listener的backlog/accept timeout和
routing也只能从同一validated runtime manifest安装。architecture gate禁止production component绕过factory或直接构造未登记primitive；
startup projection是运行时capacity/routing的输入authority，不是事后猜测摘要。

把全部startup manifest的pool与semaphore identity digests合并后，每个identity在整个Candidate中必须恰出现一次，包括同role的
pool↔semaphore；任何alias都fail closed。identity表示role-scoped logical allocation family，不是容量值、整组config digest或Pod/进程实例；
不同logical primitive必须有不同identity，同一role的replica各自实例化physical member但共享该role family identity。`model_artifact_producer/v1`
固定pool names `{database,kms,object_store}`及semaphore names
`{declared_bytes,global_stream,per_tenant_stream_registry,wire_buffer}`；Model Producer projection把DB/object/KMS pool limit、global stream、
declared/wire weighted bytes及`total streams + streams_per_tenant`逐项编码为ticket；其他profile的exact集合也只来自registry并由上述factory逐项消费，
不能以generic runtime map、未消费entry或直接构造primitive绕过。

`artifact_workload_producer/v1`固定`projection_requirement=ArtifactWorkloadProducer`，pool names exact为
`{database,kms,object_store}`，semaphore names exact为
`{accepted_backlog,database_waiters,declared_bytes,global_stream,kms_waiters,object_store_waiters,per_tenant_bytes_registry,
per_tenant_stream_registry,wire_buffer}`。projection必须把三个pool connection上限、三个waiter上限、accepted backlog、global stream、
declared/wire bytes及`total + per_tenant` streams/bytes逐项编码为construction ticket；`wire_chunk_bytes`与accept timeout进入同一startup config/
runtime digest并由factory adapter安装framing/timeout，不能成为无capacity identity的环境变量。两个Producer profile的任一pool/semaphore identity
在全Candidate范围alias都fail closed。

只要Candidate包含MCP OAuth绑定，`deployment_config_digest`覆盖的closed Egress配置还必须包含exact Auth Policy revision、完整
Auth Profile、允许的非对称JWT算法、public JWKS及其canonical digest，以及OAuth写入所用ServiceIdentity Principal。运行时不得从
issuer动态补齐、刷新或替换该信任根；key rotation通过新Candidate发布并重新资格，而不是在旧Candidate内静默漂移。

`WorkerManifestDigest`必须是`contracts/platform-v1/schemas/worker-manifest.schema.json` closed document的canonical digest。
CR-165 Draft目标使用07的`manifest_version=2`：每份document只允许一个exact WorkClass，并冻结`component_role`、`worker_role`、
`region`、`adapter_runtime_digest`、协议版本、业务最大并发和正数`critical_control_reserved_slots`；Model role还必须且只有它可以携带closed
`model_output_materialization { slots, aggregate_bytes }`。CandidateManifest中的不同digest不能在运行时合并成共享semaphore。当前
wire对非Model必须完全省略该property，`null`非法；Model必须提供closed object，两个值为正、`slots <= max_concurrency`、
`aggregate_bytes <= 9007199254740991`，并分别不超过Candidate profile的effective worker slots/aggregate bytes。`component_role`必须存在于
`component_images`且在WorkerManifest集合中唯一，并与对应startup manifest的role/region逐值相等；`worker_role`独立标识claim role且仍须唯一。任一
capacity变化都必须改变canonical WorkerManifest digest。
当前
checked-in v1 contract不具备该字段，不能证明Artifact-backed output；本地pool primitive的unit evidence也不是CandidateManifest，
不能替代Gate E/Q1负载证据。

非Job-claim服务的runtime capability、routing与本地容量使用独立closed `ComponentRuntimeManifest`，不能伪装成WorkerManifest或只藏在opaque
`deployment_config_digest`中。首批两个closed variant固定为：

```rust
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ComponentRuntimeManifest {
    ArtifactWorkloadProducer(ArtifactWorkloadProducerRuntimeManifestV1),
    ModelArtifactProducer(ModelArtifactProducerRuntimeManifestV1),
}

enum AdmissionQueueMode { RejectWhenSaturated }

struct ArtifactWorkloadProducerRuntimeManifestV1 {
    manifest_version: u32, // const 1
    component_role: ComponentRole,
    region: CanonicalRegion,
    storage_binding_digests: Vec<Digest>,
    admission_queue_mode: AdmissionQueueMode, // exact RejectWhenSaturated
    transport_accept_backlog: u32,
    transport_accept_timeout_milliseconds: u32,
    wire_chunk_bytes: u32,
    in_flight_streams: u32,
    in_flight_declared_bytes: u64,
    in_flight_buffer_bytes: u64,
    streams_per_tenant: u32,
    declared_bytes_per_tenant: u64,
    database_connections: u16,
    database_waiters: u32,
    object_store_connections: u16,
    object_store_waiters: u32,
    kms_connections: u16,
    kms_waiters: u32,
}

struct ModelArtifactProducerRuntimeManifestV1 {
    manifest_version: u32, // const 1
    component_role: ComponentRole,
    region: CanonicalRegion,
    storage_binding_digests: Vec<Digest>,
    content_validation_profile_digests: Vec<Digest>,
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

`ComponentRuntimeManifest`使用flat internally-tagged JSON；两个variant的discriminator exact为
`"kind":"artifact_workload_producer" | "kind":"model_artifact_producer"`，其`manifest_version=1`与
`admission_queue_mode="reject_when_saturated"`都为const，不能用外层`spec`、untagged shape、跨variant字段或`null`代替；
`component_role`使用02 nominal并与对应startup/image scope相等，不再固定为单一literal。
`region`使用02 `CanonicalRegion`并与startup、storage及对应17 ordinary staging request或16 Model output request逐字段比较；Model variant还要与
对应Worker逐字段相等。`storage_binding_digests` required 1～64项，按raw bytes严格升序且
唯一；每项必须存在于Candidate的15 storage catalog且manifest region逐值相等，同一variant的不同Producer scope不得重复认领。Workload与
Model Producer可以因不同typed write authority使用同一binding，但不能共享runtime manifest、routing、pool或permit。所有object
`additionalProperties=false`，所有JSON u64数值还必须不超过`9007199254740991`。

`ArtifactWorkloadProducerRuntimeManifestV1`不允许`content_validation_profile_digests`或任一Model-only字段。除
`admission_queue_mode`外所有容量字段都必须为正；`transport_accept_backlog <= in_flight_streams`、
`streams_per_tenant <= in_flight_streams`、`declared_bytes_per_tenant <= in_flight_declared_bytes`。checked
`in_flight_streams * wire_chunk_bytes <= in_flight_buffer_bytes`，且每个accepted stream只能持有一个复用buffer，不能按frame累积。
八个transport/byte字段分别不超过HardLimitProfile v5的Candidate effective字段：`transport_accept_backlog`精确映射
`artifact.workload_producer_transport_backlog`，`transport_accept_timeout_milliseconds`精确映射
`artifact.workload_producer_transport_timeout_milliseconds`，其余六项以runtime字段名直接追加`artifact.workload_producer_`前缀；每个Job Attempt的
grant `max_bytes`还必须同时不超过`artifact.single_bytes`、selected storage binding `maximum_object_bytes`与当前tenant staging quota。
三个connection值各不超过`in_flight_streams`，三个waiter值各不超过`in_flight_streams`；`database_connections`还不超过effective
`control_data.database_connections`，所有role DB pool总和加migration/incident reserve仍低于DB max connections。Q1每个Workload Producer
scope的pool/waiter exact值固定为database `16/64`、object-store `64/64`、KMS `32/64`（`connections/waiters`），并进入startup projection、
capacity ticket与Gate E实际limit证据；它们不是环境变量默认值。

每个Workload Producer runtime manifest的canonical digest、同role startup manifest/profile及全部construction ticket都是Candidate projection的
必填输入；sealed projection digest按上述v1 preimage派生并校验，不是额外Candidate数组。readiness必须从进程实际startup document重投影并逐字节复验同一runtime值、identity与ticket；缺失、
多余、局部漂移或只在部署说明中声明都使该role不ready并使Candidate/Gate E证据无效。

`content_validation_profile_digests` required 1～64项，按raw bytes严格升序且唯一；它是Model Producer image/startup实际安装的closed validator
集合，不是动态tenant catalog。16 port只把Policy路由到同时认领storage binding且包含exact validation digest的唯一Model Producer scope；
validation result的exact Model Producer binding必须显式携带被选择的`content_validation_profile_digest`，不能只返回Producer manifest digest后让
runtime重选。每个digest必须解析为15 registry中的完整`ModelOutputContentValidationProfileV1`，Candidate builder/readiness逐字段验证validator
implementation已由exact Model Producer image安装，且evidence validity不超过Candidate effective hard-limit字段；每个descriptor的
`canonical_response_contract_digest`还必须逐值等于从同一Candidate `contract_digest`解析出的root manifest唯一
`contracts/platform-v1/schemas/model/canonical-model-response.schema.json` entry，经raw SHA/length与parsed JCS重验后得到的16 sealed digest；Candidate内
不允许混装两个response contract，也不能把aggregate root digest当component digest。缺失/重复entry、额外、descriptor/digest或
response-contract漂移都fail closed。
readiness用同一startup adapter重投影实际validator集合。04 tenant encryption-domain projection则
单独把动态domain解析到该storage manifest的exact KMS binding，不把tenant ID写进Candidate。

对`ModelArtifactProducerRuntimeManifestV1`，除`admission_queue_mode`外所有容量值必须为正；Model Producer不允许application admission queue，饱和必须立即返回16的typed transient
failure。`transport_accept_backlog <= in_flight_streams`、`streams_per_tenant <= in_flight_streams`；backlog、accept timeout、streams、
declared bytes、buffer bytes与per-tenant streams六项分别不超过HardLimitProfile v5同名Candidate effective字段。checked
`in_flight_streams * (effective model_output_chunk_bytes + protobuf_envelope_hard_overhead)`不得超过`in_flight_buffer_bytes`；
`in_flight_declared_bytes`是按每个header声明总量加权的并发准入，不代表把完整response聚合进内存。DB pool必须同时不超过该组件stream
上限、effective `control_data.database_connections`及installation DB总连接预算；object-store/KMS pool各不超过stream上限。所有角色的
DB pool总和加migration/incident reserve仍须低于DB max connections。两个Producer的manifest digest、上述canonical
`component_startup_manifests`与exact startup config一并进入Candidate closure；Candidate必须拒绝任一局部pool/semaphore identity重复，单个
整组摘要或opaque `deployment_config_digest`不能证明该不变量。目标schema路径为
`contracts/platform-v1/schemas/component-runtime-manifest.schema.json`；当前尚未checked in，故现有
Candidate证据不适用于Artifact Workload Producer或Model Artifact Producer。

`transport_accept_timeout_milliseconds`不是只用于配置检查的数字。transport front在连接进入bounded accepted backlog时立即记录
`accepted_monotonic_at`，并以effective manifest/HardLimit较小值建立唯一monotonic deadline；TLS handshake、service-role authorization、
backlog/global-stream/wire-buffer permit等待以及完整首个Header的bounded decode都必须在该deadline内完成。kernel/listener backlog和ingress
handshake/idle timeout不得大于同一effective值。到期必须取消transport、释放已取得的全部permit且在尚无valid Header identity时只返回
body-free status，不创建/修改Receipt或其他数据库事实。valid Header完成current授权并取得冻结Attempt absolute deadline后，后续Data、
Terminal、DB/S3/KMS等待改由该Attempt deadline封顶，不能用重新计时延长它。

`ArtifactStorageBindingManifestDigest`是15 closed manifest canonical bytes的digest，也就是04/15中的`storage_binding_digest`；不能用
opaque deployment config或endpoint字符串代替。Candidate使用15的pure timing validator，以effective `artifact.staging_seconds`验证每个
binding的installation quiescence下界；动态ArtifactIo Policy的grace/tenant encryption binding配对只在16 port的Deployment create/activate、
Release scan与Run admission验证，Candidate builder不得读取tenant catalog。不满足生产等价backend/proxy合同的binding不得进入Candidate。目标schema尚未checked in，因此既有
Candidate不能作为该write-quiescence合同的实现证据。

其中`protobuf_envelope_hard_overhead`不是自由配置：16 RPC machine contract固定
`MODEL_OUTPUT_PROTOBUF_ENVELOPE_OVERHEAD_BYTES=4096`。目标machine carrier是closed
`contracts/platform-v1/protocol/model-output-rpc.json`与对应schema，两者必须进入root contract digest；公开Rust const与后续protobuf逐值
复验同一document。两文件当前尚未checked in，现有常量不是完整目标证据；交付后Candidate builder只能用该const做checked
add/multiplication，环境变量、Helm与HardLimitProfile均不能覆盖。
Candidate的installation mode为`artifact_capable`时，必须同时存在exact Model Worker v2 manifest及至少一个完整Model Producer scope；未部署
scope时不得把孤立runtime/startup manifest计作可用容量。18必须从上述sealed component projections通过唯一pure/versioned函数同时构造
Candidate closure与16 `InstalledModelOutputCapabilitiesV1`实现，不能从mutable cache/Helm或另一份摘要重建能力。每个
ArtifactCapable Model Deployment在创建/激活/Run admission时通过该port证明其resolved Policy storage/KMS/encryption binding与
content-validation digest路由到exact Model Producer scope，并证明至少一个匹配adapter/protocol/region的Model Worker满足
`slots >= 1 && aggregate_bytes >= maximum_materialized_bytes`，且Model Producer满足
`in_flight_declared_bytes >= maximum_materialized_bytes`与
`in_flight_buffer_bytes >= effective model_output_chunk_bytes + 4096`，15 binding还必须满足
`maximum_object_bytes >= maximum_materialized_bytes`；否则拒绝Deployment激活、Release切换或Run admission，不能让Job永久Ready到deadline。

Release切换使用有界preflight + generation CAS，不在持锁事务中扫描catalog。machine constants固定
`MAX_ACTIVE_MODEL_DEPLOYMENTS_PER_INSTALLATION=4096`、`MAX_RELEASE_COMPATIBILITY_SCAN_PAGE=256`与
`MAX_INTERNAL_INSTALLATION_CAS_RETRIES=3`：

1. 完成transport/authentication、InstallationOperator权限、closed body/size校验并计算request digest后，用第一个短事务按03 rank只claim/lock exact
   installation Command Receipt。terminal same-key/same-digest立即返回原结果，发生在manifest resolver、If-Match和current state检查之前；same-key/
   different-digest返回409。新Receipt或expired Processing takeover写入短lease与递增的`claim_generation`后commit；active Processing只返回带
   bounded Retry-After的in-progress 503，不启动第二次scan；这是观察另一个current claim的幂等状态，不是本claim的dependency/CAS失败，因而在
   If-Match检查前返回；
2. current claim holder立即执行capture短事务：先锁同一Processing Receipt并复验request digest与`claim_generation`，再锁
   InstallationReleaseState；校验public If-Match及仅依赖current state/request的promote/rollback transition guard，并读取expected
   generation/state digest/count后commit。If-Match失配在同一锁序下terminalize `Rejected`并稳定返回412；rollback@Uninitialized或新key指向
   current exact target等transition guard失败在同一事务terminalize `Rejected`并稳定返回409，不能留下会随state变化改成成功的Processing Receipt；
3. capture成功后，current claim holder在事务外解析并验证incoming approved Release/Candidate完整immutable closure、签名/digest、未过期
   qualification/approval以及上述installed database schema exact equality；resolver I/O期间不持有事务或行锁。capture后的每一种退出——包括
   manifest不存在、digest/approval/schema确定性不兼容、dependency/deadline transient——都必须进入同一个短classification事务，先锁Receipt并
   CAS current `claim_generation`，再锁InstallationReleaseState复验captured generation/state digest与public If-Match。只要ETag已漂移就优先
   terminalize 412；仅在ETag仍逐值相等时，确定性结果才分别terminalize稳定404/409，transient才保持Processing、缩短/保留可接管lease并返回503；
4. 在事务外的只读repeatable snapshot中，按`(tenant_id, deployment_id)` keyset、每页最多256项扫描02 exact bindable Model predicate：
   Resource lifecycle Active、gate Enabled、active target为exact Model Deployment且Deployment gate Enabled；逐项用incoming Candidate port验证，
   即使发现不兼容也继续到bounded EOF并记录第一个canonical failure；总数不得超过4096并必须等于captured active count。需要续租时只用
   Receipt-only短事务CAS同一claim，不持有installation/catalog锁；scan dependency/deadline transient使用步骤3相同的Receipt→InstallationReleaseState
   classification，不能在未重验public ETag时返回503；
5. 最终短事务先锁同一Receipt并复验Processing、request digest与`claim_generation`，再锁InstallationReleaseState，复验public If-Match、
   expected generation/state digest/count、incoming manifest ID/digest、qualification/approval有效期和完整EOF scan evidence。任一active-set mutation或
   Tenant encryption-domain add/rebind/revoke都会先锁同一authority并推进generation，因此漂移改变public ETag并使旧scan失效；此时terminalize
   同一Receipt为Rejected并返回412，不能把client precondition failure内部重试成503。若state/ETag仍与capture相同但scan记录确定性
   Model incompatibility，则在该事务只terminalize Receipt为Rejected 409且不改state/Event；count/EOF invariant损坏返回500且不伪装为
   domain rejection；
6. 同一最终事务写新exact active release、generation严格加一、state digest、terminal Command Receipt及Release Event/Outbox。promote允许
   Uninitialized首次激活或Active切到不同approved target；rollback只允许Active切到不同approved、database-schema-exact-compatible旧Release，
   不要求扫描Event证明曾Active，也不down-migrate或patch current state。新key指向current exact target稳定拒绝；terminal same-key/same-digest
   Receipt在重验If-Match/current state前返回原结果。两种command分别使用17冻结的Receipt/Event discriminator，不创建ManagementOperation；
7. capture后的serialization/transient CAS只有在classification证明public ETag仍逐值相等时才可在内部最多重试三次；耗尽返回503，保留可由
   同key续租/接管的非终态Processing Receipt且没有terminal winner。进程在最终commit前崩溃时old pointer保持完整；重放从步骤1恢复或接管，
   永不新建第二Receipt。

scan阶段没有“待生效pointer”，只有最终CAS改变current authority，因此不是异步双写窗口。Kubernetes rollout/readiness只能在durable
commit后推进流量。root Runtime API对全部Model候选验证后在创建Run事务中复验同一generation/digest并冻结02 binding；child使用parent
binding。zero active Model Deployment是合法EOF；第4097个activation在修改Resource前稳定拒绝。

### 4.1 当前证据边界（非规范性）

旧专用表族、catalog、fixture和历史候选记录已全部撤销，不计入任何当前Gate、CandidateManifest或ReleaseManifest，也不得作为ADR目标
baseline的历史前置条件。当前可复现的数据库foundation及版本只由ADR/implementation plan记录，本规范不复制物理名称或计数。Phase 1/2真实PostgreSQL 16
integration fixture已覆盖generic Resource/Security、Run/Job/Task/Subagent/controller、并发claim、fence/retry/wait/recovery、
bounded safety scan、独立business/critical-control pool以及lease-fenced executor start/heartbeat/handoff。这些记录只属于开发期
Contract/Functional子证据：尚未绑定immutable CandidateManifest、production-equivalent images/config/topology和完整Q1 dataset，
因此不能声明Gate D/E、Q1或Release资格。Artifact/Invocation、外部backend、50 active Runs、跨WorkClass饱和、24小时soak与DR
证据必须在对应实现阶段重新产生，不能沿用已撤销设计的数字或报告。

此前CandidateManifest基础的closed Rust type、checked-in JSON Schema、canonical digest与WorkerManifest v1/HardLimitProfile v4
exact-closure validator已经交付并进入`insight.platform/v1`根合同digest；它们尚无本节新增的materialization mode、component-runtime、
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
不登记Gate或Phase完成。Artifact Workload Broker、Artifact Workload Producer与Artifact Maintenance Authority也尚无accepted machine service、binary、Deployment、
restricted DB/storage identity或real-process evidence；现有Runtime/Registry/Capability/Context/MCP读取路径及Artifact scanner/GC路径不能被声明为
已通过新Broker/Producer/Authority，普通workload输出进入`JobAttempt + WorkloadBound` staging/scan flow以及scanner/GC移除直接S3/KMS
credential也仍只是Draft目标。Artifact Upload Gateway与Artifact Download Gateway的
双Deployment/ServiceAccount/credential/pool/permit/HPA拆分同样没有Candidate或real-process证据，现有逻辑Gateway/route不能证明两个failure
domain。独立Model Artifact Producer尚无domain/RPC实现、binary、
Model Worker client-stream组合、Deployment、ServiceAccount、
write-limited数据库role/pool、S3/KMS write identity、NetworkPolicy、autoscaling或故障/容量fixture；现有Model Worker也没有独立Producer
mTLS client、连接池或permit。该组合未绑定真实CandidateManifest，Artifact output仍为Inline，read Broker不得被临时扩权为output writer。
Model text delta内部publisher已有exact fence、canonical credential-free envelope、将容量permit
保留到有界批次flush结束的双重有界non-blocking queue和TLS/mTLS NATS组合；它不发布tool argument/Provider metadata，NATS故障不阻断durable执行。但Artifact-backed
output、六个internal service加两个public Gateway Deployment的八role/lane隔舱、真实S3/KMS、公开SSE消费、真实NATS/Provider/process-kill/
cross-workclass saturation资格证据仍缺失，
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
platform-artifacts     Workload/Model/Sandbox Artifact read Brokers, Artifact Workload/Model Producers, Artifact Maintenance Authority, Artifact Upload/Download Gateways, scanner controller, GC
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
| Artifact Workload Broker | 2 per storage region/boundary | stateless Runtime/Registry/Capability/Context/MCP read gRPC | 五个exact method-specific client mTLS allowlist、专用read-only PostgreSQL pool、S3/KMS read identity | 否，存在任一对应Artifact binding时 |
| Artifact Workload Producer | 2 per storage region/boundary | stateless Registry/Capability/Context/MCP/Sandbox staging client-stream gRPC | 五个exact method-specific client mTLS allowlist、独立write-limited PostgreSQL pool、S3/KMS staging identity | 否，存在任一对应output binding时 |
| Model Artifact Broker | 2 per storage region/boundary | stateless Model-only read gRPC | exact Model Worker read mTLS、Model专用read-only PostgreSQL pool、S3/KMS read identity | 否，存在Artifact-backed Model request时 |
| Model Artifact Producer | 2 per storage region/boundary | stateless Model-output client-stream gRPC | exact Model Worker output mTLS、独立write-limited PostgreSQL pool、S3/KMS write identity | 否，允许Artifact-backed Model output时 |
| Sandbox Artifact Broker | 2 per storage region/boundary | stateless WASI+microVM internal gRPC | exact Sandbox Controller mTLS、Sandbox专用restricted PostgreSQL pool、S3、KMS | 否，存在Sandbox Package/Artifact绑定时 |
| Artifact Maintenance Authority | 2 per storage region/boundary | stateless exact scan/head/delete gRPC | exact scanner/GC method allowlist、专用restricted PostgreSQL pool、S3/KMS read+exact-generation-delete identity | 否 |
| Egress Broker | 2 per external region/boundary | stateless internal gRPC | Security Authority RPC、private DNS、KMS/Secret Manager、exact remote endpoints | 否，生产外部绑定存在时 |
| Security Authority | 2 | stateless internal gRPC | PostgreSQL restricted role、Policy | 否 |
| Capability Worker | 2 per required manifest | queue worker | PostgreSQL、remote backend | 否，生产绑定存在时 |
| Context Worker | 2 | queue worker | PostgreSQL、index/Artifact | 否 |
| Interaction/Approval Worker | 2 | queue/deadline worker | PostgreSQL、Policy | 否 |
| Dataset Builder | 1+ | separate queue | PostgreSQL、Artifact/index | 是 |
| MCP Host | 2 | session/queue worker | PostgreSQL、Secret、remote MCP | 否，生产绑定存在时 |
| Artifact Upload Gateway | 3 | stateless public HTTPS StagingWrite/upload stream | current principal+opaque upload grant、upload-only restricted PostgreSQL pool、S3/KMS staging-write identity | 否 |
| Artifact Download Gateway | 3 | stateless public HTTPS GET/HEAD/Range stream | current principal+opaque read grant、download-only restricted PostgreSQL grant-use pool、S3/KMS exact-generation read identity | 否 |
| Artifact Scanner/Finalizer | 2 | queue worker | PostgreSQL、Artifact Maintenance Authority、Sandbox；无S3/KMS credential | 否 |
| Artifact GC/Reconciler | 2 | shard lease | PostgreSQL、Artifact Maintenance Authority；无S3/KMS credential | 否 |
| Sandbox Gateway/Controller | 2 | durable queue controller | PostgreSQL、Artifact/Secret Broker | 否 |
| WASM/gVisor/microVM Executor | capacity-based | dedicated nodes | Sandbox Controller | 按已验证cold-start policy |

Workload/Model/Sandbox三个read Broker、Workload Producer、Maintenance Authority、Model Producer、Upload Gateway与Download Gateway是八类不可合并
lane。Q1单storage region/boundary时它们至少映射为八个不同`ComponentRole`/image/startup-manifest scope；多region/boundary可为同一lane类增加
更多opaque scope，但每个scope仍在全Candidate capacity identity检查中使用互不相等的DB/storage/KMS pool与permit identity。
统一public hostname不能使Upload/Download共用component role、ServiceAccount或HPA；同一role的最小副本只表示该role内部HA，不增加额外authority。

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
Ingress -> Management/Runtime/SSE/Artifact Upload Gateway/Artifact Download Gateway by closed host+route+method registry
Authorized platform authorities -> their exact private PostgreSQL/NATS/Artifact/Secret endpoints
Workers -> credential-free closed request -> Egress Broker
Egress Broker -> Security Authority RPC -> PostgreSQL restricted Secret authority
Egress Broker -> KMS/Secret Manager/private DNS/exact Provider/MCP/remote capability
Runtime Worker -> exact mTLS ReadRuntimeArtifact -> Artifact Workload Broker
Registry Validation Worker -> exact mTLS ReadRegistryArtifact -> Artifact Workload Broker
Capability Worker -> exact mTLS ReadCapabilityArtifact -> Artifact Workload Broker
Context Worker -> exact mTLS ReadContextArtifact -> Artifact Workload Broker
MCP Host -> exact mTLS ReadMcpArtifact -> Artifact Workload Broker
Registry Validation Worker -> separate exact mTLS client-stream StageRegistryArtifact -> Artifact Workload Producer
Capability Worker -> separate exact mTLS client-stream StageCapabilityOutput -> Artifact Workload Producer
Context Worker -> separate exact mTLS client-stream StageContextOutput -> Artifact Workload Producer
MCP Host -> separate exact mTLS client-stream StageMcpOutput -> Artifact Workload Producer
Model Worker -> exact mTLS -> Model Artifact Broker
Model Worker -> separate exact mTLS client-stream -> Model Artifact Producer
Sandbox Controller -> exact mTLS -> Sandbox Artifact Broker
Sandbox Controller -> separate exact mTLS client-stream StageSandboxOutput -> Artifact Workload Producer
Artifact Scanner/Finalizer -> exact mTLS ReadForScan/HeadExactGeneration -> Artifact Maintenance Authority
Artifact GC/Reconciler -> exact mTLS HeadExactGeneration/DeleteExactGeneration -> Artifact Maintenance Authority
Artifact Upload Gateway -> its own upload-only PostgreSQL pool / private S3/KMS staging-write identity and permit
Artifact Download Gateway -> its own grant-use PostgreSQL pool / private S3/KMS exact-generation read identity and permit
Artifact Workload Broker -> its own read-only PostgreSQL pool / private S3/KMS read identity
Artifact Workload Producer -> its own write-limited PostgreSQL pool / private S3/KMS staging identity
Model Artifact Broker -> its own read-only PostgreSQL pool / private S3/KMS read identity
Model Artifact Producer -> its own write-limited PostgreSQL pool / private S3/KMS write identity
Sandbox Artifact Broker -> its own restricted PostgreSQL pool / private S3/KMS read identity
Artifact Maintenance Authority -> its own restricted PostgreSQL pool / private S3/KMS scan-head-exact-delete identity
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
- Runtime/Registry Validation/Capability/Context/MCP调用方不得直连S3、KMS或workload-identity endpoint；只有Artifact Workload Broker
  持有general-workload读取所需的physical identity。Broker只注册17五个exact read method，每个method都同时验证其唯一URI SAN、15
  `workload_kind`、durable owner/Job fence、Artifact Link、purpose、byte limit与deadline，并只返回bounded bytes；
- Registry Validation/Capability/Context/MCP/Sandbox普通输出不得使用public Upload bearer、read Broker、Model Producer或直连S3/KMS；只经
  Artifact Workload Producer五个exact client-stream method。Producer逐项验证method/SAN、`JobAttempt + WorkloadBound + StagingWrite`、typed
  owner、attempt/lease/worker/request/grant/staging fence和deadline，最多提交Uploaded并创建或重放既有scan Job；其DB/storage role不能GET/list、
  scan、Verified/Ready、finalize/reference、修改owner terminal authority或处理Model output；
- Sandbox Controller不得直连S3、KMS或workload-identity endpoint；只有Sandbox Artifact Broker持有相应物理读取identity；
- Model read Broker保持read-only且只注册`ReadModelRequest`；其数据库role不得写Artifact或共享聚合，storage identity不得PUT、
  删除或枚举bucket，进程不得注册output upload/finalize或generic object RPC；
- Artifact Scanner/Finalizer与GC/Reconciler不得直连S3、KMS或workload-identity endpoint；只有Artifact Maintenance Authority持有maintenance
  identity。Authority只注册`ReadForScan`、`HeadExactGeneration`与`DeleteExactGeneration`，按method-specific scanner/GC URI SAN和15 exact
  Job/lifecycle fence执行；响应只含scan bytes或bounded typed evidence，不含locator、KMS plaintext或bucket credential；
- Model Artifact Producer只注册closed Model-output client-stream RPC，只接受exact `model-worker.artifact-output` URI SAN并拒绝read
  client的`model-worker` URI SAN；stream
  header、chunk、总bytes、deadline、attempt/lease和idempotency均有硬界，Provider正文、object locator和storage credential不回传Worker；
- Artifact Workload Broker、Model Artifact Broker、Sandbox Artifact Broker、Artifact Workload Producer、Artifact Maintenance Authority与
  Model Artifact Producer使用六个不同Service、ServiceAccount、数据库credential/pool、mTLS server identity、storage/KMS workload identity、
  NetworkPolicy、connection pool和in-flight permit；任一进程/listener只安装一个17 exact service。三个read Broker拒绝两个Producer与
  Maintenance identity；Workload Producer拒绝public bearer、read/Model/Maintenance identity及跨method role；Maintenance拒绝全部业务read/
  Producer identity；Model Producer拒绝全部read、Workload Producer与Maintenance RPC；
- public `Artifact Gateway` hostname按closed route+method registry分别路由到Artifact Upload Gateway与Artifact Download Gateway，不能按
  body/token内容动态选后端。两者不复用上述任一internal Service，也彼此使用不同Deployment、ServiceAccount、数据库credential/pool、
  storage/KMS identity、listener、permit与HPA；Download只经15 exact principal/token/grant-use authority构造单次read projection，Upload只接受
  `Principal + OpaqueBearer + StagingWrite`写exact object并拒绝workload mTLS/`WorkloadBound`，均不得取得generic Artifact/Blob mutation或
  prefix/list权限，也不注册或代理internal gRPC；
  两者都必须代理有界bytes且不得redirect或返回object-store URL、bucket credential或KMS material；
- Model Producer的write-limited数据库role只能读取16的closed `ModelOutputStageAuthorizationProjectionV1`（覆盖exact Model admission/Job fence、
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
- connection pool按role硬隔离；Workload/Model/Sandbox三个read Broker即使具有相同只读表集合也必须使用三套不同credential和pool，
  Artifact Workload Producer使用第四套write-limited staging credential/pool，Artifact Maintenance Authority使用第五套restricted maintenance
  credential/pool，Model Artifact Producer使用第六套write-limited
  credential/pool；Artifact Upload Gateway与Artifact Download Gateway再使用两个独立restricted pool。任一role都不能借用worker、其他Artifact
  service或相邻public lane的pool；所有pool最大值总和 + migration/admin reserve必须低于DB max connections；
- Workload/Model/Sandbox Broker数据库role只允许构造各自15 closed authorized-read projection所需的exact SELECT；Maintenance role只允许构造
  exact maintenance projection和提交其typed backend evidence所需的最小行级读写。Runtime/Registry/Capability/Context/MCP、Artifact scanner/GC
  client role不得读取sealed locator/KMS字段，也不得继承Broker/Authority数据库credential；
- Workload Producer role只允许读取exact Attempt/grant/staging authorization projection，并对预留Artifact/Blob/WorkloadBound grant与upload
  Receipt执行closed行级INSERT/UPDATE；Uploaded winner只能调用15现有schedule-scan repository command创建或重放同一scan Job及其既有
  Receipt/Event/Outbox事务输出。它没有generic Job/Event/Outbox mutation、Ready正文/list/filter、owner Job terminal、Verified/Ready/finalize/
  reference权限；
- Model Producer role对Model/Run/Policy/Quota/Artifact/Blob/grant/Receipt只有构造16 closed row-scoped projection所需的column-level SELECT或
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
- bucket/object无public ACL/website，访问只给Artifact Upload Gateway、Artifact Download Gateway与六个internal Artifact service各自的exact
  workload identity；普通
  Runtime/Registry/Capability/Context/MCP/Model/Sandbox/scanner/GC client都无direct S3/KMS access；
- Model Artifact Producer使用独立write-limited identity执行exact staging PUT/HEAD、仅对同reservation exact staging generation执行
  verifier/recovery所需的GET，并按exact context执行KMS seal/unseal；它不能list tenant prefix、读取任意Ready object或复用Workload/Model/
  Sandbox/Workload Producer/Maintenance identity。Workload、Model与Sandbox三个read Broker只能对各自已授权exact generation执行HEAD/GET与KMS unseal，不能PUT/DELETE；
- Artifact Workload Producer使用独立staging-write identity，只能对其exact `JobAttempt + WorkloadBound` reservation执行PUT/HEAD及完成Uploaded
  所需的bounded metadata verification；不得GET/list Ready或其他staging object、DELETE、scan、使用public bearer或访问Model reservation；
- Artifact Maintenance Authority只在15 exact Job/lifecycle fence下执行scan GET、exact-generation HEAD或exact-generation DELETE；其storage
  policy不得list/prefix scan、PUT、读取非请求Ready object或返回locator/KMS plaintext。Scanner/Finalizer和GC/Reconciler自身没有该identity；
- Artifact Upload Gateway与Artifact Download Gateway使用不同ServiceAccount、S3/KMS identity/client、connection pool、byte permit和cloud IAM
  action set；Upload只能以`Principal + OpaqueBearer`写exact Staging generation，Download只能读exact authorized generation，两者不能互换identity或借用internal service；
- Workload/Model两个Producer的write permit、S3/KMS client、连接池、byte budget和timeout彼此分离，并与三个read Broker、Maintenance及public
  两条lane全部分离；
- Workload Producer partial upload/crash由exact Job Attempt、WorkloadBound grant、upload Receipt与既有scan/GC flow收敛；它只创建或重放
  scan Job，不claim/执行scan，也不能把未扫描内容推进Ready；
- Model Producer partial upload、crash或Model terminal first-winner失败必须由durable staging fact、同Attempt stage Receipt和bounded GC收敛；
  Model Producer不创建或claim Artifact Job，
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
gRPC消费04拥有的closed trusted SecretBinding resolution projection（exact binding ref、sealed opaque reference、reference digest与key identity），
并独占普通执行角色中的KMS/Secret Manager调用权限；Management、Runtime、Host与普通Worker只持有`ExactSecretBindingRef`。Security Authority
是唯一可从logical SecretBinding aggregate构造该projection并
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

旧Deferred poll/callback专用evidence持久化设计及其adapter checkpoint已撤销，不属于当前baseline、部署状态或Gate证据；具体物理记录只保留在
Git历史与ADR。

HardLimitProfile machine contract仍以checked-in schema和CandidateManifest精确引用的实例为唯一输入。当前revision固定
`profile_version=4`并新增必填
`capability_sandbox.runtime_bundle_bytes={unit:"bytes",hard_max:67108864,q1_default:33554432,
overflow_outcome:"content_rejected"}`。SandboxPackage发布必须从Ready `runtime_bundle_artifact`取得可信byte length，并拒绝长度为零或
大于67108864；Q1 effective limit为33554432且只能被deployment/tenant进一步收紧。WASI ABI的16 MiB module限制仍是backend-specific
更严格上限。缺失该字段、旧profile version、错误单位/outcome或越界Package都必须在Candidate/发布阶段fail closed，不能形成永远无法执行的Job。
schema/Q1实例、Rust exact validator、Package publication fixture和Candidate closure门禁已经通过；这只构成该合同切片的实现证据，不单独构成Phase或Gate完成证据。
Deferred execution、callback ingress、timer/wake Worker 与 Q1 资格必须在对应 Phase 3～6 重新生成证据，不得沿用旧候选记录。

CR-165的Draft目标合同要求下一revision固定为`profile_version=5`并新增以下全部必填字段；数字是目标machine contract，不是当前
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
| `artifact.workload_producer_transport_backlog` | count | 4096 | 64 | `temporarily_unavailable` |
| `artifact.workload_producer_transport_timeout_milliseconds` | milliseconds | 60000 | 1000 | `temporarily_unavailable` |
| `artifact.workload_producer_wire_chunk_bytes` | bytes | 1048576 | 262144 | `content_rejected` |
| `artifact.workload_producer_in_flight_streams` | count | 4096 | 64 | `temporarily_unavailable` |
| `artifact.workload_producer_in_flight_declared_bytes` | bytes | 4398046511104 | 6710886400 | `temporarily_unavailable` |
| `artifact.workload_producer_in_flight_buffer_bytes` | bytes | 4294967296 | 16777216 | `temporarily_unavailable` |
| `artifact.workload_producer_streams_per_tenant` | count | 1024 | 8 | `temporarily_unavailable` |
| `artifact.workload_producer_declared_bytes_per_tenant` | bytes | 107374182400 | 838860800 | `temporarily_unavailable` |
| `artifact.ready_retention_seconds` | seconds | 315576000 | 2592000 | `invalid_request` |
| `artifact.model_output_content_evidence_validity_seconds` | seconds | 315576000 | 2592000 | `invalid_request` |

上述十九个tuple在profile v5中逐字段exact，不能只验证正数或`q1_default <= hard_max`；所有Limit的`hard_max/q1_default`还必须不超过
JSON safe integer。Candidate qualification的installation effective值就是其exact profile的`q1_default`；Deployment/tenant/Attempt只能在
typed closure中进一步收紧，不能扩大或静默改写Candidate manifest。Worker/Producer component manifest因此与Candidate effective值比较，而不是直接
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
workload_producer_wire_chunk_bytes <= artifact.single_bytes
workload_producer_in_flight_declared_bytes
  == workload_producer_in_flight_streams * artifact.single_bytes
workload_producer_in_flight_buffer_bytes
  >= workload_producer_in_flight_streams * workload_producer_wire_chunk_bytes
workload_producer_transport_backlog <= workload_producer_in_flight_streams
workload_producer_streams_per_tenant <= workload_producer_in_flight_streams
workload_producer_declared_bytes_per_tenant
  == min(durable_quota.artifact_staging_bytes,
         workload_producer_streams_per_tenant * artifact.single_bytes)
```

任一add/multiply溢出都拒绝profile/Candidate，不能saturate。Q1对应关系固定为
`16*4194304=67108864`、`64*4194304=268435456`和`64*(65536+4096)=4456448 <= 16777216`；hard关系固定为
`1024*16777216=17179869184`、`4096*16777216=68719476736`和
`4096*(262144+4096)=1090519040 <= 4294967296`。Workload Producer的hard关系固定为
`4096*1073741824=4398046511104`、`4096*1048576=4294967296`及
`min(107374182400,1024*1073741824)=107374182400`；Q1关系固定为
`64*104857600=6710886400`、`64*262144=16777216`及
`min(10737418240,8*104857600)=838860800`。这些等式是machine invariant，不是解释性示例。

`artifact.model_output_chunk_bytes`冻结`StageModelOutput` canonical Data frame大小，且不能超过该Attempt的
`maximum_materialized_bytes`；`artifact.workload_producer_wire_chunk_bytes`分别冻结17五个ordinary staging method共享的canonical Data frame上限，
且不能超过该Attempt的`maximum_bytes`。两者的effective值都只能进一步收紧。Worker两字段封顶07 manifest的slot+weighted bytes；Model Producer transport与四项in-flight字段、Workload Producer
transport/chunk与五项in-flight/per-tenant字段分别封顶对应ComponentRuntimeManifest和
每个RPC的双层weighted admission；Ready retention封顶16 Model Deployment的exact duration。当前checked-in `profile_version=4`没有这些
字段，现有schema/Q1/Candidate证据不能证明Artifact Workload Producer或Artifact-backed Model output；实现本组CR-165 Producer合同时必须原子升级schema、Q1实例、Rust exact
validator、WorkerManifest v2、15 ArtifactStorageBindingManifest、ComponentRuntimeManifest、ComponentStartupManifest/profile registry/
projection/factory、16 installation compatibility port/generation、RPC protocol carrier、Candidate digest和正负向fixture，不能以环境变量或Helm自由值补字段。

### 14.2 Capacity contract

| 字段族 | 必须冻结的上限 |
|---|---|
| API | header/URL/compressed与decoded body、JSON depth/properties/items、list page、SSE event/buffer/connection |
| Registry/Plan | Draft/package/schema bytes、definitions/nodes/edges、branch/map/loop/model round、dependency closure |
| Run/Scheduler | active/waiting Run、descendants、ready rows、inline Value bytes、ValueRef count、claim batch、attempts、lease/heartbeat、deferred poll base/max、wake contracts |
| Model/Context/MCP | request/response/delta、tokens、tool calls、candidates/items/pages、sessions/tasks/subscriptions |
| Capability/Sandbox | input/runtime bundle/output/progress、queue、CPU/memory/pids/files/IO/network、wall time、cleanup deadline |
| Artifact | single/total bytes、parts、references/grants、Model output canonical chunk/Worker slot+bytes/Model Producer streams+declared+buffer bytes/per-tenant streams、Workload Producer global/per-tenant streams+declared bytes+wire buffer、public upload/download、Workload/Model/Sandbox read、maintenance scan/head/delete各lane并发/bytes、scan expansion/page/object、staging/Ready retention/batch |
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
| Artifact | Artifact Upload Gateway、Artifact Download Gateway、Workload read Broker、Model read Broker、Sandbox read Broker、Workload Producer、Maintenance Authority与Model Artifact Producer八类lane中每个logical scope各自的in-flight/bytes/DB+storage pool与HPA；两个Producer staging/upload及scanner/GC durable backlog |

- autoscaling不能超过DB connection、Provider quota、node/KVM、NATS/S3和tenant hard capacity；
- scale-up前保留control/cancel/cleanup slots；
- scale-down先readiness false/drain，不能中止lease而伪造failure；
- mandatory control/SSE/API/Scheduler/Recovery不scale-to-zero；
- Sandbox warm pool有独立memory ceiling且不挤占running capacity；
- node pressure/eviction/spot只允许经过failure qualification的worker pool；
- 单workclass的HPA不能使用全平台共享queue长度导致连锁扩容；
- Artifact Workload Broker按自己的per-method request/byte、DB pool与S3/KMS read latency扩缩；Maintenance Authority按scan/head/delete
  request/byte、durable scanner/GC backlog、DB/storage pool与delete latency扩缩。两者不得消费worker本地队列之外的相邻Broker、Producer或
  Artifact Upload/Download Gateway permit；worker saturation也不得让其直接fallback到S3/KMS；
- Artifact Upload Gateway按upload stream/bytes、staging backlog与upload-only DB/S3/KMS pool扩缩；Artifact Download Gateway按GET/HEAD/Range
  stream/bytes与download-only DB/S3/KMS pool扩缩。两套HPA不得消费同一个permit/queue或因统一hostname产生联动scale/readiness；
- Artifact Workload Producer按五个closed method的active stream/bytes低基数观测、global/per-tenant admission、Uploaded/scan backlog与自己的
  DB/S3/KMS staging pool扩缩；per-method只用于观测/HPA分解，不构成第五套未登记的method/role quota或fairness authority。它
  不得消费public Upload、read Broker、Maintenance或Model Producer permit/queue，饱和时调用方只能按同Attempt有界重试，不能fallback直写storage；
- Model Artifact Producer按自己的active stream、weighted declared/buffer bytes、durable staging/cleanup backlog、oldest production age、write permit、DB pool和S3/KMS latency扩缩；
  它不得消费Workload/Model/Sandbox read、Workload Producer、Maintenance或Artifact Upload/Download Gateway指标、semaphore与扩缩容预算，任一Artifact lane达到hard capacity不得
  触发另外七条lane连锁扩容或拒绝；
- Model Artifact Producer在TLS/service-role authorization后、读取bounded header前先取得global stream与weight exact为
  `effective model_output_chunk_bytes + 4096`的唯一per-stream wire-buffer permit，所有frame复用该buffer；解析并授权valid header后、
  读取首个data frame前再取得declared bytes与tenant stream permit，不重复取得data buffer。全部持有到唯一terminal、stream drop或absolute deadline；
  第一阶段饱和返回body-free unavailable，第二阶段返回`DependencyUnavailable + RetrySameAttempt`，不得进入application queue。DB/S3/KMS
  pool waiters受global stream permit封顶，client library不得再建立无界内部队列；transport accept开始的同一monotonic timeout必须同时覆盖
  TLS、bounded backlog、第一阶段permit等待和完整Header decode，silent/fragmented pre-header流到期释放全部资源；
- Artifact Workload Producer在TLS/service-role authorization后只允许进入runtime manifest冻结的accepted backlog，并在读取bounded header前取得
  global stream及exact `wire_chunk_bytes`的单一复用buffer；valid header授权后、首个data frame前再取得declared-byte、per-tenant stream与
  per-tenant declared-byte permit。DB/S3/KMS connection和waiter都只由同一projection ticket构造，饱和立即拒绝且不得转入application queue、
  public Upload lane或worker直写；accept timeout覆盖TLS、backlog、上述首阶段permit与完整Header decode，所有退出路径释放已取得的permit；
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
- Provider/MCP/Sandbox/Artifact单pool rollout不触发无关服务rollout；Artifact Workload Broker、Model read Broker、Sandbox Broker、
  Artifact Workload Producer、Artifact Maintenance Authority、Model Artifact Producer、Artifact Upload Gateway与Artifact Download Gateway八类lane的每个logical scope必须可以分别独立
  drain/rollout，不通过共享Pod、ServiceAccount、credential、pool、permit、HPA或readiness联动；统一public hostname只在edge保持合同稳定；
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
100% Artifact Upload Gateway permit/DB/storage-pool saturation while Artifact Download Gateway, all six internal Artifact services, API and control remain admissible
100% Artifact Download Gateway permit/DB/storage-pool saturation while Artifact Upload Gateway, all six internal Artifact services, API and control remain admissible
100% Artifact Workload Broker read permit/DB-pool saturation while public lanes, Model/Sandbox reads, Workload/Model Producers, Maintenance, API and control remain admissible
100% Model Artifact Broker read permit/DB-pool saturation while public lanes, Workload/Sandbox reads, Workload/Model Producers, Maintenance and Model control/cancel remain admissible
100% Sandbox Artifact Broker read permit/DB-pool saturation while public lanes, Workload/Model reads, Workload/Model Producers, Maintenance and Sandbox control remain admissible
100% Artifact Workload Producer staging permit/DB-pool saturation while public lanes, all three read Brokers, Maintenance, Model Producer and workload control/cancel remain admissible
100% Artifact Maintenance Authority operation/byte permit/DB-pool saturation while public lanes, all three read Brokers, both Producers, scanner/GC control and API remain admissible
100% Model Artifact Producer write permit/DB-pool saturation while public lanes, all three read Brokers, Workload Producer, Maintenance and Model control/cancel remain admissible
20 concurrent public Artifact transfers split across upload/download, typical 10 MiB, boundary fixture 100 MiB
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

Artifact topology fixture必须从同一Candidate的protobuf descriptor、startup projection、Kubernetes manifest、PostgreSQL grants、cloud IAM与
NetworkPolicy证明：internal surface恰为17六个不可合并service及其列明method；Workload Broker五个read method、Workload Producer五个staging
method和Maintenance三个method的URI SAN allowlist
逐项exact；一个public hostname的closed route/method registry只把upload与download分别路由到两个不可合并Gateway Deployment，且两者不能发现或
转发internal gRPC、redirect或返回object-store URL/credential；public Upload只接受`Principal + OpaqueBearer`，Workload Producer只接受
`JobAttempt + WorkloadBound + StagingWrite`，互换必须在body/DB/storage前拒绝。Runtime/Registry/Capability/Context/MCP、scanner与GC身份都无法
直连S3/KMS、读取sealed locator或借用Broker/Producer/Authority credential。real-process fixture还必须逐一耗尽八role/lane的request/byte permit、
DB pool与storage client pool，证明其他七lane及API/control仍可准入，不能用只检查Deployment数量或NetworkPolicy文本替代实际拒绝/饱和证据。
同一fixture还必须从17 stage-route registry与实际client startup manifests重算Q1五项enabled kind，并证明至少一个完整Workload Producer scope、
其storage-binding集合为Candidate catalog的不相交全集分区，以及`enabled kind × storage binding`每个pair恰有一个method/profile/SAN/audience/
Artifact owner/Job typed owner/JobKind/WorkClass/port/purpose/scope route；零scope、漏项、重项或只检查service存在都不合格。

Gate A的CR-165 machine fixture还必须覆盖：非v5 profile、十九项Artifact Producer tuple任一unit/value/outcome漂移及installation scan/count常量漂移；Worker v1、
缺/错component role或region、Model缺capacity、非Model额外/null capacity、slot/aggregate越界；Component runtime unknown kind/field、错误queue
mode、跨variant字段、零值或profile越界、backlog/stream/declared-bytes/buffer/per-tenant checked arithmetic、pool/waiter越界、
content-validation列表空/无序/重复/超限及wire-buffer checked add/multiply溢出；Storage空catalog、
错误backend/region/addressing/write/observation、零timeout/object limit、超safe uncertainty/object limit，以及uncertainty 1/1000/1001、
staging/grace等号/相邻/溢出边界；Candidate两个mode、四个digest数组缺字段/无序/重复/超限/missing/extra、零Workload Producer scope、
Workload storage partition漏/重/额外binding、enabled client profile与stage-kind漏/重/交换、任一kind×binding route漏失/重复，以及Producer三处partial/orphan
closure、worker/startup/runtime role或region与image不匹配、unknown/duplicate startup profile、projection requirement/schema/provenance drift、
`ArtifactWorkloadProducer` runtime或`artifact_workload_producer/v1` startup/profile缺失、错配或被public/Model role引用，
六个stage-client profile任一缺失/额外、WorkClass/SandboxController projection互换、`P/S` name set或unit漂移、Sandbox Controller冒充
`StartupOnly`，capacity property缺失/null/多余、primitive name set偏差、ticket identity/kind/unit/`fixed|per_key` shape/value偏差、非法pool/semaphore单位组合、
未消费/重复ticket、projection preimage version/length/startup bytes或manifest digest漂移、optional absent/null互换、把projection digest自身纳入或置零后hash、
绕过factory及任意跨role或
pool↔semaphore alias；Producer多scope role/region/binding重叠与错误路由；协议document/schema/Rust常量与4096值漂移。

Policy fixture必须逐字段mutate完整resolved ArtifactIo，覆盖相同storage但不同encryption domain/KMS/content-validation、rules body/digest/ref
swap、effective maximum/staging边界、15 timing result及reservation/projection/Header漂移。Producer fixture必须区分Worker
`model_response_semantic_evidence_digest`与Producer计算的15 tagged content evidence，覆盖semantic preimage及profile/runtime/rules/storage/
encryption/object generation/evidence digest任一替换、profile validity边界，并在pre-I/O、各checkpoint、post-I/O和owner terminal前并发
revoke/rebind Tenant encryption domain。Installation fixture覆盖`ins`/旧`svc`、Uninitialized/Active shape、singleton、
state digest/count/generation、ADR定义的fresh-schema target、无fake tenant的Receipt/Event/Outbox scope、active set 4096/N+1、scan page/EOF/count、
terminal replay发生在resolver/If-Match前、Processing lease接管、以及进程在resolve/scan/final CAS/response窗口崩溃。还要分别证明public
ETag因active-set/encryption变化而失配时terminal 412、ETag未变的serialization/transient race三次后503、无public If-Match的root admission
internal generation race三次后503；activation/deactivation/suspend/switch/root admission并发只能提交完整generation。Run fixture必须让全部Model候选中仅一个不兼容并证明
整体回滚，root只冻结完整old/new binding，child继承parent binding。

Qualification supply-chain fixture覆盖Bundle/Approval closed schema与canonical digest、签名信任根、reports 1/256/N+1及排序/重复、evidence
digest/size/media/retention漂移、missing object、Bundle/Approval有效期边界和final CAS期间过期；Release/Candidate/Bundle/Approval任一ID或digest swap都
稳定拒绝。tenant ArtifactId/ArtifactRef、fake tenant、03 Receipt/Task或mutable tag/latest不得通过resolver类型检查；promotion/rollback只接受
exact content-addressed refs且response loss重放不重新选择证据。

正向fixture必须分别证明纯Inline Candidate、零Model Deployment的Artifact-capable Candidate及后续Inline/ArtifactCapable Deployment反向激活
验证稳定，以及包含Workload Producer但仍保持`inline_only`的Candidate；readiness对同一startup document重投影出byte-identical
startup/Worker/runtime/ticket closure；并发mutation由同一compatibility
generation产生单一winner。错误映射还要证明只有public If-Match返回412，内部race耗尽返回detail-free
`application/problem+json` 503，不泄露ID/digest/region/catalog。

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
kill or saturate Artifact Upload Gateway while Artifact Download Gateway and all six internal Artifact services remain admissible
kill or saturate Artifact Download Gateway while Artifact Upload Gateway and all six internal Artifact services remain admissible
kill or saturate Artifact Workload Broker while public lanes, Model/Sandbox Broker, Workload/Model Producers and Maintenance remain admissible
kill or saturate Model Artifact Broker while public lanes, Workload/Sandbox Broker, Workload/Model Producers and Maintenance remain admissible
kill or saturate Sandbox Artifact Broker while public lanes, Workload/Model Broker, Workload/Model Producers and Maintenance remain admissible
kill or saturate Artifact Workload Producer before/during/after staging PUT while public lanes, three read Brokers, Maintenance and Model Producer remain admissible and scan/GC converges
kill or saturate Artifact Maintenance Authority during scan/head/delete while public lanes, three read Brokers and both Producers remain admissible and worker retry converges
kill or saturate Model Artifact Producer before/during/after staging PUT while public lanes, three read Brokers, Workload Producer and Maintenance remain admissible and cleanup converges
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
- 周期执行 Worker kill、NATS outage、backend 429、Sandbox saturation、六个Artifact internal service及Artifact Upload/Download Gateway八lane逐一独立
  饱和和SSE reconnect；
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
- Gate E：Q1容量、fairness、bulkhead和SLO达标；必须把Artifact Upload Gateway、Artifact Download Gateway、Artifact Workload Broker、
  Model Artifact Broker、Sandbox Artifact Broker、Artifact Workload Producer、Artifact Maintenance Authority与Model Artifact Producer八lane
  逐一压到100% admission/DB/storage capacity。每次饱和时另外七lane、API、Scheduler、对应worker control/cancel与security revoke仍满足其准入和延迟门槛，且不发生
  pool、credential、permit、queue、readiness或autoscaling联动。Artifact Workload Producer测得的accepted backlog、wire-buffer、global/per-tenant
  stream与declared-byte limit、DB/S3/KMS connection及waiter limit必须逐值等于同一Candidate runtime manifest与对应construction ticket；
  wire-chunk与accept-timeout没有capacity identity，必须逐值等于同一startup/runtime digest并由协议流量实测。任一运行配置漂移、应有票据缺失、
  为chunk/timeout伪造票据或只证明Helm/Deployment文本都使Gate E失败；
- Gate F：连续24小时soak达标；
- Gate G：从正式backup恢复并达到RPO/RTO；
- 任何Gate失败使后续证据无效或需要从受影响Gate重跑；
- approval不允许覆盖correctness/security failure。

当前Artifact Workload Broker、Artifact Workload Producer、Artifact Maintenance Authority、Artifact Upload/Download双Gateway物理拆分与独立Model Artifact Producer尚未实现；
既有Inline output、Model request read Broker、Sandbox Broker、逻辑Artifact Gateway、worker直连storage路径或静态NetworkPolicy证据均不能替代
六个exact internal RPC service、method-specific mTLS、JobAttempt/WorkloadBound staging、
restricted数据库/storage role、scanner/GC去S3/KMS凭证、durable cleanup及八lane故障/Q1容量证据。在这些证据绑定同一CandidateManifest并通过
对应Gate前，不得关闭Phase 4/6，也不得登记Gate A、B、C、D或E通过。

## 32. Qualification Evidence

```rust
enum QualificationEvidenceMediaTypeV1 {
    ApplicationJson,
    ApplicationNdjson,
    ApplicationZstd,
    ApplicationPdf,
    TextPlain,
    ApplicationOctetStream,
}

struct QualificationEvidenceRefV1 {
    content_digest: Digest,
    byte_length: u64,
    media_type: QualificationEvidenceMediaTypeV1,
    retain_until: DateTime<Utc>,
}

enum QualificationGateV1 {
    Contract,
    Functional,
    Security,
    RecoveryChaos,
    CapacitySlo,
    Soak24h,
    DrRestore,
}

enum QualificationGateOutcomeV1 { Passed }

struct QualificationGateResultV1 {
    gate: QualificationGateV1,
    outcome: QualificationGateOutcomeV1, // const Passed in a releasable bundle
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    evidence_digests: Vec<Digest>,
}

struct QualificationBundleV1 {
    schema_version: u32, // const 1
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
    profile_digest: Digest,
    environment_manifest_digest: Digest,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    gate_results: Vec<QualificationGateResultV1>,
    reports: Vec<QualificationEvidenceRefV1>,
    raw_evidence_manifest: QualificationEvidenceRefV1,
    signer_key_digest: Digest,
}

enum ReleaseApprovalDecisionV1 { Approved }

struct ReleaseApprovalV1 {
    schema_version: u32, // const 1
    release_id: ReleaseId,
    candidate_id: ReleaseCandidateId,
    candidate_manifest_digest: Digest,
    qualification_bundle_digest: Digest,
    decision: ReleaseApprovalDecisionV1, // const Approved
    approver_identity_digest: Digest,
    approved_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
}
```

Evidence至少包含commands/test binary digests、seed、load profile、component/image/schema/config digests、metrics query和
raw export、trace/log/audit sample、fault timeline、DB invariant output、Artifact inventory、security/DR报告。Secret/
token/raw tenant 正文必须 redact；redaction 本身版本化。

`QualificationBundleV1`、`ReleaseApprovalV1`及其evidence由installation supply-chain resolver按SHA-256 content digest读取和验证，
不是15 tenant-scoped Artifact/ArtifactRef、03 Receipt/Task或业务表事实，也不允许fake/system tenant。ReleaseManifest中的
`qualification_bundle_digest`与`release_approval_digest`分别是两个closed canonical document的digest；resolver验证signed envelope、
安装信任根、canonical bytes与digest，不能按mutable tag/latest查找。Approval逐字段回绑Release ID、Candidate ID/digest和Bundle digest，
因此不存在未定义的`ApprovalReceiptId`生命周期或第二current release authority。

`QualificationEvidenceMediaTypeV1` wire逐值固定为`application/json | application/x-ndjson | application/zstd | application/pdf |
text/plain | application/octet-stream`。reports required 1～256项，按content digest raw bytes严格升序且唯一；所有ref的byte length必须为正，
`retain_until >= bundle.valid_until`，`started_at <= completed_at < valid_until`，Bundle canonical bytes不超过1 MiB；Approval必须满足
`approved_at < valid_until`。final promote/rollback CAS使用同一PostgreSQL `db_now`复验Bundle与Approval都仍有效。preflight必须从resolver exact
resolve全部ref并验证digest/size/media；resolver的retention authority不得在`retain_until`前删除内容，missing/expired/ref漂移均使目标
确定性不兼容。只保存截图/摘要/人工结论不足；环境不等价、image/config变化或证据缺失同样使qualification无效。

`QualificationGateV1` wire闭集依次为`contract | functional | security | recovery_chaos | capacity_slo | soak_24h | dr_restore`，outcome唯一
合法值为`passed`。`gate_results`必须恰有七项、按上述A→G顺序排列且每个gate恰好一次；每项时间满足bundle
`started_at <= gate.started_at <= gate.completed_at <= bundle.completed_at`，相邻gate不得时间倒退。每项`evidence_digests`为1～64项、按raw bytes
严格升序且唯一，并且每个digest必须逐值命中`reports[].content_digest`或`raw_evidence_manifest.content_digest`。失败、跳过、未知或部分Gate不能编码为
releasable `QualificationBundleV1`；它们只形成CI/qualification运行失败证据，不得生成可被Release引用的Bundle。Gate A fixture必须覆盖七项完整正例，
以及missing/duplicate/out-of-order/unknown/not-passed、空或悬空evidence、时间越界/倒退和第8项负例。

## 33. Release、Promotion 与 Rollback

```text
Built -> Staged -> Qualifying -> Qualified -> Approved -> Deploying -> Active -> Draining -> Retired
```

- CI构建一次immutable image，环境间只promotion digest，不重建；
- image/package有signature、SBOM、provenance，admission按digest验证；
- promotion检查 CandidateManifest、`QualificationBundleV1`与`ReleaseApprovalV1`的ID/digest、签名、有效期及evidence closure完全匹配，再生成
  ReleaseManifest；rollback走同一exact resolver/有效期验证，不能把历史Event或tenant Artifact当作批准证据；
- immutable ReleaseManifest不充当mutable active pointer；唯一current release/Candidate ID/digest、active Model count与compatibility generation
  只由本规范`InstallationReleaseStateV1`拥有。promotion/rollback使用上述bounded scan+final CAS，Model Deployment bindability mutation与root
  Run admission按03锁序锁定/复验同一authority；禁止ConfigMap、Helm value、Event、Candidate resolver或进程cache形成第二current pointer；
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

CR-165的Release/Qualification、supply-chain evidence、capacity manifest与Artifact-capable closure仍需完成全量cross-review；关闭前本规范
保持Draft且不得作为实现输入。具体云、Kubernetes发行版、managed PostgreSQL/NATS/S3/Secret/telemetry产品和microVM VMM可以在满足本规范
conformance后选择；任何产品选择都不能削弱PostgreSQL authority、隔舱、tenant/Secret边界、durable recovery、SLO与Gate A～G。
