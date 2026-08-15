# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-09 |
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
```

`GitCommit` wire值必须是带算法标签的完整小写object ID：`sha1:<40-hex>`或`sha256:<64-hex>`；分支、tag、缩写SHA和
`latest`均非法。`ComponentRole`是稳定的小写部署角色键，必须匹配`[a-z][a-z0-9_.-]{0,127}`，不得用临时Pod名或副本名。
`database_schema_version`精确表示`insight-platform-postgres`导出的schema contract version（当前候选基线为`6`），不是
migration文件数量、数据库产品版本或payload schema version。Candidate创建器必须从实际安装的closed `WorkerManifest`集合和
`HardLimitProfile`计算canonical digest closure；worker digest按字节升序且唯一，重复role、缺失或额外manifest、limit digest漂移均拒绝。

只要Candidate包含MCP OAuth绑定，`deployment_config_digest`覆盖的closed Egress配置还必须包含exact Auth Policy revision、完整
Auth Profile、允许的非对称JWT算法、public JWKS及其canonical digest，以及OAuth写入所用ServiceIdentity Principal。运行时不得从
issuer动态补齐、刷新或替换该信任根；key rotation通过新Candidate发布并重新资格，而不是在旧Candidate内静默漂移。

`WorkerManifestDigest`必须是`contracts/platform-v1/schemas/worker-manifest.schema.json` closed document的canonical digest。
每份document只允许一个exact WorkClass，并冻结`worker_role`、`adapter_runtime_digest`、协议版本、业务最大并发和正数
`critical_control_reserved_slots`；CandidateManifest中的不同digest不能在运行时合并成共享semaphore。当前本地pool primitive
的unit evidence不是CandidateManifest，也不能替代Gate E/Q1负载证据。

### 4.1 当前证据边界（非规范性）

旧177表设计的migration 1～35、catalog、fixture和CR-090/092/093/094候选记录已全部撤销，不计入任何当前Gate、
CandidateManifest或ReleaseManifest，也不得作为新`0001`的历史前置条件。

当前可复现的数据库foundation只有23表、单一`0001_platform_baseline.sql`与对应schema verifier。Phase 1/2真实PostgreSQL 16
integration fixture已覆盖generic Resource/Security、Run/Job/Task/Subagent/controller、并发claim、fence/retry/wait/recovery、
bounded safety scan、独立business/critical-control pool以及lease-fenced executor start/heartbeat/handoff。这些记录只属于开发期
Contract/Functional子证据：尚未绑定immutable CandidateManifest、production-equivalent images/config/topology和完整Q1 dataset，
因此不能声明Gate D/E、Q1或Release资格。Artifact/Invocation、外部backend、50 active Runs、跨WorkClass饱和、24小时soak与DR
证据必须在对应实现阶段重新产生，不能沿用已撤销设计的数字或报告。

CandidateManifest的closed Rust type、checked-in JSON Schema、canonical digest与实际Worker/HardLimit exact-closure validator已经交付，
并进入`insight.platform/v1`根合同digest。当前尚未生成绑定production-equivalent images/config/topology的Candidate实例，也没有任何
Gate A～G结果或ReleaseManifest；因此这项machine-contract foundation本身不构成资格证据。

Sandbox expired-lease runtime现也有独立`WorkClass::Sandbox` business/critical-control permit、分片scan、backend evidence与fenced
commit driver；unit fixture证明Sandbox业务permit耗尽时critical-control scan仍运行。Core NATS control adapter也已实现exact
WorkerProcessGeneration subject、bounded closed request/reply和signal-digest binding。Helm已把WASI与microVM拆为独立DaemonSet/node
selector；microVM Pod内又把非root Executor和唯一持有KVM/cgroup/jail/state权限的Provider按volume、credential与capability拆开，并由
default-deny NetworkPolicy和ValidatingAdmissionPolicy锁定。该渲染合同已通过静态门禁，但仍未在authenticated NATS、真实KVM node、
PostgreSQL故障窗口或Q1饱和环境资格化，因此只属于开发期Contract/Functional子证据。

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
PostgreSQL pool和Model Worker mTLS Egress客户端；chart提供双副本rolling Deployment、PDB、HPA、topology spread、Restricted Pod、
无入站的default-deny NetworkPolicy及只到DNS/Egress/PostgreSQL的出口。CI同时拒绝mutable image、单副本、空PostgreSQL allowlist和非法
HPA。durable cancel driver现使用reserved critical-control permit，把当前generation的bounded PostgreSQL safety scan、Egress exact cancel和
旋转fence下的保守terminal结算组合起来；unit fixture证明业务permit饱和不阻止取消，数据库fixture覆盖取消/完成first-winner。但该组合仍是
Inline-only，未绑定真实CandidateManifest，也没有Artifact-backed IO、live delta、真实Provider/process-kill/
cross-workclass saturation证据，因此只属于Contract/Functional输入，不能登记Gate B～E通过。

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
platform-artifacts     Artifact Gateway, scanner controller, GC
platform-sandbox       Sandbox Gateway/Controller/Brokers
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
Sandbox guest -> Artifact/Secret broker and approved egress proxy only
OTel SDK -> local/central Collector
```

- API/Runtime/Scheduler不能直接访问untrusted internet；
- Egress Broker是唯一同时接触resolved Secret和untrusted Internet的普通执行角色；它必须使用独立Pod、workload identity、
  connection pool、并发/字节bulkhead和default-deny NetworkPolicy，且不能拥有任何数据库credential或直连PostgreSQL；
- Security Authority使用独立Pod、service account、mTLS listener和restricted PostgreSQL role；它只向exact Egress workload identity提供
  SecretBinding受信读取和prepared winner登记两个closed method，不能访问公网、private DNS resolver、KMS、Secret Manager或远端backend。
  resolution调用不改变数据库；prepared registration只能复用04冻结的Receipt/Event/Outbox原子事务，不能成为通用业务mutation API；
- Provider/MCP/remote/Sandbox egress按Revision/tenant policy经过proxy、DNS/TLS/allowlist；
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
- connection pool按role硬隔离，所有pool最大值总和 + migration/admin reserve必须低于DB max connections；
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

当前 HardLimitProfile machine contract 以 checked-in schema 和 CandidateManifest 精确引用的实例为唯一输入。
Deferred execution、callback ingress、timer/wake Worker 与 Q1 资格必须在对应 Phase 3～6 重新生成证据，不得沿用旧候选记录。

### 14.2 Capacity contract

| 字段族 | 必须冻结的上限 |
|---|---|
| API | header/URL/compressed与decoded body、JSON depth/properties/items、list page、SSE event/buffer/connection |
| Registry/Plan | Draft/package/schema bytes、definitions/nodes/edges、branch/map/loop/model round、dependency closure |
| Run/Scheduler | active/waiting Run、descendants、ready rows、inline Value bytes、ValueRef count、claim batch、attempts、lease/heartbeat、deferred poll base/max、wake contracts |
| Model/Context/MCP | request/response/delta、tokens、tool calls、candidates/items/pages、sessions/tasks/subscriptions |
| Capability/Sandbox | input/output/progress、queue、CPU/memory/pids/files/IO/network、wall time、cleanup deadline |
| Artifact | single/total bytes、parts、references/grants、scan expansion/page/object、staging/retention batch |
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
| Artifact | upload/scan/download/GC backlog、bytes/IO |

- autoscaling不能超过DB connection、Provider quota、node/KVM、NATS/S3和tenant hard capacity；
- scale-up前保留control/cancel/cleanup slots；
- scale-down先readiness false/drain，不能中止lease而伪造failure；
- mandatory control/SSE/API/Scheduler/Recovery不scale-to-zero；
- Sandbox warm pool有独立memory ceiling且不挤占running capacity；
- node pressure/eviction/spot只允许经过failure qualification的worker pool；
- 单workclass的HPA不能使用全平台共享queue长度导致连锁扩容；
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
- Provider/MCP/Sandbox/Artifact单pool rollout不触发无关服务rollout；
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
PostgreSQL primary failover and connection reset
NATS total outage, duplicate and reordered hints/events
S3 timeout, partial upload, missing object, digest/KMS failure
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
- 周期执行 Worker kill、NATS outage、backend 429、Sandbox saturation 和 SSE reconnect；
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
- Gate E：Q1容量、fairness、bulkhead和SLO达标；
- Gate F：连续24小时soak达标；
- Gate G：从正式backup恢复并达到RPO/RTO；
- 任何Gate失败使后续证据无效或需要从受影响Gate重跑；
- approval不允许覆盖correctness/security failure。

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
