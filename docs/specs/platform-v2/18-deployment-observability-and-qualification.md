# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-220 Sandbox activation and runner-boundary revision |
| 日期 | 2026-09-04 |
| 依赖 | 00～17 |
| 直接下游 | cross-review、implementation-plan |

> CR-220 impact：Sandbox activation不再把256-bit secret通过OpenSandbox Server proxy发给runner；Dispatcher-only
> Ed25519 seed对exact sandbox/boot/request/input frame签名，runner create config只持有公钥。runner/Package用不同fixed UID，
> state path、process group、signal/session escape和terminal quiescence成为必测负向边界。Dispatcher readiness必须低频运行
> inert create/list/Armed/delete/absence，不得以metadata list单独表示Ready。实现与L1～L3/本机Kind证据完成前，
> 不得关闭对应review P1；正式L4～L6仍为Not run。

> CR-219 impact：Sandbox L1～L3增加真实生产入口的cancel/timeout资格，而非直接构造terminal helper。L1覆盖typed intent摘要、
> second-intent/late-result拒绝和pre-claim零provider；L2 fresh PostgreSQL覆盖显式cancel、database-time deadline scan、四维quota exact-once、
> control/result first-winner及terminal/cleanup原子性；L3真实OpenSandbox覆盖started workload cancel/timeout、Dispatcher kill恢复、provider
> unavailable时业务terminal不回滚且cleanup恢复后取得absence。完成这些门禁仍不等于production-equivalent L4，L4～L6继续Not run。

> CR-219 evidence：`a5aceabb`/`58a84199`已通过受影响L1、fresh schema contract 8 PostgreSQL L2及真实三节点
> Kind/OpenSandbox L3。started cancel在intent写入后SIGABRT，Server缩容至零时由新进程提交Job/Invocation/quota/Event/Outbox终态；
> Server恢复后通过idempotent DELETE 404与absence proof清理。started deadline另进入TimedOut并清理。最终无BatchSandbox/Pod残留，
> Sandbox控制面3/3 Deployment Ready。证据范围只关闭L1～L3，不提升正式L4～L6。

> CR-217 impact：production资格至少执行86,400秒持续soak，manifest起止时间必须覆盖profile声明；每个gate至少有一个
> 不被其他gate复用的专属content digest，artifact list为无别名、无悬空项的exact closure。该结构合同只证明声明闭包，
> 仍必须由受保护CI producer、artifact store和签名/GitOps链证明真实执行，手写JSON不能成为production-ready证据。
> L4 topology绑定live BatchSandbox CRD规范化spec digest，workload门禁关闭所有标记Platform namespace中的Deployment/DaemonSet，
> 采集并核验Sandbox ServiceAccount、Role/Binding、ClusterRole/Binding和ValidatingAdmissionPolicy/Binding，并把完整NetworkPolicy
> inventory纳入摘要且拒绝namespace-wide或unbounded allow规则。callback/cleanup归入既有`mcp_host`，
> dataset pool归入既有`context_worker`。不可变release只能在candidate资格通过、final ReleaseBundle纳入资格evidence并重签后创建；
> 这些修复本身不改变L4～L6 Not run状态。

> CR-216 revision 1 impact：首版 Sandbox deployment/qualification clean-cut 为 Sandbox Dispatcher、OpenSandbox Kubernetes Server、
> BatchSandbox Controller 与 containerd/runc；不修改 OpenSandbox 源码。删除 WASI/gVisor/attestor/Docker-provider target topology；增加
> inert candidate discovery/selection、Armed runner activation、provider/controller restart、Dispatcher reclaim、runner-start uncertainty、
> Direct/Disabled CNI、TTL/delete/orphan cleanup 及零 Platform 业务权限门禁。该目标是 developer preview，L4 强隔离、
> production HA、capacity/soak/restore/promotion均Not run。

> CR-205 impact：P1 management matrix必须逐一覆盖十三个closed noun，并证明四个definition-only noun可publish exact
> Version但不能创建Deployment；Model Provider可创建exact Deployment。fresh full场景不得用SQL fixture替代这些authoring步骤。

> CR-206 impact：Operation正负矩阵增加typed result：Context Dataset成功必须公开与exact generation read一致的`dgen + digest`；
> queued/running/failed不得暴露result，其他Job不得携带generation variant，payload/created Version漂移必须fail closed。

> CR-203 impact：增加public Agent authoring P1 fixture：调用方只持有Draft内容和Artifact，不能预知Version ID；publish后
> Interface/Plan exact IDs不同且同属一个Agent publish batch，fresh Run materialization以Plan v5 contract digest与exact owner
> 双重校验成功。wrong digest、cross-Agent Interface/Plan拼接、旧Plan v4均须在Node/Job写入前fail closed。

> CR-207 impact：产品体验L1～L3增加shared Agent manifest compiler、产品DTO/错误映射、Agent/Run list cursor与CLI/Console
> 北极星旅程。release侧增加四target CLI archive、immutable Console/runtime/guest image、ReleaseBundle/SBOM/provenance/signature和
> single-node starter/feature profile门禁。starter证据始终是development/non-production；真实L4～L6状态不变。

> CR-209 impact：`model_chat.instructions`必须随exact Agent Revision冻结并生成独立`AgentInstruction` user/untrusted block；L1/L3
> 增加缺失/空/NUL/超限、digest漂移、role提升、active-head漂移与prompt/log/Event/Problem泄漏负向。

> CR-210 impact：`deterministic`模板只允许input/output schema canonical digest相同；L1证明不同schema在I/O前失败，生成Plan的
> Return exact RunInput port同时满足Interface input/output与05 terminal invariant。

> CR-211 impact：L1 conformance corpus逐字节锁定Interface contract与`primary_model` requirement的closed v1 preimage及digest，
> 防止CLI/Console、publish恢复或server ID物化产生不同算法。

> CR-212 impact：L1/L2/L3增加authoring name物化与稳定性门禁：manifest/Agent Revision/AgentSummary/RunSummary逐字节一致；
> Draft重命名、display-name替代、Artifact/lock猜测、wrong owner/tenant与Revision漂移全部fail closed，不新增表或tenant-wide name index。

> CR-213 impact：L1/L2/L3增加required feature物化与投影门禁：compiler intent、Agent Draft/Revision与AgentSummary逐字节一致；
> 空/重复/乱序/unknown/超限集合以及从Plan Artifact、Deployment、client lock或Event猜测均fail closed，不新增表、route或projection。

> CR-214 impact：L1/L2/L3增加input classification与default deadline物化门禁：manifest/compiler intent、Agent Draft/Revision及
> CLI/Console Run request逐字节一致；lock丢失、adopt、跨设备读取、classification降级、零/超限deadline及从profile/Deployment/Event
> 猜测全部fail closed，不新增表、route、role或projection。

> CR-215 impact：L1证明authoring-profile DTO closed/bounded/digest-valid且无credential；L2证明PostgreSQL read按tenant/principal隔离并拒绝
> missing/suspended/digest-drift Policy authority且无cache/projection table；L3证明Management role/auth/permission绑定与Console仅消费exact
> response，不写browser persistence或使用bundle默认。

> CR-201 completion scope：本规范的仓库交付包括Kubernetes/GitOps manifests、closed QualificationProfile、candidate/evidence validator、
> topology/workload preflight、CI producer和runbook，以及L1～L3与静态部署负向门禁。项目未执行真实多节点Kubernetes、`runsc`、production
> telemetry、mixed-load/soak、restore或人工promotion；这些项目是部署方声明production-ready前的release gate，不再阻塞00～18进入Verified。
> 不得因为spec已Verified而把任何未执行门禁标记passed，或发布production CapacityProfile/SLO、真实隔离、HA和恢复能力声明。

> CR-200 impact：Artifact Data Worker startup manifest必须登记bounded installed write storage binding digests；tenant v3 policy选择不受支持的
> binding、错encryption domain kind、catalog drift或caller注入storage authority时，必须在object write前fail closed。L1～L4覆盖zero-I/O与rollout。

> CR-199 impact：Artifact Data Worker candidate/startup manifest登记bounded supported scanner contract digests；TenantConfig指向的`ArtifactIo` v2
> scanner digest不受支持时readiness/claim/stage fail closed。L1～L4增加v1/缺字段/超限、policy drift、unsupported scanner与rollout canary矩阵。

> CR-198 impact：MCP discovery使用`mcp_host` ComponentRole下独立workload pool，拥有自己的ServiceAccount、restricted PostgreSQL pool、
> Egress/Artifact Data Worker mTLS clients、claim/permit/queue metrics与bounded drain；不复用RPC-only Tool Host或subscription Resource Host的
> pool。Artifact验证仍由既有Data Worker role/pool执行。资格增加stage前后、verify前后、wake后/final owner commit前后的kill/reclaim窗口。

> CR-197 impact：qualification增加public traceparent正负、Gateway→Scheduler/Worker→MCP/Egress/Sandbox/Artifact跨进程同trace/new-span、
> kill/reclaim continuity、Event/problem correlation和第三方零trace-header计数。`tracestate`/`baggage`、payload/identity canary必须在动态采集结果中
> 为零；静态source扫描不能替代该门禁。

> CR-181/203 impact：资格矩阵增加Plan v5 publication identity、external leaf dispatch、candidate selection、result binding与crash recovery；静态manifest或
> repository单元fixture不能替代多进程owner-boundary证据。

> CR-185 impact：L1/L2增加Skill frame canonicalization、截断/溢出/trailing bytes、path/digest/length mismatch与错误media拒绝；
> L3覆盖Scheduler exact slot/deployment/revision/lease经Artifact Data Worker mTLS materialization且无storage credential泄漏。

> CR-188 impact：Capability Worker镜像/startup evidence必须枚举bounded exact installed codec manifest；L1～L4增加错codec identity、module、
> descriptor、Worker manifest、空registry与rollout drift负向fixture，并证明全部在Egress/MCP I/O前fail closed。

> CR-189 impact：Context Worker镜像/startup evidence必须枚举bounded exact adapter manifest；RemoteSearch L1～L4增加错endpoint/digest、
> Network/TLS/Trust Policy kind/digest、Worker manifest、空registry与rollout drift负向fixture，并分别证明claim前零lease/quota mutation及
> dispatch前零Egress调用。

> CR-195 impact：MCP Streamable HTTP L1校验installed trust bundle的PEM/size/config digest及exact Policy匹配；L3以独立CA/SAN和真实TLS
> socket证明只接受显式bundle、默认trust store与错bundle均在HTTP业务bytes前失败；L4验证rollout中bundle/config drift使readiness关闭。

> CR-196 impact：OAuth verification/startup binding增加exact Trust Policy与bounded token-endpoint PEM roots。L1覆盖PEM parse/size、Auth/Trust/
> endpoint/config digest漂移；L3以独立CA/SAN真实token endpoint证明default-root与错bundle在authorization code bytes发送前失败；L4覆盖rollout drift。

## 1. 决策摘要

发布、promotion与rollback是GitOps/CI/CD与Kubernetes部署事实，不是平台业务aggregate。运行时数据库
不保存`InstallationReleaseState`、`Candidate`、`GateResult`、`ReleaseManifest`或安装级compatibility generation。

CI产生不可变的build/provenance/SBOM/test/qualification artifacts，GitOps存储环境期望image/config/schema/profile digest，
Kubernetes执行rollout/rollback。应用启动时对照同一typed startup manifest，漂移时readiness fail closed。

首版 Sandbox 部署 Sandbox Dispatcher、internal OpenSandbox Server、BatchSandbox Controller 与 sandbox Pod；OpenSandbox 显式使用
Kubernetes provider 和 containerd/runc。Artifact 只部署 Gateway、Data Worker 和 Maintenance 三个 role。
MCP只是remote Streamable HTTP。Model只支持Inline request/response。

## 2. 环境与发布单元

至少有development、staging和production三类环境，分别使用独立namespace/account、PostgreSQL、NATS、Artifact store、
KMS/Secret scope和workload identity trust domain。禁止production credential出现在开发/测试环境。

发布单元是Git commit + image digest + Helm/Kustomize manifest digest + baseline schema version + HardLimit/Capacity profile digest
的闭包。任一镜像使用mutable tag、未签名artifact或不可重现build都不得进入production。

Draft规范和未通过资格的代码不是current behavior。CI报告只表示它实际运行过的门禁，不通过数据库
写入成功记录来宣称功能完成。

## 3. 物理组件与隔舱

| 组件/role | 必须独立的资源 |
|---|---|
| Management API | Deployment、ServiceAccount、DB role/pool、rate limit |
| Runtime API | Deployment、ServiceAccount、DB role/pool、SSE budget |
| Registry Validation Worker | queue、独立DB pool、ServiceAccount、tenant-scoped validator identity、permit |
| Scheduler/Recovery | Deployment、critical-control pool、lease/scan budget |
| Model Worker | queue、DB pool、provider client、permit/rate limit |
| Capability Worker | Native/Remote role、DB pool、permit |
| Context Worker | queue、DB pool、index/client permit |
| MCP Host | queue、DB pool、Egress client、session/subscription budget |
| Sandbox Dispatcher | Sandbox Job queue、restricted DB pool、OpenSandbox client、permit与cleanup reconcile |
| OpenSandbox Server | internal lifecycle API、Kubernetes provider client、API/candidate/resource budget |
| OpenSandbox Controller | BatchSandbox/Pod reconciliation、TTL/delete、独立 ServiceAccount/RBAC/leader election |
| Artifact Gateway | public ingress、DB/storage pool、stream budget |
| Artifact Data Worker | internal identity、DB/storage pool、stage/read/verify budget |
| Artifact Maintenance | queue、DB/storage pool、scan/delete/GC budget |
| Egress/Secret Broker | workload identity、network、provider client、secret-resolution budget |

可以在同一Rust workspace编译多个binary，但上表声明的物理隔舱不得因代码复用而合并运行时权限。
一个role饱和不得使其他role的readiness失败或占用critical-control reserve。

`registry_validation_worker`是candidate manifest、CapacityProfile、startup manifest、Helm/GitOps与workload
preflight共同承认的closed `ComponentRole`。其image可以与其他trusted Rust role共享同一runtime image digest，但必须使用独立
Deployment、ServiceAccount、NetworkPolicy、DB pool和WorkerManifest（唯一`WorkClass=RegistryValidation`）。L1覆盖
payload/validator/profile/draft/dependency闭包与summary canonicalization；L2覆盖tenant permission、CAS、stale fence、
Job/Resource/Event/Outbox/Receipt原子回滚与replay；L3覆盖独立进程claim/start/kill/reclaim及Draft update race。L4～L6的
mTLS/RBAC/NetworkPolicy、lane saturation、rollout/restore与GitOps evidence仍为未执行的environment gate，不能标作passed。

Scheduler到Artifact Data Worker的Typed Plan listener必须有独立mTLS route与NetworkPolicy，只允许Scheduler ServiceAccount/workload
URI；Sandbox Controller与Model Worker identity不能调用该service。Scheduler表达式求值使用自己的有界CPU/memory/permit和exact
RunValue读取budget，不获得Provider、MCP、Context、Secret或Sandbox egress。表达式饱和不得占用critical-control连接reserve。

## 4. 部署与容器安全基线

所有Platform workload必须：

- 固定image digest、runAsNonRoot、readOnlyRootFilesystem、drop capabilities和seccomp profile；
- 显式CPU/memory/ephemeral-storage request和limit；
- 默认deny ingress/egress NetworkPolicy，只开放exact service flow；
- 独立ServiceAccount和least-privilege RBAC，默认不automount Kubernetes token；
- topology spread、PodDisruptionBudget、graceful drain和bounded startup/readiness/liveness probe；
- 从同一startup manifest对照component role、region、image、protocol、profile和policy digest。

任何 role 都不得挂载 Docker/containerd/CRI socket；Dispatcher、OpenSandbox、Controller、sandbox Pod、Gateway、Scheduler、Model、MCP
与普通 Worker 必须由 static manifest 和 live preflight 证明。sandbox Pod 显式使用 containerd/runc，禁止 privileged、host PID/IPC/network/
path、device、runtime socket、service-account token、Platform/Kubernetes credential 与 capability 追加；固定 non-root、read-only root、
`allowPrivilegeEscalation=false`、qualified seccomp、capability drop、pids/CPU/memory/ephemeral-storage/deadline limits。

OpenSandbox API 只允许 Dispatcher source/audience 且不公开 ingress；runner fixed port 只允许 Dispatcher。physical store 是 Kubernetes API/
BatchSandbox CR，不使用 OpenSandbox SQLite 或 Platform 业务表。Server `informer_enabled=false`，developer Profile 的 Server/Controller
均单副本，Controller 开 leader election；这些 minimum 不得生成 gVisor/microVM 等价、HA 或强多租户证据。

## 5. Network 与依赖拓扑

```text
Client -> Gateway -> Management/Runtime API
API/Workers -> PostgreSQL
Components -> NATS (wake/outbox delivery only)
Artifact roles -> S3/KMS
Workers/Hosts -> Egress Broker -> catalog-approved external endpoints
Sandbox Dispatcher -> OpenSandbox Server -> Kubernetes API -> BatchSandbox Controller -> Pod/containerd-runc
Sandbox Dispatcher -> private fixed runner protocol
Sandbox workload (Direct profile only) -> DNS/external network; internal/metadata CIDR denied
```

PostgreSQL、NATS、S3/KMS、Secret Manager和Egress的凭据按role分离。外部Provider/MCP失败不使整个API readiness
失败；PostgreSQL和该component必需的startup contract失效时readiness fail closed。

## 6. PostgreSQL 与migration

PostgreSQL是业务current state、CAS、lease、Receipt、Event和Outbox权威。连接池按role和business/critical-control分离，
使用statement/lock/idle-in-transaction timeout、有界batch和covering indexes。

clean-cut的未发布migration 1～35候选集替换为一个经review的minimal baseline migration。目标schema contract为v7，
保持23张总表/22张业务表，不增加Installation Release或ManagementOperation表。该数字是目标设计预算，
不是功能完成证据。

基线发布后migration immutable、forward-only，每次只表达真实物理schema change。发布pipeline先执行兼容的
schema step，再rollout应用；不在规范中用migration checksum、object/trigger count作为行为门禁。

## 7. NATS、Artifact store 与Secret Manager

NATS只传输committed outbox message或wake hint。consumer ack不是业务commit，重复/丢失消息通过Receipt和safety scan
安全处理。stream/subject/consumer有retention、max bytes/age/delivery和dead-letter上限。

Artifact store使用versioned object/generation、encryption、lifecycle与access logs；数据库中的exact Blob identity才是reference authority。
首版storage/KMS binding是deployment-time manifest，不提供运行时自助切换API。

Secret Manager是Secret value authority。数据库只保存reference identity/digest/evidence，应用通过workload identity与Egress/Secret
Broker在最后一跳解析。Secret不进入manifest、Git、DB、log、Event或qualification artifact。

## 8. Configuration 与capacity profile

HardLimitProfile拥有不可被tenant放大的安全上限，CapacityProfile拥有某一环境的副本、pool、permit、
queue、lease/heartbeat、scan batch、HPA和SLO target。两者是versioned typed files，受schema、canonical digest、code review和GitOps管理。

首个包含closed expression evaluator的profile版本为`HardLimitProfile v5`。其`registry_plan`必须增加以下closed字段，不能借用
`plan_nodes`、`plan_edges`或`branch_legs`代替：

| 字段 | unit | hard max | Q1 default | overflow |
|---|---:|---:|---:|---|
| `expression_instructions` | `count` | 4096 | 2048 | `content_rejected` |
| `expression_input_ports` | `count` | 64 | 32 | `content_rejected` |
| `expression_stack_depth` | `depth` | 256 | 128 | `content_rejected` |

代码内绝对上限与profile `hard_max`必须一致，runtime使用`q1_default`或经deployment选择的不更大值。旧version、缺字段、错误unit、
零值、`q1_default > hard_max`或任何值超过代码绝对上限都使startup/publication fail closed；tenant policy不得放大这些值。

配置先在CI做cross-field validation，启动时再同样fail closed。重要规则至少包含：

- heartbeat interval < lease / 3；
- deadline/attempt/retry/queue/stream/body均有hard max；
- business与critical-control pool不能别名；
- Sandbox Dispatcher/OpenSandbox、Model、MCP、Artifact各role不能共用被禁的DB/storage/semaphore；
- OpenSandbox Kubernetes是唯一Sandbox provider，containerd/runc是显式runtime且无Docker/WASI/gVisor/host fallback；
- candidate count/quiescence/time均有hard max，`ActivationAuthorized/PotentiallyStarted`后禁止replacement；
- controller TTL >= sandbox absence/reconcile window，Kubernetes API/BatchSandbox physical store必须可恢复；
- Model input/output hard limit不超过Inline RunValue上限。

规范不把未资格容量数字声明为current behavior。具体profile只在其CI/load/soak证据通过后才可用于production。

## 9. Autoscaling、availability 与rollout

HPA/KEDA按role使用ready count、oldest age、in-flight、permit utilization、dependency latency和CPU/memory组合信号。
Sandbox按weighted resource units，Model按request/token throughput，Artifact按stage/read/verify/delete backlog，不只看CPU。

滚动发布要求：

1. 新Pod验证startup manifest并readiness通过；
2. 旧Pod进入Draining，停止新claim；
3. 已有lease在grace内terminal/handoff，超时停止heartbeat而不伪造failure；
4. safety scan从PostgreSQL恢复失去的work；
5. SLI/error budget达标后继续promotion。

不兼容的Worker/protocol必须保留可处理旧binding的pool直到已存Job清空，或在clean cut前证明没有已存工作。

## 10. Observability

所有组件使用OpenTelemetry-compatible trace/metric/log，并传播trace ID与tenant-safe business IDs。标准维度只包含
component role、environment、region、work class、operation、state/outcome、stable problem/failure code和low-cardinality backend kind。

禁止进入metric label/log/Event/trace attribute的数据包含Secret/token、prompt/response/tool arguments、code/file body、URL query、
object key、tenant/user/resource高基数ID。受限debug trace必须经RBAC、脱敏、审计和retention。

最低SLI类别：

- API availability/latency/error rate；
- Run admission、scheduler drive、Job queue age/throughput/lease loss；
- Model/Capability/Context/MCP/Sandbox/Artifact各role的latency/outcome/saturation；
- PostgreSQL/NATS/S3/KMS/Secret/Egress dependency health；
- recovery lag、outbox lag、orphan/delete backlog和critical-control availability。

具体SLO只由已通过qualification的CapacityProfile指定。alert要求symptom-first、可操作、带runbook，不以单个
外部tenant/provider失败触发全平台page。

## 11. Backup 与drill

PostgreSQL使用加密backup + PITR，Artifact store使用versioning/lifecycle/replication（如profile要求），KMS/Secret的恢复由
各自authority负责。NATS可从Outbox/Event/Job重建，不作为唯一backup对象。

restore drill必须验证PostgreSQL与Artifact point-in-time一致性、无效lease/recovery fence、outbox重放、Secret reference可解析、
orphan reconciliation和新环境workload identity。RPO/RTO由CapacityProfile声明并用定期drill验证。

## 12. 资格层级

证据只在它证明的层级有效：

| 层级 | 目的 | 典型证据 |
|---|---|---|
| L1 Domain | 状态、schema、policy、determinism | unit/property/model tests |
| L2 Repository | CAS、lease、transaction、tenant、migration | real PostgreSQL integration |
| L3 Component | adapter/protocol/runtime、crash/restart | fake/real dependency + process tests |
| L4 Topology | mTLS/RBAC/NetworkPolicy、role隔离 | production-equivalent cluster tests |
| L5 Capacity | load、saturation、soak、SLO/error budget | approved CapacityProfile run |
| L6 Release | supply chain、upgrade/rollback、restore | signed CI artifacts + GitOps evidence |

一个test可以产生多个观测，但不在每个文档/层级复制为独立proof。共享原始观测可以被多个gate消费；每个gate还必须至少
生成一个只绑定该gate的专属summary artifact digest，防止一个任意文件被声明为全部L1～L6证据。artifact links按name规范排序，
content digest不得别名，且每一项都必须被result引用。fixture manifest只记录输入profile、code/image/schema digest、seed、topology、
start/end time、tool version、result和artifact links，由CI artifact store保留，不写入运行时GateResult表。manifest validator只验证
结构、digest与时间闭包；受保护producer身份、真实执行日志、签名和GitOps rollout history仍由CI/release authority验证。

production profile的`sustained_soak`固定要求至少86,400秒；Evidence manifest的`completed_at - started_at`不得小于
`minimum_soak_seconds`。缩短profile数值或只修改时间戳均不能替代由受保护producer保存的连续运行证据。

CR-201将“spec qualification”和“environment qualification”分开：L1～L3、机器合同、部署静态闭包及candidate producer属于仓库spec证据；
需要目标集群或外部服务的L4～L6属于environment release evidence。后者未执行不阻塞spec关闭，但任何production发布声明仍必须明确列出
未运行门禁，并由部署方决定是否在自己的目标环境执行。

## 13. 必须资格矩阵

CR-216 Sandbox target矩阵：

- L1：closed provider/runtime/profile/request/provisioning token/candidate/activation/runner/result schema 与 canonical digest；unknown provider、
  mutable image tag、wrong lifecycle/CRD/controller/template/runner/CNI digest、shell entrypoint、oversized input/result/diagnostic 与非法 config fail closed；
- L2：real PostgreSQL 覆盖 concurrent claim、provisioning intent、candidate selection CAS、activation authorization、lease rollover、stale result、
  terminal first-winner、cancel/timeout、quota settlement与orphan decision；OpenSandbox evidence不能直接推进Run/Invocation；
- L3：真实 OpenSandbox Server + Kubernetes provider + BatchSandbox Controller + containerd/runc 覆盖 concurrent create、response loss、
  Server/Controller restart、Dispatcher 在 create/select/activate/result/commit 窗口强杀、activation replay/conflict、boot rollover/runner-start
  uncertainty 不重发、TTL/delete/absence 与 orphan cleanup；
- L3 network/security：Direct 只可达 DNS/external 且 internal/metadata deny，Disabled 零 egress；二者均无 public ingress、host network/path/
  socket/device/Platform credential，wrong Dispatcher API key/source 零 create，OpenSandbox/Controller/runner 无 Platform DB/NATS/业务 RPC；
- L4～L6：强隔离、OpenSandbox HA、production capacity/chaos/soak/restore/promotion 全部 Not run。不得用 Docker provider/本机流程、静态 manifest 或
  既有WASI/gVisor历史fixture冒充通过。

L4 rollout inventory必须以带`insight.platform/workload-namespace`标签的Namespace为边界：其中每个Deployment/DaemonSet都必须映射
closed ComponentRole并进入Candidate/Capacity、安全、PDB/HPA与identity校验。Sandbox ServiceAccount及其Role/ClusterRole binding必须
与reviewed最小权限逐项相等，三项fail-closed ValidatingAdmissionPolicy必须各有`Deny` binding。BatchSandbox CRD证据必须从live完整spec规范化计算digest，
不得回填source文件常量；NetworkPolicy inventory完整进入content-addressed summary，除唯一双向default-deny外不得出现选择全namespace
或缺少bounded peer/port的allow规则。`mcp-callback-api`与`mcp-cleanup-worker`是`mcp_host`的隔离pool，Context dataset pool是
`context_worker`，不得为物理pool另造ComponentRole。

下文既有跨领域矩阵继续适用；其中任何WASI、gVisor、runsc、Launcher、attestor或旧Sandbox Controller/Executor记录只说明
CR-216之前的历史实现证据，不再是目标Sandbox验收项，也不能抵扣上述OpenSandbox门禁。

CR-207产品体验矩阵：

- L1：YAML 1.2 JSON-compatible closed subset拒绝duplicate/merge/anchor/alias/tag、implicit timestamp、NaN/Infinity、非UTF-8、
  path/symlink escape、Secret/URL/shell字段；`deterministic | model_chat` positive/negative corpus在Rust CLI与Console compiler逐字节得到
  相同manifest digest、Typed Plan v5、schema digest、required feature与ordered lifecycle plan；产品text/json/DOM投影的closed字段和
  Problem→UserProblem映射覆盖unknown fallback且零authority/credential泄漏。
- L1 model assembly：`model_chat.instructions`在Agent Revision与request source map中保持exact digest，顺序固定在Plan node instruction
  后、required Skill前，role=`user`且`trusted_instruction=false`；deterministic为null，空/NUL/超限、来源漂移或合并进platform block失败。
- L1 deterministic：相同schema生成`start -> return`且Return消费相同digest的RunInput；不同input/output schema在网络、Artifact与lock
  I/O前返回`agent_compile_failed`，不得插入coercion或生成运行时必失败的Plan。
- L1 digest preimage：Rust/TypeScript对Interface contract和model requirement使用相同closed v1字段与JCS/SHA-256；unknown/missing
  字段、Unicode/case别名、Artifact/Version/Deployment ID混入或物化前后重算漂移均失败。
- L2：fresh PostgreSQL覆盖Agent/Run list上限、filter、stable keyset、snapshot、cursor replay/tamper/expiry/wrong purpose/principal/tenant，
  并发publish/activate/update/archive与Run/Task terminal下不重复、不越权、不从Event重建；Agent publication所有HTTP边界前后kill/retry通过
  Receipt/ETag/journal恢复且不重复Version/Deployment/Run effect。
- L3：预构建CLI和静态Console只经public Gateway完成空tenant `create/import -> validate -> publish -> activate -> Run -> SSE/result`；
  Gateway/CLI/browser在四个publish阶段、SSE断线与terminal result前后kill/reload仍从server authority恢复。Console覆盖两个会话CAS冲突、
  keyboard/mobile/high-contrast/accessible status与浏览器storage/DOM canary；CLI覆盖Ctrl-C resume、0600 journal损坏/权限过宽fail closed。
- Distribution：四个CLI target运行`version/doctor`；archive、runtime/guest/Console OCI、checksum、SBOM、provenance与canonical ReleaseBundle
  绑定同一commit/schema/profile/image digest并可离线重验。签名超时、partial push、wrong architecture、mutable tag与cache poisoning保持
  未发布。PR复用一次candidate，不为每个journey重复release build/sign。
- Development profile：`starter`、每个single feature与`all`有closed排序closure、identity/config/readiness/disable负向fixture；unknown、
  duplicate、dependency conflict在任何pull/provision/start前失败。fresh/warm/idle报告记录source compilation=0、download/build/start阶段、
  process/image/volume资源，并明确single-node、non-production、L4～L6 Not run。stop/start和Gateway/Worker/dependency重启不丢Run或重复effect。

上述L1～L3与distribution producer属于仓库spec证据；4 vCPU/8 GiB机器上的cold/warm/idle预算只有实际运行才可标passed。
真实registry publish、跨架构runner、production OpenSandbox topology、capacity/soak/restore与人工promotion仍分别属于适用L4～L6 environment gate。

CR-181新增：L1验证Plan v1/v2/v3、wrong slot/port/schema/budget/route；L2 fresh PostgreSQL验证伪造selection evidence、集合外candidate、
caller-supplied child entry/Task/Wake/result/resume、并发terminal first-winner；L3以独立Scheduler和Capability/Context/Model/Artifact Data/
critical-control进程贯通Run→各leaf→resume→Return，并在dispatch提交前后及leaf terminal前后kill进程，证明不重复创建owner/child/Task/
Invocation/RunValue/resume Node/Job，且external leaf terminal后不会重新claim/dispatch已终结leaf Node。L4～L6再覆盖NATS全丢、lane饱和、
DB pool隔舱、安全identity、chaos/soak/restore与GitOps rollout。

CR-182 L1覆盖三个mode、canonical candidate ordering、route canonical hash/modulo、unknown mode/version与empty document；L2覆盖
Scheduler/repository重算一致、route/candidate/policy漂移和整批回滚；L3覆盖多Scheduler进程对相同inputs始终选择同一Deployment。

CR-188 L1覆盖installed codec manifest closed schema、排序/数量、descriptor重算及unknown backend；L2覆盖remote Deployment与Invocation
冻结codec/Worker manifest及claim first-winner；L3覆盖真实Capability Worker静态registry、heartbeat/kill/recovery并证明空registry、错
codec/module/descriptor/Worker manifest在Egress/MCP调用计数仍为零时fail closed；L4覆盖镜像rollout manifest drift使readiness/claim关闭。

CR-192 L1覆盖subscription refresh request/outcome closed schema、digest/count/byte/deadline bounds、ReadOnly retry mapping以及
Observation/cache/dataset零创建；L2 fresh PostgreSQL覆盖exact `Context -> McpOperation` claim、quota reservation、JobCommit first-winner、
terminal Event/Outbox、stale fence零写入、retry与expired-lease recovery。L3必须使用独立Context Worker、MCP Host和Egress进程及真实
Streamable HTTP fake server，在claim后、Host dispatch前后、response后/terminal commit前分别kill，证明wrong workload/owner/closure零外部I/O、
ReadOnly uncertain产生新attempt、唯一terminal evidence、无Context Observation/cache row、Context/MCP permit与DB pool相互隔离。L4再以mTLS、
RBAC、NetworkPolicy和Context/MCP各自饱和验证只有允许的Context→Host→Egress路径，且任一lane饱和不使另一lane或API readiness失败。

CR-194 L1增加`resources/list` closed registry、Resources capability与ReadOnly effect、独立per-method limits及unknown/missing limit拒绝；L3的
full reconcile fake server必须观察一次有界list和对允许集合的有界read，任一步骤响应后强杀均只允许同一ReadOnly Job的新attempt重读，且Host、
Job/Event/Receipt与日志均不保存remote body或自由URI。

CR-198 L1覆盖discovery admission预分配闭包、canonical descriptor limits、same-generation stage幂等、wrong digest/fence拒绝及Verified-only
Data Worker；L2 fresh PostgreSQL覆盖MCP Job与`ArtifactScan` Job的typed关联、stage wake、verify wake、最终`Verified -> Ready` + Evidence Link +
Discovery Snapshot + 双Job/quota结算的单事务first-winner。L3使用独立Discovery Worker、Egress Broker、Artifact Data Worker和真实Streamable HTTP/
object storage fixture，在远端response前后、stage commit前后、verify前后、owner wake后/final commit前分别kill/reclaim；证明remote副作用只按
ReadOnly重试合同发生、stage不重复创建candidate、Data Worker不直推Ready、message全丢仍由DB恢复、stale fence零写入且最终只有一个Snapshot/Link。
L4验证discovery pool的ServiceAccount、DB/Egress/Artifact mTLS、NetworkPolicy、PDB/HPA与CapacityProfile聚合；任一discovery或Artifact verify lane
饱和不得消耗Tool Host、Resource Host、Context、Capability、Model或Sandbox保留容量。

L4 rollout preflight必须从待资格cluster读取live Deployment/DaemonSet、NetworkPolicy、PDB与HPA inventory，并对照同一production
CandidateManifest和CapacityProfile fail closed验证：closed ComponentRole closure、exact digest image、controller observed generation、全部
desired replica Ready、replica/autoscaling bounds、per-role ServiceAccount isolation、token automount关闭、restricted pod/container security、
CPU/memory/ephemeral-storage request/limit以及每个承载namespace的双向default-deny。静态Helm render或该validator自身的fixture只证明门禁
行为，不能替代production-equivalent cluster上的实际L4 evidence。

ComponentRole是candidate image与capacity聚合维度，不强制一个role只能有一个Kubernetes workload。Native/Remote等多pool必须逐个拥有独立
ServiceAccount、PDB/autoscaler和完整安全closure；同role所有container匹配同一candidate image digest，DaemonSet固定副本与Deployment HPA
边界聚合后精确等于该role CapacityProfile，避免漏标pool或以拆Deployment方式放大capacity。

当前分层证据：fresh PostgreSQL 16 r208已通过Native exact startup registry/Worker manifest双进程强杀、expired-lease owner recovery、
quota settlement与non-idempotent reconciliation，关闭Native部分L3；r217以真实Remote Worker+mTLS Egress RPC分别通过HTTP/gRPC
错manifest零claim/零外部调用、响应后commit-window强杀、第二进程expired-lease恢复及非幂等调用不重放，关闭Remote HTTP/gRPC L3。
MCP Host production binary已通过Capability Worker→Host→Egress双mTLS、Egress到达后强杀、`CompletionUnknown`及重启安全重放的进程
fixture；fresh PostgreSQL 16 r221进一步通过production Remote Worker→Host→Egress exact protocol/auth/discovery binding、错codec零调用、
commit-window强杀、expired-lease恢复与非幂等不重放，关闭MCP ToolsCall process L3。OAuth/subscription真实协议、隔舱容量与L4 rollout
仍未通过。fresh PostgreSQL 16 r233进一步以production Model Worker、mTLS Egress/NATS完成错manifest零Provider调用、Provider响应后
commit-window强杀、第二进程expired-lease恢复、冻结ceiling保守结算、安全重放与structured Inline terminal commit，关闭Model provider
process L3；Model tool-result整链与Context external leaf L3仍未通过。

Context recovery前置证据已在fresh PostgreSQL 16 r234通过：Deferred同attempt恢复后模拟Worker lease丢失，bounded owner scanner重验
fence/payload/reservation，原子结算旧quota与Event/Outbox并创建下一物理attempt，最终Observation唯一。该证据仅关闭L2 durable recovery，
尚不替代真实backend protocol及kill/restart的L3门禁。后续批次已增加独立`platform-context-worker`、digest-bound NativeCatalog静态
adapter和独立Helm role；该role仅允许DNS/PostgreSQL出站，不含Egress、Secret、NATS或Sandbox凭据，并以claim前exact adapter digest
扫描避免配置漂移产生lease/quota mutation。fresh PostgreSQL 16 r240又以真实双进程通过错digest零claim、terminal commit窗口强杀、
第二进程expired-lease恢复、attempt 2及唯一Observation，关闭NativeCatalog process L3；remote Context backend protocol、隔舱容量和L4
rollout仍待通过。fresh PostgreSQL 16 r241进一步以独立Remote Context Worker、真实mTLS Egress RPC及Egress侧受控协议connector通过
错Worker manifest零claim/零远端调用、响应后commit-window强杀、expired-lease恢复、attempt 2安全重放及唯一Observation；远端调用总数
严格为2；同一terminal owner transaction使原leaf唯一`succeeded`、Run恢复`running`并只激活一个ready Return resume Node/Job。
该证据关闭Remote Worker→Egress RPC和Context external-leaf terminal/resume component L3，但不替代production HTTPS last-hop、resume后
Return的进程执行、隔舱容量或L4 rollout证据。随后r242以production RemoteSearch connector、真实TLS socket、独立CA/SAN、固定DNS pin、显式PEM trust及
真实HTTP bytes关闭HTTPS wire/protocol L3；test-only loopback许可不进入production build，生产SSRF public-destination guard保持不变。
fresh PostgreSQL 16 r243随后以真实`platform-orchestration-worker`和mTLS Artifact Scheduler RPC读取exact typed Plan，把恢复后的唯一Return
Node/Job执行到Run终态；同一fixture重验错manifest零claim、commit-window kill、attempt 2、严格两次远端调用、唯一Observation/Event、
Artifact读取及最终Run output。恢复事务和terminal owner事务分别释放旧/current active-work permit，避免重试泄漏阻止terminal closure。
至此Run→Remote Context→resume→Return component L3关闭；Model tool-result整链、隔舱容量与L4 rollout仍需独立证据。

fresh PostgreSQL 16 r244进一步以真实Orchestration/Model/Native Capability三个production Worker、mTLS Artifact Scheduler、mTLS Egress与
TLS NATS跑通Run→Model tool intent→CapabilityInvocation→tool result→第二轮ModelTurn→Return。Provider严格调用2次，Model Job/Invocation
各2个、Capability Job/Invocation各1个、唯一Return Node成功、无非terminal Job，Run output精确指向独立`model_structured_output`
RunValue；完整canonical response保留为另一Inline RunValue而不冒充Agent output。该证据关闭Model tool-result production component L3；
隔舱容量、L4 rollout及其余L4～L6仍是environment production-ready声明的release blocker，不阻塞CR-201 spec关闭。

r245新增上述live workload inventory preflight并接入production qualification入口；5个正负fixture覆盖完整15-role closure以及缺role、
mutable/wrong image、rollout/replica、ServiceAccount、default-deny、restricted security/resource和HPA drift，连同既有4个真实node shape/
RuntimeClass topology fixture全部通过。该本地证据没有运行production-equivalent cluster，因此只证明L4 gate会fail closed，不能将L4标为
通过；当前部署只有在真实inventory满足同一CandidateManifest/CapacityProfile后才能生成workload digest evidence。

r247修正一个role只允许一个workload的过度实现约束，增加同role多隔离pool的聚合副本/HPA闭包和独立ServiceAccount检查；新增双Context
pool正向fixture后workload矩阵为6项。该修正不降低closed 15-role、exact image或任一安全/rollout负向门禁。

r248完成checked-in Helm的15个ComponentRole/17个隔离pool静态闭包：Context与Egress role各有两个独立pool，其余各一个；全部主workload
使用exact role label、digest image、独立ServiceAccount/PDB，Deployment有HPA，container闭合CPU/memory/ephemeral-storage request/limit，
每个namespace保持exact default-deny。跨11个chart的全局render检查和各受影响role原有静态检查通过。该证据只证明待部署manifest闭包，
live rollout、identity、mTLS/RBAC/NetworkPolicy enforcement仍须r245 preflight在production-equivalent cluster实际通过。

fresh PostgreSQL 16 r249通过OAuth start/Receipt first-winner与secret-free persistence、PKCE cleanup outbox reclaim/fence，以及subscription
discovery/create/session/notification coalescing/refresh/reconcile/termination/recovery的完整L2事务套件。该证据只关闭durable PostgreSQL owner层；
production Callback API、Cleanup Worker、MCP Host、Egress与真实OAuth/SSE endpoint的多进程L3及kill/restart仍须独立证据。

r250把同一CandidateManifest `deployment_config_digest`冻结到全部17个主workload的PodTemplate注解。全局render检查要求摘要格式合法且跨pool
唯一，live inventory preflight逐pool要求它与输入CandidateManifest完全相等，并将摘要写入canonical workload evidence；配置摘要漂移负向fixture
通过。该证据关闭静态manifest及preflight实现缺口，不代表production-equivalent cluster已通过L4，也不替代进程内typed config启动自检。

r251为Scheduler/Recovery role增加独立HTTP observability listener；startup authority全部成功后才把bounded process metric置Ready，runtime或
listener提前退出使进程失败。Helm readiness/liveness改为HTTP，并以内部Service、ServiceMonitor及exact Prometheus NetworkPolicy ingress
暴露低基数指标。该证据只覆盖此role的L1/L3 process wiring；其余role、dashboard/alert/trace以及真实scrape仍未闭合。

r252将worker health router提升为shared observability owner并接入Model Worker；closed operation label、默认NotReady、no-store响应和
Prometheus content type由共享测试覆盖。Model的startup authority、Egress/NATS与全部driver组合成功后才Ready，任一组件提前退出会使进程
整体失败；对应ServiceMonitor和Prometheus-only ingress已进入Helm。该证据不把单个外部Provider健康纳入全进程readiness，也不替代L4。

r253把shared process observability接入Capability Native/Remote；readiness分别位于exact native registry或remote codec/Egress/MCP Host
startup closure之后，worker与HTTP listener共同fail closed。两个隔离chart均增加内部Service、HTTP probes、ServiceMonitor和精确
Prometheus ingress，同时保留各自原有出站权限。该证据不包含真实scrape、业务queue/permit指标或production topology enforcement。

r254为Context Native/Remote接入同一shared process observability；Native与Remote分别在exact adapter/PostgreSQL及额外Egress mTLS
startup closure完成后Ready，driver/listener共同fail closed。两个chart使用独立Service、ServiceMonitor和Prometheus-only ingress；
该证据不包含Dataset/query SLI或production scrape。

r255为MCP Host增加独立observability listener，readiness位于Egress mTLS、Host transport、caller identity interceptor和TLS RPC server
组合之后。业务gRPC与metrics使用不同端口及不同NetworkPolicy source，任一server提前退出使进程失败。该证据不等于OAuth/subscription
production L3，也不包含真实Prometheus scrape。

r256为Security Authority与Egress Broker接入shared process observability。Authority的readiness位于restricted PostgreSQL/schema、exact
authority及TLS RPC组合之后；Egress的readiness还要求Authority mTLS、secret-provider catalog、MCP state/codecs与全部closed connector完成组合。
两个namespace分别提供与业务gRPC不同的metrics端口、HTTP probes、ServiceMonitor和Prometheus-only ingress，且不扩大Egress caller或Authority
PostgreSQL权限。任一server提前退出都会使所属进程fail closed；该证据不包含真实provider调用、Prometheus scrape或production L4 enforcement。

r257为Sandbox Controller接入shared process observability。readiness位于restricted PostgreSQL/schema、Artifact Broker mTLS、routed
process-attestor authority、executor identity interceptor及TLS RPC service组合之后；RPC与独立HTTP listener共同fail closed。Controller
Service/NetworkPolicy只向精确Prometheus identity开放metrics端口。该证据不包含WASI/gVisor Executor或attestor接线，也不替代真实runsc、
production scrape和L4 enforcement。

r258为WASI Executor与gVisor Launcher接入shared process observability。两种backend分别在exact WorkerManifest/backend、node-local registration、
Controller mTLS、backend registry、NATS control与HTTP listener组合后Ready；driver、control或listener提前退出都会取消并bounded drain其余任务。
两个pool的metrics Service与Prometheus ingress不增加WASI host authority或gVisor Kubernetes API权限。该证据不包含node/POD-local attestor
observability、真实runsc、production scrape或L4 enforcement。

r259为node-local与gVisor Pod-local两种process attestor接入shared process observability。readiness位于persistent generation registry、
procfs/node identity observer、UDS+mTLS registration、Controller mTLS proof service及HTTP listener组合之后；任一server提前退出会取消并
bounded drain其余server。node DaemonSet与Pod-local sidecar使用不同metrics端口和精确Prometheus ingress，不增加host或Controller authority。
该证据不替代真实runsc、node-loss、production scrape或L4 enforcement。

r260为Artifact Gateway接入shared process observability。readiness位于restricted PostgreSQL/schema、AWS provider catalog、bounded broker、
exact Public Gateway mTLS listener与独立HTTP listener组合之后；任一server提前退出使进程fail closed。业务与metrics端口使用不同
NetworkPolicy source，未增加Data Worker或Maintenance authority。该证据不包含真实S3/KMS、production scrape或L4 enforcement。

r261为Artifact Data Worker接入shared process observability。readiness位于独立read/work PostgreSQL、AWS provider catalog、bounded
Scheduler/Sandbox/guest broker、双TLS RPC、scan worker及HTTP listener组合之后；任一组件提前退出使进程fail closed。Scheduler/Controller、
gVisor guest与Prometheus仍由不同端口和source selector隔离。该证据不包含真实S3/KMS、production scrape或L4 enforcement。

r262将Artifact Maintenance唯一内部health listener升级为shared process observability；readiness位于restricted PostgreSQL/schema、AWS
provider catalog、bounded deletion backend、maintenance worker与HTTP listener组合之后，worker/listener共同fail closed。NetworkPolicy只允许
精确Prometheus source访问该端口，普通业务caller仍无Maintenance ingress。至此17个ComponentRole workload pool均具备shared HTTP
readiness/metrics接线；该证据仍不包含真实scrape、业务SLI、dashboard/alerts或L4 enforcement。

r263新增独立observability chart，为现有process/HTTP series提供一个role-filtered dashboard及telemetry missing、持续NotReady、有效流量下
failure ratio/p95 latency四条symptom-first alert。全部alert有stable owner/severity、HTTPS runbook URL和checked-in逐alert步骤；静态门禁拒绝
高基数/Secret label、非法threshold、非HTTPS runbook及缺失discovery label。该证据不发明尚不存在的queue/dependency/recovery/permit series，
也不替代Prometheus production scrape、alert delivery或L4演练。

r264移除LLM、SSE、MCP OAuth、conversation及worker startup production telemetry中的高基数资源/进程标识、manifest digest和原始编码错误，
并新增source-level CI门禁，拒绝规范列出的identity、Secret、prompt/response、object key及URL字段进入structured tracing或插值日志。
相关crate tests与strict Clippy通过。该负向静态证据不替代动态payload采集审计、RBAC/retention或production验证。

r265将Orchestration已有Coordinator、Safety Recovery和LocalWorkerPools快照接入shared metrics owner，固定导出active jobs、claim/recovery outcome及
business/critical-control permit available/used；该surface不把process-local wake hint表述成durable queue depth/age。dashboard扩展到8个panel，新增
critical-control permit持续耗尽与recovery scan failure ratio两条带runbook告警。静态与unit门禁通过；其他role saturation、durable queue/outbox/
recovery lag、dependency health及production scrape仍待真实owner接线和资格验证。

r266新增shared worker-permit sampler并接入Model、Capability Native/Remote和Context Native/Remote五个production pool。它周期读取各进程
同一`LocalWorkerPools`物理authority，只导出fixed business/critical-control lane的capacity-derived available/used；不暴露generation、Job或
tenant identity，shutdown token终止sampler。连同Orchestration已有接线，6个pool具备动态permit saturation series；其余11个pool及durable
backlog/dependency health仍待对应owner接线。

r267把shared permit sampler接入同一Sandbox Executor binary的WASI和gVisor两个隔离pool，均从exact `LocalWorkerPools`导出fixed lane
available/used并随process cancellation退出。相关executor/owner tests和strict Clippy通过，动态permit coverage达到8/17 pool；Sandbox Controller、
Artifact、Security/Egress等不同容量authority不能用该series代替，仍待各自owner指标。

r268实现MCP subscription→Context admission的L1 nominal boundary及负向unit fixtures，包含closed exact request、bounded Context Job payload和
stable acceptance validation。它不写数据库、不启动production worker，也没有覆盖accept commit前后kill、Receipt replay、唯一Context Job或
MCP/Context pool隔离；上述L2/L3与本规范要求的真实多进程qualification仍是release blocker。

r270在fresh PostgreSQL 16上覆盖notification admission的Job/Receipt/Event/Outbox原子提交、exact replay、唯一Context Job、stale generation
整批回滚及MCP completion引用durable work digest。full reconcile、wrong-class claim、Context handler、Host/Context独立进程kill-window和permit
隔离仍待L2/L3，因此不推进qualification状态。

r271补齐typed Host adapter unit evidence与fresh PostgreSQL full reconcile acceptance/replay；后者只在fixture明确终态化前一个Context Job后验证
下一Job，未伪装Context worker行为。wrong-class claim、真实Context handler/recovery、production binary composition、进程kill-window与permit
隔离仍是L3/L5 blocker。

r272在fresh PostgreSQL 16闭合subscription Context Job的exact manifest scan、successful admission Receipt/current source重验、Context
concurrent quota、fenced claim、JobCommit success/retry、expired running lease recovery、唯一terminal Event/Outbox与零Context Observation L2。
fixture按真实顺序先由MCP Worker清除pending marker，再由Context Worker claim，证明pending history不是第二执行权威。独立Context Worker→
MCP Host→Egress RPC、真实Streamable HTTP与kill-window仍属于未完成L3。

CR-193增加L1/L2必测项：Host调用跨越至少一次Job heartbeat后，返回的immutable execution identity仍验证成功，Context owner用最新version
terminal commit；旧version commit零写入。新lease generation/token或新physical attempt不得重用前一identity/evidence。

r273接入subscription Context Worker driver，domain/unit evidence覆盖immutable execution identity及backend error到durable retry/terminal分类；
fresh PostgreSQL 16证明调用跨heartbeat后旧version commit零写入、原Host evidence仍由latest fence成功提交。由于尚无Host Resource RPC、
production process composition与kill/restart fixture，该证据只关闭handler与CR-193 L1/L2门禁，不推进L3/L4资格状态。

r274新增Resource Refresh protobuf/client/server L1，并以真实mTLS证明Host的Context Worker audience与Capability/Model身份互斥、错误schema closed；
尚未把该service加入production binary，也未到达Egress/remote endpoint，不能计作L3外部I/O或kill-window证据。

r275以fresh PostgreSQL 16验证Host resolver只接受heartbeat后的latest Context fence并重载当前subscription/session/auth/closure；该L2证明
fail-closed authority lookup，但尚无真实Host→Egress调用或多进程kill-window，L3状态不变。

r276增加Host→Egress closed Resource Refresh transport、MCP Host-only Egress RPC与真实Streamable HTTP list/exact-root-read unit evidence；
fixture证明服务器列出的非冻结URI不会成为read target，成功只返回digest/count/byte evidence。该批没有production进程组合与kill-window，
因此只关闭CR-194 L1和协议adapter，不推进L3。

r277交付可部署的MCP Resource Host与subscription Context Worker二进制，并把Egress production service接上refresh connector；两个进程保持
独立DB/permit pool和互斥mTLS audience。all-target、既有Host process L3与strict Clippy通过。由于Helm workload/NetworkPolicy和fresh PostgreSQL
三进程强杀矩阵尚未通过，该证据只关闭process composition编译/单元门禁，不计L3完成。

r278将subscription Context与Resource Host加入同一候选镜像和现有role chart，以独立ServiceAccount/PDB/HPA/config/TLS/DB/NetworkPolicy形成
两个新pool；普通MCP Host的无PostgreSQL边界继续由部署checker验证，Resource Host仅接受subscription Context selector。Helm lint/render和
ComponentRole closure通过（19 isolated pools）。该静态证据不替代fresh PostgreSQL三进程kill-window或production-equivalent L4 inventory。

r279以fresh PostgreSQL 16、production Resource Host/Context Worker进程和真实mTLS Egress service覆盖dispatch后Host/Worker强杀、response后
terminal commit暂停强杀、两轮expired-lease恢复及唯一completed Event；三次ReadOnly refresh attempt对应唯一Job terminal。fixture还发现并修复
subscription recovery batch未收敛到仓储64项上限导致production Worker启动失败的问题。Egress service仍在测试进程内，remote Streamable HTTP
list/read也未进入该多进程fixture；因此本证据关闭Host/Context crash-window切片，但不替代独立Egress进程、真实fake MCP server、wrong identity/
closure零I/O、pool saturation及L4 rollout证据，完整subscription L3状态保持未完成。

r280关闭CR-195的L1与HTTPS wire L3切片：installed MCP endpoint对显式PEM bundle执行非空、256 KiB、证书解析与startup config反序列化校验；
reqwest POST/SSE只装载该bundle而不合并默认根。独立CA/SAN真实TLS fixture完整执行initialize/initialized/list/read，错CA在HTTP request计数为零
时失败。Egress/Broker全套与strict Clippy通过；独立Egress OS进程、L4 bundle rollout drift/readiness仍是后续门禁。

r281在fresh PostgreSQL 16以独立Egress测试进程、production Resource Refresh RPC/connector、production Resource Host/Context Worker和真实
TLS fake MCP server关闭subscription protocol/crash component L3。第一次initialize后强杀Egress/Host/Worker，第二次完整list/read后在DB terminal
commit窗口强杀Worker，第三次expired-lease恢复；方法计数3次initialize、2次initialized/list/read，唯一completed Event。显式测试feature只允许
loopback协议fixture且production build默认关闭。Context/MCP/Egress各lane saturation、真实scrape与L4 bundle/config rollout drift仍未由此关闭。

r282为`scheduler-recovery`接入PostgreSQL authority的bounded只读Job observation：数据库时间计算`due`与`expired_lease`的count/oldest lag，
且不读取Job payload或导出tenant、Job、Worker、URL、Secret、错误文本。失败保留上一份有效gauge并累加fixed PostgreSQL observation outcome。
production sampler、fresh PostgreSQL 16、owner tests与strict Clippy通过；dashboard扩展为11个panel，并新增due lag、expired-lease recovery lag和
PostgreSQL observation failure三条带runbook的symptom alert。该证据只关闭Orchestration durable backlog/recovery lag及对应dependency observation
的L1接线；Outbox、其他role authority、真实Prometheus scrape和L4～L6仍待完成。

r287为shared PostgreSQL Outbox authority增加bounded只读采样，按数据库时间输出fixed `due`、`expired_claim`、`dead` count与适用oldest lag；
不读取Event payload且不暴露tenant、Outbox/Event、claim owner或失败文本。fresh PostgreSQL 16、strict Clippy、13-panel dashboard、12条
symptom-first alert与逐alert runbook门禁通过。该证据关闭shared Outbox backlog/recovery L1接线，不替代其他role authority、
动态payload审计、真实Prometheus scrape或L4～L6。

r290完成CR-197 trace machine/runtime projection。公共HTTP入口严格解析W3C `traceparent`，Run、Invocation、Job、Task、Event与Outbox保存同一
trace ID；实际MCP、Egress、Artifact、Sandbox与Security mTLS/UDS RPC在workload identity授权后、业务解码前校验trace，并按hop生成新span。
durable reclaim/restart从owner记录恢复原trace ID；Egress provider及gVisor guest/storage边界保持零平台trace header。合同/schema、workspace
strict Clippy、真实mTLS/UDS和fresh PostgreSQL 16恢复测试通过。该证据关闭CR-197 component L3 trace连续性，不替代动态payload审计、真实
Prometheus scrape、telemetry RBAC/retention或L4～L6。

r291以真实loopback TCP listener启动shared production observability Router。客户端先发送包含payload/identity、`tracestate`与`baggage`
canary的未知请求，再实际抓取`/metrics` Prometheus text；未知operation只计入fixed `other/rejected`，采集正文中的全部canary与header名称均为零，
并验证真实content type和graceful shutdown。该证据关闭shared metrics adapter的component real-socket scrape及动态metric payload负向切片，
不替代Prometheus deployment scrape、log/trace动态采集审计、telemetry RBAC/retention或L4～L6。

r292为公共HTTP与内部RPC task-local correlation增加fixed tracing spans。动态采集证明公共parent trace ID、每hop span ID、accepted/rejected
context outcome和internal same-trace/new-span字段存在。真实loopback OpenAI-compatible provider测试将prompt、response、token、query、tenant
identity、`tracestate`和`baggage` canary实际送入production reqwest/tracing路径，允许的request/response metadata events存在而全部canary为零；
公共扩展header拒绝span与RPC canary采集同样为零。连同r291，该证据关闭component L3动态metric/log/trace payload canary；production
telemetry backend、RBAC/retention、Prometheus deployment scrape与L4～L6仍是独立门禁。

r293新增closed `insight_platform_capacity_units` surface，并从Sandbox Controller的实际Artifact-response semaphore在scrape时读取fixed
`artifact_response` available/used。现有owner tests证明response permit持有时available下降、释放后恢复；dashboard扩展为14 panel，并增加持续
capacity exhaustion symptom alert及逐alert runbook。chart正负、真实TCP scrape、相关tests与strict Clippy通过。动态capacity coverage达到
10/19 pool；其余9个pool仍须由各自真实authority接线，production Prometheus scrape与L4～L6不由此推进。

r294从Artifact三个role的实际audience semaphore导出capacity：Gateway `download`，Data Worker `scan_read`、三类Scheduler read与
`sandbox_read`，Maintenance `delete`。scrape读取每个独立bulkhead的available/used；owner测试证明exact response lease持有期间available
归零、并发读取拒绝且drop后恢复。三process tests、strict Clippy、Artifact Helm、14-panel/13-alert observability及19-pool closure门禁通过。
动态capacity coverage达到13/19；剩余六个pool及production Prometheus scrape/L4～L6仍保持Pending。

r295从Management API与Runtime API各自实际SQLx PostgreSQL pool导出fixed `postgresql_connections` available/used。capacity使用配置上限，
used使用established减idle，available包含idle及尚可合法建立的槽位；不导出SQL、tenant或连接identity。真实PostgreSQL 16证明checkout使used
0→1，drop后pool异步归还并在有界时间恢复0。Gateway tests、strict Clippy及部署/observability门禁通过。动态capacity coverage达到15/19；
剩余MCP双Host与Security/Egress四个pool及production scrape/L4～L6保持Pending。

r296为MCP Tool Host与MCP Resource Host各自安装构造期必选的process-local RPC admission semaphore，并从同一owner导出fixed
`rpc_requests` available/used。身份及trace interceptor先于permit获取，permit又先于业务decode；饱和稳定返回`ResourceExhausted`，释放后
available恢复。closed配置/hard max、owner/config tests、真实mTLS、受影响PostgreSQL fixtures编译、strict Clippy及MCP/observability门禁通过。
动态capacity coverage达到17/19；Security Authority、Egress Broker、production scrape、telemetry backend/RBAC/retention及L4～L6保持Pending。

r297从Security Authority唯一实际SQLx PostgreSQL pool导出fixed `postgresql_connections` available/used。capacity取配置上限，used由
established减idle计算，available包含idle和未建立的合法槽位；不镜像数据库业务状态，也不添加重复admission authority。fresh PostgreSQL 16
验证checkout/drop使used 0→1→0；unit tests、strict Clippy及Security/Egress、observability门禁通过。动态capacity coverage达到18/19；
Egress Broker、production scrape、telemetry backend/RBAC/retention及L4～L6保持Pending。

r298从Egress Broker 11个实际Semaphore owner导出closed capacity：Secret resolution/store、Model、HTTP/gRPC Capability、Remote Context、
MCP OAuth、普通/订阅MCP及subscription bridge pending/active。series不含tenant、endpoint、provider或request identity。OAuth饱和测试证明
available 1→0→1且在外呼前拒绝，bridge测试证明pending/active随reservation变化；owner/RPC/broker tests、真实HTTPS/mTLS、strict workspace
Clippy及Security/Egress、observability门禁通过。19/19 pool动态capacity L1接线闭合；production Prometheus scrape、完整dependency health、
L5 mixed-load/saturation profile、telemetry backend/RBAC/retention及L4～L6保持Pending。

r299在全新PG16主/Model隔离baseline、真实NATS和当前production process binaries上完成workspace all-target/all-feature串行L1～L3门禁，
退出码0；两个需要外部S3的测试仍显式ignored。OAuth callback/token endpoint/Egress/Cleanup Worker真实TLS与kill-recovery在最新代码上8/8，
Scheduling、terminal retry、trace、timer、global queue与multi-process fixture边界同步收敛；workspace format、strict Clippy及doc tests通过。
该证据不含Model TLS NATS process fixture、外部S3/KMS、production Prometheus scrape、telemetry backend、production-equivalent Kubernetes/runsc、
L5 mixed load/soak/restore或L6 rollout/rollback，故L4～L6及clean cut继续为environment production-ready声明的release blocker，
不阻塞CR-201仓库实现关闭。

r300收紧最终release evidence门禁：`validate-release-evidence`除Profile、Candidate、Capacity与Evidence manifest外，必须接收只读artifact root；
每个`artifact_links[].name`必须解析为root下同名的真实普通文件，CLI流式重算byte length和SHA-256并拒绝缺失、符号链接、长度或digest漂移。
因此仅在manifest内部构造自洽digest/link不再能冒充content-addressed资格证据。target tests、strict Clippy、generated contract及candidate pipeline
门禁通过；该修正只保证L6 validator fail closed，不产生任何外部L4～L6通过证据。

r301增加共享dependency observation owner，dependency维度被Rust enum闭合为PostgreSQL、NATS、S3、KMS、Secret和Egress，outcome仅
success/failure；安装时拒绝空集、重复或超量依赖，运行时拒绝未安装dependency，因而调用方不能把tenant、provider、endpoint或错误正文变成label。
Security Authority首先在两条真实PostgreSQL repository调用完成后记录结果，认证前置拒绝不冒充数据库故障，并与既有真实SQLx pool capacity
共用同一process metrics surface。shared owner与Authority tests及strict Clippy通过；其余role真实调用边界、对应alerts和production scrape仍待接入。

r302把Egress Broker的Secret/KMS dependency health接到AWS SDK真实请求返回边界。Secret Broker定义不含任何业务标识的observer port；
Secrets Manager的describe/get/delete/create与KMS的describe/encrypt/decrypt只在实际`send`返回后记录success/failure，本地catalog、policy、reference、
identity或permit拒绝不冒充外部故障。Egress composition把两种nominal依赖映射到shared `secret`/`kms` series，并与既有11-lane capacity同surface导出；
observer不接收tenant、provider、endpoint、ARN、错误或Secret正文。Secret/Egress tests、strict Clippy及部署/redaction门禁通过；真实AWS fault仍归L4～L5。

r303把Artifact Gateway、Data Worker与Maintenance三role的S3/KMS health接到Artifact AWS adapter真实SDK请求返回边界。KMS
encrypt/decrypt/describe与S3 head-bucket/head-object/get-object/delete-object分别记录fixed success/failure；presign、本地授权、binding、object key、
generation、limit或catalog拒绝不冒充外部调用。共享observer不接收bucket、object、tenant、binding、endpoint、error或bytes，三role各自把同一
nominal observer映射到自身process metrics，并保持既有独立capacity与权限。Broker/三binary tests、strict Clippy、redaction及Artifact/observability
部署门禁通过；PostgreSQL health、真实S3/KMS fault和production scrape仍归后续批次/L4～L5。

r304增加共享PostgreSQL dependency health sampler：每15秒从现有restricted SQLx pool执行一次只读`SELECT 1::bigint`，只向nominal observer
报告success/failure；不携带URL、database、role、SQL或错误详情，不改变readiness，missed tick不追赶且shutdown可中断正在等待的probe。
Artifact Gateway/Maintenance各接一个sampler，Data Worker对独立read/work pool各接一个；sampler和HTTP/RPC/worker进入同一cancel/drain边界，意外停止使
process fail closed。不可用pool、pre-cancel及三binary lifecycle tests、strict Clippy通过；可选真实database成功test已checked-in，但本轮无运行中的
PG16，故该fixture未取得新证据，production PostgreSQL health仍须L4实际scrape。

r305将Model Worker既有restricted PostgreSQL pool接到共享sampler，并为NATS live-delta driver增加nominal observer port。NATS只在实际TLS
connect、每批publish+flush及shutdown drain返回后报告success/failure；server、subject、tenant/run、payload和error均不跨越port，本地envelope或
backpressure拒绝不冒充依赖失败。PostgreSQL sampler与NATS/permit/claim/cancel/HTTP组件共用既有JoinSet cancellation/drain，且不改变readiness。
实际连接失败、adapter、library/binary tests和strict Clippy通过；可选真实TLS NATS fixture已同时断言connect/publish success，但本轮未配置该fixture，
故未取得新的真实NATS/PG或production scrape证据。Model Egress streaming observation仍归后续批次。

r306为Capability Native与Remote role各自的business及critical-control SQLx pool安装共享PostgreSQL sampler。每个process的两个probe只汇总到固定
`component_role + postgresql + success|failure` series，不输出database、pool、SQL或error；它们与permit sampler组成一个受监督任务，复用既有
worker/HTTP cancellation，在正常shutdown join，意外退出使process fail closed，不参与readiness判定。shared adapter、两个binary tests、strict Clippy及
Native/Remote deployment、redaction和observability门禁通过；本轮没有新的真实PG或production scrape证据，Remote Egress/MCP observation仍待后续批次。

r307为Context Native、Remote与Subscription三个role的restricted SQLx pool各安装一个共享PostgreSQL sampler，仅汇总到本process固定
`component_role + postgresql + success|failure` series，不输出database、pool、SQL或error。每个role的permit与DB sampler组成受监督任务；signal、worker、
HTTP或sampler任一退出都会cancel并join其余组件，Subscription此前异常分支未等待peer的生命周期缺口同时关闭，readiness语义不变。shared adapter、三binary
tests、strict Clippy、Context部署、redaction与observability门禁通过；本轮没有新的真实PG或production scrape证据，Remote Egress与Subscription MCP Host
observation仍待后续批次。

r308为MCP Resource Host和OAuth Cleanup Worker各自的restricted SQLx pool安装共享PostgreSQL sampler，仅汇总固定
`component_role + postgresql + success|failure` series。Resource Host把sampler纳入RPC/HTTP cancellation及bounded drain；Cleanup Worker在signal、HTTP或
sampler退出时cancel并等待peer。二者均不改变readiness，且不预装尚未接线的Egress series。两个adapter/binary tests、strict Clippy、MCP Host/Cleanup部署、
redaction与observability门禁通过；本轮没有新的真实PG或production scrape证据，MCP Tool/Resource/Cleanup Egress observation仍待统一RPC observer批次。

r309为Sandbox Controller restricted PostgreSQL authority pool安装共享sampler，仅汇总固定`component_role + postgresql + success|failure`；probe不占用
Sandbox execution或Artifact response capacity。sampler与RPC/HTTP复用cancellation及原有shutdown deadline，任一组件异常退出都会cancel并等待peer；
readiness不变，也不预装尚未接线的Artifact Broker/node attestor RPC series。adapter/binary tests、strict Clippy、Sandbox deployment、redaction与
observability门禁通过；本轮没有新的真实PG或production scrape证据，Artifact/attestor observation仍待后续批次。

r310为Callback API restricted PostgreSQL command pool安装共享sampler，并附加到既有OAuth callback process metrics；仅汇总固定
`component_role + postgresql + success|failure`，不输出database、pool、SQL、OAuth state或error。signal、HTTP server与sampler互相监督，正常shutdown复用
既有grace，超时会中止残余任务；readiness和callback outcome语义不变，也不预装Egress series。adapter/binary tests、strict Clippy、Callback deployment、
redaction与observability门禁通过；本轮没有新的真实PG或production scrape证据，OAuth Egress observation仍待统一RPC observer批次。

r311为Management与Runtime Gateway各自restricted SQLx pool安装共享PostgreSQL sampler，并与已有connection capacity共用process metrics surface；
每个deployment仅汇总自身固定`component_role + postgresql + success|failure`，不输出database、pool、SQL或error。signal、HTTP server与sampler互相监督，
配置的完整shutdown grace现在用于实际bounded drain，超时中止残余任务；readiness与HTTP/API语义不变。adapter/8个binary tests、strict Clippy、Gateway
deployment、redaction与observability门禁通过；本轮没有新的真实PG或production scrape证据，Runtime Artifact RPC observation仍待统一RPC observer批次。

r312补齐反向审计发现的间接SQLx owner：Orchestration Worker通过`PostgresConnectionBulkheads`持有business与critical-control两个pool，现均接到共享
15秒sampler并汇总为固定`component_role + postgresql + success|failure`，不输出pool、database、SQL或error；既有Job/Outbox backlog/lag query保持独立。
signal、HTTP、runtime-finished或sampler退出都会关闭runtime、HTTP、sampler与bulkheads，readiness不变且不预装Artifact Scheduler RPC series。adapter/binary
tests、strict Clippy、Orchestration deployment、redaction与observability门禁通过；本轮没有新的真实PG或production scrape证据，Artifact observation仍待后续批次。

r313把14-panel runtime dashboard中的scheduler-only PostgreSQL panel扩展为按`component_role + dependency + outcome`聚合的固定六依赖概览，并用
`InsightPlatformDependencyFailureRatioHigh`替换scheduler-only alert。表达式必须同时超过closed失败率和最小观测数，避免单次provider/tenant失败触发；runbook只按
fixed role/dependency分诊并明确禁止endpoint、database、subject、object key、error或tenant数据。Helm负向阈值、13-alert inventory、panel expression、HTTPS
runbook锚点与低基数checker通过；该批闭合仓库内消费端合同，不提供production scrape、真实fault或L5 profile证据。

r314为共享`EgressBrokerGrpcClient`增加只接收fixed success/failure的transport observer，并在Model建连/流读取/取消、Capability HTTP/gRPC
调用与取消、Remote Context、MCP OAuth/cleanup/Tool/Resource及subscription建连/首帧/持续读取的实际tonic返回边界记录结果。请求编码、closed validation等
本地拒绝不产生观测；成功传输后返回业务`Failed`仍是transport success。observer不接收metadata、tenant、provider、endpoint、payload或error。真实mTLS
成功与不可达端点失败测试及strict Clippy通过；本批只建立共享client port，尚未把observer注入各production process，故不生成role Egress series，也不提供
production scrape/fault或L4～L5证据。

r315把共享Egress observer注入production Model Worker，并与既有PostgreSQL/NATS observer共用同一process metrics surface；只有真实Model Egress
建连、stream read与cancel RPC结果形成固定`model-worker + egress + success|failure` series，不携带provider、endpoint、tenant/run、payload或error，且不改变
readiness、adapter或cancellation语义。adapter/binary tests、strict Clippy、Model deployment、observability与redaction门禁通过；本轮没有production scrape、
真实Egress fault或L4～L5证据，其余Egress client role仍待注入。

r316把共享Egress observer只注入production Capability Remote Worker的HTTP/gRPC调用与取消client；Remote与既有双PostgreSQL sampler共用process
metrics surface，Native安装路径仍仅声明PostgreSQL并显式断言没有Egress observer。输出固定`capability-remote-worker + egress + outcome`，不携带codec、
endpoint、tenant/invocation、payload或error，不改变dispatch/cancel/readiness语义。三个binary target tests、strict Clippy、Native/Remote deployment、
observability与redaction门禁通过；本轮无production scrape或真实fault，其余Egress/MCP client仍待注入。

r317把共享Egress observer只注入production Remote Context Worker的查询client，并与其PostgreSQL sampler共用process metrics surface；Native与
Subscription Context安装路径保持PostgreSQL-only并显式断言无Egress observer，Subscription的独立MCP Host边界不被误记为Egress。实际查询RPC只输出固定
`context-remote-worker + egress + outcome`，不携带endpoint、tenant/query、payload或error，不改变resume/readiness语义。四组binary target tests、strict
Clippy、Context/Remote deployment、observability与redaction门禁通过；本轮无production scrape或真实fault，其余Egress/MCP client仍待注入。

r318把共享Egress observer注入production MCP Tool Host、Resource Host与OAuth Cleanup Worker。Tool Host只安装Egress；Resource/Cleanup分别与
既有PostgreSQL sampler共用process metrics。普通Tool调用、Resource Refresh、OAuth exchange/PKCE delete及subscription建连/读取只输出各自固定
`component_role + egress + outcome`，不携带server/endpoint、tenant/task/resource、payload或error，不改变RPC/readiness/cleanup语义。四组binary target
tests、strict Clippy、MCP Host/Cleanup deployment、observability与redaction门禁通过；本轮无production scrape或真实fault，Callback/Sandbox Egress client
仍待注入。

r319把共享Egress observer注入production Callback API的OAuth exchange client，并与既有PostgreSQL sampler共用process metrics；实际exchange RPC
只输出固定`mcp-callback-api + egress + outcome`，不携带OAuth state/code、tenant/task、endpoint、token或error，不改变callback receipt、commit或readiness
语义。binary tests、strict Clippy、Callback deployment、observability与redaction门禁通过；本轮无production scrape或真实fault，Sandbox Egress client仍待
注入。

r320反向清点全部`EgressBrokerGrpcClient`构造点并把清单固化到observability checker：七个first-release production client必须使用
`new_with_observer`，任何新增no-op production构造均fail closed。余下no-op只允许shared client自身测试、PostgreSQL component fixture和明确不进入release
Docker/Helm的deferred Firecracker/microVM provider；首发WASI/gVisor Sandbox没有Egress client，故r319所称“Sandbox待注入”更正为非首发路径，不构成release
dependency series缺口。observability、Sandbox deployment与redaction门禁通过；至此first-release production Egress client L1接线完整，但production scrape、
真实fault及L4～L5仍Pending。

r321在上述跨crate接线后重跑workspace all-target/all-feature L1～L3门禁，发现rolling-summary fixture把18轮串行SQLite summary压力与仅3秒的无关
owner retirement deadline耦合，单测可稳定触发`RUN_INTERRUPTED`。该fixture现把owner lease扩大为30秒，而heartbeat仍为1秒，production配置/owner逻辑及专用
lease失败测试均未改变。修复后目标测试与完整workspace tests通过，两个外部S3 fixture保持ignored；workspace strict Clippy、format及doc tests也通过。该批只
稳定仓库门禁，不提供外部S3、production scrape、Kubernetes/runsc或L4～L6证据。

r322补齐first-release Sandbox WASI/gVisor Executor的Core NATS control dependency health。共享transport observer只接收success/failure，并在实际
request、subscribe+flush、reply publish、stream closure与unsubscribe返回边界记录；Executor另在production TLS connect返回边界记录，并把同一observer注入
listener。subject/envelope/worker/tenant/job/payload/error不跨越port，本地校验拒绝零观测；两个backend共享固定`component_role + nats + outcome`。
RPC/Executor tests、真实mTLS、strict Clippy及Sandbox deployment/observability/redaction门禁通过；可选真实NATS fixture已增加request/reply/timeout观测断言，
但本轮未配置外部NATS，故没有新的真实NATS或production scrape证据。

r323把first-release dependency owner inventory固化到observability checker：Security、Artifact三role、Model、Capability两role、Context三role、MCP
双Host/Cleanup、Sandbox Controller/两Executor、Callback、双Gateway、Orchestration与Egress Broker均必须保留其实际PostgreSQL/NATS/S3/KMS/Secret/Egress
安装及调用边界；AWS Artifact/Secret、Model NATS、Sandbox NATS adapter port也进入清单。任一owner移除observer、sampler或production client注入都会fail
closed。observability、Sandbox及redaction门禁通过；六类external dependency的仓库内L1接线至此闭合，production scrape、fault injection、其他domain
backlog/recovery series及L4～L5仍Pending。

r324消除Orchestration process surface在r312后形成的重复Prometheus标签集。共享PostgreSQL transport observer仍是
`insight_platform_dependency_observations_total{component_role,dependency,outcome}`的唯一owner；durable Job/Outbox只读查询的观测成功/失败改用独立
`insight_platform_durable_observations_total{component_role,outcome}`，并保留last-known snapshot语义。组合render测试断言同一PostgreSQL dependency
series恰好一次，目标tests、strict Clippy、observability/redaction、format与diff门禁通过；没有production Prometheus scrape、真实fault或L4～L5新增证据。

r325把durable Job backlog renderer抽为共享`DurableJobQueueMetrics`并让Orchestration复用同一owner；PostgreSQL observation API只接受nominal
`WorkClass`，不再允许自由字符串。Model Worker以独立pool clone每秒只读采样`WorkClass::Model`，输出固定role和`due|expired_lease`，失败只增加
observation failure并保留last-known gauges，不读取payload或输出tenant/backend/database/error。dashboard增加observation outcomes，新增Model due lag、
Model expired-lease lag及跨role durable observation failure三条symptom-first alert与逐条runbook，阈值schema负向fail closed。相关目标26/26、
PostgreSQL baseline compile、strict Clippy、Model deployment、observability/redaction及format/diff门禁通过；未配置fresh PostgreSQL或production
Prometheus，故该证据只关闭Model backlog/recovery L1 wiring，不推进L2或L4～L5。

r326在Capability crate内增加共享typed durable queue sampler，Native与Remote production binary分别冻结
`WorkClass::CapabilityNative|CapabilityRemote`，各自以business pool clone观察authority并附加到既有process surface；两个sampler与permit/双PostgreSQL
health sampler同受取消和异常退出监督。指标仅含固定role与`due|expired_lease`，查询失败保留last-known gauge。两条symptom-first alert用closed固定role
集合覆盖Native/Remote并按role分组，runbook明确Remote外部effect不得手工重放。目标13/13、strict Clippy、双部署、observability/redaction及format/diff
门禁通过；无fresh PostgreSQL或production scrape，故只关闭这两个WorkClass的L1 backlog/recovery wiring，不推进L2或L4～L5。

r327处理共享WorkClass的owner歧义：PostgreSQL operational query新增closed `DurableJobOwnerKind`，首个variant把Sandbox execution固定为
`WorkClass::Sandbox + owner_kind=job`。Sandbox Controller的同一受监督health task每秒采样该selector并接入既有capacity/dependency process surface；
`owner_kind=sandbox_job`的MCP路径不会被计入Controller role。两条固定role due/expired symptom alert及runbook强调process-generation absence proof与禁止
host fallback。lib tests 14/14、strict Clippy、Sandbox全拓扑部署、observability/redaction及format/diff门禁通过；无fresh PostgreSQL、production
Prometheus或runsc，故只关闭Sandbox execution queue的L1 backlog/recovery wiring，不推进L2/L4～L5。

r328在继续Artifact/Context/MCP backlog审计时确认，真实claim lane仍靠JSON payload kind/backend区分，违反03已冻结的“Job保存kind”和typed hot
predicate要求。上游修复先交付18项nominal `JobKind`、25项closed kind/work-class/owner mapping及generated registry/checker；contracts全目标、
生成漂移、Python合同与strict Clippy门禁通过。该证据只关闭machine contract，不提供baseline persistence、repository/claim迁移、剩余queue series、
production scrape或L4～L5证据。

r329完成JobKind持久化迁移：baseline/schema contract v8、全部Job writer/reader、Artifact/Context typed claim与managed MCP Sandbox owner均对齐closed
三元组；Sandbox durable sampler同时增加exact JobKind selector，修正r327仅凭owner无法长期区分两种共享Job的问题。独立baseline checker禁止遗漏
`job_kind`的INSERT、JSON kind热路由及`sandbox_job` SQL owner；PostgreSQL all-target 35/35入口与strict Clippy通过。运行环境未提供fresh PG16、真实
production Prometheus或runsc，因此本批是仓库内L1/静态L2门禁，不新增production backlog series、fault或L4～L6证据。

r330复用typed multi-JobKind operational selector，把Artifact Data Worker的scan/rescan与Maintenance的delete/blob-cleanup队列分别接入受监督
PostgreSQL sampler和既有process metrics surface。两条role-set due/expired symptom alert、runbook及静态inventory同步锁定，目标8/8、baseline 2/2
入口、strict Clippy、Artifact部署、observability和redaction门禁通过。没有fresh PostgreSQL、production Prometheus、S3/KMS fault或L4～L5证据；
因此只关闭Artifact durable backlog/recovery的仓库内L1 wiring。

r331为Context Native、Remote与Subscription Worker按exact JobKind接入三个受监督PostgreSQL sampler，明确排除同WorkClass的Dataset build和其他owner。
Context role-set due/expired symptom alert、runbook及静态inventory同步锁定；目标13/13、strict Clippy、Context部署、observability/redaction门禁通过。
没有fresh PostgreSQL、production Prometheus、remote endpoint fault或L4～L5证据，因此只关闭三条Context Worker queue的仓库内L1 wiring。

r367补齐MCP Discovery durable queue的运营闭包。既有production sampler已按exact `McpDiscovery + Mcp + mcp_operation`从共享Job
authority导出`due|expired_lease` count/lag，role-filtered durable Job dashboard也已覆盖该series；本批新增两条仅匹配固定
`mcp-discovery-worker` role的symptom-first lag alert，并为排查Egress、Artifact verification、lease/fence与禁止手工restage/发布Snapshot提供逐项
runbook。observability checker以28条exact inventory、HTTPS runbook、高基数/Secret label负向约束fail closed。该证据关闭已接线durable queue
role的仓库内dashboard/alert缺口，不替代production Prometheus scrape、alert delivery、L5 SLO/error budget或L4～L6。

r370修复真实GitHub CI暴露的Security/Egress exact RPC inventory漂移。CR-198已评审的credential-free、object-locator-free
`DiscoverMcpStreamableHttp`是MCP Discovery Worker唯一新增的远端discovery transport，但checker仍冻结在CR-192时的13项集合。当前closed
inventory精确为14项，门禁继续以总数相等拒绝任意第15项；本批不改变proto、authority、credential/locator边界、部署拓扑或L4～L6状态。

r371修复GitHub CI实时RustSec门禁报告的`h2 0.4.15`与`wasmtime 42.0.0`漏洞，不增加ignore。`h2 0.4.16`关闭unbounded empty DATA
frame；首发restricted WASI exact runtime升级为`wasmtime 46.0.2`，覆盖该run报告的13项Wasmtime公告并同步feature baseline。WASI 10/10、
workspace all-target/all-feature tests、format、strict Clippy、RustSec audit、cargo-deny与crate-boundary本地通过；该依赖安全修复不改变
两种首发backend、Sandbox authority或L4～L6状态。

r372～r381使L1～L3 qualification runner与PostgreSQL evidence在真实CI时序下保持fail closed且可重现。隔离Model baseline现在非交互认证；
实时queue age按数据库时间的单调关系比较；Task恢复只claim fixture自己的exact root Job；Child Run deadline在写入JSON closure与
`timestamptz`前统一为PostgreSQL微秒精度；`SqlCatalog`则作为首发native Context adapter进入`ContextQueryNative` durable lane。fresh
PostgreSQL 16精确测试已覆盖Phase 2恢复与Phase 3 SQL Catalog→Observation→Text2SQL read-only admission，目标format/strict Clippy通过。
terminal-only Artifact staging catch-up也以未来`available_at`隔离production后台pump，确定性证明101行跨两个bounded batch清空。Capability
input Task与MCP OAuth external-authorization Task的deadline均在Receipt前统一为PostgreSQL微秒，避免typed列与JSON snapshot产生伪binding
drift或replay `not_found`。MCP Discovery与Resource Subscription的operation deadline也已统一；Subscription在规范化前验证客户端原始
`request_digest`，随后才claim Receipt，因而外部幂等意图与内部JSON/typed-column权威同时闭合。fresh PG16完整Subscription 3/3及GitHub CI
run `33102457010`四个Job全部成功。这些结果关闭仓库qualification的确定性缺口与首发Context lane映射；真实multi-node rollout、runsc、
Prometheus scrape、mixed load、soak、restore与signed promotion仍须L4～L6外部执行。r382另把实时cargo-deny刚发现已撤回的
`chacha20 0.10.1`更新为兼容未撤回的`0.10.2`并同步exact dependency baseline；不增加advisory/yank ignore。GitHub CI run
`33105053408`的Test、Lint、Dependency policy与MCP interoperability全部成功，该依赖卫生修复同样不构成L4～L6证据。

r288实现production candidate供应链入口。workflow action、toolchain、base image与GitOps environment输入均固定不可变revision；runtime和sandbox guest分别生成exact
image digest、SPDX SBOM、SLSA/GitHub provenance及keyless signature，随后由确定性生成器构造15-role CandidateManifest和7项实际
WorkerManifest闭包。gVisor guest digest冻结在`sandbox-executor.gvisor.adapter_runtime_digest`，不会因其不是主workload role而丢失。
application Helm/Docker closure与exact GitOps environment closure共同形成`deployment_config_digest`。唯一baseline、SBOM、测试报告、Candidate
签名均进入canonical release-bundle index并再次签名。仓库门禁只证明producer结构与本地合同通过；
registry、GitOps和目标环境验证尚未运行，故L6 gate仍为Pending。

r383建立private GitHub GitOps environment authority及`production/closure`与`releases`分离布局，避免candidate输入闭包引用其自身
`deployment_config_digest`。candidate Environment只允许`main`，跨私有仓库checkout改用environment-repository scoped只读deploy key；
closed `environment.json`必须绑定exact application repository/commit、canonical QualificationProfile digest、multi-node/runsc/admission要求、
受保护的WASI/gVisor/attestor selector及无credential Git策略，任一漂移在build/image publish前fail closed。该闭合提供真实GitOps输入和权限边界，
但尚未产生registry candidate、L4～L6目标环境证据或人工promotion，故L6继续Pending。

r289复核当前render closure并更正累计计数：subscription Context Worker与MCP Resource Host使15个role对应19个隔离pool，动态permit覆盖为
9/19；r248～r267的17-pool数字保留为历史批次证据。Security/Egress deployment checker同时登记既有CR-192 `RefreshMcpResources` method，
以当时的exact 13-method集合继续拒绝任意额外RPC；后续CR-198增加仅用于discovery的第14项，当前清单以r370为准。该批仅修复门禁/审计漂移，
不改变protocol或部署拓扑。

r283为独立MCP OAuth PKCE Cleanup Worker接入shared process observability。readiness位于closed config、PostgreSQL/schema、mTLS Egress client
和durable cleanup owner之后，HTTP listener提前退出会使process失败；Helm以HTTP probe、独立Service/ServiceMonitor及Prometheus-only ingress
替换原PID探针，同时保持数据库与Egress的exact出口。binary tests、strict Clippy和chart静态正负门禁通过；该process surface不计作新的
ComponentRole authority，也不替代真实OAuth endpoint、kill/restart、production scrape或L4。

r284在fresh PostgreSQL 16使用production Cleanup Worker、独立Egress测试进程、production Egress RPC service和真实mTLS workload identity
完成PKCE cleanup crash L3。第一次exact Secret delete进入后强杀Egress/Worker，expired lease由第二个进程以claim epoch 2 reclaim，最终唯一
`cleanup_completed`且旧fence不能结算；Task terminal payload与SecretBinding均通过current PostgreSQL owner重验。该证据关闭Cleanup/Egress
删除链的component L3，不替代Callback真实token endpoint/exchange、OAuth rollout、lane saturation、production scrape或L4～L6。

r286以真实独立CA HTTPS token endpoint、production OAuth reqwest broker、mTLS Egress RPC、Callback ingress owner与fresh PostgreSQL 16
完成OAuth callback/exchange crash component L3。token store成功而Callback commit未开始时强杀Callback/Egress，替代进程从prepared metadata
恢复且不重发one-time code；endpoint调用严格为1，Task/Receipt/Event各只有一个终态。该证据不替代真实Secret Manager rotation、OAuth
config rollout、lane saturation、production scrape或L4～L6。

r246将Management与Runtime API拆为两个startup role及独立Kubernetes identity/DB/NetworkPolicy/PDB/HPA；closed path guard在认证和repository
调用前拒绝错role noun，Management不持有Runtime的Artifact mTLS或cursor Secret。unit、Helm正负render和静态权限证据通过，关闭这两个role
的manifest隔舱偏差；其余role inventory与真实cluster mTLS/RBAC/NetworkPolicy矩阵仍必须由L4 preflight实际验证。

部署方要将某个environment声明为production-ready时，至少覆盖：

- clean baseline migration、upgrade/rollback rehearsal和backup/restore；
- concurrent Run admission/activation、Job claim/lease loss、Receipt replay、Event/Outbox recovery；
- cross-tenant authorization、ID/owner kind、schema/JSONB、Secret/log redaction负向测试；
- MCP remote Streamable HTTP、OAuth、discovery、Task和subscription的真实协议fixture；
- MCP subscription notification→Context admission以真实PostgreSQL和独立Host/Context进程覆盖accept commit前后kill、Receipt replay、唯一
  Context Job、stale session/fence零Job以及MCP/Context pool互不占用；
- 同一`McpOperation` owner下`Mcp`与`Context`两种Job的claim负向矩阵必须证明wrong WorkClass、wrong invocation kind或wrong typed payload零claim；
- OpenSandbox candidate/activation/runner/cleanup 门禁必须使用固定真实 Server、Kubernetes provider、BatchSandbox Controller 与
  containerd/runc，覆盖并发 create、response loss、restart、kill/reclaim、boot rollover、late result、Direct/Disabled network 与零 Platform 权限；
- Artifact Gateway/Data Worker/Maintenance三role权限矩阵、S3/KMS fault、retention/GC和饱和测试；
- Model Inline hard limit、tool loop、budget、provider fault和无Artifact fallback测试；
- Capability HTTP/gRPC/MCP exact installed codec、required Worker manifest、长调用heartbeat、process kill与外部I/O前fail-closed矩阵；
- 一个隔舱饱和时API/Scheduler/critical-control和其他隔舱仍满足profile SLO；
- L1/L2覆盖所有expression opcode、type/stack/output bounds与unknown-field；L2真实PostgreSQL覆盖wrong Plan/Artifact/RunValue digest、
  Node version、lease/fence、跨tenant/run和Compute/Scope/Node/Job/Event原子回滚；L3多进程从Artifact Data RPC物化Plan并自行推进
  Branch/Map/Loop/Compute，禁止fixture注入observation；classification fixture覆盖多级input lattice join、空input `Internal`、caller降级、
  Artifact metadata漂移及整批原子回滚；Map fixture覆盖wrong producer/item schema、每item独立RunValue、动态Scope隔离和批次重放；
  Loop fixture覆盖carried pair producer/schema负向、body output缺失、rollover ID冲突整批回滚、下一Scope condition/body复用、false exit
  关闭Scope、两个iteration不串值及crash/replay不重复RunValue；Return/Raise fixture覆盖Plan v1/v2拒绝、wrong producer/schema、
  lexical Scope shadow、缺失/跨Run/terminal Scope value、Inline正文或Artifact digest漂移、Agent output/error schema失败、stale fence、
  ID冲突整批回滚及Receipt replay；L3从Artifact Data RPC物化terminal正文后kill/restart仍由owner transaction产生唯一Run terminal；
- rolling drain、Pod/Node/DB/NATS/S3/KMS/Egress fault injection和整体recovery；
- 持续soak中无无界queue/memory/connection/Artifact orphan/recovery lag增长。

表数、trigger数、migration checksum、route count或广泛snapshot不是资格替代物。

## 14. Release、promotion 与rollback

CI/CD流程：

1. 可重现build，生成签名image、SBOM、provenance和migration artifact；
2. 执行L1～L3开发门禁并生成仓库spec证据；
3. 如部署方要求production-ready声明，部署ephemeral/staging production-equivalent topology并执行适用L4～L6；
4. 只有实际通过相应environment门禁时，才将通过的exact digest更新到GitOps environment repository并经人审批promotion；
5. 监视rollout SLI/error budget，失败时将GitOps指针回滚到上一个已资格闭包；
6. 数据库仅执行已reviewed forward migration/runbook，不靠业务Release row决定当前环境版本。

Git、container registry、CI artifact store和Kubernetes rollout history是发布证据authority。平台API不提供promote/rollback命令。

## 15. 完成定义

spec/implementation phase在以下仓库条件成立时可以标记完成：

- 上游规范是Reviewed/Accepted，cross-review无P0/P1冲突；
- implementation、migration、manifests、runbooks和tests与合同同一commit/release闭包；
- 仓库适用的L1～L3、静态部署、candidate producer和negative gates实际通过，证据可追溯到exact commit/digest；
- 没有把Draft目标、fake adapter、单进程fixture或对象数量声明为production behavior；
- 未执行的真实L4～L6被明确标为Not run / environment release gate，且没有生成伪造的passed evidence或CapacityProfile。

某一environment只有在其适用L4～L6实际通过、证据可追溯到exact digest/profile/topology，且known residual risk有明确release decision时，
才可声明production-ready。spec Verified与environment production-ready是两个独立状态。

## 16. 明确推迟

- restricted WASI、gVisor、Kata、microVM/Firecracker/KVM、GPU、heavy compute和cross-region active-active；
- Managed MCP stdio与persistent sandbox session；
- Model Artifact-backed output和Model专用Artifact role；
- runtime Installation Release/Gate API、dynamic storage/KMS API；
- service mesh、multi-cluster scheduler和全自动无人值promotion。

## 17. 未决问题

CR-216没有阻塞合同review的问题，也没有修改OpenSandbox源码的硬前置。真实OpenSandbox Kubernetes/BatchSandbox/Armed runner实现、
真实PostgreSQL L2与单节点Kubernetes/containerd-runc L3已执行并通过；workspace、contract、CLI/profile、deployment与docs gates也已通过。
L4 production topology/fault、L5 capacity/soak和L6 restore/promotion均Not run。首个production CapacityProfile只能由目标环境实测冻结；
当前仓库没有OpenSandbox production capacity、SLO、HA、强隔离或restore声明，也不声明production-ready。
