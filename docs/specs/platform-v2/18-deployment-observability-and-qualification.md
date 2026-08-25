# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-189 |
| 日期 | 2026-08-25 |
| 依赖 | 00～17 |
| 直接下游 | cross-review、implementation-plan |

> CR-181 impact：资格矩阵增加Plan v4 external leaf dispatch、candidate selection、result binding与crash recovery；静态manifest或
> repository单元fixture不能替代多进程owner-boundary证据。

> CR-185 impact：L1/L2增加Skill frame canonicalization、截断/溢出/trailing bytes、path/digest/length mismatch与错误media拒绝；
> L3覆盖Scheduler exact slot/deployment/revision/lease经Artifact Data Worker mTLS materialization且无storage credential泄漏。

> CR-188 impact：Capability Worker镜像/startup evidence必须枚举bounded exact installed codec manifest；L1～L4增加错codec identity、module、
> descriptor、Worker manifest、空registry与rollout drift负向fixture，并证明全部在Egress/MCP I/O前fail closed。

> CR-189 impact：Context Worker镜像/startup evidence必须枚举bounded exact adapter manifest；RemoteSearch L1～L4增加错endpoint/digest、
> Network/TLS/Trust Policy kind/digest、Worker manifest、空registry与rollout drift负向fixture，并分别证明claim前零lease/quota mutation及
> dispatch前零Egress调用。

## 1. 决策摘要

发布、promotion与rollback是GitOps/CI/CD与Kubernetes部署事实，不是平台业务aggregate。运行时数据库
不保存`InstallationReleaseState`、`Candidate`、`GateResult`、`ReleaseManifest`或安装级compatibility generation。

CI产生不可变的build/provenance/SBOM/test/qualification artifacts，GitOps存储环境期望image/config/schema/profile digest，
Kubernetes执行rollout/rollback。应用启动时对照同一typed startup manifest，漂移时readiness fail closed。

首版Sandbox只部署restricted WASI与gVisor。Artifact只部署Gateway、Data Worker和Maintenance三个role。
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
| Scheduler/Recovery | Deployment、critical-control pool、lease/scan budget |
| Model Worker | queue、DB pool、provider client、permit/rate limit |
| Capability Worker | Native/Remote role、DB pool、permit |
| Context Worker | queue、DB pool、index/client permit |
| MCP Host | queue、DB pool、Egress client、session/subscription budget |
| Sandbox Controller | queue、DB pool、executor admission |
| WASI Executor | node/pod pool、runtime、CPU/memory/fuel budget |
| gVisor Executor | node/pod pool、runsc runtime、CPU/memory/PID/I/O budget |
| Artifact Gateway | public ingress、DB/storage pool、stream budget |
| Artifact Data Worker | internal identity、DB/storage pool、stage/read/verify budget |
| Artifact Maintenance | queue、DB/storage pool、scan/delete/GC budget |
| Egress/Secret Broker | workload identity、network、provider client、secret-resolution budget |

可以在同一Rust workspace编译多个binary，但上表声明的物理隔舱不得因代码复用而合并运行时权限。
一个role饱和不得使其他role的readiness失败或占用critical-control reserve。

Scheduler到Artifact Data Worker的Typed Plan listener必须有独立mTLS route与NetworkPolicy，只允许Scheduler ServiceAccount/workload
URI；Sandbox Controller与Model Worker identity不能调用该service。Scheduler表达式求值使用自己的有界CPU/memory/permit和exact
RunValue读取budget，不获得Provider、MCP、Context、Secret或Sandbox egress。表达式饱和不得占用critical-control连接reserve。

## 4. Kubernetes 安全基线

除下述gVisor Launcher的exact Kubernetes token例外外，所有workload必须：

- 固定image digest、runAsNonRoot、readOnlyRootFilesystem、drop capabilities和seccomp profile；
- 显式CPU/memory/ephemeral-storage request和limit；
- 默认deny ingress/egress NetworkPolicy，只开放exact service flow；
- 独立ServiceAccount和least-privilege RBAC，默认不automount Kubernetes token；
- topology spread、PodDisruptionBudget、graceful drain和bounded startup/readiness/liveness probe；
- 从同一startup manifest对照component role、region、image、protocol、profile和policy digest。

gVisor node/runtime必须显式标记并且只允许`runsc`，不允许runc fallback。guest Pod不允许privileged、hostPath、device、
host PID/network、metadata、Kubernetes API或runtime socket。Launcher使用独立ServiceAccount，只允许execution namespace中的
`create/get/watch/patch/delete pods`和`get pods/status`（`patch`只释放UID/resourceVersion fenced scheduling gate）；禁止Pod log、Secret、ConfigMap、ServiceAccount、RBAC、Node、RuntimeClass、
exec、attach和port-forward，并由fail-closed admission锁定可创建Pod的完整安全closure。WASI与gVisor使用不同pool与identity，
都不与API/Scheduler Pod共享进程或service account。

gVisor Launcher Pod必须启用shared process namespace并包含一个非特权process-attestor sidecar。二者只通过`emptyDir` UDS通信；
sidecar以Pod UID与`SO_PEERCRED`封装process-generation evidence，通过Pod IP向Controller提供验证/缺席证明，且不得挂载
hostPath、hostPID或Kubernetes token。只有Launcher container可挂载短期、显式projected API token。

## 5. Network 与依赖拓扑

```text
Client -> Gateway -> Management/Runtime API
API/Workers -> PostgreSQL
Components -> NATS (wake/outbox delivery only)
Artifact roles -> S3/KMS
Workers/Hosts -> Egress Broker -> catalog-approved external endpoints
Sandbox Controller -> WASI/gVisor Executors
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
- Sandbox、Model、MCP、Artifact三role不能共用被禁的DB/storage/semaphore；
- gVisor不允许runtime fallback；
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

一个test可以产生多个观测，但不在每个文档/层级复制为独立proof。fixture manifest只记录输入profile、
code/image/schema digest、seed、topology、start/end time、tool version、result和artifact links，由CI artifact store保留，
不写入运行时GateResult表。

## 13. 必须资格矩阵

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
隔舱容量、L4 rollout及其余L4～L6仍是release blocker。

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

r246将Management与Runtime API拆为两个startup role及独立Kubernetes identity/DB/NetworkPolicy/PDB/HPA；closed path guard在认证和repository
调用前拒绝错role noun，Management不持有Runtime的Artifact mTLS或cursor Secret。unit、Helm正负render和静态权限证据通过，关闭这两个role
的manifest隔舱偏差；其余role inventory与真实cluster mTLS/RBAC/NetworkPolicy矩阵仍必须由L4 preflight实际验证。

每个production release至少覆盖：

- clean baseline migration、upgrade/rollback rehearsal和backup/restore；
- concurrent Run admission/activation、Job claim/lease loss、Receipt replay、Event/Outbox recovery；
- cross-tenant authorization、ID/owner kind、schema/JSONB、Secret/log redaction负向测试；
- MCP remote Streamable HTTP、OAuth、discovery、Task和subscription的真实协议fixture；
- real WASI与真实`runsc` RuntimeClass gVisor的ABI/limit/escape/cleanup/process-kill/watch-restart/node-loss测试；
- gVisor Launcher RBAC逐verb/resource/subresource负向矩阵，以及绕过runtimeClass/image/resource/volume/network/fence字段的
  admission负向矩阵；
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
2. 执行L1～L3开发门禁；
3. 部署ephemeral/staging production-equivalent topology，执行L4～L6；
4. 将通过的exact digest更新到GitOps environment repository，经人审批后promotion；
5. 监视rollout SLI/error budget，失败时将GitOps指针回滚到上一个已资格闭包；
6. 数据库仅执行已reviewed forward migration/runbook，不靠业务Release row决定当前环境版本。

Git、container registry、CI artifact store和Kubernetes rollout history是发布证据authority。平台API不提供promote/rollback命令。

## 15. 完成定义

一个phase/release只能在以下条件全部成立时标记完成：

- 上游规范是Reviewed/Accepted，cross-review无P0/P1冲突；
- implementation、migration、manifests、runbooks和tests与合同同一commit/release闭包；
- 适用的L1～L6门禁实际通过，证据可追溯到exact digest/profile/topology；
- 没有把Draft目标、fake adapter、单进程fixture或对象数量声明为production behavior；
- known residual risk有owner、deadline和explicit release decision，不被隐藏在“稍后完成”中。

## 16. 明确推迟

- microVM/Firecracker/KVM、GPU、heavy compute和cross-region active-active；
- Managed MCP stdio与persistent sandbox session；
- Model Artifact-backed output和Model专用Artifact role；
- runtime Installation Release/Gate API、dynamic storage/KMS API；
- service mesh、multi-cluster scheduler和全自动无人值promotion。

## 17. 未决问题

CR-181已确认L1～L3 executable qualification定义与04～17一致并恢复Accepted；证据尚未全部通过，L4～L6仍是release blocker。

基础部署与资格流程无未决设计问题。首个production CapacityProfile的数值需要在实现完成后通过
L4～L6测量冻结，Draft期间不声明为current capacity。
