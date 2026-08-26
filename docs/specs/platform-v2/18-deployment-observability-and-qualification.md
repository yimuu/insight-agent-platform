# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-197 |
| 日期 | 2026-08-26 |
| 依赖 | 00～17 |
| 直接下游 | cross-review、implementation-plan |

> CR-197 impact：qualification增加public traceparent正负、Gateway→Scheduler/Worker→MCP/Egress/Sandbox/Artifact跨进程同trace/new-span、
> kill/reclaim continuity、Event/problem correlation和第三方零trace-header计数。`tracestate`/`baggage`、payload/identity canary必须在动态采集结果中
> 为零；静态source扫描不能替代该门禁。

> CR-181 impact：资格矩阵增加Plan v4 external leaf dispatch、candidate selection、result binding与crash recovery；静态manifest或
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

CR-192 L1覆盖subscription refresh request/outcome closed schema、digest/count/byte/deadline bounds、ReadOnly retry mapping以及
Observation/cache/dataset零创建；L2 fresh PostgreSQL覆盖exact `Context -> McpOperation` claim、quota reservation、JobCommit first-winner、
terminal Event/Outbox、stale fence零写入、retry与expired-lease recovery。L3必须使用独立Context Worker、MCP Host和Egress进程及真实
Streamable HTTP fake server，在claim后、Host dispatch前后、response后/terminal commit前分别kill，证明wrong workload/owner/closure零外部I/O、
ReadOnly uncertain产生新attempt、唯一terminal evidence、无Context Observation/cache row、Context/MCP permit与DB pool相互隔离。L4再以mTLS、
RBAC、NetworkPolicy和Context/MCP各自饱和验证只有允许的Context→Host→Egress路径，且任一lane饱和不使另一lane或API readiness失败。

CR-194 L1增加`resources/list` closed registry、Resources capability与ReadOnly effect、独立per-method limits及unknown/missing limit拒绝；L3的
full reconcile fake server必须观察一次有界list和对允许集合的有界read，任一步骤响应后强杀均只允许同一ReadOnly Job的新attempt重读，且Host、
Job/Event/Receipt与日志均不保存remote body或自由URI。

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

r288实现production candidate供应链入口。workflow action、toolchain、base image与GitOps environment输入均固定不可变revision；runtime和sandbox guest分别生成exact
image digest、SPDX SBOM、SLSA/GitHub provenance及keyless signature，随后由确定性生成器构造15-role CandidateManifest和7项实际
WorkerManifest闭包。gVisor guest digest冻结在`sandbox-executor.gvisor.adapter_runtime_digest`，不会因其不是主workload role而丢失。
application Helm/Docker closure与exact GitOps environment closure共同形成`deployment_config_digest`。唯一baseline、SBOM、测试报告、Candidate
签名均进入canonical release-bundle index并再次签名。仓库门禁只证明producer结构与本地合同通过；
registry、GitOps和目标环境验证尚未运行，故L6 gate仍为Pending。

r289复核当前render closure并更正累计计数：subscription Context Worker与MCP Resource Host使15个role对应19个隔离pool，动态permit覆盖为
9/19；r248～r267的17-pool数字保留为历史批次证据。Security/Egress deployment checker同时登记既有CR-192 `RefreshMcpResources` method，
以exact 13-method集合继续拒绝任意额外RPC。该批仅修复门禁/审计漂移，不改变protocol或部署拓扑。

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

每个production release至少覆盖：

- clean baseline migration、upgrade/rollback rehearsal和backup/restore；
- concurrent Run admission/activation、Job claim/lease loss、Receipt replay、Event/Outbox recovery；
- cross-tenant authorization、ID/owner kind、schema/JSONB、Secret/log redaction负向测试；
- MCP remote Streamable HTTP、OAuth、discovery、Task和subscription的真实协议fixture；
- MCP subscription notification→Context admission以真实PostgreSQL和独立Host/Context进程覆盖accept commit前后kill、Receipt replay、唯一
  Context Job、stale session/fence零Job以及MCP/Context pool互不占用；
- 同一`McpOperation` owner下`Mcp`与`Context`两种Job的claim负向矩阵必须证明wrong WorkClass、wrong invocation kind或wrong typed payload零claim；
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
