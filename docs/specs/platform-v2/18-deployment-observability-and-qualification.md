# Platform v2 Deployment、Observability 与 Qualification 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-175 |
| 日期 | 2026-08-23 |
| 依赖 | 00～17 |
| 直接下游 | cross-review、implementation-plan |

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
- 一个隔舱饱和时API/Scheduler/critical-control和其他隔舱仍满足profile SLO；
- L1/L2覆盖所有expression opcode、type/stack/output bounds与unknown-field；L2真实PostgreSQL覆盖wrong Plan/Artifact/RunValue digest、
  Node version、lease/fence、跨tenant/run和Compute/Scope/Node/Job/Event原子回滚；L3多进程从Artifact Data RPC物化Plan并自行推进
  Branch/Map/Loop/Compute，禁止fixture注入observation；
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

基础部署与资格流程无未决设计问题。首个production CapacityProfile的数值需要在实现完成后通过
L4～L6测量冻结，Draft期间不声明为current capacity。
