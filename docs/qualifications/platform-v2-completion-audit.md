# Platform v2 spec00～18 完成度审计

状态：In Progress / repository and production gaps remain

日期：2026-08-27

本审计按 `00-overview.md` 的统一完成定义和 `implementation-plan.md` 四阶段 exit gate 核对当前工作树。
它记录可以复现的证据与缺口，不改变合同，也不把存在源码、测试或静态清单等同于 production behavior。

## 1. 结论

00～18均已完成CR-197 cross-review（历史CR-173～196结论保留）并处于Accepted，但没有任何一份可以推进到Verified或Archived。Phase 1的仓库内
实现与真实PostgreSQL门禁已闭合；Phase 2的production Orchestration、Model、Capability、Context与wait/Subagent主要L3链路已经闭合；
Phase 3的MCP subscription真实HTTPS、OAuth Cleanup/Egress删除链及Callback/token exchange多进程L3已闭合，仍缺外部Sandbox/Artifact资格。Phase 4 public API和15-role/19-pool静态部署闭包已完成，
完整observability及production-equivalent L4～L6仍未交付。

因此：

- `docs/current`继续描述旧current behavior；
- `implementation-plan.md`保持In Progress；
- 不生成通过的QualificationEvidenceManifest或production CapacityProfile；
- 不执行GitOps clean cut、规范归档或状态升级。

## 2. 已证明证据

| 范围 | 当前证据 | 结论 |
|---|---|---|
| 合同 | 00～18 CR-197 cross-review闭合；Plan v4 external leaf、MCP subscription durable execution、OAuth token TLS trust及恢复安全trace identity/header边界均冻结；generated contracts checker与CR-197 machine projection通过 | 合同及trace实现闭合，不证明production资格完成 |
| Persistence | schema contract v7、唯一`0001_platform_baseline.sql`；PG16/17 fresh baseline与事务/并发测试 | Phase 1 persistence闭合 |
| Rust workspace | workspace all-target/all-feature tests与Clippy `-D warnings`通过 | L1～L3范围内有效 |
| NATS/MCP | real NATS integration与外部TypeScript/Go MCP SDK interop通过 | 证明被执行的协议fixture，不证明production MCP Host部署 |
| Public API | `/v1` OpenAPI/owner schema、route负向conformance与root public API baseline通过 | public contract实现闭合 |
| Typed Plan materialization | Agent Revision冻结`typed_plan_artifact_id`与digest；发布事务校验Ready JSON Artifact/Verified Blob；Scheduler专用mTLS Data RPC以Run、Job lease、exact Plan Revision和ArtifactRef双重授权读取 | 闭合Scheduler物化输入与传输边界，不代表production Scheduler handler完成 |
| Typed Plan v4 wire | RuntimePlan保存closed dependency slots及全部external leaf payload，拒绝v1/v2/v3并验证slot kind、output producer、input reachability与bounded budget；fresh PG的phase2 Run kernel和真实coordinator既有路径通过 | L1/L2 wire与controller已闭合；Timer/Signal/HumanTask/ChildAgent、Model tool-result、Capability和Context的production component L3均有独立证据 |
| Candidate selection owner | `PolicyKind::Selection`要求非空schema v1 document且`rules_digest`绑定canonical bytes；共享纯evaluator实现only-candidate/ordered-first/route-hash、canonical candidate order与evidence digest；各owner按Run冻结exact Policy/Revision重算并拒绝伪造结果 | L1/L2 owner闭合，production Model/Capability/Context dispatch已在对应L3链路重验exact binding |
| 已有部署 | 11个chart覆盖全部15个ComponentRole、19个隔离pool；Gateway双role、Orchestration、Model、Capability Native/Remote、Context Native/Remote/Subscription、MCP Tool/Resource Host、Sandbox、Artifact三role及Security/Egress全局render门禁通过 | L1静态闭包；不替代live L4 |
| HTTP observability | shared bounded-label owner；全部19个ComponentRole workload pool、Sandbox两种process attestor及OAuth Cleanup Worker具备ready、`/metrics`及ServiceMonitor/NetworkPolicy，公网role另有request/outcome/latency；真实TCP fixture验证Prometheus text scrape和metric canary为零 | process wiring与component real-socket scrape闭合，不代表Prometheus deployment scrape或完整业务observability |
| Dashboard/alerts | 独立chart提供role-filtered process/HTTP、capacity、Orchestration及Outbox业务dashboard、13条symptom-first PrometheusRule和逐alert checked-in runbook；CI拒绝非法threshold、非HTTPS runbook、高基数/Secret label与缺失discovery metadata | 已有series的L1运营合同闭合；不替代完整业务SLI或真实alert delivery |
| Worker/queue telemetry | 9个LocalWorkerPools、Sandbox Controller、Artifact三role、MCP双Host与Egress 11个隔舱均从实际semaphore导出capacity；Management/Runtime API与Security Authority从各自SQLx pool导出PostgreSQL connection capacity；Orchestration另有claim/recovery和PostgreSQL Job/Outbox backlog/lag | Orchestration Job/shared Outbox及19/19 pool动态capacity L1 telemetry闭合；production scrape、完整dependency health与L5 saturation profile仍待外部证据 |
| Trace correlation | public W3C入口、Run/Invocation/Job/Task/Event/Outbox durable owner及首版MCP/Egress/Artifact/Sandbox/Security mTLS/UDS hop保持同一trace ID/new span；fixed public/internal spans的动态采集验证parent trace、per-hop span与context outcome，reclaim恢复原trace，provider与guest/storage边界不转发header | CR-197 machine/runtime、component L3连续性与动态correlation采集闭合；不替代production telemetry backend验证 |
| Telemetry redaction | production Rust source静态门禁拒绝identity、Secret、prompt/response、object key及URL进入structured tracing或插值日志；真实TCP metrics与真实loopback provider tracing动态注入payload/identity/token/query、`tracestate`及`baggage` canary，采集结果均为零且允许的bounded metadata存在 | source-level与component L3 dynamic metric/log/trace负向合同闭合；不替代RBAC/retention或production backend验证 |
| gVisor | Launcher RBAC/admission脚本、chart和fail-closed preflight已实现 | development静态证据；无真实runsc L4结果 |
| Qualification contracts | QualificationProfile/Candidate/Capacity/Evidence nominal type、closed schema与digest validator；live topology/workload preflight对照Candidate/Capacity并拒绝rollout、image、config、identity、安全和容量漂移 | 可验证证据形状与preflight行为，不证明任一外部门禁通过 |
| Runbooks | production dependency recovery与GitOps clean-cut手册已提交 | 操作准备完成，execution evidence pending |

最近一次完整仓库复核使用全新PG16数据库、NATS和all-feature workspace测试；工作树完成批次均按单一目的提交。
这些结果在代码或环境改变后必须由CI重新产生，不能长期当作release evidence复用。

r296为MCP Tool Host与MCP Resource Host各自安装构造期必选的真实RPC admission semaphore，并从同一owner导出fixed `rpc_requests`
available/used。permit在身份/trace授权后、业务decode前获取；饱和返回`ResourceExhausted`，drop后恢复available。closed配置/hard max、
owner/config tests、真实mTLS、受影响PostgreSQL fixtures编译、strict Clippy及MCP/observability部署门禁通过。动态capacity coverage达到17/19；
Security/Egress两个pool、production scrape、telemetry backend/RBAC/retention及L4～L6保持Pending。

r297从Security Authority唯一实际SQLx PostgreSQL pool导出fixed `postgresql_connections` available/used；不新增第二admission authority。
fresh PostgreSQL 16验证checkout/drop使used 0→1→0；unit tests、strict Clippy及Security/Egress、observability门禁通过。动态capacity coverage
达到18/19；Egress Broker、production scrape、telemetry backend/RBAC/retention及L4～L6保持Pending。

r298从Egress Broker 11个真实Semaphore owner导出Secret、Model、Capability、Context、MCP及subscription bridge capacity。OAuth/bridge owner
tests验证占用、饱和拒绝及释放恢复；真实HTTPS/mTLS、strict workspace Clippy及Security/Egress、observability门禁通过。19/19 pool动态capacity
L1接线闭合；production scrape、完整dependency health、L5 mixed-load/saturation profile、telemetry backend/RBAC/retention及L4～L6保持Pending。

r299使用两个全新PG16 baseline（主authority、独立Model conformance）、真实NATS和当前production process binaries完成串行workspace
all-target/all-feature回归，退出码0；两个外部S3测试显式ignored。workspace format、strict Clippy、doc tests及最新OAuth 8/8真实TLS/kill-recovery
复验通过。该批同时关闭Scheduling JSON-null候选污染、terminal transaction serialization重试、MCP trace、OAuth exact token binding/event kind、
数据库时钟timer和多进程fixture scoping缺口。本轮没有Model TLS NATS process fixture、外部S3/KMS、production scrape或L4～L6环境，故本审计
仍为In Progress，不生成release通过证据、不执行clean cut。

r300把最终release evidence validator与实际证据bytes绑定：每个manifest artifact link必须在显式artifact root下解析为同名普通文件，且
byte length与流式SHA-256必须匹配；缺失、symlink和内容漂移均拒绝。target tests、strict Clippy、contract与candidate pipeline检查通过。
这关闭了伪造自洽manifest即可通过最终CLI的仓库门禁缺口，但没有提供任何production artifact或推进L4～L6状态。

r301建立六种固定dependency与两种固定outcome的共享指标owner，并把Security Authority真实PostgreSQL repository结果接入同一metrics surface；
身份前置拒绝不会计为数据库失败，未安装依赖不能被动态创建。shared/Authority tests与strict Clippy通过。其余role的NATS、S3、KMS、Secret、
Egress及PostgreSQL真实调用接线仍是仓库内缺口，production scrape和L5 health profile仍是外部门禁。

r302把Egress Broker的Secret/KMS series接到七类实际AWS SDK请求返回边界；本地校验或容量拒绝不污染外部依赖失败计数，observer不接收任何
identity、provider、endpoint、ARN、error或Secret material。Secret/Egress tests、strict Clippy、redaction与部署门禁通过。真实AWS fault/rotation、
production scrape及L5 profile仍待外部环境；其他role的Egress/NATS/S3/PostgreSQL接线仍是仓库内缺口。

r303把Artifact三role的S3/KMS series接到KMS encrypt/decrypt/describe及S3 head-bucket/head/get/delete真实SDK返回边界；本地授权、presign、
binding、key、generation或limit拒绝不污染依赖计数，observer不接收业务或存储标识。Broker/三binary tests、strict Clippy、redaction及部署门禁
通过。Artifact PostgreSQL health仍是仓库内缺口；真实S3/KMS fault、production scrape与L5 profile仍是外部门禁。

r304以共享15秒只读`SELECT 1::bigint` sampler补齐Artifact Gateway、Data Worker read/work pool和Maintenance PostgreSQL health接线；sampler
不改变readiness、不暴露连接/SQL/error字段，并与各process shutdown/drain闭合。不可用pool、pre-cancel及三binary tests与strict Clippy通过；可选
真实database成功test已checked-in，但本轮没有运行中的本地PG16，因此该fixture未产生新证据。production PostgreSQL/S3/KMS scrape/fault与L5 profile仍待外部环境。

r305把Model Worker PostgreSQL pool接入同一15秒sampler，并在实际NATS TLS connect、publish+flush和drain返回边界记录fixed success/failure；
observer不接收server、subject、tenant/run、payload或error。两个observer与permit、worker、cancel、live-delta和HTTP组件共用既有JoinSet cancel/drain，
不改变readiness。真实连接失败、adapter、library/binary tests及strict Clippy通过；可选真实TLS NATS fixture已扩展为同时验证connect与publish成功观测，
但本轮未配置该外部fixture，故没有新增真实NATS/PG成功或production scrape证据。Model Egress流式RPC观测仍待后续独立批次。

r306把Capability Native/Remote各自独立business与critical-control PostgreSQL pool接到共享15秒sampler；四个probe只汇总到各process固定
`component_role + postgresql + outcome` series，不暴露database、pool、SQL或error。每个process把permit与两个DB sampler组成受监督任务，和worker/HTTP
共用cancel并在正常shutdown join；sampler意外退出使process fail closed且不改变readiness。shared adapter、两个binary tests及strict Clippy通过；本轮仍无
真实PG成功fixture或production scrape，Remote Egress/MCP调用观测留待独立批次。

r307把Context Native、Remote与Subscription三个restricted PostgreSQL pool接到共享15秒sampler；每个process仅导出固定
`component_role + postgresql + outcome`，不暴露database、pool、SQL或error。permit与DB sampler成为受监督任务，所有signal/worker/HTTP/sampler退出路径
均cancel并join其余组件；同时补齐Subscription此前异常分支未等待peer的问题，不改变readiness。shared adapter、三binary tests及strict Clippy通过；本轮无
真实PG成功fixture或production scrape，Remote Egress与Subscription MCP Host调用观测仍待后续批次。

r308把MCP Resource Host与OAuth Cleanup Worker各自restricted PostgreSQL pool接到共享15秒sampler，只导出固定
`component_role + postgresql + outcome`。Resource Host sampler进入RPC/HTTP cancel与bounded drain，Cleanup sampler在signal/HTTP/sampler退出时cancel并等待peer；
两者均不改变readiness，也不安装尚未接线的Egress series。两个adapter与binary tests、strict Clippy通过；本轮无真实PG成功fixture或production scrape，
MCP Tool/Resource/Cleanup的Egress实际调用观测仍待统一RPC observer批次。

r309把Sandbox Controller restricted PostgreSQL authority pool接到共享15秒sampler，仅导出固定`component_role + postgresql + outcome`；
probe不消耗Sandbox execution或Artifact response capacity。sampler与RPC/HTTP共用cancellation和原有shutdown deadline，任一组件异常退出都会cancel并等待peer，
readiness不变且不预装Artifact/attestor RPC series。adapter/binary tests与strict Clippy通过；本轮无真实PG成功fixture或production scrape，Artifact Broker与
node attestor实际RPC观测仍待后续批次。

r310把Callback API restricted PostgreSQL command pool接到共享15秒sampler，并附加到既有OAuth callback process metrics；仅导出固定
`component_role + postgresql + outcome`，不暴露database、pool、SQL、state或error。signal、HTTP server与sampler互相监督，正常shutdown共用既有grace，
超时中止残余任务；readiness与callback outcome语义不变，也不预装Egress series。adapter/binary tests与strict Clippy通过；本轮无真实PG成功fixture或
production scrape，OAuth Egress实际调用观测仍待统一RPC observer批次。

r311把Management/Runtime Gateway各自restricted PostgreSQL pool接到共享15秒sampler，并与已有connection capacity共用process metrics surface；
每个部署只导出自身固定`component_role + postgresql + outcome`，不暴露database、pool、SQL或error。signal、HTTP server与sampler互相监督，使用配置的完整
shutdown grace实际bounded drain，超时中止残余任务；readiness和HTTP/API语义不变。adapter/8个binary tests与strict Clippy通过；本轮无真实PG成功fixture或
production scrape，Runtime Artifact RPC observation仍待后续统一RPC observer批次。

r312补齐反向审计发现的间接SQLx owner：Orchestration Worker通过`PostgresConnectionBulkheads`持有business/critical-control两个pool，现均接到共享15秒
sampler并汇总为固定`component_role + postgresql + outcome`，不暴露pool、database、SQL或error；既有每秒Job/Outbox backlog/lag观测保持独立。signal、HTTP、
runtime-finished或sampler退出都会关闭runtime、HTTP、sampler和bulkheads，readiness不变且不预装Artifact RPC series。adapter/binary tests与strict Clippy通过；
本轮无真实PG成功fixture或production scrape，Artifact Scheduler实际RPC观测仍待后续批次。

r313把14-panel dashboard中的scheduler-only PostgreSQL panel扩展为按`component_role + dependency + outcome`聚合的六依赖概览，并以通用
`InsightPlatformDependencyFailureRatioHigh`替换scheduler-only alert。告警必须同时超过closed失败率与最小观测数，避免单次provider/tenant失败触发；runbook按固定
role/dependency分诊且禁止记录endpoint/database/subject/object/error/tenant字段。Helm负向阈值、alert inventory、panel expression与runbook锚点checker通过；该批只闭合
消费端合同，不产生production scrape或fault-injection证据。

r314为共享Egress RPC client补齐只接收fixed success/failure的transport observer；Model streaming、Capability HTTP/gRPC、Remote Context、MCP
OAuth/cleanup/Tool/Resource/subscription均只在实际tonic返回边界观测，本地编码/校验拒绝不污染计数，业务失败不冒充transport failure。真实mTLS成功与不可达端点
失败测试、strict Clippy通过。该批尚未把observer注入各production binary，故role Egress series、production scrape/fault与L4～L5仍为缺口。

r315把该observer注入production Model Worker并合入现有PostgreSQL/NATS process metrics surface；Model建连、stream read与cancel只导出固定
`model-worker + egress + outcome`，不暴露业务或错误字段。adapter/binary tests、strict Clippy及部署/observability/redaction门禁通过；无production scrape或
真实fault证据，其余Egress client role仍待注入。

r316只为production Capability Remote Worker安装Egress observer，HTTP/gRPC调用与取消映射为固定role/dependency/outcome并与双PostgreSQL sampler
同surface；Native路径保持PostgreSQL-only且显式无Egress observer。target tests、strict Clippy及双角色部署/observability/redaction门禁通过；无production
scrape或真实fault，其余Egress/MCP client仍待注入。

r317只为production Remote Context Worker安装Egress observer并与PostgreSQL sampler同surface；Native/Subscription保持PostgreSQL-only，实际查询
RPC仅导出固定role/dependency/outcome。四组target tests、strict Clippy及Context部署/observability/redaction门禁通过；无production scrape或真实fault，
其余Egress/MCP client仍待注入。

r318为production MCP Tool Host、Resource Host与OAuth Cleanup Worker注入Egress observer；Tool为Egress-only，Resource/Cleanup为PostgreSQL+
Egress。Tool/Resource/OAuth/cleanup/subscription实际transport只导出固定role/dependency/outcome。四组target tests、strict Clippy及MCP部署/observability/
redaction门禁通过；无production scrape或真实fault，Callback/Sandbox Egress client仍待注入。

r319为production Callback API OAuth exchange client注入Egress observer并与PostgreSQL sampler同surface；实际RPC只导出固定role/dependency/
outcome。binary tests、strict Clippy及Callback部署/observability/redaction门禁通过；无production scrape或真实fault，Sandbox Egress client仍待注入。

r320以静态清单确认七个first-release production Egress client均强制注入observer，新增no-op production构造会fail closed；其余no-op仅存在于shared
client测试、PostgreSQL component fixture及release明确排除的Firecracker/microVM provider。首发WASI/gVisor Sandbox无Egress client，故r319的Sandbox
待注入表述不构成release缺口。observability、Sandbox deployment与redaction门禁通过；production scrape/真实fault及L4～L5仍Pending。

r321在完整workspace all-target/all-feature门禁中修复rolling-summary测试的无关3秒owner lease时序耦合；测试专用lease改为30秒，production owner逻辑与
专用lease failure测试不变。修复后目标/全workspace tests、strict Clippy、format与doc tests通过，两个外部S3 fixture保持ignored；无外部或L4～L6新证据。

r322为首发Sandbox WASI/gVisor Executor接入Core NATS dependency observer，实际TLS connect、subscribe/flush、request/reply、stream closure与unsubscribe
只导出固定role/nats/outcome，本地校验和业务字段不进入指标。RPC/Executor tests、真实mTLS、strict Clippy及Sandbox部署/observability/redaction门禁通过；
可选真实NATS fixture本轮未配置，故无真实NATS或production scrape新证据。

r323以observability checker锁定全部first-release dependency owner及AWS/NATS adapter inventory，移除任一observer、sampler或production client注入均
fail closed。门禁通过；PostgreSQL/NATS/S3/KMS/Secret/Egress六类external dependency仓库内L1接线闭合，但production scrape/fault、其他domain backlog/
recovery series及L4～L5仍Pending。

## 3. Phase 1 审计

### 已满足

- nominal ID/owner/kind/problem/receipt registries与生成投影；
- 23张总表/22张业务表的单一未发布baseline，无Installation Release、ManagementOperation或SandboxJob表；
- shared Resource/Version/Deployment、Run/Invocation/Job/Task/Event/Receipt/Outbox/Artifact repositories；
- typed JSONB、canonical digest、size/unknown-field/tenant/CAS/lease/fence/Receipt测试；
- microVM、Managed stdio和Model Artifact路径不进入default/release composition。

### 仍需在release时重验

- schema baseline尚未发生production首次发布；发布前仍须确认目标数据库为空且不存在已发布candidate migration。

## 4. Phase 2 审计

### 已满足

- Resource Draft→publication→immutable Deployment→active binding与RunBindingsSnapshot；
- Run/Node/Subagent、Job/Task/Receipt、activation/admission race、lease/fence和safety scan repositories及tests；
- capacity-aware coordinator、lease-fenced executor、orchestration/artifact/sandbox safety driver库；
- ModelTurn、CapabilityInvocation、Task与Inline hard-limit domain/repository闭包。

### 当前证据边界

本节较长的逐项记录保留早期实现轨迹；以下后续证据覆盖其中“仍待”表述：r199闭合Timer/Signal/HumanTask/ChildAgent，r208闭合Native
Capability，r217/r221闭合Remote HTTP/gRPC/MCP ToolsCall，r240/r241/r242/r243闭合Native/Remote Context与Orchestration resume，r233/r244
闭合Model provider及tool-result整链。它们均为各自声明范围的L3，不自动提升为L4～L6。

1. 独立Orchestration Worker binary、process config、startup/readiness/drain、restricted Helm Deployment和critical-control safety composition已闭合。
   fresh PostgreSQL 16 r199已完成Timer→Signal→HumanTask→ChildAgent→Return五进程kill/recovery，parent/child终态、typed child output及唯一finish Node均通过。
2. `StartedOrchestrationJobHandler`已有正式materialize→commit/handoff生命周期adapter；PostgreSQL durable Plan store已闭合
   Start/Compute/Branch/Fork/Join/Map/Loop/ErrorBoundary成功控制路径的durable fact读取、Inline RunValue物化、精确mutation-slot
   规划、通用或derived fenced commit与retry handoff；FailNode现在也从locked facts推导ErrorBoundary/structured-exit/wake/
   sibling-cancellation槽，并在失败owner transaction重验expression evidence。Inline Return/Raise terminal、ChildAgent和HumanTask已接入该store；
   Timer/Signal durable wait与Model/Capability/Context dispatch均已接入；Artifact-backed RunValue的Scheduler侧Data RPC materializer、exact leased resolver、Broker reader与读前/读后
   authority现已接入，但Orchestration Worker/Artifact Data Worker的production-equivalent RPC kill/restart仍待L3验证，且Model/Capability/Context下游role尚未全部闭合；
   exact typed-plan Artifact的Scheduler专用Data RPC已闭合canonical envelope、
   exact workload identity、Job lease/Run/Plan/Artifact PostgreSQL authority、读取前后双重授权和deadline/stream backpressure；
   Scheduler侧也已用当前fence从PostgreSQL解析descriptor并完成canonical JSON、Plan limits和semantic digest复验，但其余external leaf
   及完整Subagent terminal lifecycle的production handler仍未实现。
   closed expression owner、纯确定性evaluator、Plan节点与HardLimitProfile v5消费现已落地，production driver API也不接受外部
   observation；ChildAgent deferral现要求exact Plan v4、冻结slot Selection Policy和candidate evidence，并在同一SERIALIZABLE owner
   transaction中按当前Scope重解析input/可选route RunValue、锁定Policy/Revision当前gate且不依赖active head、重跑共享evaluator；fresh PostgreSQL
   覆盖dispatch facts、only-candidate正向、伪造input/classification/selected Deployment整批回滚。PostgreSQL durable Plan store现已消费
   `CreateChildRun`，在事务外仅物化可选route正文、生成typed child identities/evidence并调用上述owner command；独立Worker的
   parent→child→parent terminal L3已在r199闭合。HumanTask owner现从exact Plan v4重验definition、response schema与timeout，runtime
   store消费`CreateDurableWait::HumanTask`并生成共享Task mutations；Task first-winner transaction现把成功response RunValue绑定到当前Scope，并在
   Node payload持久化owner-derived succeeded/declined/timed_out/cancelled事实，恢复controller会重验该事实后resume或稳定失败，不再重复创建Task；
   fresh PostgreSQL r77保持response/expiry/late response/first-winner/replay并验证response Scope binding；完整Task wait/resume L3已在r199闭合。
   Timer owner不再接受caller-supplied WakeContract，而是从exact Plan v4和数据库时间派生due time、generation与仅Timer wake source；过早wake、
   Plan digest漂移和first-winner均fail closed，runtime store已消费`CreateDurableWait::Timer`。fresh PostgreSQL r79通过，独立Timer scanner/process L3已在r181/r199闭合。
   CR-176 Scope data-port environment owner、root/child binding、bounded lexical lookup、exact Inline/Ready Artifact
   authority读取与stale fence拒绝已经实现并通过fresh PostgreSQL Phase 2。derived commit现已对Branch/Map/Loop在事务内重验
   input/evaluation/classification evidence并重新执行pure evaluator，fresh PostgreSQL覆盖Branch正向提交和伪造classification整批回滚。
   Compute现已在同一事务写immutable output RunValue、owner-derived classification与Scope environment CAS，再提交既有Node/Job/
   Receipt/Event/Outbox；fresh PostgreSQL同时覆盖output ID冲突整批回滚。Map现在按冻结的batch cursor为每个item写immutable
   RunValue，以owner-derived classification和exact Plan v2 item port绑定新MapItem Scope，并与Scope/Node/Job/Receipt/Event/Outbox
   原子提交；fresh PostgreSQL覆盖多batch、动态Scope隔离与Receipt replay。Loop body settlement现在从当前Scope的exact body output
   复制immutable carried RunValue，预建下一open Scope并切换continuation；false condition原子关闭该Scope并从固定父Scope激活exit，
   fresh PostgreSQL覆盖ID冲突整批回滚、classification/value复制、Scope切换和Receipt replay。数据库现在还能从冻结pending
   payload与exact Scope集合重建Map settlement/Join observation，并从与commit共用的shape推导精确activation、pending、Scope、wake、
   rollover及active-remainder cancellation槽；fresh PostgreSQL覆盖Quorum cancel集合与已提交Join facts。手工注入
   `ControllerObservation`仍不计production证据。fresh PostgreSQL runtime fixture现已把真实claim/start、lease heartbeat/handoff/recovery、
   exact Typed Plan authority读取与canonical materialization、数据库派生Start facts及fenced Start→Return activation串成同一条
   coordinator链；独立Orchestration Worker binary与wait-node/Subagent多进程crash/restart证据已在r199闭合，但Model/Capability/Context下游进程仍待接入。
   CR-177的L1/L2 owner规则已实现，L3完整process boundary仍待完成。
   CR-178的Plan version 2、exact Map item port owner validation、version 1/wrong producer L1负向、每item RunValue/MapItem Scope原子写及
   L2 batch/replay fixture已实现；process crash/restart的L3 fixture仍待production handler闭合后完成。
   CR-179的exact pair producer/schema/region L1与两轮rollover/Scope复用/不串值/false-exit L2已实现，并证明所有iteration Scope保持
   同一root controller owner、词法深度不随轮次增长；process crash/restart L3仍待production handler闭合后完成。
   CR-180的Plan version 3 wire、Return/Raise exact terminal port、producer/reachability负向，以及Agent Revision完整input/output/error
   `ClosedJsonSchema` authority已经实现；runtime Plan绑定重验terminal port与exact output/error schema digest。Plan terminal owner transaction
   现已从open lexical Scope重解析existing RunValue并重验value/schema/content/classification与正文，以同一事务提交Run/Node/Scope/Job、
   quota、Receipt/Event/Outbox；fresh PostgreSQL覆盖Inline Return、wrong value整批回滚和Receipt replay。正式PostgreSQL durable store与
   coordinator现已把claim/start、Plan materialization、Start commit、Artifact-backed Return RunValue的leased resolver/reader/canonical
   materialization和Run Succeeded串成同一链，并断言exact output value。Raise safe Failure正向提交及不安全Failure拒绝/整批回滚
   fixture也已在fresh PostgreSQL通过。CR-180 L1/L2 terminal证据已闭合；独立Scheduler与Artifact Data Worker之间的L3 kill/restart
   fixture仍未完成，因此尚不能计入完整Phase 2 production terminal证据。
3. 独立Capability Native/Remote、Context Native/Remote production process、role-scoped pool/permit和deployment已闭合；跨role容量隔舱仍须L5实测。
4. r243/r244已分别贯通Remote Context→Return及Model→Capability→Model→Return；production-equivalent network/identity与滚动故障仍归L4。

## 5. Phase 3 审计

### 已满足

- Artifact Gateway/Data Worker/Maintenance binaries、role grants、mTLS调用边界和Helm清单；
- Context/Dataset/Text2SQL domain、repository与negative fixtures；
- remote Streamable HTTP MCP协议/OAuth/Task/subscription实现与SDK互操作fixture；
- restricted WASI runtime、gVisor Controller/Launcher/guest/attestor协议和静态准入闭包；
- Model provider/turn/adapters、Inline-only与独立Model Worker清单；
- Security Authority与Egress/Secret broker binaries及隔离清单。

### 仓库内或外部缺口

1. MCP Host production binary/Helm与ToolsCall process L3已闭合；subscription的production Resource Host/Context Worker/独立Egress进程、真实TLS
   initialize/list/read与三轮kill/recovery已由r281闭合component L3；OAuth Cleanup Worker与独立Egress的mTLS Secret delete/lease reclaim已由r284闭合。
   r286又以真实独立CA HTTPS token endpoint、mTLS Egress RPC和Callback owner关闭token-store后/commit前双进程强杀恢复，并证明one-time code不重发；
   Context/MCP/Egress lane saturation、bundle/config rollout和live cluster identity仍归L4～L5。
2. Context Native/Remote和Capability Native/Remote production composition已闭合；Dataset build/Text2SQL、Artifact和各外部依赖仍须按
   production qualification matrix取得适用的真实协议、故障与隔舱证据。
3. S3/KMS/Secret Manager只有adapter/fixture和deployment contract，没有production-equivalent fault/rotation/restore证据。
4. gVisor没有真实`RuntimeClass=runsc`多节点执行、escape/cleanup/process-kill/watch-restart/node-loss证据。
5. 单lane saturation对其他lane与critical-control的production profile SLO影响尚未测量。

## 6. Phase 4 审计

### 已满足

- minimal `/v1` Resource/Run/Task/Artifact/MCP binding/SSE contracts与Gateway实现；
- If-Match、OIDC principal、Receipt/problem/cursor/body/rate/quota负向合同；
- 已存在role清单的ServiceAccount、NetworkPolicy、PDB/HPA、digest与security context静态检查；
- shared低基数HTTP telemetry owner，以及Public Gateway、Callback API的request/outcome、latency histogram、ready指标和受NetworkPolicy约束的ServiceMonitor；
- durable MCP OAuth PKCE cleanup worker已进入workspace/runtime image，并具备独立Deployment、PDB、default-deny与精确PostgreSQL/Egress网络边界；
- OAuth Cleanup Worker在上述authority组合完成后才开放shared HTTP readiness/metrics，且具有独立ServiceMonitor与Prometheus-only ingress；
- production QualificationProfile、Candidate/Capacity/Evidence validator、拓扑preflight和资格运行手册。
- commit-SHA pinned production candidate workflow、确定性Candidate/WorkerManifest生成器、exact runtime/guest image签名、SPDX SBOM、
  GitHub provenance及传递闭合的signed release-bundle index。

### 仓库内缺口

1. 15个ComponentRole已由19个独立workload pool闭合；Candidate image与`deployment_config_digest`已进入全局render/live preflight门禁，
   但真实cluster startup/readiness、mTLS、RBAC和NetworkPolicy enforcement仍未执行。
2. 全部19个ComponentRole workload pool及Sandbox process attestor已有shared process metrics；Orchestration、Model、Capability Native/Remote、
   Context Native/Remote/Subscription、Sandbox WASI/gVisor共9个pool已有动态permit指标，Orchestration另有claim/recovery指标，production tracing/log字段已有
   静态脱敏门禁；Orchestration现另有PostgreSQL authority的due/expired-lease Job及due/expired-claim/dead Outbox backlog/lag与observation health；六类
   external dependency的仓库内L1 owner接线已闭合，仍缺其余role domain backlog/recovery series、production Prometheus scrape及L5 mixed-load/saturation
   profile证据。
3. 全部19个pool及Sandbox process attestor已有ServiceMonitor；process/HTTP/Orchestration/Outbox dashboard及逐alert
   runbook已扩展到14个panel和13条alert，包含operational capacity、Orchestration Job、shared Outbox lag/dead queue及通用dependency failure ratio；其他role
   domain backlog/recovery与saturation对应的panel/alert仍待指标owner接线后补齐。
4. 全部role的render、digest image、config digest、PDB/HPA、resource、default-deny与ServiceAccount互斥已有全局checker；DB role/pool、
   mTLS与live identity enforcement仍须production-equivalent L4验证。
5. 可重现的signed image/SBOM/provenance candidate producer已实现并由CI静态/合同测试约束；尚无实际registry run artifact、GitOps
   environment repository输入及人工promotion证据。

### 外部门禁

- production-equivalent多节点Kubernetes、独立WASI/gVisor node pool、exact runsc与支持范围内kubectl/server版本；
- L4 RBAC/mTLS/NetworkPolicy/admission与真实协议/故障矩阵；
- L5 mixed load、lane saturation、SLO/error budget和不少于86,400秒soak后冻结CapacityProfile；
- L6 signed supply chain、upgrade/rollback、backup/restore、GitOps rollout/rollback与人工promotion；
- clean `/v1` replacement后更新`docs/current`，再将00～18推进Verified并归档。

## 7. 下一实现顺序

按上游到下游执行，且每批通过后提交：

1. 补其他role domain backlog/recovery/permit业务指标；
2. 为新增业务series补dashboard、symptom-first alerts及逐alert runbook；
3. 在受保护CI environment实际运行signed candidate producer并把exact bundle交给GitOps environment repository；
4. 外部L4～L6、GitOps clean cut、current文档与规范归档。

如果实现发现domain port不足以支持production handler，必须先按02→06/07/09/10→17/18修订合同并重新cross-review，
不得在binary中以自由JSON、in-memory authority或host process execution绕过缺口。
