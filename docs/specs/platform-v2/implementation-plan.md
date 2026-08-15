# Platform v2 依赖驱动实施计划

| 属性 | 值 |
|---|---|
| 状态 | Active / CR-165 Architecture Revision Gate |
| 日期 | 2026-08-15 |
| 输入 | 既有Accepted实现基线；CR-165相关00/02～10/12/14～18、Cross-review与ADR-0001当前为Draft |
| 目标协议 | `insight.platform/v1`，公共路由 `/v1` |
| 替换方式 | Clean replacement；无兼容层、双写、旧 migration 或 runtime fallback |
| 当前行为 | cutover 前仍以 `docs/current` 为准 |

## 1. 已完成 foundation

以下工作已经完成，可作为后续实现输入：

1. 根 `AGENTS.md` 冻结规范、持久化、JSONB、migration、测试和 Git 规则；
2. 首轮00～18全量cross-review撤销了旧migration 1～35与177表设计；CR-165已重新打开相关合同，不沿用“全部Accepted”结论；
3. 02、03、10 重写为 Resource/Version/Deployment、Job/Task/Receipt/Event、Invocation 合同；
4. 04～17 的专用表族全部改为共享聚合映射；
5. 当前实现证据是23张总表/22张业务表/schema contract v6；Draft ADR目标是24张总表/23张业务表/v7；
6. 当前单一`0001_platform_baseline.sql`、schema contract和migration verifier已交付，但尚未包含目标Installation Release authority；
7. shared PostgreSQL repository 首片：typed bounded payload、Resource publish/deploy、Job claim/fence/commit、
   Receipt/Event/Outbox、Quota reserve/settle；
8. PostgreSQL 16 fixture 证明 fresh/replay migration、精确表集、stale fence rollback、idempotency replay/conflict、
   Event/Outbox 原子性和并发 quota oversubscription 防护。

这些证据只关闭 persistence foundation，不把业务 API 或执行面声明为已实现。

## 2. 实施原则

- domain 先交付 closed Rust state machine 与 command，再交付 repository mutation，再接 API/Worker；
- 一个 command 在 caller-owned transaction 完成 current state、Receipt、Event、Outbox 和 quota ledger；
- NATS 只传 wake/event projection；所有丢失、重复和乱序都由 PostgreSQL scan/Receipt 收敛；
- 新 ResourceKind/WorkClass/TaskKind/EventKind/ReceiptKind 默认扩展 closed type/payload，不扩表；
- JSONB payload 必须使用 `TypedPayload` 等价 nominal type、schema version、canonical digest 与 hard limit；
- clean-cut基线首次发布前，Reviewed/Accepted的schema修订直接原位重写单一`0001`、schema contract与fresh fixture；首次发布后才从
  `0002`开始forward-only。任何阶段都不得恢复旧表或compatibility view；
- 每个阶段都必须保持 crate tests、strict Clippy、schema contract 和真实 PostgreSQL fixture可判定。

## 3. Phase 1：Identity、Security 与 Registry

范围：02、04、05、09、11、12、13、14、16。

交付：

- Tenant/Principal membership、SecretBinding、Policy Resource 与 permission evaluator；
- generic Resource draft/validate/publish/activate/suspend command；
- Deployment exact binding closure 和 RunBindingsSnapshot builder；
- Agent/Skill/Capability/Context/MCP/Model/Sandbox 的 closed Resource payload types；
- Artifact-backed authoring package、validation Job 与 immutable ResourceVersion；
- management domain ports，不开放 public route。

退出门禁：跨 tenant/FK/permission/Secret canary negative fixture；所有 ResourceKind 共用同一 lifecycle repository；
增加一个 ResourceKind 不增加表。

## 4. Phase 2：Durable Run Kernel 与 Scheduler

范围：03、06、07、08。

交付：

- root Run admission、RunBindingsSnapshot v2、Run/Node/Value current state；root固定current Installation Release/Candidate binding并校验
  全部Model candidates，child逐字段继承parent historical binding并对自身全部Model candidates重验；
- typed plan controller、branch/fork/join/map/loop/error boundary；
- Job ready/claim/start/heartbeat/retry/wait/recovery/cancel/timeout；
- Task approval/input first-winner；
- SchedulerState WDRR、quota bundle、critical-control reserve 与 bounded safety scan；
- Subagent child Run relation、ancestry、budget 与 cancellation propagation。

退出门禁：多 runtime concurrent claim、stale epoch、kill window、lost wake、retry limit、cancel/timeout/result race、
waiting release permit、fairness starvation simulation 和 database connection bulkhead。

## 5. Phase 3：Artifact 与 Invocation Domains

范围：10、12、15、16。

交付顺序：

1. Artifact prepare/upload/verify/finalize、ArtifactLink grant/reference/hold/provenance；
2. generic Invocation admission/current state；
3. Capability synchronous/deferred/input/progress/reconcile；
4. ModelTurn request/stream/tool-intent/usage；
5. Context query/item/citation/Text2SQL read-only path；
6. Artifact scan/rescan/delete/GC Job。

退出门禁：Ready-only Artifact、object generation、callback/poll first-winner、uncertain Effect、schema validation、usage quota、
cross-tenant citation/link/grant、shared Blob delete closure。

## 6. Phase 4：Execution Integrations

范围：09、13、14、16。

交付：

- Native/HTTP/gRPC Capability adapters；
- MCP Host transport、OAuth、session、discovery、Task/Elicitation/notification；
- Model provider adapters；
- Sandbox Gateway/Controller/Executor service；
- audience-isolated Model Artifact Broker与Sandbox Artifact Broker；前者只提供Model RPC，后者只提供WASI+microVM RPC，二者分别拥有
  进程/Deployment/ServiceAccount/DB pool/permit；
- HardLimitProfile version 4与SandboxPackage runtime bundle发布边界；
- policy-selected WASI、gVisor 或 microVM backend；
- executor Pod/节点池与 API/Orchestration 完全隔舱；
- callback ingress authentication、bounded body Artifact 与 Receipt dedupe。

退出门禁：backend conformance、SSRF/OAuth/Secret、sandbox escape boundary、process kill、remote timeout/late callback、
one workload saturation 不影响其他 work class。

当前Capability Worker执行组合已交付：PostgreSQL claim事务返回exact Deployment/Implementation/Input、physical attempt、lease与
WorkerProcessGeneration fence；`ClaimedCapabilityExecution`只在Running Job与完整fence重验通过后生成credential-free adapter request。
Worker通过process-installed dispatcher执行Native/HTTP/gRPC/MCP adapter，并只经`CapabilityExecutionAuthority`提交
`CommitCapabilityOutcome`或cancellation outcome，adapter不接触SQL或直接修改Invocation。成功、永久失败、安全重试和不确定结果均复用同一fenced
Receipt/Event/Outbox/quota事务；NonIdempotentWrite的dispatch后失败强制进入Uncertain而非重试，attempt耗尽的retryable failure转为
terminal failure。claim reserve ledger identity与terminal settle ledger identity是两组不同ID，组合层在I/O前拒绝复用。

Model Worker的独立执行组合现由`insight-platform-model-worker`承载：只在`model-worker`/`WorkClass::Model`精确manifest下，先预留
process-local business permit再发有界PostgreSQL claim，并把返回的Job、WorkerManifest、lease token/generation、request digest和四条
reservation ledger identity逐项回绑。request materializer、Provider adapter host与output materializer都在permit内执行；Provider I/O期间按
HardLimitProfile heartbeat续租，heartbeat只旋转Job optimistic version，已规范化结果随后用新fence提交而不会仅因续租重放付费请求。
进程drain超时要求终止整代Worker，使未完成lease由恢复扫描接管。Inline request/output实现已有明确上限；当冻结的Provider响应上限无法由
Inline承载时，在Provider dispatch前提交`model_output_artifact_required`拒绝，不产生调用或保守计费。Artifact-backed request现由生产进程
通过独立versioned Model Artifact Broker RPC读取：`ArtifactModelBrokerService`只暴露一个closed Model read方法，exact Model Worker URI SAN在
  进入authority前完成门禁，Worker再逐片复验bounded stream。Artifact-backed output的目标合同正由CR-165重新cross-review，尚未冻结或交付。
  Model stream中通过完整fence校验的text delta现在被编码为credential-free canonical内部envelope，并经
双重message/byte有界、non-blocking队列投影到TLS/mTLS NATS tenant/run scoped subject；容量permit保留到有界批次flush结束，不能被NATS
客户端内部缓冲绕过。tool argument与Provider metadata不会进入该通道，
NATS断连、背压或单帧超限只丢弃live observation，不阻断PostgreSQL中的ModelTurn/Job执行。20项adapter、11项worker、7项Model Artifact Broker与
2项process-config unit通过；环境提供真实TLS NATS fixture时还会验证跨进程发布。
因此该组合关闭执行驱动缺口，但不关闭CR-132、CR-148或Phase 4。

Model Worker的独立候选进程与Kubernetes隔舱也已交付：`platform-model-worker`启动时复验canonical config digest、exact
`model-worker`/Model WorkerManifest及OpenAI Responses、Anthropic Messages两个process-installed adapter descriptor，使用独立bounded
PostgreSQL pool做schema verify/claim/heartbeat/commit，并以Model Worker URI SAN的独立mTLS客户端分别访问Egress Broker与Model Artifact Broker。候选镜像已包含该binary；
独立namespace/ServiceAccount/Deployment/PDB/HPA/default-deny NetworkPolicy只开放DNS、exact Egress/Model Artifact Broker pod、PostgreSQL和配置allowlist中的NATS
TLS端口，禁止Service/Ingress、
云Provider credential、Kubernetes API token及直接Provider客户端。静态部署门禁含错误副本、mutable image、空PostgreSQL CIDR及非法HPA的负向渲染。
Model durable control也已进入同一候选进程：bounded PostgreSQL safety scan只发现当前WorkerProcessGeneration仍持有lease的
`Cancelling` ModelTurn/Job，使用保留的critical-control permit调用Egress exact-generation cancel，再以旋转后的Job fence提交
`CommitModelCancellationOutcome`。Egress重试失败不提交terminal；`Unsupported`或畸形结果fail closed；已dispatch但无法取得可信usage时按
admission token/cost ceiling生成Reconciled保守结算，late completion仍由first-winner fence拒绝。该路径复用ModelTurn/Job/Quota/
Receipt/Event/Outbox，不增加表或migration。当前进程还把exact text delta投影到上述非权威NATS内部通道；公开SSE消费、断线后的durable
terminal校准与live-gap/backpressure资格仍属于Phase 5/6。进程已组合Brokered Artifact-backed request materializer，output materializer仍只支持Inline；
Artifact-backed output、real-process Provider/kill/saturation资格仍未
闭合，因此这只是production-shaped候选组合，不是Phase 4或Candidate资格结论。

durable control winner现由`ControlledCapabilityExecution`与原claim共同构造cancel job：只旋转Job optimistic version，tenant、Invocation、Job、
physical attempt、lease generation/token、WorkerProcessGeneration和Deployment/Input必须保持exact，旧generation在adapter I/O前fail closed。
Native adapter使用process-installed cancel port，HTTP/gRPC使用Egress exact live-request cancel；transport确认永不伪造no-effect proof，write Effect
无可信proof时进入ReconciliationRequired。取消在原execution deadline后仍可于由frozen backend total-timeout派生且受平台hard limit约束的
cleanup window提交同一fence evidence。

12项Capability adapter/worker unit、8项Invocation unit、29项Egress unit和fresh PostgreSQL 16端到端fixture实际通过；后者覆盖
`claim -> exact adapter_job -> Native dispatcher -> PgRepository authority -> RunValue/Receipt/Event/Outbox/quota settle`及replay，
也确定性覆盖`durable control -> rotated fence -> Native cancel -> write reconciliation -> quota settle`及cancel/completion first-winner，
并证明错误reserve/settle ID由组合层fail closed。该切片不增表或migration。HTTP/gRPC Egress现已有HTTPS-only exact catalog、DNS全量
public-IP校验与连接pinning、SSRF/no-proxy/no-redirect、late Secret resolution、bounded HTTP/2 gRPC framing/response和exact-generation cancel，
但真实远端服务、Secret Manager/TLS/mTLS production composition、callback ingress、MCP/Model/Sandbox剩余provider与独立
Pod/NetworkPolicy资格尚未完成，
因此Phase 4保持进行中。

## 7. Phase 5：Public `/v1`

范围：17。

交付：

- Management draft/validate/publish/deploy/suspend/operation；
- Runtime run/admission/query/result/control/task/artifact；
- Installation Release GET/promote/rollback，使用installation权限、strong ETag、Idempotency-Key、bounded preflight与final CAS；
- stable ApiProblem、ETag、Idempotency-Key、cursor与closed GatewayRateLimitProfile；
- Event/Outbox 到 SSE durable envelope，live delta 单独通道；
- OpenAPI、JSON Schema、protobuf 与 SDK fixture。

退出门禁：API 不拥有第二套状态；public sequence strictly per Run；cursor 每次读取重新授权；unknown field/type fail closed；
慢 SSE client 不阻塞 outbox 或 runtime control。

## 8. Phase 6：Deployment 与 Qualification

范围：18。

按 Gate A～G 执行：Contract、Functional、Security、Recovery/Chaos、Capacity/SLO、24h Soak、DR Restore。

Gate A前先交付并验证Candidate/Release manifests、唯一InstallationReleaseState、24表/v7 fresh baseline、active Model集合4096上限/
256分页scan、promotion/rollback/activation/root Run final CAS及无fake tenant的Receipt/Event/Outbox scope；这些是qualification输入而非Gate结果。

只有同一 CandidateManifest 的全部 Gate 有效，才能生成 ReleaseManifest。任何 migration、image、config、policy baseline 或
hard-limit profile 改变都会使受影响证据失效。

## 9. Phase 7：Clean Replacement

前提：Phase 1～6 全部完成且用户明确授权 cutover。

顺序：冻结写入窗口、最终 backup、部署新 schema/app、切 `/v1` ingress、验证 canary/invariant、移除旧服务和 fallback、
更新 `docs/current`。不迁移旧 Run/Conversation 数据，不 dual-read/dual-write，不 down migration。

## 10. 变更批次

每个批次至少包含：

- 受影响 spec/ADR；
- closed Rust type 与 transition tests；
- repository command 和 real PostgreSQL positive/negative/concurrency fixture；
- Event/Receipt/Outbox 与 quota 影响；
- API/worker adapter（若进入该阶段）；
- `cargo fmt`、tests、strict Clippy、schema contract check；
- 对表数、payload bounds、index/claim query 的回归检查。

禁止以“先建空表以后补 FK/owner”“先存任意 JSON 以后补 schema”“先发消息以后补 outbox”拆分批次。

## 11. 当前下一步

Phase 1 的 generic Resource lifecycle、Security/Policy/Secret 与 closed registry 已落在共享 23 表 baseline；Phase 2 的
domain/runtime functional exit 已关闭，仍不等于 Phase 6 qualification 或 Verified。
CR-110已经关闭跨batch Map的即时停止缺口：`fail-fast`与`bounded-error-count`使用Pending durable admission barrier，停止条件
winner同事务取消活动sibling、冻结partial settlement且不创建未准入Scope/Job；真实PostgreSQL 16 fixture覆盖三项输入/两项批次、
quota结算、exact replay与最终Run失败，`all-settled`继续使用有界流水化。

CR-111已经关闭expired/deadline safety drive缺口：HardLimitProfile version 3独立冻结`recovery_batch`与
`recovery_shards`；`insight-platform-runtime`使用四条独立transient high-water cursor、jitter timer和critical-control permit/pool
驱动expired lease、due retry、convergence及expired Task。真实PostgreSQL 16 fixture覆盖满页cursor推进、短页回绕、错误shard
隔离、lease recovery，以及business本地permit和数据库connection同时饱和时critical-control恢复仍推进；schema仍为23表/单一
baseline migration。该证据关闭功能缺口，但不替代Gate E容量资格。

CR-112已经关闭生产orchestration executor lifecycle缺口：`LeaseFencedOrchestrationExecutor`用同一generation fence提交atomic
start、profile heartbeat+jitter和最新version terminal commit；coordinator Draining先广播shutdown token并尝试durable handoff，
grace超时才abort本地future。真实PostgreSQL 16 fixture覆盖start、heartbeat、business connection饱和下critical-control retry
handoff、quota/active permit释放及safety promotion；无wake周期drive和10000轮mixed-cost WDRR模拟分别覆盖lost wake与有界
starvation窗口。

CR-116 关闭了 Phase 2 最后的开发期并发/公平/隔舱功能门禁：fresh PostgreSQL 16 fixture 创建 50 active Runs、5 个 tenant，
从同一 integration-test executable 派生 4 个独立 OS worker process；PostgreSQL shared advisory barrier 保证所有进程到齐后才
同时竞争同一 orchestration fairness head。fixture 验证 50 个 Job 各有一个 lease、至少两个 worker 实际获胜、每 tenant 的
durable WDRR `successful_claims` 精确为 10、Run active-work 与 quota reservation 总量均为 50。另在 orchestration business
connection pool 100% 占用时，对独立 Sandbox-role pool 连续取 20 个 probe 并检查 p95 不超过 250ms，同时验证本 role 的
critical-control reserve 可用。该 fixture 使用真实进程、真实 pool 与真实数据库 authority，但不是 CandidateManifest、30 分钟
混合负载或 production-equivalent topology，因此只能关闭 Phase 2 functional exit，不能计入 Gate E 或把 Phase 2 标记为 Verified。

Phase 3 已按第5节顺序交付并关闭 functional exit。任何发现baseline无法表达的独立生命周期仍先回到ADR审查，不直接增加领域表；
Phase 5 前不提前开放公共Artifact/Invocation `/v1`行为。

CR-117/CR-118/CR-119关闭了Artifact事实归属、retention bootstrap与Blob安全域/并发复用缺口。`insight-platform-artifacts`现拥有closed prepare、
CompleteUpload、Begin/CompleteVerification与FinalizeAndReference command/state machine，以及caller-owned transaction port；
PostgreSQL adapter用单一事务提交command Receipt、`artifact.write`授权、exact Retention revision、staging-byte quota、
Staging/Uploaded/Verifying/Verified/Ready Artifact与Blob、ManagementOperation、UploadGrant/Reference Link和Event/Outbox。
fresh PostgreSQL 16 fixture覆盖optional digest/media、grant token/generation、object generation/size/digest、stale CAS、
exact replay/conflict、跨tenant/permission/quota拒绝、顶层rollback、bearer token不落库、Ready-only ArtifactRef及quota
reserve→settle。相同tenant/backend/storage/classification/retention/encryption安全域的相同content由事务级content-key fence
收敛为一个verified Blob；候选对象原子进入Deleting并创建exact-generation cleanup Job，不同安全域不复用。fresh fixture同时
覆盖顺序复用、双事务并发first-winner与跨安全域隔离；baseline仍为23表/单一`0001`，schema contract version 6。

CR-120关闭了Artifact Job machine contract与实际持久化的漂移：Artifact jobs统一使用closed `WorkClass::Artifact`；面向调用方的
deletion Job由exact ManagementOperation拥有，候选generation cleanup Job由exact InternalBlob拥有，通用Job claim/start不再接受
`artifact_io`、`artifact`或`artifact_blob`等未注册owner组合。ArtifactLink hold/provenance/reference release与shared Blob
两阶段删除transaction closure也已交付：exact Retention revision执行GC grace和approval，Reference/Hold/lineage fail closed；同Blob
aliases在锁内选择`artifact_only`或`blob_generation`，worker completion受Job lease/epoch/token、exact object generation、backend receipt和
absence evidence约束。fresh PostgreSQL 16 fixture覆盖错误approval、live Reference、错误alias/generation evidence的全事务rollback，
Artifact-only tombstone、last-alias物理删除、exact replay及Receipt/Event/Outbox原子性。

CR-121关闭了generic Invocation admission的frozen cancellation与Approval binding缺口。`insight-platform-invocations`现拥有pure
admission/current-state decision与caller-owned transaction port；PostgreSQL adapter在Run/Node/Deployment/Interface/Implementation/
Policy/RunValue/Artifact锁和授权通过后，原子提交Invocation、optional Approval Task与ArtifactLink、Receipt、Event和Outbox。
fresh PostgreSQL 16 fixture覆盖inline与Ready Artifact输入、exact replay/idempotency conflict、跨tenant与permission拒绝、错误candidate/
eligible-rule拒绝，以及approve/reject并发first-winner；non-idempotent write且无idempotency proof时attempt limit冻结为1。baseline仍为
23表/单一`0001`。

该当前切片仍以generic `ResourceId`字段承载Invocation的Approval/Input引用，允许normalized backend input request携带Input ID并做runtime
`ResourceKind::ApprovalTask | Interaction`检查；CR-165目标改为nominal `ApprovalTaskId(apr_)`/`InteractionId(int_)`字段，Input ID只由owner
JobCommit first-winner事务分配并稳定重放，同时新增禁止`tsk_`、错误kind/跨tenant owner及逐状态Task/Invocation first-winner的machine schema/
真实PostgreSQL fixture。因此既有Phase 3证据继续有效但不关闭10的Architecture Revision。

Capability synchronous/deferred/input/progress/reconcile闭环已经交付。caller-owned PostgreSQL transaction原子覆盖quota bundle
claim/start、output、Deferred/Input Task、callback/poll wake、progress、control/cancellation与manual reconciliation。fresh PostgreSQL 16
fixture证明`attempt_limit=1`下callback与Input两次resume保持同一物理attempt、callback/poll并发first-winner、stale wake/progress
隔离、uncertain write、cancel/completed race、Receipt/Event/Outbox原子性与quota归零；pure Job fixture证明RetryScheduled仍消耗新attempt。
CR-123因此关闭，baseline保持23表/单一`0001`。

CR-124先关闭了ModelTurn的registry输入缺口：Provider/Profile ResourceVersion现在冻结installed adapter manifest、credential/request
limits、identity/modalities/context/tool/structured-output/artifact/usage/data-handling/model limits与catalog evidence；Provider/Model
Deployment按角色冻结exact Policy和canonical bounded generation defaults。该变更仍使用共享ResourceVersion/Deployment与23表baseline，
作为ModelTurn admission/dispatch的可信输入。

CR-125关闭了ModelTurn request/stream/tool-intent/usage的domain与PostgreSQL transaction slice。`insight-platform-models`提供closed
canonical request/response、local schema/tool validation、stream epoch/sequence fence、retry/control/cancellation和attempt accounting；
PostgreSQL adapter在caller-owned transaction内锁定exact Run/ModelLoop/Provider/Profile/Capability tool/Policy/Ready Artifact事实，复用
shared Invocation/Job/RunValue/ArtifactLink/Receipt/Event/Outbox和Quota authority。claim一次原子reserve tenant concurrency、model request、
token ceiling与cost ceiling四条quota line；每个physical attempt单独settle，retry复用Job并创建新reservation。fresh PostgreSQL 16 fixture
覆盖create/replay、非法schema全事务rollback、两次attempt独立计费、tool-intent output、stale fence及cancel/completion first-winner；
baseline保持23表/单一`0001`。live delta仍是非持久化hint，durable terminal通过带Run identity的Event/Outbox唤醒ModelLoop。

CR-126先关闭Context persistence输入冲突：ContextBinding冻结为Agent Deployment/RunBindings内嵌immutable snapshot，`xcb`不拥有
独立current row；Dataset使用generic ContextDataset Resource和`dgen` ResourceVersion持有独立active data head。该修复不增表、不改
单一`0001`，是Context query admission开始前的authority输入。

CR-127补齐`PinAtRunAdmission`的时间边界：RunBindings保存exact、规范排序且进入canonical digest的Context dataset-view；Run admission
同事务固定active `dgen`，ContextQuery不得重新追随head。该修复仍只使用现有RunBindings JSON、23表和单一`0001`。

CR-128关闭Context执行身份与Receipt authority缺口：worker outcome绑定exact WorkerProcessGeneration和Job fence并使用`JobCommit`
Receipt；callback/poll/timer使用无Principal的signal audit与`Callback` Receipt，stale/late signal稳定终结为`rejected_stale`且不写
current state/Event/Outbox。Context domain单测、strict Clippy与fresh PostgreSQL 16 fixture已覆盖Deferred→wake→同attempt resume、
worker fence、stale signal、citation digest、quota与Event/Outbox原子性；不增加表或migration。

CR-129冻结Context完整Observation schema、禁止item内部未链接ArtifactRef，并要求Implementation backend contract与Deployment
binding字段级兼容；完整Observation超限时只通过一个Ready Artifact和exact ArtifactLink承载。domain negative fixture、生成合同、
strict Clippy和fresh PostgreSQL 16 Context transaction fixture均通过，不增加表或migration。

CR-130识别出既有Artifact生命周期测试不能替代真实Job合同：initial verification无Job fence，rescan缺失，current scan evidence
未冻结，duplicate-Blob cleanup Job无完成协议，deletion outcome还复用Principal command audit。实现已将其统一为closed tagged
Artifact Job payload、`ArtifactWorkerAudit`/`JobCommit` Receipt、Artifact current evidence与typed Scanner/Blob ports；保持23表/单一
`0001`。typed Scanner/Blob backend、scan/rescan/delete/duplicate-Blob cleanup worker、失败提交、Job fence与exact
object-generation/absence evidence由23个domain/worker fixture覆盖；fresh PostgreSQL 16 fixture进一步覆盖rescan排队即Quarantined、
exact worker completion、corruption、delete/shared-Blob alias、cleanup replay与不确定backend failure。domain、PostgreSQL、strict Clippy、
schema/contract/cutover和crate-boundary门禁均通过，CR-130及Phase 3 functional exit因此关闭。

CR-139关闭Text2SQL组合准入缺口：Capability Interface的规范性`qualified_name`已进入closed ResourceVersion与frozen
Capability admission snapshot；`ReadOnlySqlPlan`引用committed SqlCatalog Query/Observation、catalog projection、database identity、dialect、
exact Capability Interface/Deployment及Effect。generic Invocation repository在同一caller-owned transaction锁定这些事实，只有名称精确为
`database.query.readonly`且Effect为ReadOnly才允许创建Invocation。domain fixture覆盖名称/effect/foreign Run/Observation drift，fresh
PostgreSQL 16 fixture覆盖成功/replay、forged foreign citation与drift rollback且不遗留Invocation/Receipt；不增加表、migration或名称路由。

Phase 3的全量fresh PostgreSQL 16 suite已在同一新建数据库实际执行baseline、Phase 1、Phase 2、Artifact、Context/Text2SQL、
Capability、ModelTurn以及现有Phase 4事务fixture，全部通过；新增Capability name与Text2SQL domain测试、相关crate strict Clippy、
machine contract、23表schema、cutover residual与29-crate DAG门禁也通过。该证据只关闭Phase 3 functional exit，不等于Phase 4
生产执行集成、Phase 5 public `/v1`或Phase 6 qualification。

CR-131（进行中）建立MCP Host执行集成：exact Deployment/Server/Profile/Auth/Snapshot resolver、Streamable HTTP与Managed
stdio broker边界、discovery worker、typed transport failure、cancel/retry/expiry recovery和PostgreSQL safety scan均复用共享
Invocation/Job/Receipt/Event/Outbox，MCP Job使用closed tagged payload，不增加表或migration。OAuth callback首片现已增加shared
`ExternalAuthorization` Task、state/nonce/PKCE digest/exact-Secret闭包、credential-free verified grant、Callback Receipt first-winner以及
Task+AuthorizationBinding+Event/Outbox原子PostgreSQL command；raw code/token无持久化字段。shared binding位于contracts，MCP Host不反向依赖
Task domain。按tenant与数据库时间有界领取pending Task的expiry safety scan也已交付；Task first-winner与内部Event/Outbox同事务，
cleanup hint只含PKCE SecretBinding ID/generation。固定redirect的strict callback ingress application service与独立Egress中的生产HTTPS
credential broker首片也已交付：state先认证、exact Auth Profile/issuer/redirect/resource绑定、DNS全量public-IP校验与连接pinning、
no-proxy/no-redirect、late PKCE/client Secret resolution、closed duplicate-safe token response、token verifier和prepared Secret Manager
端口、独立in-flight permit以及credential-free grant。terminal Event中的typed cleanup hint严格只含PKCE SecretBinding ID/generation；
Host cleanup consumer先从可信Event envelope取得tenant/Task/cause，再由PostgreSQL重验exact terminal Task与Pinned binding，Egress adapter才允许
Secret Manager删除精确version，stale hint不得触达Secret Manager。随后Host又交付AES-256-GCM state codec：固定callback digest进入AEAD audience、state仅密文携带
tenant/Task identity、active key与最多4个verification key支持有界rotation、TTL/clock-skew/长度均有hard limit，篡改、未知key、错误callback与
过期统一fail closed。独立`insight-platform-api`候选transport提供固定`GET /v1/mcp/oauth/callback` adapter，只接受空body与bounded raw query，
返回不反射输入/内部错误的静态响应并强制no-store/CSP/no-referrer；非GET在application dispatch前拒绝。contracts 62项、Egress 21项、
Platform API 3项、Task 2项unit及targeted check/strict Clippy和29-crate DAG门禁通过。该router已进入target OpenAPI和独立候选进程；
Phase 7 cutover前仍不声明当前public行为。
authorization-start application service与Egress preparation broker现已补齐exact Auth Profile解析、256-bit PKCE/nonce、AEAD state、stable
Secret Manager `prepare-or-load`、canonical S256 authorization URL和Task commit重新验证；fresh PostgreSQL first-winner/replay/Secret canary
fixture已在全新PostgreSQL 16数据库实际执行通过。

durable Resource subscription现已交付exact binding、加密session、strict notification parser、独立notification permit/keyed rate authority、
generation first-winner与coalescing、fenced session worker、Streamable HTTP/Managed stdio credential-free subscription port、下游Context/Discovery
durable acceptance以及按tenant/数据库时间运行的bounded periodic reconcile safety scan；全部复用shared Invocation/Job/Receipt/Event/Outbox，
不增表或migration。建立成功/终止phase evidence进入同一请求摘要、Receipt和Event；显式session-loss与expired lease/session scan会清除旧opaque state、
CAS重排同一Job，并要求新session generation完成一次下游durable full reconcile后才回到Waiting。此前subscription PostgreSQL fixture覆盖
resolver、session阶段、并发notification、refresh evidence、scan/wake/reconcile、session-loss/expired-lease rebuild和Secret canary，并已在同一
全新PostgreSQL 16 suite实际执行通过。

Resource subscription的生产Streamable HTTP connector现已进入独立Egress：复用process-installed exact Deployment catalog、全DNS公网验证、连接
pinning、no-proxy/no-redirect与late Pinned token resolution，按`2025-11-25`执行`initialize -> notifications/initialized ->
resources/subscribe`。connector先返回加密且generation-bound的prepared session；Host只有在同一session generation的Ready已durable commit后才
不可失败地发布独立GET/SSE驱动，关闭notification先于Ready的竞态。GET只携带敏感`Mcp-Session-Id`和可选`Last-Event-ID`，SSE event/idle/
session/reconnect/event-count均有硬限制；有event ID的重投保持稳定去重identity，无ID data在连接关闭后强制full reconcile，断线通过exact auth/
session/Worker generation的Host ingress和PostgreSQL command清除opaque state并重排同一Job。合法sub-resource update只保留URI digest，始终重新读取
exact published root。生产UUIDv7 ingress identity只分配Receipt/Event/Outbox identity，transport-loss幂等scope绑定exact session/Worker。
Host 56项unit、Egress MCP 18项定向unit、三crate all-target/all-feature check与strict Clippy通过；PostgreSQL subscription fixture已扩展
sub-resource、真实redelivery和异步transport termination，并已在全新PostgreSQL 16数据库实际执行通过；该开发期fixture仍不是
real-process conformance或Phase 6资格证据。

Streamable HTTP operation的生产connector现已进入独立Egress：process-installed exact MCP Deployment catalog固定唯一HTTPS
endpoint与Protocol/Network/TLS/Trust/Auth Policy，连接前校验全部DNS答案为公网地址并pin到该连接，强制no-proxy/no-redirect、late Pinned
token Secret resolution和独立in-flight permit。每个operation按冻结`2025-11-25`合同执行`initialize -> notifications/initialized -> method
POST`，固定`MCP-Protocol-Version`和敏感`Mcp-Session-Id`，接受`application/json`或bounded SSE首个response event；JSON-RPC request ID、
version、result/error互斥、duplicate key、响应字节/header/SSE event/idle/initialize/request deadline均fail closed。重新initialize的capability必须与
admission冻结的Discovery Snapshot精确相等；HTTP 401/403只返回challenge digest。experimental Task-aware `tools/call`现按冻结profile显式附加
`task.ttl`，只接受已协商的`tasks.requests.tools.call`；远端task/session由AES-256-GCM keyring封装，绑定tenant、exact Deployment/Auth generation、
Invocation/Job/physical attempt、Discovery/Profile与deadline后才写入同一Capability Job payload。Poll continuation保持同一物理attempt和原session，
只访问已绑定task ID，执行`tasks/get -> tasks/result`并校验related-task；网络抖动复用原密文handle，poll上限对read终止、对write进入
reconciliation且保留密文handle。Task取消沿同一密文handle与原session发送`tasks/cancel`，只有同一task ID的closed `cancelled`结果才记为
accepted；协议错误、状态漂移或超时均不伪造成功，且accepted不作为此前Effect未发生的证据。Task handle、session和token均不进入Event、Artifact名或日志。Egress 40项、MCP Host 54项、Capability adapter
13项unit及fresh PostgreSQL 16 Capability fixture已通过；该fixture还覆盖密文state claim/wake/resume、callback/poll first-winner、Input RunValue
恢复和同一Job/attempt，不增加23表或单一`0001` migration。远端Task返回`input_required`时，`tasks/result`现在允许在硬上限内跳过
`notifications/progress`，只接受绑定同一remote Task的`elicitation/create`；MCP form schema被收敛为closed、bounded、non-secret的平台
Interaction schema，精确tenant/principal/session写入共享Task。用户`accept | decline | cancel`都以generation first-winner恢复原Capability Job和
原physical attempt；请求发送后结果不确定时保留同一action/response与remote-state digest供安全重发。新增路径的Egress 43项、MCP Host 54项、
Capability adapter 14项、Contracts 64项和Invocation 9项unit及相关strict Clippy已通过；PostgreSQL repository测试已编译，但最新扩展尚未重新
取得fresh PostgreSQL 16执行证据，因此不沿用此前fixture作该扩展的证明。上述开发期fixture均不是real-process conformance或Phase 6资格。

生产PKCE cleanup delivery现由独立双副本Worker组合：它只从共享Outbox按`SKIP LOCKED`领取三种terminal OAuth Event，以
WorkerProcessGeneration/claim epoch/lease作为exact fence，先由PostgreSQL重验terminal Task与Pinned binding，再经MCP Host身份的Egress
mTLS方法删除exact Secret Manager version。成功只把同一Outbox行推进到`cleanup_completed`而不设置`published_at`，因此Phase 5 dispatcher
仍可投影同一committed Event；临时失败与不确定结果有界退避，永久合同错误进入`cleanup_dead`。fresh PostgreSQL 16 fixture实际证明lease
reclaim后旧worker无法settle、新worker first-winner完成且事件仍未published；5项cleanup domain、1项worker config、strict Clippy及独立
Deployment/PDB/default-deny NetworkPolicy门禁通过。该路径不增表或migration。Managed Runner provider尚未交付，因此仍不得关闭MCP或
Phase 4；两个显式ignored RustFS qualification test仍不计为资格证据。

CR-132（进行中）建立独立`insight-platform-model-adapters`：按qualified name + signed WorkerManifest digest + adapter contract
digest精确选择进程内adapter，Provider SDK/wire类型保持在port之后；canonical request、exact Provider/Profile/Deployment、
deployment级delta/timeout、stream sequence/terminal、本地response validation、cancel和panic containment均fail closed。Model worker把
规范化结果经materializer转换为RunValue/Artifact形状，并只通过fenced PostgreSQL authority提交；claim现在返回精确fence、usage
reservation、quota ledger IDs以及与冻结RunValue和ArtifactLink逐字段复核的exact request input。Inline正文按canonical digest复核后交付，
Artifact-backed正文由独立Model Artifact Broker RPC和生产Worker materializer按closed authority读取；terminal command在Provider I/O前拒绝复用reservation ledger ID。
dispatch后未知结果按冻结上限保守结算且attempt耗尽不重放。OpenAI Responses与Anthropic Messages
production wire adapter现通过credential-free Connector边界实现固定endpoint/protocol request与bounded SSE normalization；共同fixture覆盖
text stream、usage、tool intent、本地structured/tool schema、请求digest及未知字段fail-closed。brokered connector现在还把credential-free
request交给独立Egress broker port，并对raw SSE执行incremental总量/line/event限制、strict JSON重复key拒绝、closed content-type/status/
`[DONE]`处理；fixture增至20项并通过strict Clippy。独立Model Worker binary/Deployment/HPA已通过静态
部署门禁；durable cancel safety scan、reserved control permit、exact Egress cancel与fenced conservative terminal commit也已组合并有unit/数据库
fixture。Artifact-backed request现由生产进程组合`BrokeredModelRequestMaterializer`与独立versioned Model Artifact Broker gRPC读取；exact
Model Worker URI SAN、bounded canonical chunk、双重授权和restricted PostgreSQL read role均有正负向fixture。生产storage/KMS catalog
provisioning、Artifact-backed output、real-process Provider conformance与饱和/故障资格仍未完整
交付。Model text delta的内部publisher已经通过exact fence、canonical credential-free envelope、持有容量permit到批次flush结束的双重有界
non-blocking队列和TLS/mTLS NATS
组合；tool argument与Provider metadata保持私有，NATS故障不影响durable terminal。公开SSE消费及live-gap/backpressure资格仍未交付。
生产HTTPS Egress首片已转入CR-136跟踪，因此CR-132和Phase 4保持进行中。

CR-133（进行中）建立Sandbox执行权威首片：`insight-platform-sandbox`冻结exact Capability Deployment、Runtime、Package、Profile、
isolation backend、Artifact/Secret/callback grant和closed resource envelope；plain OCI/runc不能注册为Sandbox backend。Gateway admission
与Executor的Preparing→Starting→Running→Collecting→terminal五次提交均只通过共享Job fence、Worker `JobCommit` Receipt、Event、Outbox
和四条Sandbox quota line；PostgreSQL事务同时校验active exact revision、Deployment gate/closure、Invocation、permission、Artifact grant、
quota并在terminal结算及释放grant，不增加表或migration。58个contract fixture、20个Sandbox domain/worker fixture、PostgreSQL测试编译、
strict Clippy、crate DAG和23表/单一`0001`门禁通过。真实PostgreSQL Sandbox transaction fixture已在全新PostgreSQL 16数据库实际
执行通过。Executor现已支持exact cancellation token、wall timeout、backend abort、process-tree/grant/cleanup evidence，
并允许相同Worker fence只在有界cleanup grace内提交terminal。Capability控制Event显式冻结`control_kind`；bounded Controller从已提交Event
或分片PostgreSQL safety scan构造带source Event/version/payload digest的exact stop signal，只扫描目标WorkerProcessGeneration拥有的lease，
并在tenant/Sandbox Job/Invocation/request digest/attempt/lease generation/WorkerProcessGeneration全部匹配时投递本地Executor token；重复投递
保持first-winner且不建立第二持久化状态。未领取Ready Job现在由source Event驱动的controller command原子终结，0 usage释放quota/grant且
不伪造Executor/cleanup evidence；generic claim拒绝Sandbox，专用claim在取lease前重验parent Invocation和exact deployment/runtime/package/profile
gate。bounded expired-lease scan与exact recovery command现已区分三条路径：未进入backend的`Accepted`在deadline前安全回到Ready，
deadline已到则无Executor/cleanup evidence地TimedOut，已提交`Preparing`及以后phase必须先提交destroy或node quarantine、grant撤销和
uncertainty evidence才可映射`Lost -> reconciliation_required`；旧version/generation迟到提交被first-winner拒绝。真实PostgreSQL
pre-start control、resolver/scan、claim与lease-recovery fixture均已实际执行通过。Core NATS跨Podcontrol adapter现按
exact WorkerProcessGeneration subject执行bounded request/reply，closed reply回绑`signal_digest`，超时/断连由durable safety scan重试；
3个wire/config fixture和strict Clippy通过。生产WASI与Firecracker adapter及既有Broker读取路径已交付；Model/Sandbox
Artifact Broker双audience进程/Deployment/ServiceAccount/DB pool/permit隔离属于当前实施批，相关静态、mTLS、真实数据库与饱和门禁
通过前不能把新拓扑声明为已交付；
gVisor adapter、authenticated NATS real-process/ACL fixture、生产backend reconnect/abort/quarantine、真实S3/KMS/Secret Manager以及
escape/process-kill/saturation qualification仍未交付，CR-133和Phase 4保持进行中。

`SandboxRecoveryDriver`现已把上述scan/backend evidence/recovery commit接到`WorkClass::Sandbox`专用WorkerManifest与独立
critical-control permit；Sandbox业务permit耗尽时恢复扫描仍可推进，backend失败只保留候选等待下次bounded scan，数据库不可用按
配置backoff，invariant failure则fail closed。该runtime unit evidence不等于production backend或Gate D/E。

CR-141关闭Sandbox双Job合同冲突：Sandbox backend不再先生成`CapabilityRemote` Job。Gateway admission原子锁定Ready
Capability Invocation及expected version，校验exact input/output与执行closure后，直接创建唯一`work_class=sandbox`共享Job并把
Invocation推进为Deferred、绑定同一Job；该物理执行只使用共享`JobId`，不存在`SandboxJobId`或同UUID typed alias，预留output使用
独立`RunValueId`并通过owner binding关联。
Executor只提交该Job的fenced physical terminal，独立Capability owner controller再归并Invocation，不创建或重写第二个Job。
safe retry到期时Gateway以`RetryScheduled -> Deferred`的单事务直接创建下一条Sandbox Job，旧terminal Job保留且全局attempt严格递增；
该裁决不增表、不改单一`0001`，后续Sandbox实现与fixture必须以此为准；在terminal归并和生产backend完成前CR-133仍保持进行中。

CR-142关闭Sandbox terminal owner收敛与retry时钟权威缺口：`SandboxOutcomeDriver`只占Sandbox角色reserved critical-control permit，
terminal Event通过coalesced wake handle加速，bounded sharded PostgreSQL keyset scan作为正确性兜底；扫描只返回当前Invocation绑定的
terminal Job exact version/Event digest，merge以source Event Receipt、Invocation optimistic version和database transaction clock实现
first-winner/replay。retry backoff由Capability admission冻结并复制到Sandbox request，controller副本不能选择绝对`retry_at`。
runtime unit与PostgreSQL fixture已接入该路径；不增表、queue或migration。真实PostgreSQL运行证据、authenticated NATS composition及
Phase 6 Gate D/E前，本段不声明production qualification。

CR-134已补齐ModelProvider/Profile Deployment的repository语义闭包：创建时验证exact published document、
protocol、credential purposes、Provider Deployment digest/revision、generation-default schema与Ready conformance Artifact。负向
PostgreSQL fixture已在全新PostgreSQL 16数据库实际执行通过。

CR-135正在关闭Accepted SecretBinding合同冲突：Deployment closure统一使用包含ID、observed generation、
provider、purpose、完整resolution policy及其canonical digest的`ExactSecretBindingRef`，不再仅保存ID。closed type、
canonical/purpose/generation负向fixture、Deployment创建期exact校验、运行期active/revoke门及Capability、Context、MCP、Model、
Sandbox执行传递已交付；60个contract tests与定向domain/adapter tests、workspace check、strict Clippy、23表/schema、
public contract、cutover residual和crate DAG门禁通过。该变更不增表或migration。PostgreSQL错误generation负向fixture已编译，
并已在全新PostgreSQL 16数据库实际执行通过；CR-135合同与Phase 3对应门禁关闭。

CR-136正在关闭生产出站所有权与取消fence缺口：规范已冻结独立`platform-egress`角色作为exact endpoint catalog、late Secret
resolution、DNS pinning、SSRF/TLS/redirect/proxy enforcement和bounded HTTP的唯一普通执行所有者；该角色不拥有数据库表或业务
current state。Model wire/cancel必须携带tenant、Turn、Job、attempt、lease、Worker generation与Deployment完整identity，旧generation
不能终止当前连接。独立`insight-platform-egress` crate的生产HTTPS首片已交付：process-installed exact catalog、每次解析的全量public-IP
校验和reqwest连接pinning、HTTPS-only/no-proxy/no-redirect、Pinned/Follow Secret evidence、固定敏感auth header、canonical body、bounded
response与in-flight permit/first-winner cancel。Capability HTTP与HTTP/2 gRPC也已进入同一角色：只消费process-installed exact
Deployment/contract/endpoint/policy/Secret闭包，执行全DNS public-IP验证和连接pinning、bounded framing/response，并以tenant、Invocation、
Job、attempt、lease、Worker generation与Deployment完整identity取消exact live request；无保护write的dispatch后断线直接归类Uncertain。
相同角色现同时承载MCP OAuth token exchange和同步无Task Streamable HTTP operation：前者固定token endpoint、PKCE/client credential
late resolution、strict token response、verification/store端口和独立bulkhead；16项fixture覆盖mixed/private DNS、closure drift、rotation、
重复请求、stale Worker/protocol cancel、OAuth duplicate/unknown token字段、兑换顺序和Secret non-interference。后者使用exact MCP endpoint/
policy catalog、全DNS公网验证和连接pinning，执行initialize/initialized/operation三段Streamable HTTP，严格校验JSON/SSE/Session/capability/
deadline并拒绝尚未可恢复的remote Task。定向strict Clippy与
29-crate DAG通过；完整workspace all-target test与strict Clippy也已通过；当前Egress 35项unit中8项专门覆盖Capability HTTP/gRPC、6项覆盖MCP operation
exact catalog、DNS/Secret、framing、response、Effect/idempotency failure与stale cancel。两个需要真实RustFS与重启阶段的qualification test保持
显式ignored且不计入Gate证据。
CR-143已交付late Secret resolution的可信组合内核：新增无持久化、无公共API的`insight-platform-secret-broker` crate，复用现有
`secret_bindings`行与Egress resolver port，把PostgreSQL current Active/revoke/generation门、KMS/AEAD opaque reference解封与digest、
CandidateManifest安装的Provider catalog、独立permit/总超时及Pinned/Follow实际版本证据串成单一fail-closed路径。Secret reference/material
均non-clone、Debug redacted并在drop清零；不增表、cache/session authority或migration。5项Broker、47项Egress、1项Security unit、targeted
check/strict Clippy、public contract、23表schema与30-crate DAG通过；PostgreSQL trusted-read/revoke/cross-tenant fixture已编译接入，但本批次
未配置`PLATFORM_TEST_DATABASE_URL`，不登记新的fresh PG运行证据。

CR-144已补齐MCP OAuth prepared-write收敛：exact Auth Profile分别冻结PKCE/token Provider；token preparation绑定Task/授权码digest/
scope/audience/issuer/deadline，Egress在任何兑换I/O前先load prepared winner。无winner才兑换与验证；外部Provider `prepare-or-load`成功后，
Secret Broker执行KMS sealing，并由受信ServiceIdentity用preparation digest幂等登记到现有`secret_bindings`，同事务终结Receipt并写
Event/Outbox。Provider成功而数据库响应丢失时，下一次请求load同一winner修复登记，不重用one-time code；provider/version/evidence
漂移均fail closed。exact-version cleanup也重验current authority后只删除Pinned版本。该路径新增9项Broker、10项OAuth定向Egress、14项Host
OAuth测试；真实PostgreSQL authority fixture已编译。本机Docker daemon无响应，因此本批次仍不登记fresh PG运行证据；不增表或migration。

CR-145已交付首条target `/v1`机器合同及候选部署组合：callback总raw query按全平台`url_bytes` hard max从16384收紧为8192 bytes，Axum在进入
application service前拒绝超限。Rust generator现生成`/v1/mcp/oauth/callback`的operation/authentication/permission/idempotency/rate/audit、
字段闭集与静态状态响应，manifest digest覆盖该YAML；独立checker禁止未review的额外path和敏感token字段。4项API、14项Host OAuth测试及
generator/checker通过。独立`platform-callback-api` binary组合PostgreSQL callback authority、AEAD state keyring、UUIDv7
Receipt/Event/Outbox identity与MCP Host身份的Egress RPC；Helm固定exact Ingress、状态key Secret、Egress mTLS、双副本/PDB及只允许
Ingress/Egress/DNS/PostgreSQL的default-deny NetworkPolicy。候选配置与key material digest在开放listener前校验。OpenAPI仍为
`implementing-not-current`直到Phase 7 cutover，因此不声明当前公共行为。

CR-146冻结并交付首个真实Sandbox isolation backend。WASI ABI v1只接受Wasmtime 42.0.0、零imports、恰好一个bounded
32-bit memory/零table，以及`memory`、`insight_alloc(i32)->i32`、`run(i32,i32)->i64`三个exact exports；输入和输出是
closed strict canonical JSON，结果handle高32位为pointer、低32位为length。独立`insight-platform-sandbox-wasi` crate是唯一
链接Wasmtime/Cranelift的角色，module经Sandbox Artifact Broker重验length/digest，Store实施fuel/memory/instance/table limits，output经
exact schema validator，I/O超限形成显式resource failure，terminate/abort/recovery等待guest call实际退出后才撤销grant并提交cleanup
evidence。重启后的内存缺失必须由process-generation isolation authority证明旧WorkerProcessGeneration已终止；当前generation缺失状态
fail closed。10项真实Wasmtime conformance与22项Sandbox domain tests通过；独立Executor已有开发期实现，但双audience Broker隔离门禁、
runtime bundle version 4边界及当前批次回归完成前不声明新生产拓扑已交付；当前仍缺
gVisor、真实Linux KVM互操作、escape/process-kill/saturation及CandidateManifest资格，故Phase 4保持进行中。

CR-147已闭合Capability Interface机器合同与执行校验缺口。共享`ClosedJsonSchema`冻结schema version、closed profile、完整document、
canonical digest及262144-byte上限；Capability Interface ResourceVersion现保存完整input/output/error schema、Artifact ports、DataFlow policy
与Interface limits，Invocation admission只冻结运行所需digest和exact policy snapshot。Sandbox Controller从同一exact Interface全文验证
WASI输入/输出，并同时重验tenant、Invocation、Job、request digest、WorkerProcessGeneration、lease generation、phase、classification与
schema digest。68项Contracts、10项真实Wasmtime、受影响Capability/Model的34项unit、strict Clippy与fresh PostgreSQL 16 Phase 4 fixture
通过；数据库fixture同时证明错误placement摘要、陈旧Worker generation和字段类型错误的输入/输出均fail closed。该切片保持23表、单一
`0001` migration，checked-in Platform v1合同无漂移。

CR-148已交付Artifact-backed逻辑值语义和Model request读取的下一片：显式Artifact字段使用nominal ArtifactRef，而因存储阈值转为Artifact-backed的整个
逻辑JSON仍按物化正文schema验证；admission不会把ArtifactRef metadata误当正文。WASI input/output现共用Controller value-validator，
Inline路径已取得上述真实PostgreSQL正负向证据。Sandbox PostgreSQL fixture使用Ready Artifact-backed RunValue、Invocation reference与
per-Job read grant，并由真实Broker core在正确physical phase读取runtime bundle与输入；同一fixture以受限Broker数据库role完成读取且证明
Job更新和Secret读取返回`42501`。
Model claim现在构造closed Artifact read request，精确绑定Turn、当前Job version/fence/lease/Worker generation、request digest、deadline、
RunValue及active ModelTurn-owned ArtifactLink；PostgreSQL authority与Model Broker core在object I/O前后授权，Worker materializer再检查strict
canonical JSON与逻辑digest。fresh PostgreSQL 16 fixture证明有效读取可重放且陈旧Job fence被拒绝；Broker/Worker unit还覆盖
非canonical正文拒绝。目标生产组合固定为两个audience-isolated服务：Model Broker只接受唯一closed Model read方法，Sandbox Broker只接受
WASI与microVM两个closed read方法；它们使用不同进程/Deployment/ServiceAccount/DB pool/permit和exact URI SAN。既有authority/stream fixture
仍可证明canonical digest、sequence、长度、唯一terminal及restricted SELECT语义，但不能替代双部署与交叉饱和证据。generic Capability producer、Artifact-backed output与真实
S3/KMS qualification仍未完成，因此CR-148与Phase 4保持进行中。

CR-149已关闭内部Sandbox workload identity混淆缺口。registration、verify与absence端点统一从已通过client CA验证的leaf certificate
提取恰好一个`spiffe://insight.platform/workload/<closed-workload-role>` URI SAN，并按方法匹配Executor或Controller exact role；CN、DNS
SAN、header和payload自报身份均不参与传输授权。真实loopback mTLS fixture证明正确角色成功、同CA错误角色与错误CA在进入authority前拒绝，
随后仍独立重验WorkerProcessGeneration、lease和Job fence。独立Pod/NetworkPolicy静态合同也已通过；Phase 6生产证书轮换资格不属于
该cross-review关闭条件，不增表或migration。

CR-150既有fixture证明旧ArtifactLink `active -> released` helper与terminal事务不会双写，但CR-165把目标统一为15的closed ArtifactGrant
capability及`Active -> Revoked`状态，并要求runtime bundle也使用exact grant。实现必须在同一clean-cut中替换helper/fixture，保留Sandbox owner唯一
revoke authority、重复revoke幂等与terminal全集断言；目标仍映射进共享ArtifactLink，不增表或migration。完成前该既有证据不能恢复14/15 Accepted。

CR-151正在交付Sandbox Artifact Broker生产组合：受信PostgreSQL read authority、二次授权、strict canonical locator、CandidateManifest
storage/KMS catalog、workload-identity AWS S3/KMS provider以及HEAD+GET exact version/length/digest复验已进入独立无持久化crate；Sandbox
Controller只经versioned closed mTLS RPC请求runtime bundle和input，不再持有Artifact provider catalog、AWS workload token、S3/KMS client或
对应直出网络。WASI与microVM保留不同closed请求，Broker在object I/O前后分别调用typed PostgreSQL authority。fresh PostgreSQL 16 Sandbox
fixture已用真实Broker core和受限数据库role读取Package runtime bundle与Artifact-backed输入，并证明错误Worker、错误purpose/grant、terminal
后读取及数据库越权均fail closed；loopback mTLS与Helm静态门禁分别覆盖错误workload role和网络/credential漂移。real object-store/KMS negative
qualification完成前不关闭CR-151或Phase 4。

CR-152的machine type、Controller proxy和production attestor已交付：closed请求/回执exact绑定tenant、Sandbox Job、request、旧
WorkerProcessGeneration、已提交Executor identity、attestor identity、观察时间及`process_absent | node_quarantined`处置；Controller只能
经独立attestor mTLS client取得证据，DB lease、NATS/RPC超时、Pod缺失或Node NotReady均不能合成回执。Linux真实进程fixture已入库，但当前
macOS开发机只能编译而不能执行；Candidate Linux node上的process-kill/node-quarantine资格前CR-152保持Open，不增表或migration。

CR-153的registration RPC和production node attestor已交付：Executor在claim前只提交非实例字段，attestor从mTLS identity、Unix peer
credentials和只读procfs/runtime authority观察WorkerProcessGeneration、Pod/node UID、runtime sandbox/cgroup locator及process-start
identity，再签发sealed executor identity；首次Preparing及后续phase只提交该摘要。absence反查同一登记并以start identity防止PID/cgroup
复用，现有fixture覆盖同一进程绑定两个generation、PID reuse与attestor重启。Linux Candidate real-process negative资格前CR-153保持Open。

CR-154的production registration listener现使用node-local Unix socket上的mTLS HTTP/2，TLS URI SAN证明closed Executor role，Unix
peer credentials提供宿主PID；wire仅携带generation/manifest/backend，Pod/node/cgroup/start identity由attestor从只读procfs/runtime
fixture核对。Controller verify/absence使用独立mTLS TCP listener且没有registration权限；sealed record进入node-local bounded registry并
可在重启后恢复，存活generation不按墙钟过期，确认absence后才按hard wall+cleanup保留期回收。真实socket recovery、错误CA/role、PID
reuse与registry corruption fixture已通过；跨节点Linux Candidate与伪造负向资格前CR-154保持Open。

CR-155关闭DaemonSet跨节点路由空洞：sealed登记证据新增private node-IP/fixed-host-port `attestor_route`，并贯穿Executor claim、首次Preparing、
现有Sandbox Job payload、expired scan和absence request；Controller只连接配置中冻结node CIDR与fixed port内的exact route，仍执行mTLS
server identity校验，不引入Service负载均衡、Kubernetes API或中心route registry。route已进入canonical executor/evidence digest，
漂移与public/loopback/link-local/DNS目标均拒绝。Sandbox Helm现将Controller、Executor和hostPID Attestor拆为三个workload及五条default-deny
NetworkPolicy，静态合同通过；Sandbox相关strict Clippy、Attestor 8项和Sandbox RPC 6项测试通过。Linux real-process/跨节点网络资格前
CR-155及Phase 4保持Open。

具体KMS/真实Secret Manager Provider adapter、real-process Provider conformance、
独立Pod/NetworkPolicy及Phase 6资格通过前CR-136/CR-137/CR-143/CR-144仍保持Open。

CR-137正在修复MCP OAuth的credential scope：旧实现把per-user token SecretBinding错误地要求为immutable Deployment closure成员，
无法支持动态用户授权与refresh。Server的deployment credential requirements与authorization credential purpose现已分开；
Deployment只冻结client/mTLS/runner Secret，AuthorizationBinding独立冻结Pinned exact token Secret并以authorization generation隔离
principal/session。closed contract、Host、transport与PostgreSQL dependency validation已实现；负向fixture覆盖purpose重叠、Follow
rotation和nested session-key tamper；strict callback ingress与Egress credential broker已证明raw callback/token不进入Host、Receipt、
Event或数据库命令，返回的token binding仍须Pinned且由repository重验active exact Secret。该修复复用现有Resource与SecretBinding，
不增加表或migration；真实Secret Manager adapter/token verifier/state authenticator production composition、fresh PostgreSQL authorization竞态fixture与
Phase 4 qualification通过前CR-137保持Open。

CR-156正在关闭Egress部署矩阵与数据库权限冲突。目标组合拆成两个不可合并的角色：Egress Broker持有受控外网、KMS/Secret
Manager和process-installed endpoint/provider catalog，但没有数据库credential；Security Authority持有restricted PostgreSQL role，只有
exact Egress workload identity可调用SecretBinding受信读取和prepared winner登记两个closed internal gRPC method，并且该角色没有外网、
DNS resolver、KMS或Secret Manager权限。prepared登记继续复用现有Receipt/Event/Outbox事务，读取不产生mutation；不增表或migration。
两个独立deployable binary已经交付；Worker→Egress十一方法和Egress→Authority两方法internal protobuf均使用bounded、canonical、
digest-bound closed envelope，mTLS逐方法要求exact Model/Capability/Egress URI SAN，错误角色、同CA未知角色、tamper与跨operation replay
均fail closed。Helm合同将两者放入独立Namespace/ServiceAccount/Deployment/ClusterIP Service/PDB/default-deny NetworkPolicy；Egress只有
Authority、DNS、workload identity与exact external/provider CIDR出口且没有数据库配置，Authority只有PostgreSQL出口且没有DNS、外网、KMS或
Secret Manager权限。restricted-role grant脚本只授权schema verification、trusted read及prepared登记所需的精确SELECT/INSERT/Receipt列UPDATE；
CI资格fixture用该role执行schema verification、trusted read和prepared winner事务，并要求业务`runs`读取与SecretBinding任意UPDATE以`42501`
拒绝。internal RPC、mTLS和静态部署门禁已实际通过；fresh PostgreSQL 16最小权限fixture也已实际通过并进入CI，CR-156关闭。Phase 4仍由
CR-131/132/133/136/137/143/144/148/149/151～155及对应production composition/qualification门禁保持进行中。

CR-157已关闭MCP OAuth token verifier与生产组合缺口。当前profile只接受signed JWT access token与`openid` ID token；
CandidateManifest安装exact Auth Policy、完整Auth Profile、`EdDSA/ES256/RS256`算法allowlist和canonical public JWKS digest。Egress在任何
one-time code相关副作用前解析exact local catalog，兑换后验证issuer/audience/subject/time/type/key、共同subject及Task nonce，验证证据只含
domain-separated token digest而不含正文。独立Egress binary现组合AWS Secret Provider、Security Authority、prepared token store、local
verifier和exact PKCE cleaner；Worker→Egress internal proto新增authorization-code exchange与PKCE delete两个closed method，只有exact MCP
Host URI SAN可调用。真实Ed25519、catalog drift/key-order、无副作用拒绝和mTLS正/负向fixture通过，不增表或migration。该关闭只消除
production token verifier缺口，不代表MCP或Phase 4完成。

MCP同步及experimental Task-aware Streamable HTTP operation现已通过独立Egress进程边界：internal proto增加operation execute与
remote-task cancel两个closed unary method，MCP Host client和Egress service都重验canonical/digest-bound envelope、bounded payload、
exact workload URI SAN与closed outcome/failure wire shape。生产Egress启动时安装exact endpoint catalog和limits，并从只读Kubernetes
Secret投影目录装载最多4把精确32-byte AES-256-GCM active/verification key；key material不进入ConfigMap、环境变量或日志。Helm与静态
部署门禁要求该Secret volume。Resource subscription现通过第十一个closed双向流RPC跨独立进程：同一mTLS连接先完成Egress
prepare并返回加密session evidence，Host durable Ready提交后才发送activation；notification/termination使用bounded、digest-bound frame
回流。单连接保证两帧命中同一Egress副本，不依赖sticky Service、共享临时表或第二状态authority；pending/active/事件buffer各有硬上限，
Egress或流丢失仍由Host已有session-loss与full reconcile收敛。operation、Task、subscription session共用只读key投影但使用不同AEAD
associated-data domain。该切片关闭subscription跨进程激活缺口；随后交付的durable cleanup Worker又关闭了生产outbox cleanup缺口，
CR-131现在只由Managed Runner provider及其进程终止/恢复资格保持Open。

CR-158修正了Managed stdio的物理Job所有权：现有直接Runner port不能作为生产组合，因为它会先领取`CapabilityRemote` Job再等待
Sandbox，违反单物理attempt、独立bulkhead和Worker permit释放合同。目标实现改为由exact MCP transport在admission时直接路由到唯一
`work_class=sandbox` Job；Sandbox source冻结完整Capability/MCP/Discovery/Auth/operation与Package/Runtime/Profile/Policy closure，
claim后才绑定物理fence。Managed subscription按独立生命周期保留逻辑MCP Job并为每generation关联至多一个Sandbox session Job。
该修订不增表或migration。operation路径的domain/repository及production Firecracker Provider组合现已交付：Provider只从Controller
closed Artifact proxy按exact tenant/Job/request/Executor generation/Provider generation/sandbox identity/lease/deadline请求Package runtime bundle和
Artifact-backed逻辑输入，二次复核完整`ArtifactRef`长度与SHA-256，再以bounded canonical chunk在private vsock上先完成一次性guest
materialization、后发送同一fence的execute command；主逻辑输入只接受exact `read_whole` grant。进程配置不再拒绝已安装且digest闭合的
`managed_mcp_server` runtime。定向29项Sandbox、1项Provider config和10项microVM protocol/Firecracker socket fixture实际通过。
Managed subscription的durable admission首片也已交付：Host删除直接Managed subscription broker且普通subscription Worker只接受
Streamable HTTP；closed Sandbox payload新增Managed session workload，完整冻结双向Job/generation与MCP、Sandbox、grant、resource、callback
closure。PostgreSQL以单事务锁定逻辑Invocation/MCP Job和全部exact authority，创建唯一Ready Sandbox Job，将逻辑Job停回Waiting，提交
Artifact/Secret grant、四维quota reservation及Receipt/Event/Outbox。全新PostgreSQL 16并发fixture已实际通过唯一winner、exact replay、
同idempotency key请求漂移冲突、双向身份、grant/quota和Secret canary，且仍为23表/单一baseline migration。后续domain/repository切片
已经交付专用Managed claim和`Preparing -> Starting -> Running` fenced phase authority：普通Sandbox claim不可见该workload，并发Managed
claim只有一个winner；`Starting`与逻辑`Initializing`、Ready与逻辑`Active/Ready`分别在同一事务提交Receipt/Event/Outbox。加密opaque
session只由逻辑Invocation保存；物理Job从`Starting`保存credential-free exact prepared binding并在`Running`追加ready binding。新的全新PostgreSQL 16 fixture实际覆盖队列隔离、并发claim、
phase replay、stale fence、后续阶段的admission replay及双状态Ready原子性。该开发期证据仍不包含Managed session provider的实际prepare、
durable Ready返回后的同实例activation、terminal/session-loss recovery、真实Linux KVM/jailer/guest-agent互操作、
process-kill或escape/saturation资格，因此CR-131/CR-158和Phase 4继续保持Open。Sandbox domain随后新增closed establishment
Worker/Provider port，将顺序收紧为`Preparing commit -> prepare -> Starting commit -> initialize但保持通知关闭 -> Ready commit ->
same-instance activate`，并在任一post-prepare合同、authority或provider失败时要求exact destroy。两个unit fixture已证明Ready提交先于
activation及Ready提交失败时destroy且不activation。cleanup port现支持按exact request/fence、无prepared evidence销毁，为后续Provider RPC
prepare响应丢失收敛保留closed路径。独立Managed session authority internal gRPC已由Controller以`PgRepository`和node attestor组合，只有
exact microVM Executor URI SAN可调用claim/phase/Ready；Executor library专用claim driver与普通Sandbox共享`LocalWorkerPools`，执行
reserve-before-claim并在长生命周期command future结束前持有permit。定向authority RPC、Executor和Sandbox domain测试分别9、3、33项通过。
Controller的microVM Artifact proxy现保留完整closed请求与workload tag，不再转换成普通WASI读取；它与WASI proxy均通过exact Sandbox
Controller URI SAN调用Sandbox Artifact Broker的对应closed RPC。只有WASI与microVM在Sandbox audience进程内共享有界object-store/KMS
client、二次授权runtime和in-flight bulkhead，并分别调用各自typed PostgreSQL authority；Model RPC、Model DB pool与Model permit不得进入
该进程。Managed runtime bundle读取被限制为物理
`Starting`、exact Executor lease、deadline和active `read_whole` package grant；grant回收按workload分流，并以Managed
Job/request/attempt/lease/Executor及Ready sandbox identity幂等验证。全新PostgreSQL 16 Managed fixture和既有Sandbox回归fixture实际通过，
错误Executor/workload均fail closed，重复回收返回相同evidence且active grant归零；不增加表或migration。
Managed Secret one-time delivery也已交付其authority/broker/RPC/deployment切片：microVM Provider以exact workload identity调用Egress，
Egress经Sandbox Controller reserve后才解析Secret，随后由Controller重新锁定并复验exact Job/request/attempt/current lease/Executor、
Provider process generation、sandbox identity、完整prepared canonical digest和ScopedSecretGrant再commit。只有fresh reserve与fresh commit
同时成功才返回bytes；reserve/commit replay与响应丢失均fail closed。`maximum_reads`复用现有Receipt计数，commit写Receipt/Event/Outbox而
不修改Job version；Controller不见明文、Egress无数据库credential、Provider无数据库/KMS/Secret Manager权限，仍为23表/单一migration。
本切片最终通过workspace all-target/all-feature test与doc-test、strict Clippy、public API baseline、crate boundary、cutover residual及
Sandbox deployment合同门禁；两个显式ignored RustFS资格测试仍不计入证据。
实际Managed microVM session Provider已进入独立Provider进程并完成guest Artifact/一次性Secret注入与同实例activation。Managed authority的
非事件化heartbeat也已贯穿closed domain、PostgreSQL和独立gRPC：每次只用exact Job/version/lease/Worker/token续租，返回的新version成为
下一次mutation fence，不延长request deadline/session expiry，也不创建Receipt/Event/Outbox。domain和RPC测试已执行，fresh PostgreSQL
fixture已在全新PostgreSQL 16数据库实际执行通过，覆盖heartbeat、session lost与expired-lease recovery；该证据仍不替代真实Linux
KVM/process qualification。Sandbox domain establishment Worker现在会在
Provider prepare/initialize/activate等待期间按profile heartbeat，将每次返回的新version串行带入下一phase；heartbeat失败时先等待
Provider调用收敛，再对任何已创建实例执行exact destroy，避免取消中的RPC留下孤儿VM。长期liveness heartbeat、terminal supervisor与
expired-lease recovery已在后续切片组合进Executor进程；真实Linux KVM/process资格仍未交付，因此Phase状态不变。
同时修正共享Sandbox Job恢复扫描的队列隔离：有限Capability expired-lease scan现在在SQL候选阶段要求closed
`workload_kind=capability_execution`，不会因同表中的Managed session payload解码失败而阻断整个分片；Managed expired lease仍由待交付的
专用terminal/absence recovery负责。

Managed session fenced lost authority随后贯穿domain、PostgreSQL与internal gRPC：最新物理Job fence、exact cleanup、usage reservation与
四个terminal quota ledger identity共同绑定请求；单事务把旧物理Job推进为`Lost/ReconciliationRequired`并清除lease，保守结算未知
CPU/output、确认Artifact grant已释放，随后清除逻辑opaque session/物理link、设置full reconcile、重排逻辑MCP Job并提交独立
Receipt/Event/Outbox。domain 37项、MCP Host 61项、authority RPC 10项测试及目标strict Clippy已通过；扩展PostgreSQL fixture已在全新
PostgreSQL 16数据库实际执行通过。Provider lifecycle随后补齐closed
liveness与cleanup RPC：observation exact绑定session/request、Executor/Provider generation、lease和sandbox identity；Linux实现同时观察
child exit和PID/start identity，RPC失败不能被解释为`Exited`。cleanup outcome显式区分未创建实例的`Absent`和可供terminal authority使用的
`Destroyed(evidence)`，持久tombstone支持byte-stable evidence重放。Sandbox domain 38项、microVM 5项和RPC 10项定向测试通过。长期
Executor supervisor随后也已组合：microVM进程同时运行有限和Managed两条closed claim driver，共享同一`LocalWorkerPools`；Managed
future在整个session期间持有permit，按profile执行exact observation/heartbeat，并在guest退出、deadline、process drain或观察/续租失败时
先取得`Destroyed(evidence)`，再生成fresh terminal audit/quota identity并以最新fence提交lost。该长期supervisor已交付；expired-lease路径的
后续进度见下文。

absence recovery前置的durable prepared binding也已补齐：`Starting` command、Job payload与replay现在保存并逐字段验证Provider generation、
sandbox identity、旧Executor generation、lease及完整prepared canonical digest；opaque session仍只存在逻辑Invocation。该路径不增加表或
migration，专用scan/proof/recovery现已在下述切片闭合。

Managed expired-lease的专用domain/PostgreSQL authority现已交付：scan按closed workload、manifest/backend、node-local route、bounded shard和
包含tenant的keyset cursor返回token-free exact observation；commit使用旧lease generation的stable Receipt做CAS，支持Accepted requeue、
deadline timeout零使用量结算，以及旧process absence后Provider exact observation的started Lost。业务request digest不绑定恢复Worker，但每次
调用仍携带完整当前Executor registration供Controller重新鉴权；逻辑/物理Job、quota、grant、Receipt/Event/Outbox同事务提交，保持23表与单一
`0001`。三条closed internal RPC现分别承载scan、旧process absence证明与recovery commit；Controller在scan/commit前重验当前recovery
Executor registration和exact microVM URI SAN，absence只委托node-local attestor且不可由RPC失败推断。扫描page新增闭合cursor、batch、backend、
route与database-observation重验。microVM Executor新增专用recovery driver，仅使用reserved critical-control permit；业务permit饱和时仍执行
`Accepted -> Ready | TimedOut`，started候选严格执行`absence/quarantine -> same-node Provider observation -> CAS`，任何证明/Provider失败均保持
durable状态不变。driver已组合进有限claim、Managed claim与NATS control所在的同一shutdown supervisor。Sandbox 40项、Executor package 6项、
microVM backend配置独立冻结recovery shard/scan/jitter/backoff，Helm正向与错误shard负向门禁通过，避免复用100ms业务claim轮询频率。
authority RPC 10项及相关strict Clippy实际通过；PostgreSQL fixture已在全新PostgreSQL 16数据库实际执行scan/requeue/replay通过。
此功能切片已闭合，真实Linux KVM/process-kill/
node-quarantine/escape/saturation Candidate资格仍属于Phase 4/6开放门禁。

随后补齐了Firecracker生产拓扑前置项：新增独立`executor-microvm` DaemonSet与专用KVM node selector/taint toleration，非root Executor
只经node-local mTLS Unix socket调用同Pod的最小Provider。只有Provider容器挂载KVM、host cgroup、持久化jail/state并持有closed Linux
capability allowlist；Executor与Provider的TLS/queue/attestor volume互斥，均无Kubernetes API token。独立ConfigMap生成closed Executor/
Provider JSON，WorkerManifest canonical digest与backend digest共同绑定两端；default-deny NetworkPolicy开放Controller、NATS、DNS和Egress
Broker的必要边，ValidatingAdmissionPolicy逐容器锁定hostPath、credential、capability及专用node contract。部署脚本实际通过4 workload、
6 NetworkPolicy、immutable image、JSON解析及capacity drift负向检查，两个进程config unit共3项通过。该切片建立production-equivalent
部署合同但没有生成Candidate或真实KVM互操作证据，Managed establishment/Provider/guest Secret/terminal开放项及Phase 4/6状态不变。

CR-159关闭Phase 6入口的CandidateManifest machine-contract空洞：closed Rust type与checked-in JSON Schema冻结full tagged Git object ID、
`cand`/`qpr` nominal identity、schema contract version、bounded component image map、canonical WorkerManifest digest set、deployment/limit/
policy/contract digest和UTC创建时间。builder从实际WorkerManifest与HardLimitProfile计算closure，复验拒绝重复role、缺失或额外worker、
limit drift及非canonical顺序；schema已进入`insight.platform/v1`根合同digest，并由Rust和独立Python checker共同验证。这里的
`database_schema_version`是当前值为6的PostgreSQL schema contract version，不是migration数量。该开发期合同不等于实际Candidate，
尚未绑定production-equivalent images/config/topology，也没有Gate A～G或ReleaseManifest，因此Phase 6保持Pending。

CR-160关闭真实Phase 4 PostgreSQL验证暴露的shared Network Policy合同冲突：`PolicyKind::Network`同时服务MCP、Model、Capability与
Sandbox，通用Revision允许用closed AuthoringPackage加`rules_digest`承诺领域正文，不能被Resource合同强制携带Sandbox typed body；
只有被Sandbox Profile引用的exact Revision必须携带完整`SandboxNetworkPolicyDocument`，且Sandbox repository继续逐字段和digest
重验。Contracts定向测试以及全新PostgreSQL 16的OAuth、subscription/Managed与Sandbox suites实际通过；同时把cleanup evidence时间
fixture改为使用数据库时钟，避免宿主与数据库微小时钟偏差制造伪失败。该修订不放宽Sandbox egress、不增表或migration；跨节点clock-skew
与真实网络隔离仍属于Phase 6资格。

CR-161关闭Model Artifact-backed request在claim与object read之间缺少exact authority的问题：closed read request冻结tenant、ModelTurn、
当前Job version/fence/lease/Worker generation、request digest、deadline、exact RunValue与active ModelTurn ArtifactLink；PostgreSQL在同一
snapshot校验逻辑值、grant和Ready Artifact/Verified Blob后生成非持久物理投影，Model Artifact Broker在I/O后以同一请求再次授权，Worker再按
Model hard limits复核strict canonical JSON与逻辑content digest。fresh PostgreSQL 16 fixture证明有效授权可稳定重放且陈旧Job fence被拒绝，
Broker/Worker unit覆盖同一exact-object pipeline与非canonical正文拒绝。该切片不增表、migration或locator泄漏；独立versioned Broker RPC和
生产Model Worker组合现已交付：`ArtifactModelBrokerService`只有一个Model read方法，exact Model Worker URI SAN在进入authority前门禁，流式响应逐项验证
request/content/chunk digest、sequence、长度和唯一terminal；独立服务使用restricted read-only repeatable-read PostgreSQL role。真实
PostgreSQL 16 fixture证明该role完成同一授权路径并拒绝业务更新与Secret读取；既有Model endpoint的Helm/网络门禁仍是有效子证据，
但不能证明CR-162要求的双audience物理隔离。Artifact-backed output、
真实S3/KMS负向资格仍属Phase 4开放项。

CR-162重写Artifact Broker物理边界：当前实现不再是一个三方法进程，而是Model与Sandbox两个audience-isolated服务。Model Broker进程只注册
`ArtifactModelBrokerService.ReadModelRequest`，Sandbox Broker进程只注册`ArtifactSandboxBrokerService.ReadWasiArtifact`与
`ReadMicroVmArtifact`；只有WASI与microVM共享Sandbox audience bulkhead。两者必须使用不同Deployment/Service/ServiceAccount、restricted
PostgreSQL credential/pool、mTLS server identity、storage workload identity、NetworkPolicy和process-local permit，禁止单listener动态选
audience或交叉RPC。代码与Helm已完成该切分；Controller只持Artifact Broker mTLS client，WASI/microVM outward proxy共享一个在上游read前
取得的process-local response permit，默认1、hard max 4，并以逐chunk stream持有到completion/drop/absolute deadline。静态拓扑、错误caller
mTLS、跨lane saturation、慢消费者、零字节与deadline回收门禁通过；真实集群credential互换、单边滚动重启和真实S3/KMS资格仍Open，故不关闭Phase 4。
该修订复用Artifact/Blob/Link与无状态实现library，不增加表、migration或第二read authority；Artifact-backed output、真实S3/KMS及
完整Phase 4资格仍保持开放。

CR-163冻结Sandbox runtime bundle容量合同：HardLimitProfile version 4新增必填
`capability_sandbox.runtime_bundle_bytes={unit:bytes,hard_max:67108864,q1_default:33554432,overflow_outcome:content_rejected}`。
SandboxPackage发布以同一事务锁定Ready runtime-bundle Artifact与Verified Blob，在创建ResourceVersion前拒绝零字节、超过hard max或
ArtifactRef元数据漂移，并要求Published document canonical digest等于validated draft，禁止换成另一个合法Ready bundle；admission、Sandbox
Broker与Executor从同一Candidate profile读取effective limit且只能收紧。WASI module继续受16 MiB更严格上限。checked-in schema/Q1实例、
Rust exact validation、Candidate digest门禁及fresh PostgreSQL缺失/状态/digest/size/swap正负fixture已通过；真实执行与Phase 4/Gate A/E其余资格
仍Open。复用ResourceVersion/Artifact，不增加表或migration。

CR-164修复microVM候选部署闭包：此前Helm直接执行`platform-sandbox-microvm-provider`并引用Firecracker/kernel/rootfs路径，但默认
Dockerfile既未构建/复制该Provider，也没有为runtime bytes声明image或mount，旧静态检查会把必然启动失败的Pod误判为合格。现在
Dockerfile提供`sandbox-microvm-executor-runtime`和`sandbox-microvm-provider-runtime`目标；builder/runtime base冻结multi-arch digest，builder
直接运行于requested target platform，每个target只复制自己的平台可执行文件且不复制对方或shared platform payload。shared image明确不含
root Provider；Helm要求三者使用不同repository与immutable digest。WASI、attestor与microVM node selector改用kubelet不可自贴的
`*.node-restriction.kubernetes.io/*`标签，三者固定同一Linux `amd64 | arm64`架构；attestor有独立selector并只额外容忍microVM taint，因而
覆盖两个执行pool而WASI Executor不能进入KVM pool。全部workload/NATS Secret名称互异。Provider的KVM、runtime asset、jail、state与cgroup
hostPath固定且互不别名；只有Provider在Kubernetes 1.33+递归只读挂载root-owned `/opt/insight/microvm-runtime-assets`。部署声明只接受Firecracker `1.16.1`并将
Firecracker/jailer路径绑定同一version segment，kernel/rootfs保持在asset root；Provider ready前复验leaf owner、mode、length和exact SHA-256，
但version字符串不替代真实per-arch bytes证明。

AdmissionPolicy现覆盖`pods`与`pods/ephemeralcontainers`，逐角色闭合volume source/mount、image/command、env/probe、credential、CPU/memory
resource、nodeName和Pod/container seccomp/AppArmor/SELinux/capability边界，拒绝额外volume、`subPath`、lifecycle、envFrom、runtimeClass、
Pod-level/DRA/extended-resource device、secondary-CNI metadata和debugger注入；全部Binding使用Kubernetes维护的exact namespace-name label，
受限子资源policy恒拒绝Executor namespace的exec/attach/port-forward/resize；Pod固定default scheduler，binding只接受Candidate配置中经cluster
audit确认的exact scheduler identity、Node target、空annotation及region/zone topology label，CREATE只接受经audit确认的exact DaemonSet
controller identity与唯一role ownerReference，UPDATE保持owner逐字段不变。部署门禁通过4 workload、
6 NetworkPolicy正向渲染；Dockerfile instruction及Pod security-projection mutation和Helm错误override覆盖Provider build/copy/leak、mutable
base、build-host binary、shared image/Secret、hostPath alias、非递归只读asset mount、错误node/version/asset path，最终CEL由Kubernetes 1.35.6 server-side dry-run编译。
当前Dockerfile的两个release target已按默认`linux/amd64`实际构建并检查arch、platform binary inventory与UID/GID；相关22项定向测试及strict
Clippy通过。该切片只关闭静态启动依赖与已知准入绕过；真实Admission Deny与cluster audit identity fixture、runtime asset bytes及ancestor/TOCTOU、node
provisioning、镜像签名/SBOM/provenance、Linux capability充分性、KVM/jailer/guest-agent与Candidate资格仍Open，Phase 4/6状态不变。

CR-165正在关闭Model Artifact-backed output及Installation Release authority的合同空洞；全量cross-review完成前不得登记合同关闭或继续生成
对应实现。现有Model Artifact Broker继续保持
`ReadModelRequest`单RPC、restricted read-only PostgreSQL role与独立read permit；不得把object write、KMS seal、Artifact mutation或大正文
上传塞入该进程。新增目标角色Model Artifact Producer只接受exact
`spiffe://insight.platform/workload/model-worker.artifact-output` mTLS的closed client-stream stage RPC并拒绝read client身份，同时使用独立进程、
Deployment、ServiceAccount、write-limited PostgreSQL credential/pool、S3/KMS write identity和process-local permit；它也不得注册Model/Sandbox
read RPC或推进Model current state。

Model claim必须在Provider dispatch前冻结04 typed Model-output ArtifactIo revision、Retention revision、Model Deployment exact Ready duration、
effective Inline/response/Artifact上限与最坏staging容量；start事务用数据库时钟计算`attempt_deadline + staging_grace`，预留Artifact、candidate
Blob、duplicate-cleanup Job、grant、stage/terminal Receipt、Output Link/RunValue，以及Artifact-owned count/logical和candidate-Blob-owned
upload/staging/physical两个Quota bundle。Job `expected_version`只作为同generation单调lower bound；Producer每个短事务仍重验immutable
attempt、lease generation/token、WorkerProcessGeneration、request/admission/binding/deadline并取得会与cancel/takeover/terminal冲突的共享guard。
Candidate还要冻结closed storage-binding manifest及最大PUT completion uncertainty；staging grace必须严格越过write-quiescence boundary，
cleanup在`staging_retain_until`前不能采纳delete/absence或释放Blob bundle。Producer transport timeout从accepted backlog开始覆盖TLS、permit等待与
完整Header decode，valid Header后才切换到Attempt deadline，防止pre-header slowloris占满stream/buffer。

Producer执行`Staging -> Uploaded -> Verifying -> Verified`受限physical protocol与closed failure矩阵：Processing transient
Dependency/InProgress只缩短/观察lease，Conflict不改existing Receipt，fresh stale/deadline不由Producerterminalize，TooLarge/Invalid写Rejected，
Integrity写Failed并允许candidate从current Staging/Uploaded/Verifying进入Quarantined，成功把Verified evidence与Succeeded Receipt原子提交。完整security-domain dedupe区分
`PreexistingHit | CandidateWinner | RacingCandidateLoser`；receipt返回candidate/resolved Blob、新增physical bytes及candidate cleanup
generation/Job ID。race loser由预留InternalBlob cleanup Job收敛；Producer不创建业务Reference、Ready、RunValue、quota settlement、Event或
Outbox，也没有application queue。

Model terminal仍是唯一caller-owned PostgreSQL first-winner：按Receipt、Model+两个output quota bundle、current/cleanup Job、Artifact/Blob的
排序锁序，原子执行`Verified -> Ready`、以terminal数据库时钟加冻结duration保存absolute retention、建立Output Link/RunValue、提交
ModelTurn/Job/Receipt/Event/Outbox并按dedupe disposition结算两个bundle。Artifact删除只Refund count/logical；new-winner Blob的physical
bundle跟随Blob到最后alias物理删除，preexisting/race candidate按无对象/cleanup evidence Close。Inline/no-object关闭两bundle；已有candidate
时只先关闭Artifact bundle，Blob bundle保持到GC。Model-output路径本身不新增专用表、第二Artifact lifecycle或terminal authority；唯一新增
持久authority是installation singleton，因此clean-cut总体目标为24张总表/23张业务表/schema v7，仍直接使用单一`0001`。

CR-165重开前已存在一段局部实现证据：04的closed `ModelOutputArtifactIoPolicyDocument`、独立checked-in JSON Schema与root contract digest、
`PolicyResourceSpec` exact variant/rules digest validation，以及显式接收effective staging/Ready界限和Candidate PUT uncertainty的pure checked
time closure。fixture覆盖unknown field、media/ID/digest、ceil+margin、staging窗口、Ready duration与terminal绝对时间；全workspace all-target
compile保持通过。它只能作为当前局部证据，不能越过Draft门禁，也不表示Model Deployment、installation state、reservation、Producer或
Artifact-backed current path已实现；相关规范恢复Accepted前不继续扩写该目标实现。

实现前machine审计沿00 §3既有DAG修订，再按18 deployment/release→17 API→18 qualification的章节顺序执行cross-review：Candidate草案
显式冻结Inline-only/Artifact-capable mode，四个manifest digest集合具有required/空集/上下界/顺序/唯一语义，
ComponentRuntime/Storage manifest wire closed，typed startup closure的role-scoped pool/semaphore identity执行全Candidate alias拒绝，4096-byte
protobuf overhead由protocol document/schema进入root machine contract。00、02～10、12、14～18及ADR-0001在终审关闭前都不是新增目标实现输入。

cross-review关闭后，下一实现切片按依赖顺序交付：

1. 公共`InstallationId`、`CanonicalRegion`、`ComponentRole`与RunBindingsSnapshot v2，并把`encryption_domain/enc` registry exposure原子改为
   public、删除无shared owner的internal `task/tsk`，shared Task只接受`apr/int`；10的Invocation字段同时改为nominal
   `ApprovalTaskId`/`InteractionId`并交付closed owner/state machine schema；BackendInputRequest不得含ID，Input `int_`由owner
   JobCommit first-winner事务分配并通过Receipt/result ref稳定重放；
2. 先直接重写未发布单一`0001`、schema contract v7和verifier，交付唯一`InstallationReleaseState`的Uninitialized provisioning、operator
   bootstrap audit、03 installation scope/锁序/Receipt ID-state-lease-result与repository CAS；在此步骤结束前不得实现任何会推进
   compatibility generation的tenant mutation、Candidate或Release switch；
3. 在步骤2 authority上先交付15 `ArtifactStorageBindingManifest` schema与installed binding resolver port，再交付shared Approval Task
   owner/state、04 `TenantEncryptionDomainBinding`与current encryption fence，最后接17 encryption-domain approval-request/apply `/v1` API；
   apply必须用exact resolved storage/KMS manifest验证proposal，锁定并推进`InstallationReleaseState.compatibility_generation`，fixture覆盖
   ETag/幂等/权限、approve/deny/cancel/expiry、Task/Invocation state coupling及add/rebind/revoke generation CAS；
4. 先交付canonical Model response唯一machine schema及digest/sub-digest、closed `ModelResponseSemanticEvidenceV1`、Rust/protobuf
   success+tagged failure、15 Model-output content-validation profile registry；再原子升级HardLimitProfile v5十一项、WorkerManifest v2、
   `ComponentRuntimeManifest`、ComponentStartupManifest/startup-profile registry、sealed same-source projection与
   唯一capacity primitive factory，并完成16 pure installation compatibility result及canonical response-contract逐值相等fixture；
5. 只消费步骤1～4 sealed inputs构造Candidate exact closure，交付Candidate machine schema/builder、content-addressed Candidate resolver及
   Inline-only/Artifact-capable、digest集合、全Candidate alias、storage timing和4096-byte overhead正负向fixture；Candidate不得在builder内重新实现
   response-contract或compatibility逻辑；
6. 在Candidate closure完成后交付content-addressed Qualification/Approval resolver、exact A→G GateResult、ReleaseManifest builder，再实现
   Receipt-first capture→resolver/active-Model scan→final CAS及activation/promotion/rollback/root-child Run并发fixture，最后接17 Installation
   Release GET/promote/rollback adapter；该批次追加encryption add/rebind/revoke与Release switch/root Run admission的真实并发fixture，不得用
   Release scan或API adapter反向补齐Candidate compatibility；
7. 交付15 current content-evidence aggregate、closed ArtifactGrant/Receipt token replay、Ready read projection、Gateway-only proxied download、三个
   read Broker的terminal-use spool及两个quota bundle reservation；
8. 先交付ordinary-output stage machine schema/protobuf、五个exact method映射、per-attempt identity/failure matrix与ArtifactVerify/scan Job/Event事务，
   再实现Artifact Workload Producer repository/RPC、restricted DB/staging S3/KMS role、ComponentRuntime/startup profile、process binary及
   Registry/Capability/Context/MCP/Sandbox caller adapter；Candidate必须至少安装一个scope并覆盖全部enabled ordinary-output binding；
9. 在步骤7～8 shared foundation上实现Model Producer core/RPC、two-phase admission、restricted PostgreSQL projection、S3/KMS/dedupe/checkpoint、
   Hybrid materializer与独立生产进程；
10. 完成owner-finalize、candidate/orphan cleanup、shared-Blob quota lifecycle、真实PostgreSQL并发/崩溃fixture，最后交付八个Artifact role的独立
   Helm/NetworkPolicy/credential互换、逐lane饱和与真实S3/KMS资格。

全部代码、部署和fixture落地前，checked-in schema v6/23张总表、profile v4/WorkerManifest v1及Inline output仍是当前证据，
`model_output_artifact_required`仍是缺功能的pre-dispatch拒绝；普通Artifact-backed output也尚无上述Workload Producer目标实现。CR-132/CR-148、
Phase 4及Gate B～E继续Open，既有generic producer不得替代步骤8的exact服务合同与隔离证据。

clean-cut baseline现由部署期独立provisioning流程对fresh PostgreSQL target一次性安装；Platform运行时crate已删除DDL apply入口，
API/Scheduler/Worker只做read-only schema verification。旧`coordinator.rs`实现路径改为role-neutral orchestration模块，cutover gate
不再把ADR-defined外部migration ledger表名误判为运行时migration authority，但会继续拒绝任何Rust `apply_migrations`或旧
runtime migration symbol。
