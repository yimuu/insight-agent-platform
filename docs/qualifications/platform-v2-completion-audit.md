# Platform v2 spec00～18 完成度审计

状态：In Progress / repository and production gaps remain

日期：2026-08-26

本审计按 `00-overview.md` 的统一完成定义和 `implementation-plan.md` 四阶段 exit gate 核对当前工作树。
它记录可以复现的证据与缺口，不改变合同，也不把存在源码、测试或静态清单等同于 production behavior。

## 1. 结论

00～18均已完成CR-196 cross-review（历史CR-173～195结论保留）并处于Accepted，但没有任何一份可以推进到Verified或Archived。Phase 1的仓库内
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
| 合同 | 00～18 CR-196 cross-review闭合；Plan v4 external leaf、MCP subscription→Context durable admission、closed owner pair、refresh execution/outcome及OAuth token TLS trust均冻结；generated contracts checker通过 | 合同可作为实现输入，不证明实现/资格完成 |
| Persistence | schema contract v7、唯一`0001_platform_baseline.sql`；PG16/17 fresh baseline与事务/并发测试 | Phase 1 persistence闭合 |
| Rust workspace | workspace all-target/all-feature tests与Clippy `-D warnings`通过 | L1～L3范围内有效 |
| NATS/MCP | real NATS integration与外部TypeScript/Go MCP SDK interop通过 | 证明被执行的协议fixture，不证明production MCP Host部署 |
| Public API | `/v1` OpenAPI/owner schema、route负向conformance与root public API baseline通过 | public contract实现闭合 |
| Typed Plan materialization | Agent Revision冻结`typed_plan_artifact_id`与digest；发布事务校验Ready JSON Artifact/Verified Blob；Scheduler专用mTLS Data RPC以Run、Job lease、exact Plan Revision和ArtifactRef双重授权读取 | 闭合Scheduler物化输入与传输边界，不代表production Scheduler handler完成 |
| Typed Plan v4 wire | RuntimePlan保存closed dependency slots及全部external leaf payload，拒绝v1/v2/v3并验证slot kind、output producer、input reachability与bounded budget；fresh PG的phase2 Run kernel和真实coordinator既有路径通过 | L1/L2 wire与controller已闭合；Timer/Signal/HumanTask/ChildAgent、Model tool-result、Capability和Context的production component L3均有独立证据 |
| Candidate selection owner | `PolicyKind::Selection`要求非空schema v1 document且`rules_digest`绑定canonical bytes；共享纯evaluator实现only-candidate/ordered-first/route-hash、canonical candidate order与evidence digest；各owner按Run冻结exact Policy/Revision重算并拒绝伪造结果 | L1/L2 owner闭合，production Model/Capability/Context dispatch已在对应L3链路重验exact binding |
| 已有部署 | 11个chart覆盖全部15个ComponentRole、19个隔离pool；Gateway双role、Orchestration、Model、Capability Native/Remote、Context Native/Remote/Subscription、MCP Tool/Resource Host、Sandbox、Artifact三role及Security/Egress全局render门禁通过 | L1静态闭包；不替代live L4 |
| HTTP observability | shared bounded-label owner；全部19个ComponentRole workload pool、Sandbox两种process attestor及OAuth Cleanup Worker具备ready、`/metrics`及ServiceMonitor/NetworkPolicy，公网role另有request/outcome/latency | process wiring与静态部署闭合，不代表真实scrape或完整业务observability |
| Dashboard/alerts | 独立chart提供role-filtered process/HTTP、Orchestration及Outbox业务dashboard、12条symptom-first PrometheusRule和逐alert checked-in runbook；CI拒绝非法threshold、非HTTPS runbook、高基数/Secret label与缺失discovery metadata | 已有series的L1运营合同闭合；不替代完整业务SLI或真实alert delivery |
| Worker/queue telemetry | Orchestration、Model、Capability Native/Remote、Context Native/Remote/Subscription、Sandbox WASI/gVisor共9个pool从各自LocalWorkerPools导出business/critical-control available/used permit；Orchestration另有active jobs、claim/recovery outcome及PostgreSQL authority的due/expired-lease Job与due/expired-claim/dead Outbox count、oldest lag及observation health | Orchestration Job和shared Outbox backlog/recovery lag及9/19 pool permit L1 telemetry闭合；其余role authority及saturation仍待接线 |
| Telemetry redaction | production Rust source静态门禁拒绝identity、Secret、prompt/response、object key及URL进入structured tracing或插值日志；现有LLM/SSE/MCP OAuth/conversation/worker启动日志已清理 | source-level L1负向合同；不替代动态payload审计、trace propagation、RBAC/retention或production验证 |
| gVisor | Launcher RBAC/admission脚本、chart和fail-closed preflight已实现 | development静态证据；无真实runsc L4结果 |
| Qualification contracts | QualificationProfile/Candidate/Capacity/Evidence nominal type、closed schema与digest validator；live topology/workload preflight对照Candidate/Capacity并拒绝rollout、image、config、identity、安全和容量漂移 | 可验证证据形状与preflight行为，不证明任一外部门禁通过 |
| Runbooks | production dependency recovery与GitOps clean-cut手册已提交 | 操作准备完成，execution evidence pending |

最近一次完整仓库复核使用全新PG16数据库、NATS和all-feature workspace测试；工作树完成批次均按单一目的提交。
这些结果在代码或环境改变后必须由CI重新产生，不能长期当作release evidence复用。

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
   静态脱敏门禁；Orchestration现另有PostgreSQL authority的due/expired-lease Job及due/expired-claim/dead Outbox backlog/lag与observation health；仍缺其余role
   backlog/dependency health、其余10个pool的role saturation、跨进程trace
   propagation、动态payload采集审计和production scrape证据。
3. 全部19个pool及Sandbox process attestor已有ServiceMonitor；process/HTTP/Orchestration/Outbox dashboard及逐alert
   runbook已扩展到13个panel和12条alert，包含Orchestration Job、shared Outbox lag/dead queue及PostgreSQL observation failure；其他role backlog/
   dependency与saturation对应的panel/alert仍待指标owner接线后补齐。
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

1. 补其他role dependency/recovery/permit业务指标、跨进程trace propagation和动态payload采集审计；
2. 为新增业务series补dashboard、symptom-first alerts及逐alert runbook；
3. 在受保护CI environment实际运行signed candidate producer并把exact bundle交给GitOps environment repository；
4. 外部L4～L6、GitOps clean cut、current文档与规范归档。

如果实现发现domain port不足以支持production handler，必须先按02→06/07/09/10→17/18修订合同并重新cross-review，
不得在binary中以自由JSON、in-memory authority或host process execution绕过缺口。
