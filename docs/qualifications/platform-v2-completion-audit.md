# Platform v2 spec00～18 完成度审计

状态：In Progress / repository and production gaps remain

日期：2026-08-24

本审计按 `00-overview.md` 的统一完成定义和 `implementation-plan.md` 四阶段 exit gate 核对当前工作树。
它记录可以复现的证据与缺口，不改变合同，也不把存在源码、测试或静态清单等同于 production behavior。

## 1. 结论

00～18均已完成CR-180影响cross-review（未受影响合同保留CR-173～179语义版本）并处于Accepted，但没有任何一份可以推进到Verified或Archived。Phase 1的仓库内
实现与真实PostgreSQL门禁已闭合；Phase 2/3已有大量domain/repository/runtime库和L1～L3证据，但缺少若干production
composition；Phase 4只有public API及部分role清单，完整物理拓扑、observability和L4～L6尚未交付。

因此：

- `docs/current`继续描述旧current behavior；
- `implementation-plan.md`保持In Progress；
- 不生成通过的QualificationEvidenceManifest或production CapacityProfile；
- 不执行GitOps clean cut、规范归档或状态升级。

## 2. 已证明证据

| 范围 | 当前证据 | 结论 |
|---|---|---|
| 合同 | 00～18 CR-180影响cross-review闭合；00、05～07、17～18为`Accepted / CR-180`，其余保留既有CR语义版本；generated contracts checker通过 | 合同可作为实现输入，不证明实现/资格完成 |
| Persistence | schema contract v7、唯一`0001_platform_baseline.sql`；PG16/17 fresh baseline与事务/并发测试 | Phase 1 persistence闭合 |
| Rust workspace | workspace all-target/all-feature tests与Clippy `-D warnings`通过 | L1～L3范围内有效 |
| NATS/MCP | real NATS integration与外部TypeScript/Go MCP SDK interop通过 | 证明被执行的协议fixture，不证明production MCP Host部署 |
| Public API | `/v1` OpenAPI/owner schema、route负向conformance与root public API baseline通过 | public contract实现闭合 |
| Typed Plan materialization | Agent Revision冻结`typed_plan_artifact_id`与digest；发布事务校验Ready JSON Artifact/Verified Blob；Scheduler专用mTLS Data RPC以Run、Job lease、exact Plan Revision和ArtifactRef双重授权读取 | 闭合Scheduler物化输入与传输边界，不代表production Scheduler handler完成 |
| 已有部署 | Gateway、Callback、MCP cleanup、Model Worker、Artifact三role、Sandbox、Security/Egress Helm静态门禁通过 | 只证明这些checked-in清单的静态边界 |
| HTTP observability | shared bounded-label owner；Gateway与Callback具备request/outcome、latency、ready、`/metrics`及ServiceMonitor/NetworkPolicy | 闭合两个公网HTTP role，不代表全平台observability |
| gVisor | Launcher RBAC/admission脚本、chart和fail-closed preflight已实现 | development静态证据；无真实runsc L4结果 |
| Qualification contracts | QualificationProfile/Candidate/Capacity/Evidence nominal type、closed schema与digest validator | 可验证证据形状，不证明任一外部门禁通过 |
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

### 仓库内缺口

1. `insight-platform-runtime`只有library，没有Scheduler/Recovery production binary、process config、startup manifest、
   readiness/drain和Helm Deployment。
2. `StartedOrchestrationJobHandler`已有正式materialize→commit/handoff生命周期adapter；PostgreSQL durable Plan store已闭合
   Start/Compute/Branch/Fork/Join/Map/Loop/ErrorBoundary成功控制路径的durable fact读取、Inline RunValue物化、精确mutation-slot
   规划、通用或derived fenced commit与retry handoff；FailNode现在也从locked facts推导ErrorBoundary/structured-exit/wake/
   sibling-cancellation槽，并在失败owner transaction重验expression evidence。Run terminal及Leaf/Task/Subagent分派尚未接入该store，
   Artifact-backed RunValue仍缺Scheduler侧Artifact Data RPC materializer，因此完整production composition仍未闭合；
   exact typed-plan Artifact的Scheduler专用Data RPC已闭合canonical envelope、
   exact workload identity、Job lease/Run/Plan/Artifact PostgreSQL authority、读取前后双重授权和deadline/stream backpressure；
   Scheduler侧也已用当前fence从PostgreSQL解析descriptor并完成canonical JSON、Plan limits和semantic digest复验，但把该Plan
   交给真实Plan/Capability/Task/Subagent状态机的production handler仍未实现。
   closed expression owner、纯确定性evaluator、Plan节点与HardLimitProfile v5消费现已落地，production driver API也不接受外部
   observation；CR-176 Scope data-port environment owner、root/child binding、bounded lexical lookup、exact Inline/Ready Artifact
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
   coordinator链；它证明正式adapter/store的进程内组合，但仍不是独立Scheduler binary或多进程crash/restart证据。
   CR-177的L1/L2 owner规则已实现，L3完整process boundary仍待完成。
   CR-178的Plan version 2、exact Map item port owner validation、version 1/wrong producer L1负向、每item RunValue/MapItem Scope原子写及
   L2 batch/replay fixture已实现；process crash/restart的L3 fixture仍待production handler闭合后完成。
   CR-179的exact pair producer/schema/region L1与两轮rollover/Scope复用/不串值/false-exit L2已实现，并证明所有iteration Scope保持
   同一root controller owner、词法深度不随轮次增长；process crash/restart L3仍待production handler闭合后完成。
   CR-180已冻结Plan version 3的Return/Raise exact terminal port与Agent output/error schema authority；当前代码仍是无字段terminal的
   Plan version 2，因此Plan v3 publication validation、terminal RunValue物化及owner transaction尚未实现，不能计入Phase 2完成证据。
3. 没有独立Capability Worker与Context Worker process composition、role-scoped DB pool/queue/permit和deployment。
4. 当前多进程end-to-end证据由fixture拼装ports，不能替代上述production composition。

## 5. Phase 3 审计

### 已满足

- Artifact Gateway/Data Worker/Maintenance binaries、role grants、mTLS调用边界和Helm清单；
- Context/Dataset/Text2SQL domain、repository与negative fixtures；
- remote Streamable HTTP MCP协议/OAuth/Task/subscription实现与SDK互操作fixture；
- restricted WASI runtime、gVisor Controller/Launcher/guest/attestor协议和静态准入闭包；
- Model provider/turn/adapters、Inline-only与独立Model Worker清单；
- Security Authority与Egress/Secret broker binaries及隔离清单。

### 仓库内或外部缺口

1. remote MCP Host缺少production binary和独立Helm release；现有cleanup worker不等于Host。
2. Context query/build和Capability execution缺少production worker composition，因此无法形成完整real end-to-end链。
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
- production QualificationProfile、Candidate/Capacity/Evidence validator、拓扑preflight和资格运行手册。

### 仓库内缺口

1. 18列出的Scheduler/Recovery、Capability Worker、Context Worker、MCP Host等独立物理role没有release chart。
2. 除Public Gateway与Callback API的首批Prometheus SLI外，其余Platform v2 binaries仍只有结构化日志或process-local snapshots；缺少完整
   Prometheus/OTel export、低基数queue/dependency/recovery指标、trace propagation/redaction的process wiring。
3. Gateway与Callback已有ServiceMonitor；其余role仍缺少ServiceMonitor/PodMonitor，且全平台尚无dashboard、symptom-first PrometheusRule与逐alert runbook。
4. 没有把全部role render/startup manifest/NetworkPolicy/DB pool/identity互斥纳入一个完整release topology checker。
5. 没有可重现的signed image/SBOM/provenance build pipeline与GitOps environment repository输入。

### 外部门禁

- production-equivalent多节点Kubernetes、独立WASI/gVisor node pool、exact runsc与支持范围内kubectl/server版本；
- L4 RBAC/mTLS/NetworkPolicy/admission与真实协议/故障矩阵；
- L5 mixed load、lane saturation、SLO/error budget和不少于86,400秒soak后冻结CapacityProfile；
- L6 signed supply chain、upgrade/rollback、backup/restore、GitOps rollout/rollback与人工promotion；
- clean `/v1` replacement后更新`docs/current`，再将00～18推进Verified并归档。

## 7. 下一实现顺序

按上游到下游执行，且每批通过后提交：

1. Plan v3 Return/Raise publication validation、terminal RunValue物化与owner transaction；
2. Scheduler/Recovery binary、真实orchestration handler和独立release chart；
3. Capability Worker、Context Worker、remote MCP Host production composition与charts；
4. 将已闭合的shared low-cardinality HTTP observability boundary逐role接入，并补queue/dependency metrics、trace/redaction；
5. ServiceMonitor/dashboard/alerts/runbooks和完整topology静态checker；
6. reproducible signed candidate pipeline与production runner入口；
7. 外部L4～L6、GitOps clean cut、current文档与规范归档。

如果实现发现domain port不足以支持production handler，必须先按02→06/07/09/10→17/18修订合同并重新cross-review，
不得在binary中以自由JSON、in-memory authority或host process execution绕过缺口。
