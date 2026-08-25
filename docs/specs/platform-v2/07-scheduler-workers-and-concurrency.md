# Platform v2 Scheduler、Worker 与并发规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-189 |
| 日期 | 2026-08-25 |
| 依赖 | 02、03、04、06 |
| 直接下游 | 08、10、12、14、16、17、18 |

> CR-188 impact：Capability Native/Remote Worker startup manifest必须分别报告exact installed adapter/protocol codec集合；claim在
> PostgreSQL owner transaction重验Invocation冻结的required Worker manifest，dispatcher再重验exact codec descriptor。现有WorkClass、
> lease、fairness、permit和drain合同不变。

> CR-189 impact：Context Worker适用同一双层门禁。claim在lease/quota mutation前匹配Context Deployment required Worker manifest；
> remote dispatcher在Egress I/O前再次匹配installed adapter、canonical endpoint digest及Network/TLS/Trust closure。漂移实例停止claim
> 并使readiness失败，不能先占额度再发现配置不兼容。

## 1. 决策摘要

Scheduler只从PostgreSQL已提交事实推进durable work，Worker按WorkClass隔离执行。NATS只携带
wake hint。每个Job只有一个当前lease generation，旧generation不得heartbeat或commit。

首版不为Model Artifact输出、Managed stdio或microVM增加专用WorkClass、claim池或容量模型。

## 2. 目标与非目标

目标：

- 多副本安全claim、续租、fence commit与崩溃恢复；
- tenant、WorkClass和backend层级并发与公平调度；
- 业务工作、Sandbox、Artifact与critical-control容量隔舱；
- 延迟工作不占用常驻future或Worker permit；
- 丢失通知、进程崩溃与滚动发布时最终推进。

非目标：

- 不把NATS、Redis或内存queue作为Job authority；
- 不保证全局FIFO；
- 不在Scheduler线程执行Provider、MCP、Context、HTTP或代码；
- 不为未证明的负载提前建立专用调度子系统。

## 3. WorkClass

```rust
enum WorkClass {
    RegistryValidation,
    Orchestration,
    Model,
    CapabilityNative,
    CapabilityRemote,
    Mcp,
    Context,
    Sandbox,
    Interaction,
    Artifact,
    Recovery,
}
```

WorkClass是closed machine registry，由03拥有。backend映射固定如下：

- Native Capability → `CapabilityNative`；
- HTTP/gRPC Capability与MCP Tool adapter → `CapabilityRemote`；
- MCP discovery、remote subscription等Host自有工作 → `Mcp`；
- WASI或gVisor代码 → `Sandbox`；
- Artifact stage/read/verify/maintenance → `Artifact`。

一次Sandbox物理执行只有一个shared Job，不先建CapabilityRemote Job再建Sandbox Job。

## 4. 角色与边界

Scheduler：

- claim Orchestration/Recovery工作；
- 调用纯Plan transition函数；
- 提交Node、Job、timer、cancel与outbox command；
- 不执行leaf I/O。

Scheduler开始每个Orchestration generation后，必须以当前`(JobId, lease_generation, lease_token_digest, worker_process_generation_id)`
和Run binding向Artifact Data Worker请求exact Typed Plan。Data RPC只接受Scheduler workload identity；PostgreSQL在读取前后重验
Run、Plan Revision、Artifact/Blob与lease，RPC deadline不得越过lease/job deadline。Scheduler随后用05 closed evaluator读取exact
immutable RunValue并生成带evidence digest的controller command。调用方、NATS hint和Worker结果不能直接提供Branch target、Map item
count、Loop condition或Compute output。表达式求值消耗Orchestration本地CPU/内存permit，不调用Provider/MCP/Context/HTTP/Sandbox，
也不占leaf WorkClass pool。

Scheduler可携带事务外物化的value正文，但不能决定Compute output classification；repository在提交事务从exact input RunValue rows
按05 lattice规则重算effective classification，空input闭包使用`Internal`，并把同一结果写入全部Compute output。

Return/Raise使用05当前Plan v4（CR-180首次引入于v3）冻结的exact terminal port。Scheduler只能请求物化该port解析出的immutable RunValue正文并生成bounded
terminal evidence；不能选择另一个value、声明schema/classification或把Worker outcome直接当Run terminal。owner transaction必须重新解析
当前Scope环境、重验RunValue及Interface schema，并以同一提交关闭Node/Job/root Scope/Run及写Receipt/Event/Outbox。Artifact Data RPC
deadline不得越过Job/Run deadline，读取前后沿用exact lease与Run binding双重授权。

External leaf使用05 Plan v4。Scheduler读取node的exact input/route port，经同一brokered RunValue read路径获得正文，执行04 closed
candidate selector并生成`CandidateSelectionEvidence`，再调用06按leaf kind区分的owner command。Scheduler只负责纯选择和command transport；
repository重新加载exact Plan/Policy/binding并重验evidence。Task definition、child budget、retry、timeout、output port与resume target不得由
Scheduler配置文件、NATS payload或Worker outcome补充。dispatch提交后原Orchestration Job已terminal，Scheduler crash/replay只能得到同一
Invocation/Task/Wake/ChildRunLink；external leaf terminal由对应WorkClass owner及critical-control recovery终结当前leaf Node，并按exact
Plan `resume`创建目标NodeExecution和新的Orchestration Job，不重新claim同一leaf Node。

CR-182 selector evaluator是Scheduler与repository共享的纯Rust owner library：canonical candidate ordering与route hashing只能实现一次，
两侧消费同一nominal type；不得在SQL、Worker或adapter中各写一套近似算法。

Loop body settlement的owner command必须携带bounded rollover mutation slots（下一Scope、每个carried RunValue及Event/Outbox ID），
但不能携带carried正文、classification或port。repository从exact Plan和当前iteration Scope解析`body_output_port`，复制immutable
RunValue并绑定`next_iteration_port`；随后以同一事务关闭当前Scope、预建下一Scope并wake continuation。Scheduler不得在进程内缓存
iteration counter或在condition为true时另建重复Scope。

Worker：

- 只claim manifest声明的一个WorkClass；
- 读取immutable binding并执行exact `(JobId, lease_generation)`；
- heartbeat、提交有界progress和closed outcome；
- 不决定下游Plan控制流。

Worker manifest v1只包含exact WorkClass、ComponentRole、CanonicalRegion、runtime/protocol digest、
业务并发上限和critical-control reserve。Model首版Inline-only，因而manifest没有Model output
materialization或weighted-byte专用字段。

Capability Worker的role startup closure在通用Worker manifest之外携带09的bounded installed adapter/codec manifest列表，并把列表的
canonical digest纳入`runtime/protocol digest`。Native与Remote lane分别报告，不能把同一空列表或另一lane列表复用。Job claim只比较
Invocation冻结的required Worker manifest digest；具体codec descriptor由dispatcher静态registry二次重验，避免Scheduler解释backend合同。

## 5. Claim 与启动

claim必须在数据库事务中完成：

1. 按tenant公平性选择Ready Job；
2. 验证typed owner、deadline、policy、quota与Worker manifest；Capability remote还重验Invocation冻结的required manifest；
3. 领取层级quota bundle；
4. CAS到`Leased`并增加`lease_generation`；
5. 返回不含Secret value的immutable execution snapshot。

Worker必须在claim前预留本地RAII slot。少领、claim失败、identity不匹配或future drop都必须归还未使用slot。
atomic start receipt才把Job从`Leased`推进为`Running`并增加attempt count。

## 6. 并发与隔舱

一个leaf start同时需要：

- Worker role本地物理slot；
- Job-generation-owned tenant/WorkClass quota bundle；
- 可选AgentDeployment、Run、Principal与backend quota line。

最低独立pool：

```text
orchestration | model | remote-capability | mcp | context | sandbox | artifact | critical-control
```

Sandbox不得与API、Orchestration或Model共用Pod、queue、DB pool或semaphore。Artifact Data Worker和
critical-control也有独立pool。一个pool的100%饱和不得使其他pool的readiness失败。

## 7. 公平性与背压

首版使用deterministic weighted deficit round-robin：tenant是第一维，work age是第二维，
`low | normal | high`是受限修正。`critical_control`只由平台生成。调度状态只保存
round、cursor和bounded tenant deficit，不复制Job current state。

背压顺序：

1. Run/tenant admission limit；
2. WorkClass、tenant、backend与local permit；
3. remote `Busy/429` 的bounded retry-after；
4. live observation队列满时丢弃非权威progress，terminal result仍durable commit。

所有queue有item、byte、age与overflow上限。

## 8. Lease、延迟与恢复

- heartbeat间隔必须小于lease的三分之一，并使用数据库时间验证；
- 每次heartbeat返回的新Job version是下次操作的expected version；
- deferred Task/remote callback进入durable WakeContract，释放Worker lease与permit；
- retry写入`retry_at`，Worker不在内存sleep；
- NATS丢消息时由bounded safety scan恢复；
- lease过期后Recovery复核owner、generation、effect、deadline与attempt budget后first-win；
- 对外部非幂等Effect的timeout不推断“没有发生”，而进入reconciliation。

## 9. 关停、可观测性与安全

Draining立即停止新claim，已开始generation在grace内提交结果或durable handoff；超时后停止heartbeat，
不伪造failure。

最低指标：`ready_count`、`oldest_ready_age`、`in_flight`、`permit_utilization`、
`claim_total{outcome}`、`lease_lost_total`和`fairness_lag`。tenant/backend ID不进metric label。

Worker只有自身WorkClass的claim/commit权限；Sandbox Executor无数据库直连；Secret以scoped handle延迟解析；
tenant无法生成critical-control work。

## 10. 验收标准

- 多进程竞争同一Job时只有一个当前lease，旧generation commit被拒绝；
- NATS全丢时safety scan最终推进；
- Sandbox和Artifact各自饱和时API、Scheduler、Model和critical-control仍可准入；
- 单tenant持续backlog不使其他tenant永久饥饿；
- deferred work不持有常驻future、外部连接或business permit；
- shutdown不制造假terminal或双commit；
- wrong Scheduler SPIFFE identity、Plan/Artifact/Run binding drift、lease过期、非canonical Plan、unknown expression opcode、伪造
  observation/input digest均在任何Node/RunValue mutation前拒绝；
- manifest/startup视图漂移在claim前fail closed；
- 容量与公平性测试在fixed seed下可重复。

## 11. 分层证据

- domain：state/fairness/property tests；
- repository：PostgreSQL CAS、lease、quota与scan集成测试；
- process：多Worker竞争、崩溃和drain测试；
- release：production-equivalent隔舱和饱和资格测试。

下层证据不重复声明为上层发布资格。

## 12. 明确推迟

- 跨region scheduler、GPU、spot与Kubernetes Job backend；
- Managed stdio session、microVM与Model Artifact-backed output专用容量；
- 自动跨implementation failover。

## 13. 未决问题

基础调度合同无未决设计问题。具体容量数值只由18已资格的CapacityProfile拥有。
