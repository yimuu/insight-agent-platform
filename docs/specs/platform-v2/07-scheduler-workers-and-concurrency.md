# Platform v2 Scheduler、Worker 与并发规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-10 |
| 依赖 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) |
| 直接下游 | 08、10、12、14、16、17、18 |

> Persistence ruling：Scheduler 只 claim 03 的共享 `Job`，配额只使用 04 的共享 quota 聚合，公平性只使用一个
> scheduler state 聚合。历史 Attempt/Continuation 专用 projection 与编号 migration 记录均已废止。

## 1. 决策摘要

Scheduler 是只推进 durable facts 的协调器，Worker 是按 work class 隔离的执行者。所有工作从 PostgreSQL
有界 claim；NATS 只唤醒。平台使用层级配额、每类独立 permit/connection pool、tenant 公平性和显式背压。
Sandbox、Model、MCP、Context 和 Remote Capability 必须使用独立 Deployment 或至少独立 Worker role，
任何一类饱和都不能吞噬其他类别并发。

## 2. 目标与非目标

### 2.1 目标

- 多 runtime 安全 claim、执行、续租和 fence commit；
- 把 controller drive 与外部 leaf execution 分开；
- 对 tenant、work class、backend 和 Capability 实施有界并发；
- 避免每个 Worker 独立全表轮询；
- 在消息丢失、Worker 丢失和负载突增时保持最终推进；
- 提供 pause、drain、shutdown、autoscaling 和 capacity signals；
- 证明 Sandbox 饱和不影响 API、Model 和调度恢复。

### 2.2 非目标

- 不把 Scheduler 构建为通用 Kubernetes batch scheduler；
- 不使用 NATS/Redis queue 作为 work ownership；
- 不保证全局 FIFO；
- 不允许无限排队或无限租约；
- 不把一个全局 semaphore 用于所有工作类别；
- 不在 Scheduler 线程执行 Provider、MCP、HTTP、Context 或脚本 I/O。

## 3. Work Class

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

WorkClass 是闭合集合并进入`contracts/platform-v1/registries.json`机器合同，Scheduler、Worker manifest、Quota、SQL和
telemetry只能复用该registry，不能各自维护字符串。它决定 queue、lease、permit、connection pool、SLO 和 Worker role。具体
CapabilityImplementation 可以进一步定义 backend concurrency key，但不能跨 WorkClass 借 permit。

Capability backend 到 WorkClass 的映射同样是闭合合同：Native 使用`CapabilityNative`，HTTP/gRPC及由Capability adapter
调用MCP Host的Tool使用`CapabilityRemote`，Sandbox code直接使用`Sandbox`。`Mcp`只承载discovery、subscription等Host自有
durable work。Sandbox不得先创建一个`CapabilityRemote` Job再创建Sandbox Job；一次物理执行始终只有一个共享Job、一个
attempt counter、一个lease/epoch/fence authority。

WorkClass与父aggregate的closed owner-kind pair同样进入machine registry；`work_id`必须由03/06的typed-source verifier
回查，Scheduler不得创建generic Work resource或把任意合法prefix当作可claim work。

## 4. Scheduler 与 Worker 角色

### 4.1 Scheduler

Scheduler：

- claim `Orchestration` work；
- 调用纯 Plan transition function；
- 提交 Node/Scope/token/continuation/leaf work commands；
- 处理 terminal convergence、pause/cancel propagation 和 recovery drive；
- 不调用外部 backend。

### 4.2 Worker

Worker：

- 只 claim 自己声明的 WorkClass；
- 获取本地和 durable permits；
- 读取 immutable binding；
- 执行一个精确 `(JobId, lease_generation)`；
- heartbeat、发布受控 progress、提交闭合 outcome；
- 不决定下游控制流。

Worker capability manifest使用`contracts/platform-v1/schemas/worker-manifest.schema.json`的closed shape；每份manifest只声明
一个exact WorkClass、adapter/runtime digest、协议版本、业务最大并发和正数`critical_control_reserved_slots`。它以canonical
digest进入CandidateManifest，一份manifest不能把多个WorkClass映射到同一个semaphore。启动时schema、typed validator、
WorkerProcessGeneration和Deployment binding任一不匹配都拒绝claim。

## 5. Coordinator

每个进程/Worker role 使用一个 WorkCoordinator，而不是每个 task 独立 poll：

```text
local commit hint ─┐
NATS wake hint ────┼→ coalescing coordinator → capacity-aware claim
deadline timer ────┤
safety scan ───────┘
```

- wake 在短窗口合并；
- claim 数量不超过当前空闲 permit 与 batch hard limit；
- 没有 capacity 时不 claim，避免租约空耗；
- safety scan 使用 shard/cursor，间隔有 jitter；
- coordinator shutdown 先停止 claim，再 drain 当前 Job generation。

Orchestration 的 claim loop 与 safety loop 是同一 Worker role 内的两个有界 application loop：前者只使用business permit/pool，
后者只使用独立保留的critical-control permit/pool。safety loop依次驱动expired Job lease、due retry、cancel/timeout/deadline
convergence和expired Task；四类扫描各自持有可丢失的进程内high-water cursor。满页推进cursor，短页清空cursor并从该shard起点
回绕；进程重启也从起点重扫。每个page的候选数和mutation identity slot数严格等于`recovery_batch`请求，分片数量受
`recovery_shards.hard_max`约束，Q1默认使用profile version 4的batch 1000、16 shards，最大256 shards。每次mutation仍在
caller-owned PostgreSQL事务复核current fact/fence并first-win；cursor、timer和wake hint都不是持久化authority。

## 6. Claim

Claim 使用 PostgreSQL 单事务和 `FOR UPDATE SKIP LOCKED` 或等价 CAS：

```rust
struct WorkClaim {
    worker_id: WorkerId,
    worker_generation: u64,
    work_class: WorkClass,
    supported_binding_digests: BindingCapabilitySet,
    available_slots: u16,
    lease_duration: Duration,
}
```

筛选至少包括：

- state/deadline/retry_at；
- tenant 与 work-class quota；
- backend/capability circuit state；
- Run pause/cancel/suspension；
- exact implementation compatibility；
- priority/fairness score；
- attempt limit。

claim由所属work application service调用03的共享 Job repository primitive，在一个事务中原子创建/推进 Job、预留
quota、增加epoch、设置lease和父work/in-flight counters。Scheduler只选择候选，不直接改写其他domain父聚合，也不维护
第二套durable permit counter。派发前必须依据Worker
advertised capacity预留本地slot；领取后本地slot获取失败属于实现错误，Worker必须立即走typed release/shorten路径，不能持有工作
等待本地容量。

Job generation 次数使用跨WorkClass的`run_scheduler.attempts_per_work` hard limit；不能以Node专用字段约束RegistryValidation、
Model、Capability、MCP、Context、Sandbox、Interaction、Artifact或Recovery work。共享 Job 只保存 current epoch/ordinal，业务Ready、
RetryScheduled、Effect与terminal仍由typed父aggregate handler拥有。

## 7. 层级并发

一次 leaf start 必须同时获得两类容量：

```text
CandidateManifest + local Worker pool physical slot
Job-generation-owned tenant work-class quota line
optional AgentDeployment/Run/Principal quota line
implementation/backend quota line（对应leaf WorkClass交付后）
```

平台物理slot与tenant durable quota是不同事实：前者由CandidateManifest和本地Worker pool表达可立即执行的物理容量，不持久化
为跨tenant quota；后者的全部层级是同一个 Job-generation-owned leased QuotaReservation bundle中的排序line。bundle至少包含exact
WorkClass，Orchestration可再包含RunBindings证明的AgentDeployment、Run和Principal，每scope最多一条；对应leaf domain交付后
可增加其exact implementation/backend scope。任一line失败则整个command savepoint回滚并保持work Ready，terminal/recovery
winner从数据库锁定实际immutable bundle后原子settle全部line。调用方只携带reservation token/generation/ETag，不能重述或删减
budget locators。

本地pool在数据库claim之前按`min(requested, claim_batch hard max, available slots)`预留RAII slot；数据库实际返回的每个
`(JobId, lease_generation)`再与一个预留slot绑定，少领、claim失败、class/generation identity不匹配或对象drop都会释放
未使用slot。业务slot与
`critical_control`使用不同semaphore；业务100%占满时control仍可取得保留slot。本地计数不写数据库、不参与恢复，也不能借用
另一个WorkerManifest的WorkClass容量。

最低独立池：

```text
orchestration
model
remote capability
mcp
context
sandbox
artifact
```

配置不能把 Sandbox 与 Orchestration/Model 合并为同一池。

## 8. 公平性与优先级

公平调度以 tenant 为第一维，work age 为第二维，显式 priority 为受限修正：

- 每 tenant 有 weight、并发上限和 burst；
- 同一 tenant 内按 effective deadline/age；
- priority machine wire是闭合等级`low | normal | high | critical_control`；
- 用户不能提交`critical_control`；它只用于cancel、timeout、recovery和安全门；
- 每个运行实例为`critical_control`保留独立最小permit和数据库连接预算，业务work饱和不能耗尽它；
- aging 防止 low priority 永久饥饿；
- 单 tenant backlog 不能填满全局 claim batch；
- 公平算法及参数必须有 deterministic simulation tests。

Q1实现固定使用weighted deficit round-robin（WDRR）：每轮按稳定TenantId顺序从上次cursor之后开始，先给候选tenant累加
weight，再在其burst与deficit范围内选取work；tenant内按effective priority、最早enqueue round和WorkId稳定排序。aging每经过
配置的正整数round提升一级，最高只到`high`，不能把业务work提升为`critical_control`。算法接收并返回完整显式state，任何计数
溢出、重复tenant窗口、未来enqueue round、零cost或越界batch/window/burst都fail closed；相同输入与state必须得到逐字节相同结果。

共享 `scheduler_state` 按 work class 保存 round/cursor 及 bounded per-tenant exact Scheduling Policy binding、
deficit/last-served state；它不拥有 Ready work、
Job、lease或permit。scheduler state、quota、typed parent 与 Job 按固定锁序在同一事务提交，任一步失败全部回滚。
`critical_control`和 Recovery 使用独立保留池。

候选tenant不能由调用方提交。Orchestration首片在fairness head锁内从Node `Ready`、Run `Running`、未pause/cancel/timeout
control、open Scope、Run binding及tenant Scheduling binding的实时join中枚举；从cursor之后开始并在末尾环绕，tenant数和
每tenant work窗口都受`claim_batch.hard_max`限制。Node 自身保存首次进入当前 Ready generation 的 enqueue round；选择时
必须读取当前 Node exact version与状态。Node 离开后重入 Ready 会增加 generation 并重置 enqueue round。调用方只提交expected typed
claim；winner、Run/Node version或binding digest不匹配时整个savepoint回滚，不允许回退到调用方候选。

claim提交Job `Leased`、quota reservation与Run active work，但Node在Worker原子start前保持`Ready`；claim只递增Node
projection version以绑定该leased generation。新物理attempt的start receipt与Job `Leased -> Running`、
`attempt_count + 1`及Node `Ready -> Running`同事务提交；typed owner冻结的Deferred/Input continuation使用resume receipt，
重新取得generation fence但保留`attempt_count`。start前lease loss释放quota/active work并让Job回到Ready，Node不经过非法回边；
`RetryScheduled`不能被标为continuation。

weight、burst与aging参数必须来自每个 tenant 自身 exact published Scheduling Policy ResourceVersion，而不是由
Worker、Run、Scheduler或API任意传值。Repository 从 ResourceVersion typed payload 解析并验证 semantic digest，再构建
`TenantSchedulingPolicyBinding`；候选 window 只含 tenant 和 Job，不携带公平参数。scheduler state 在每个 tenant
deficit entry 内保存所用 exact policy version/digest；该 tenant policy 变化时只原子归零该 tenant 的旧
deficit，不重置其累计成功 claim/last-served evidence，也不扰动其他 tenant。不存在跨 tenant 的虚假单一
policy version；整个 typed scheduler-state payload 由自身 canonical digest 保护。未注册的 WorkClass fail closed。

## 9. Backpressure

背压分四层：

1. admission：active/waiting Run、tenant budget 超限时拒绝或 durable queue；
2. scheduling：work-class/tenant/backend permit 不足时保持 Ready；
3. dispatch：远程服务 Busy/429 使用 bounded retry-after；
4. output：progress/live stream 队列满时丢弃非权威观测，结果通过 durable commit。

所有队列都有数量、字节、最老 age 和 overflow behavior。禁止通过不断延长 lease 隐藏下游无容量。

## 10. Lease 与 Heartbeat

- lease duration 按 WorkClass 配置并有平台 hard max；
- heartbeat 间隔小于 lease 的三分之一并带 jitter；调用方时间只能作为事件时间，start/heartbeat/Worker terminal还必须由
  PostgreSQL `clock_timestamp()`证明当前lease未到期，不能通过回填旧时间续租或提交结果；
- versioned HardLimitProfile 必须对 hard maximum 和 Q1 default 同时验证上述严格比例；`q1-50` profile version 4 使用
  `lease=30000ms`、`heartbeat=8000ms`，实际带 jitter 的调度仍不得达到或越过 lease 的三分之一；
- heartbeat 只更新当前 Job lease generation，不延长 Run deadline；
- external backend 长任务必须能以 deferred task 释放 Worker，而不是无限 heartbeat；
- Worker 失联后 lease 到期；recovery 根据 Effect 决定 retry/reconcile；
- expired scan按UUIDv7随机低位shard和`(lease_expires_at, tenant_id, job_id)`high-water cursor有界读取；扫描不持有
  跨事务锁，recovery mutation复核current fence、state/version、原lease和数据库时钟后first-win；
- scan同时冻结Run/Node/control版本、Node counter与 Job admission 固定的 effective attempt limit；recovery按当前
  cancel/timeout intent和
  剩余generation budget生成唯一owner transition。普通retry必须携带deadline内的durable retry time/jitter evidence，cancel、
  timeout或limit耗尽时不得携带retry schedule；
- Artifact等leaf scan冻结其typed Operation/parent/backend projection而不是伪造Run/Node字段。deadline内仍有generation时，
  Recovery pool只结束 expired generation、写 `attempt.lost` Event 并关闭 exact permit，父状态保持可 retry；
  deadline/limit耗尽或backend effect不确定时只投递
  bounded reconciliation candidate。对应domain reconciliation authority未交付前不得自动继续claim或把父对象回滚为安全状态；
- shutdown/drain 可以缩短或安全转交未开始 lease，已开始 Job generation 遵循 cancellation policy。

## 11. Deferred Work

远程 Capability、MCP Task 和 child Run 进入 Job 的 durable WakeContract，HumanTask 使用 Task：

- 当前 Worker 提交 `Deferred/Waiting` outcome；
- release 所有 execution permits 和 lease；
- callback/poll/reconciler 作为新短 Job 处理 wake；
- wake winner只负责验证并推进exact WakeContract；原执行Job后续claim取得新的lease generation，但若其owner payload冻结
  `ResumePhysicalAttempt`，start wrapper必须保留已有`attempt_count`。只有`RetryScheduled`或owner明确决定重新dispatch时才消耗
  新的物理attempt；
- 等待时长不计入 Worker busy time，但计入 Run deadline 和 backend task quota；
- 只有有界 poll schedule，不允许每个 waiting task 常驻 async future。

同步Capability不持有Orchestration Worker等待leaf结果：controller先把owner Node停驻为Waiting并释放其permit，Capability
Job claim才增加Run active work并占用独立WorkClass quota；terminal winner在同一事务关闭permit、递减active work并唤醒Node。
远程任务建立后，Job 保存 pending WakeContract 并释放Worker；callback/poll争用同一generation，赢家推进下一代Job。
poll backoff由冻结policy与剩余预算派生；late callback通过 Receipt 与当前 WakeContract/Invocation 事实归类，不能改写赢家。

## 12. Circuit Breaker 与健康

健康状态分离：

```text
ObservedHealth: Healthy | Degraded | Unreachable
AdministrativeGate: Enabled | Suspended
Circuit: Closed | Open(until) | HalfOpen(probes)
```

- observed health 不移动 active head；
- circuit breaker 只影响对应 implementation/backend key；
- half-open probe 有独立小 permit；
- policy/suspension 优先于 circuit；
- 不进行未绑定 backend 的自动 failover；
- remote 失败不能使整体 API readiness 失败，除非它是平台 mandatory dependency。

## 13. Retry Scheduling

Worker 不在进程内 sleep 等待 retry。它提交 safe failure，由 repository 计算并持久化 `retry_at`。deadline
scanner 到时发 wake，Worker 重新 claim。Backend `Retry-After` 只作为 bounded hint，不能超过 Run deadline、
policy max 或 quota window。

## 14. Shutdown 与部署

```text
Running -> Draining -> Stopped
```

- readiness 在 Draining 立即停止接收新 external traffic/claim；
- Scheduler/Worker 停止 claim，继续 heartbeat 当前 Job generation；
- 在 grace deadline 内提交结果或 cancellation handoff；
- 超时后停止 heartbeat，让 lease 自然过期，不能伪造 failure；
- rolling deployment 的 worker manifest/digest 不匹配旧 binding 时，旧 worker 保留到历史工作清空或有兼容
  worker pool；
- Sandbox Executor 单独 Deployment/HPA，不与 API Pod 共调度生命周期。

Orchestration Q1使用`LeaseFencedOrchestrationExecutor`执行上述合同：claim返回的generation先以closed Receipt原子start，之后只对
同一`(JobId, lease_generation, worker_process_generation_id, lease_token_digest)`续租；每次heartbeat返回的新Job version成为
下一次heartbeat或terminal commit的唯一expected version。Coordinator进入Draining时先向全部active generation广播进程内
shutdown token，executor停止业务handler并在grace内用critical-control路径尝试durable retry/cancellation handoff；只有未提交
handoff才在grace结束后abort本地future并等待lease recovery。start/heartbeat/handoff的任一步都不能直接写本地terminal判断。

## 15. Autoscaling Signals

建议扩容指标：

```text
ready_work_count{class}
oldest_ready_age_seconds{class}
in_flight_count{class}
permit_utilization{class}
backend_busy_total{class}
```

CPU 只是辅助信号。Sandbox 使用 ready count + active executions，Model 使用 requests/token throughput，
Context 使用 query latency。Scale-to-zero 只允许没有 mandatory low-latency/recovery contract 的 Worker class。

## 16. 数据库压力控制

- claim batch 有硬上限；
- coordinator 合并 wake；
- ready work 使用覆盖索引和分区/tenant shard；
- heartbeat 批量化但保持每 `(JobId, lease_generation)` fence；
- progress 不写高频 durable ledger，只提交有界 coarse milestones；
- safety scan 使用 high-water cursor；
- Worker 不各自扫描全部 timer/outbox；
- statement timeout、pool size 和 transaction duration 按 role 独立配置。

## 17. 可观测性

```text
work_ready{class,priority}
work_oldest_age_seconds{class,priority}
work_claim_total{class,outcome}
work_in_flight{class}
work_permit_utilization{class}
worker_heartbeat_lag_seconds{class}
worker_lease_lost_total{class}
backend_circuit_state{backend_class,state}
scheduler_drive_duration_seconds{outcome}
tenant_fairness_lag_ratio{class,bucket}
```

backend/tenant 具体 ID 不进入 label；高 cardinality 诊断通过受控 trace/query。

## 18. 安全

- Worker 使用最小 workload identity，只能 claim/commit 自己的 WorkClass；
- Sandbox Worker 无数据库直连，只通过 Sandbox Gateway 协议；
- Worker manifest 由部署签名/镜像 digest 证明；
- claim 返回 scoped Secret handle，不返回其他 tenant/object；
- progress/output 有字节限制和 schema validation；
- `critical_control` work不能由tenant创建或降级。

## 19. 验收标准

- 两个以上 runtime 并发 claim 时一个 logical work 只有一个当前 lease；
- 旧 epoch completion 被拒绝；
- NATS 全丢失时 safety scan 恢复；
- 100% Sandbox permit 占用下，API p95、Scheduler drive 和 Model admission 保持资格阈值；
- 单 tenant 大 backlog 不使其他 tenant 饥饿；
- ready work 超过队列上限时产生稳定 backpressure，不导致内存无界；
- deferred task 释放 Worker permit，等待期间没有常驻 future/连接；
- rolling shutdown 不制造假 failure 或双 commit；
- circuit open 只影响对应 backend，不移动 binding、不击穿其他 work class；
- 负载测试证明 coordinator wake 合并和 claim batch 不造成数据库轮询风暴。

## 20. 明确推迟的工作

- 跨 region scheduler；
- spot/preemptible Worker 优化；
- GPU 调度；
- Kubernetes Job backend；
- 超过 18 中 `Q1-50` 的 capacity/SLO profile；
- 自动跨 implementation failover。

## 21. 未决问题

WDRR、PostgreSQL CAS projection、tenant/work-class exact Scheduling Policy binding以及权威有界候选数据库枚举已交付
Orchestration首片，不再把算法、参数或候选authority列为未决。CR-085的WorkerManifest与`insight-platform-worker` RAII
bulkhead现已由`insight-platform-runtime`的coalescing、capacity-aware WorkCoordinator接入真实PostgreSQL claim事务；本地slot在
claim前预留，实际`(JobId, lease_generation)`领取后绑定，shutdown先停止claim再有界drain。role-scoped PostgreSQL factory使用
独立business与critical-control pool；真实PostgreSQL 16 fixture已证明单进程business pool占满时critical-control连接仍可读取
权威状态，且drain超时不伪造Job terminal。

CR-111的expired/deadline safety application loop已交付：四类扫描使用独立transient cursor、profile-bounded batch/shard与
critical-control本地/数据库保留池；真实PostgreSQL 16 fixture证明游标推进/回绕、错误shard隔离、lease recovery，以及business
permit和business connection同时饱和时恢复仍可推进。该fixture是功能与隔舱证据，不单独构成Gate E容量证据。

CR-112的executor lifecycle缺口也已关闭：生产executor使用profile约束的heartbeat+jitter、最新version fence和drain handoff；真实
PostgreSQL 16 fixture覆盖atomic start、至少一次heartbeat、business connection饱和下critical-control retry handoff、quota/active
permit释放和后续safety promotion。无post-admission wake hint的周期drive测试以及10000轮mixed-cost persistent backlog模拟分别
覆盖lost-wake推进和有界starvation窗口。

CR-116又关闭Phase 2最后的开发期功能门禁：fresh PostgreSQL 16 fixture以50 active Runs、5 tenants和4个独立OS worker
process同时竞争同一fairness head；shared advisory barrier保证全部进程到齐后才放行。50个Job各有一个lease，多个worker实际
获胜，每tenant的durable WDRR successful-claim计数精确为10，Run active-work与quota reservation均精确闭合。orchestration
business connection pool 100%占用期间，独立Sandbox-role pool的20次probe p95不超过250ms，critical-control reserve也可用。

Phase 2 domain/runtime functional exit据此关闭。仍未完成的是Phase 6资格：同一CandidateManifest、production-equivalent topology、
完整跨WorkClass混合负载、30分钟持续吞吐和18定义的全部SLI/Gate E证据。开发期fixture不得登记为Gate E，也不得把本规范标记为
Verified；这些边界不得通过调用方自由候选/参数或复制Ready authority规避。
