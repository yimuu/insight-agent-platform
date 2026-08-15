# Platform v2 数据一致性、事件与恢复规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-15 |
| 依赖 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md)、[`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) |
| 直接下游 | 04、06、07、08、10、12、13、14、15、16、17、18 |

## 1. 决策摘要

PostgreSQL是durable current state、事务、幂等、lease、timer和outbox权威。NATS只发送wake hint、lossy live observation和
committed outbox fan-out。当前状态由少量aggregate拥有；共享Job承载物理执行、lease、retry、remote wait和recovery；共享Task
承载approval/interaction；共享Receipt承载command/callback/job commit去重；共享Event承载transition、outcome evidence和audit。

不再为每个domain、transition、outcome或rejection创建专用proof table。正确性来自typed application command、单事务CAS、
bounded immutable snapshot和共享结构约束，而不是把同一事实物化多次。

## 2. 目标与非目标

### 2.1 目标

- crash、网络断开、重复消息、迟到callback和lease过期后最终收敛；
- at-least-once dispatch与single-winner commit；
- command、callback和job outcome统一幂等；
- Run、Invocation、Job和Task各自只有一个current-state owner；
- transactional outbox安全投影已提交事实；
- 外部Effect不确定时fail closed并进入reconciliation；
- persistence模型随domain增加保持近似常数，而不是复制表族。

### 2.2 非目标

- 不承诺外部世界exactly-once；
- 不使用Redis、NATS、Kubernetes Job或内存队列作为业务权威；
- 不在数据库事务中调用HTTP/gRPC/MCP/Model/S3/Secret Manager；
- 不依赖无限事件重放恢复current state；
- 不以通用EAV或无类型JSON取代domain模型；
- 不用数据库trigger重新实现完整业务状态机；
- 不支持SQLite作为目标运行权威。

## 3. 一致性权威

| 数据 | 权威 | 说明 |
|---|---|---|
| Resource/Version/Deployment | PostgreSQL aggregate | 02共享生命周期 |
| Run/Node/Invocation | PostgreSQL aggregate | current state与optimistic version |
| Job/Task/Receipt/Event/Outbox | PostgreSQL共享primitive | 所有domain复用 |
| Artifact bytes | S3-compatible store | finalize后immutable |
| Artifact metadata/link | PostgreSQL aggregate | 15拥有业务语义 |
| Secret value | Secret Manager | PostgreSQL只保存binding identity |
| Wake/live delta | NATS或进程内 | 可丢失、可重复 |
| Integration delivery | committed Outbox | at-least-once |

客户端current read查询aggregate snapshot；Event是历史和审计，不成为第二current projection。Outbox是交付队列，不成为业务
event的第二语义定义。

## 4. Aggregate合同

所有current aggregate共享最小header：

```rust
struct AggregateHeader {
    tenant_id: TenantId,
    aggregate_id: ResourceId,
    state: ClosedState,
    version: u64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}
```

每个command只由一个application service拥有；它可以在同一事务组合多个aggregate，例如Invocation terminal同时唤醒Node，
但Worker不能拼接任意SQL。每次mutation必须携带expected version/state，成功推进version恰好一次。

一个事实只保存在一个current owner：

- Run保存Run状态、control intent、bindings snapshot和next public sequence；
- Node保存节点状态和bounded local execution data；
- Invocation保存逻辑调用状态、frozen admission、current Job和result/failure；
- Job保存物理执行、lease、remote wait、retry和backend state；
- Task保存approval/interaction/human work；
- Receipt保存去重key与稳定结果；
- Event保存不可变历史；
- Outbox保存尚待或已经发送的Event envelope。

Projection或generated column只能是读取优化，不能拥有独立写API。

## 5. 事务边界

一个业务command在一个caller-owned PostgreSQL transaction/savepoint中：

1. 解析tenant和PrincipalSnapshot；
2. claim或读取Receipt；
3. 按共享lock order锁定current aggregates；
4. 验证expected version/state、Policy与typed snapshot；
5. 修改aggregate；
6. 追加Event；
7. 必要时追加Outbox；
8. terminalize Receipt；
9. commit。

任一后置CAS失败必须回滚本command的全部修改。Repository不替caller提交外层事务。外部I/O、用户代码、容器启动、NATS ack、
无界正文解析均在事务外。

共享lock rank：Receipt；tenant quota/scheduler state；parent aggregate；按kind/id排序的child aggregate；Job/Task；Artifact；
Run event sequence；最后append Event/Outbox。一次命令的同rank对象先排序再锁；不得边遍历用户输入边加锁。

## 6. 统一Job

Job表示一个逻辑后台工作及其物理generation，不为Attempt、RemoteTask、poll或recovery建立第二current row：

```rust
enum JobKind {
    Orchestration,
    Model,
    Capability,
    Context,
    Mcp,
    Sandbox,
    Artifact,
    Recovery,
}

enum JobState {
    Ready,
    Leased,
    Running,
    Waiting,
    RetryScheduled,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    ReconciliationRequired,
}

struct Job {
    header: AggregateHeader,
    kind: JobKind,
    owner: TypedOwnerRef,
    binding_snapshot: VersionedSnapshot,
    attempt_count: u32,
    attempt_limit: u32,
    lease: Option<Lease>,
    schedule_at: Option<DateTime<Utc>>,
    wake: Option<WakeContract>,
    backend_state: Option<EncryptedBoundedState>,
    result: Option<TypedResult>,
    failure: Option<Failure>,
    deadline: DateTime<Utc>,
}
```

`TypedOwnerRef`是closed pair；JobKind决定允许的owner kind。Binding snapshot冻结Deployment、idempotency/effect key、policy和
hard-limit digest。Attempt history进入Event，current lease只在Job保存一次。

### 6.1 Lease与fence

Claim原子执行`Ready|RetryScheduled -> Leased`：

- `lease_generation`严格递增；
- `attempt_count`不在claim递增；普通新物理尝试只有在同generation的原子start成功提交
  `Leased -> Running`时才递增，因此start前丢失的lease不会消耗execution attempt budget；
- typed owner可以在`Waiting` first-winner事务中把下一次`Ready`冻结为
  `ResumePhysicalAttempt`，但只能用于已经成功Started且仍由exact WakeContract/opaque state证明的Deferred或Input continuation；
  该continuation claim仍增加`lease_generation`，原子resume保留`attempt_count`。`RetryScheduled`、lease-loss replay、
  新external dispatch或缺少exact continuation evidence时必须使用`NewPhysicalAttempt`并受remaining attempt budget约束；
- 保存owner WorkerProcessGeneration、token digest、heartbeat和expiry；
- raw token不持久化、不记录日志；
- start/heartbeat/commit同时匹配Job ID、version、lease generation、owner和token；
- database clock必须在expiry前；
- recovery takeover必须证明database clock已到expiry并增加generation；
- old generation所有heartbeat/progress/outcome都返回stale fence；
- terminal winner清除lease并原子settle quota。

### 6.2 Waiting与first-winner

Remote callback、bounded poll、signal或human response使用Job/Task中的WakeContract：

```rust
struct WakeContract {
    generation: u64,
    accepted_sources: WakeSourceSet,
    next_poll_at: Option<DateTime<Utc>>,
    poll_count: u32,
    poll_limit: u32,
    callback_binding_digest: Option<Digest>,
}
```

等待提交将Job置Waiting、清除lease/permit并保存encrypted backend state。callback、poll、cancel和timeout通过
`state=Waiting AND wake_generation=expected` CAS竞争一个winner；winner推进generation或terminal state。进程内timer只能wake
bounded due scan，不能指定另一个Job或提供权威时间。

### 6.3 Retry与Effect

Retry由owner application service根据Effect、idempotency、Policy、deadline和remaining attempt budget推导；调用方/backend只
能提供typed failure/hint。外部调用发出后结果不确定时：Pure/ReadOnly及有明确before-dispatch证据的工作可安全retry；其他Effect
进入ReconciliationRequired，除非exact contract证明可安全重放。Attempt limit在首次admission冻结，后续不能放宽。

## 7. 统一Task

Approval、Interaction和HumanTask共享Task aggregate：

```rust
enum TaskKind { Approval, Interaction, HumanWork }
enum TaskState { Pending, Completed, Declined, Cancelled, Expired }
```

Task保存typed owner、schema、safe presentation、eligible principal rule、generation和deadline。Response必须匹配tenant、principal、
kind、generation和Pending state。Task terminal与owner wake在一个事务中提交；迟到response只追加safe Event。

## 8. 统一Receipt与幂等

```rust
enum ReceiptKind { Command, Callback, JobCommit }
enum ReceiptState { Processing, Succeeded, Failed, Rejected }

struct Receipt {
    kind: ReceiptKind,
    scope: AuthorityScope,
    principal_or_binding: TypedDedupeOwner,
    operation: ClosedOperation,
    idempotency_key: BoundedKey,
    request_digest: Digest,
    claim_generation: u64,
    lease: Option<Lease>,
    result: Option<BoundedReceiptResult>,
}
```

不同ReceiptKind使用closed dedupe key shape：

- Command：scope + principal + operation + idempotency key；
- Callback：tenant + verified binding + external task identity + callback ID；
- JobCommit：tenant + Job + lease generation + commit request ID。

相同key/digest重放返回同一terminal result；不同digest返回`idempotency_conflict`。Callback先做body size/media type和认证，再创建
Receipt；payload正文使用bounded encrypted value或ArtifactRef。Receipt result可包含typed rejection reason与observation snapshot，
不为late rejection另建表。Processing receipt使用短lease并可恢复；bounded外部I/O可以跨越Receipt lease，但不能持有数据库事务或
行锁，长操作必须续租且所有最终mutation回绑current `claim_generation`。terminal Receipt的同key/digest重放先于current aggregate
authorization并且不重做I/O；只有新建或Processing lease过期接管才重新授权，接管必须递增claim generation。

## 9. Event与Outbox

Event是统一append-only历史：

```rust
struct Event {
    event_id: EventId,
    tenant_id: TenantId,
    aggregate: TypedAggregateRef,
    aggregate_version: u64,
    event_type: ClosedEventType,
    schema_version: u32,
    payload: BoundedEventPayload,
    visibility: EventVisibility,
    run_id: Option<RunId>,
    run_sequence: Option<u64>,
    created_at: DateTime<Utc>,
}
```

Event承载transition、outcome evidence、rejection observation和audit，payload由event type对应Rust类型验证。大型正文为ArtifactRef。
同一aggregate/version/event type唯一。PublicRun事件在同一事务CAS `Run.next_event_sequence`分配严格递增run sequence；不需要
独立stream-head或projection current-state表。

需要跨进程交付时，同事务创建Outbox envelope引用Event：

- dispatcher用bounded `SKIP LOCKED`领取；
- publish后标记delivery state；崩溃窗口允许重复；
- consumer按Event ID去重；
- backlog不改变业务commit，但超过安全阈值可关闭新高风险admission；
- mandatory checkpoint与retention满足后才GC。

## 10. NATS规则

NATS topic仅有wake、run live和committed integration fan-out。消息不携带Secret、可信授权结论、完整prompt或大正文。Worker收到
wake必须回PostgreSQL claim。NATS不可用时safety scan仍能推进；`run.live`丢失后客户端从durable Run/Event cursor恢复。

## 11. Callback

所有remote callback复用Callback Receipt：

1. Gateway在读取/解压正文前执行hard limits；
2. 验证mTLS、signature或opaque token并解析server-owned binding；
3. 以binding/external identity/callback ID去重；
4. claim短processing lease；
5. target service锁定Job/Task/Invocation并竞争wake generation；
6. winner原子提交target state、Receipt result、Event和Outbox；
7. late callback由服务端从current target推导stable rejection reason并terminalize同一Receipt；
8. exact replay返回同一Receipt。

Callback不能覆盖tenant、Run、Deployment、Policy或Secret binding。Capability、MCP和Sandbox不得创建自己的callback inbox。

## 12. Timer、Signal与扫描

- deadline、schedule和lease time保存在aggregate并由database clock判定；
- process timer只是加速wake；
- signal使用tenant + Run + signal kind/key的Receipt去重；
- due/expired scan按稳定keyset、shard和HardLimitProfile batch读取；
- scan candidate是短期observation，不是authority；mutation重新锁定并CAS；
- scanner不能直接选择业务terminal语义；owner service决定retry、cancel、timeout或reconcile。

## 13. Artifact一致性

Artifact写入使用prepare/upload/verify/finalize：数据库先创建bounded upload intent，bytes写staging object，Worker验证digest/media/
content policy。公共上传可以先完成独立owner finalize；Capability/Sandbox/Model等受信producer输出则最多留下不可读的Verified candidate，
由消费owner事务在同一lock order中执行`Verified -> Ready`、Reference/RunValue与业务terminal first-winner。S3成功而DB失败产生可GC
orphan；DB不得提交指向Staging/Verified、缺失object或没有同事务业务Reference的RunValue。

Model Artifact Producer的stage Receipt只证明exact Attempt bytes已Verified，不是Model outcome。Model terminal事务仍按
`Receipt -> quota -> parent/child aggregate -> Job -> Artifact -> Run sequence -> Event/Outbox`锁序提交；Producer不得在对象I/O期间持有
数据库事务，也不得修改ModelTurn/Job、quota余额、Event或Outbox。Inline/cancel/timeout/first-winner loser必须让非Ready candidate进入
Artifact GC，不能用bucket/object事实反推业务成功；quota服从04双bundle：未Ready Artifact count/logical bundle可Close，candidate Blob/
PUT不可能时upload/staging/physical bundle也可Close，已有candidate/object或dedupe race loser时Blob bundle保持Open到exact
deletion/absence evidence，不能先释放再GC。

Model output stage是明确的physical sub-protocol例外：Blob bind以及Artifact/Blob到Verified的内部物理transition不逐步追加
Event/Outbox，durable审计由claim-generation-bound JobCommit Receipt与Verified evidence承担；最终Ready或cleanup/incident owner事务
必须在自己的Event中回绑该evidence digest。Producer的Processing claim、Blob-bind、Uploaded/Verifying checkpoint与final-Verified短事务
都要按上述锁序先锁stage Receipt，再按04 canonical顺序对冻结的两个quota bundle header/line取得`FOR SHARE`并锁后重验exact generation，
然后对current ModelTurn/Job取得会阻塞cancel/lease/terminal更新的共享serialization guard，最后锁Artifact/Blob；quota Close/Expiry/
settlement取得冲突锁，外部I/O期间不持锁。
最终Verified evidence、Artifact/Blob状态和Receipt terminal必须同事务提交。该例外不允许silent业务transition或Producer写业务Event。

## 14. 典型故障语义

| 故障窗口 | 结果 |
|---|---|
| DB commit前API断开 | 重试；无terminal Receipt即未提交 |
| DB commit后响应前断开 | 重放返回既有Receipt |
| Outbox commit后publish前崩溃 | dispatcher后续发布 |
| publish后标记前崩溃 | 可能重复，consumer按Event ID去重 |
| Worker外部调用前崩溃 | lease过期后安全takeover |
| 外部调用后保存前崩溃 | 按Effect reconcile，不推断未执行 |
| old Worker迟到commit | lease generation CAS拒绝 |
| callback/poll/cancel/timeout并发 | wake/state CAS唯一winner，其余稳定Rejected |
| NATS全部丢失 | bounded safety scan最终发现工作 |

## 15. PostgreSQL与迁移边界

- PostgreSQL 16为目标authority；
- baseline按18～24表预算实现共享aggregate/primitive，不按domain复制表族；
- 应用启动不自动执行production DDL；
- clean replacement不兼容旧`insight.agent/v1`或旧177表候选；
- baseline发布后migration immutable、forward-only；
- tenant-scoped unique/FK包含tenant或等价隔离；
- 时间用`timestamptz`，version/generation使用非负`bigint`；
- database CHECK只覆盖结构、closed tag、基本shape和不可变性；业务状态机由Rust exhaustive match与CAS command证明；
- schema qualification比较真实PostgreSQL catalog，并运行并发/crash行为fixture，而非统计trigger或constraint数量。

## 16. 备份与恢复

- PostgreSQL PITR与Artifact versioning按18的RPO/RTO；
- 恢复顺序：PostgreSQL、Artifact integrity、Secret resolver、Workers、Outbox；
- NATS状态不是恢复前置；
- 恢复后所有非terminal lease由database time和generation重新接管；
- 演练覆盖callback replay、duplicate Outbox、waiting Job、uncertain Effect和missing Artifact。

## 17. 可观测性

```text
db_command_duration_seconds{command,outcome}
jobs_total{kind,state}
job_lease_expired_total{kind}
receipts_total{kind,state}
outbox_backlog_total{visibility}
recovery_scan_duration_seconds{kind}
fence_rejection_total{kind,reason}
```

tenant、Run、Job、endpoint、external ID和error body不得成为metric label。

## 18. 验收标准

- kill API/Scheduler/各类Worker后已提交工作最终收敛；
- 丢弃全部NATS wake仍由safety scan恢复；
- duplicate/out-of-order callback与outbox不产生双transition；
- old lease generation不能heartbeat、progress或commit；
- callback/poll/cancel/timeout竞态只有一个winner；
- 非幂等uncertain Effect不自动retry；
- Event history与aggregate current state不形成双写入口；
- 新ResourceKind/JobKind/TaskKind不增加重复生命周期表族；
- PostgreSQL PITR与Artifact restore可以恢复固定Run bindings；
- 静态检查和故障测试证明事务内无外部I/O。

## 19. 明确推迟

- multi-region active-active PostgreSQL；
- global event ordering；
- 仅靠Event全量重建所有current aggregates；
- third-party exactly-once协议；
- Q1 profile之外的容量与RPO/RTO。

## 20. 未决问题

没有阻止下游重写的问题。物理baseline与本规范分离审查；任何为了数据库便利引入的projection都不得获得domain写权限。
