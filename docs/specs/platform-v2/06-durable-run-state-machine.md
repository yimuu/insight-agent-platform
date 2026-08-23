# Platform v2 Durable Run 状态机规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-179 |
| 日期 | 2026-08-24 |
| 依赖 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md)、[`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) |
| 直接下游 | 07、08、10、15、16、17、18 |

> Persistence ruling：逻辑 work 及其 current execution 统一由 03 的 `Job` 表达，Attempt 仅是
> `(JobId, lease_generation)` 标识的历史观测；Continuation 统一为 Job 内的 `WakeContract`，HumanTask
> 与 Approval 统一为 `Task`。历史 `execution_attempts`、`continuations` 与 transition 表不再是目标结构。

## 1. 决策摘要

每次Agent调用创建一个固定Deployment/RunBindings的durable Run。root Run请求显式选择tenant-scoped `agent_id`，admission事务从该
Resource的enabled active Agent Deployment解析完整bindings及已验证entry node；child Run继承
parent相同的exact bindings，因此后续Resource active target或GitOps rollout不能把同一durable执行链切成两套实现。Scheduler只根据Typed Plan和已提交事实推进状态；
NodeExecution 表示逻辑执行，03 的 Job 表示一个可租约、重试和恢复的逻辑 work 及其唯一 current
generation。本文保留“Attempt”作为 Job generation 的历史观测术语，不代表独立 ID、current aggregate
或持久化表。Run、NodeExecution 和 Job 均使用闭合状态机与数据库 first-winner CAS。

## 2. 目标与非目标

### 2.1 目标

- 在任意节点边界和外部等待中恢复 Run；
- 分离静态 PlanNode、逻辑 NodeExecution、current Job 和物理 generation 历史；
- 为 branch、parallel、map、loop、wait、model loop、capability 和 child Run 提供统一状态事实；
- 明确 pause、cancel、timeout、retry、failure propagation 和唯一终态；
- 让等待态不占 execution permit；
- 让大值、错误详情和最终文件通过 ArtifactRef 传递；
- 提供稳定 snapshot、cursor 和内部 transition ledger。

### 2.2 非目标

- 不提供 terminal-only、进程内恢复或用户代码 deterministic replay；
- 不从 event ledger 重建全部 state；projection tables 是当前权威；
- 不承诺外部副作用 exactly-once；
- 不让客户端直接设置内部状态；
- 不允许跳过 Plan verifier 执行临时节点；
- 不在本规范定义跨 Run 业务补偿。

## 3. 核心身份

```rust
struct ExecutionIdentity {
    run_id: RunId,
    scope_id: ScopeInstanceId,
    node_execution_id: NodeExecutionId,
    plan_node_id: PlanNodeId,
    activation_ordinal: u32,
}

struct AttemptIdentity {
    job_id: JobId,
    lease_generation: u64,
}
```

- `PlanNodeId` 在 Agent Revision 内稳定；
- `ScopeInstanceId` 表示 root、branch leg、map item、loop iteration 或 model round 等动态作用域；
- `NodeExecutionId` 在创建时生成并永久稳定；
- Retry 不改变 NodeExecution ID 或 Job ID；每次claim增加`lease_generation`，每次成功start增加`attempt_count`，
  并按generation追加历史观测；
- 同一 PlanNode 可以因 map/loop 被多个 Scope 激活；
- 身份由 repository 分配和约束，不依赖可碰撞的路径字符串作为唯一权威。

## 4. Run 状态机

```rust
enum RunState {
    Queued,
    Running,
    Waiting,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

允许的主转换：

```text
Queued -> Running | Cancelling | TimedOut
Running -> Waiting | Cancelling | Succeeded | Failed | TimedOut
Waiting -> Running | Cancelling | Failed | TimedOut
Cancelling -> Cancelled | Failed | TimedOut
```

终态不可离开。`Waiting` 表示当前没有 ready executable work，但存在 timer、signal、human task、approval、
remote invocation、child Run、retry deadline 或 capacity wait。Capacity wait 可以让 Run 保持 Running，具体
projection 由 07 定义，但不得占 permit。

## 5. Pause 与 Admission Control

Pause 不成为 Run 主状态，而是 control intent：

```rust
struct RunControl {
    pause_generation: u64,
    pause_requested: bool,
    cancel_generation: u64,
    cancel_requested: bool,
    cancel_requested_at: Option<DateTime<Utc>>,
    cancel_reason_code: Option<StableReasonCode>,
    cancel_principal: Option<PrincipalSnapshot>,
    timeout_generation: u64,
    timeout_requested: bool,
    timeout_requested_at: Option<DateTime<Utc>>,
    timeout_observed_run_state: Option<RunState>,
    timeout_observed_run_projection_version: Option<u64>,
    deadline: DateTime<Utc>,
}
```

- pause 只阻止创建/领取新的外部工作；已经运行的 Attempt 继续到安全节点；
- timer、callback、signal、approval、cancel 和 terminal commit 在 pause 期间继续提交；
- resume 清除当前 generation 的 pause intent 并唤醒 Scheduler；
- pause/resume 使用 CAS 且幂等；
- pause/resume必须按固定顺序同时锁定Run与其control row，避免与terminal transition写偏斜；只有真实toggle才把
  control projection version与pause generation各增加1，同一exact generation上的同目标请求返回unchanged，stale
  generation返回CAS conflict；网络级exact replay仍由03 CommandReceipt证明；
- public `Paused` 是由 pause intent + 无 in-flight work 派生的视图，不是数据库主状态。

## 6. NodeExecution 状态机

```rust
enum NodeExecutionState {
    Pending,
    Ready,
    Running,
    Waiting,
    RetryScheduled,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

允许转换：

```text
Pending -> Ready | Failed | Cancelled | TimedOut
Ready -> Running | Cancelled | TimedOut
Running -> Waiting | RetryScheduled | Cancelling | Succeeded | Failed | TimedOut
Waiting -> Ready | Cancelling | Failed | Cancelled | TimedOut
RetryScheduled -> Ready | Cancelling | Cancelled | TimedOut
Cancelling -> Cancelled | Failed | TimedOut
```

`Pending/Ready/Waiting/RetryScheduled -> Cancelled`只在事务内证明没有in-flight Attempt或未决Effect时允许；否则先进入
`Cancelling`。所有终态不可离开，非法边由数据库transition constraint与domain exhaustive match共同拒绝。

语义：

- `Pending`：已创建，但控制/数据前置尚未满足；
- `Ready`：可以由 Scheduler 生成/领取工作；
- `Running`：至少一个当前 Attempt 或 controller drive 正在执行；
- `Waiting`：等待同Node的 pending WakeContract、Task 或非终态 Invocation；
- `RetryScheduled`：等待 `retry_at`，不占 permit；
- `Cancelling`：cancel 已传播，等待 child/attempt drain；
- 其余为终态。

非选择 branch 不创建 NodeExecution，因此没有 `Skipped`。Join 只等待实际被接纳的 leg。

## 7. Attempt 历史观测合同

```rust
enum AttemptObservationState {
    Leased,
    Started,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}
```

允许转换：

```text
Leased -> Started | Cancelled | TimedOut | Lost
Started -> Succeeded | Failed | Cancelled | TimedOut | Lost
```

- claim 在同一 Job 行上执行 `Ready|RetryScheduled -> Leased`，只递增`lease_generation`，并追加该
  `(JobId, lease_generation)` 的 `Leased` Event；
- Worker开始新的物理外部尝试前原子提交Job `Leased -> Running`、`attempt_count + 1`与`Started` Event，或使用
  等价的原子start receipt；start前lease丢失返回Ready且不消耗attempt budget；
- 已Started的物理外部尝试进入Deferred/Input Waiting后，typed owner只有在exact WakeContract、opaque-state digest与
  owner version仍匹配时，才能把下一次Ready冻结为同一物理attempt的resume。resume增加`lease_generation`并重新取得fence，
  但不增加`attempt_count`；`RetryScheduled`永远表示新的物理attempt，不能借resume绕过attempt limit；
- claim期间Node保持`Ready`，但其projection version随leased generation证据递增；只有原子start才提交Node
  `Ready -> Running`并设置首次`started_at`。因此start前lease loss不需要非法的Node `Running -> Ready`回边；
- lease 过期而无 terminal receipt 时追加当代 `Lost` Event；Job 本身必须由 owner recovery policy 进入
  `Ready | RetryScheduled | TimedOut | ReconciliationRequired | Failed`，不存在 `JobState::Lost`；
- stale fence outcome返回闭合`AttemptCommitDisposition::RejectedStaleFence`并写审计，不改变 Job 或
  NodeExecution 状态；`Rejected` 不是 Attempt observation state；
- generation terminal 不必等于 NodeExecution terminal，例如 retryable failure 会让 NodeExecution
  `RetryScheduled`；
- Job 只保存 current generation 的租约与 bounded backend state；开始/结束时间、backend class、safe failure、
  resource usage 和 effect key 作为 bounded Event/Receipt evidence 追加，不保存正文。
- `NodeExecution.started_at`是逻辑Node首次进入`Running`的时间，后续Attempt retry再次`Ready -> Running`不得重写；
  每代物理尝试使用 Job current generation 的 `started_at`并在换代前写入历史 evidence。

唯一物理执行与 fence 合同由 03 的 Job 提供：Job 保存 owner、work class、attempt ordinal、current epoch、lease、deadline、
retry、effect key、bounded request/result/failure 与 terminal winner；提交重放和 stale disposition 由共享 Receipt 保存。

同一 typed work 只有一个 Job current row，最多一个 `Leased | Running` generation；`lease_generation`每次claim严格递增，
`attempt_count`只对成功开始的新物理attempt单调递增。同一物理attempt可以跨多个Deferred/Input continuation generation恢复，
但每代执行权identity仍固定为`(JobId, lease_generation)`，不再生成Attempt ID。
`run_scheduler.attempts_per_work`是所有WorkClass共享的唯一machine hard max，domain/effective policy只能进一步收紧。
首次 Job admission 固定该 logical work 的 `attempt_limit`，后续 generation 必须完全继承；数据库同时约束
`attempt_count <= attempt_limit`，因此 recovery 可从 current immutable evidence 判断 generation 是否耗尽，而不能接受 caller hint。
active Job 的 quota reservation reference 必须指向 open reservation；terminal winner 在同一事务结算。许可不是Node/Run
状态字段，Scheduler 不维护第二份 durable counter。状态和 outcome 历史写共享 Event；Event 不参与当前状态裁决。

10 Capability、12 Context、13 MCP、14 Sandbox、15 Artifact和16 Model的物理执行尝试全部复用 Job、
`(JobId, lease_generation)`、lease/fence 与 commit disposition。它们可以有 domain-specific 逻辑状态，不能再定义第二份
物理尝试 current state。Job generation 只裁决物理尝试；generation/deadline 耗尽后的父状态属于 typed
owner。Run/Node按本规范生成cancel/timeout/
budget-exhausted transition；Artifact等leaf owner必须由各自reconciliation合同决定不可判定副作用，不能借用Run终态或让
generic recovery自行修改父aggregate。

## 8. ScopeInstance

```rust
enum ScopeKind {
    Root,
    BranchLeg,
    ParallelLeg,
    MapItem,
    LoopIteration,
    ModelRound,
    ErrorBoundary,
}
```

Scope 状态：`Open -> Closing -> Succeeded|Failed|Cancelled`。父 Scope 不能在所有已接纳 child
NodeExecution settled 或按策略 drain 前闭合。Map item index、loop iteration 和 parallel leg ID 是 scope
metadata；用户输入不能直接指定 scope identity。

ScopeKind machine wire固定为`root | branch_leg | parallel_leg | map_item | loop_iteration | model_round |
error_boundary`。

Child Agent 是独立 Run，不作为本表中的嵌套 Scope；父子关系由 ChildRunLink 表示。

每个Scope current payload内嵌一个bounded closed环境：

```rust
struct ScopeDataEnvironmentSnapshot {
    schema_version: u32,
    bindings: BTreeMap<ExactDataPortRef, ExactRunValueRef>,
    canonical_digest: Digest,
}
struct ExactRunValueRef { value_id: RunValueId, schema_digest: Digest, content_digest: Digest }
```

它只拥有port到immutable RunValue的current绑定，不复制值正文、classification或Artifact locator；`run_values`仍是值事实唯一authority。
root admission把请求RunValue绑定到唯一`RunInput { schema_digest }` key写入root Scope，无需读取Plan Artifact；Compute terminal、
Map item admission和Loop iteration rollover在同一事务中创建RunValue并以Scope
version CAS更新环境。解析按当前Scope→parent Scope逐级执行，深度使用`registry_plan.plan_nodes` effective limit，binding数使用
`run_scheduler.value_refs_per_run` effective limit；每级payload/digest、Run/tenant、RunValue schema/content均重验。普通写禁止覆盖
已绑定port；只有Plan声明的Map item/Loop carried shadow规则可以在新child Scope绑定同名port。

Loop iteration Scope不串成父子链：每轮Scope的controller owner固定为首次Loop NodeExecution，该owner的Scope提供稳定词法父环境。
body settlement在关闭当前Scope的同一事务预建下一轮open Scope、复制并绑定carried RunValue，同时把pending Loop continuation的
`scope_id`切到该新Scope后才置为Ready。condition为true时body NodeExecution复用continuation当前Scope；condition为false时同一事务
关闭当前Scope并把exit激活到固定词法父Scope。任何新Scope/RunValue ID冲突、body output缺失、schema/content/classification漂移、
stale Scope/Node/Job fence都会使rollover整批不可见。

## 9. Control Token 与 Data Value

Scheduler 通过持久化 control token 决定 readiness：

```text
(run_id, target_node_id, target_port, source_execution_id, scope_id) UNIQUE
```

- token 只能由 committed transition 产生；
- 重复 drive 不生成重复 token；
- data value 通过 immutable ValueRef 关联 source output port；
- ValueRef不新增global ResourceKind/ID；它以`(tenant_id, run_id, value_key)`作为owner-local identity，并以
  `(owner_kind, owner_id, owner_port)`唯一绑定typed owner，因此一个Node可按多个output port分别产值；
- 小值内联并受大小限制，大值使用 ArtifactRef；
- Artifact-backed Model output只有在typed owner计算的冻结合法输出上限严格大于effective Inline threshold时，才可预留
  Artifact/candidate Blob/duplicate-cleanup Job/Output Link/stage Receipt和04固定的Artifact-owned count/logical bundle及
  candidate-Blob-owned upload/staging/physical bundle；上限小于或
  等于threshold时必须是`InlineOnly`。预留ID只属于exact owner/Job/attempt/lease，尚未插入的
  RunValue ID不构成RunValue，实际小值仍必须由owner terminal写Inline值并释放整组未用Artifact预留；
- Model output的`Staging | Uploaded | Verifying | Verified` Artifact candidate及materialization/stage Receipt都不是ValueRef，
  不能满足data port、
  产生control token、唤醒consumer、进入public output或被恢复流程当作continuation/terminal outcome；
- 只有Model typed output owner的first-winner terminal事务可以在同一原子提交中把exact Verified candidate推进`Ready`、建立唯一
  `Reference`/Output Link、用该事务PostgreSQL time加冻结`ready_retention_seconds`计算并保存absolute `ready_retain_until`、把retention从
  冻结`staging_retain_until`切到该值并插入immutable RunValue的
  `ValueRef::Artifact`。Producer只能形成Verified candidate与digest-bound receipt，
  不能创建RunValue、建立Reference或推进Ready；任一检查失败必须使上述事实全部不可见；
- immutable RunValue的`schema_digest`是trusted producer对逻辑值正文已通过exact schema的承诺，不是
  客户端可自由声明的metadata。Inline producer必须对本地正文验证；Artifact-backed producer必须在读取并核对
  exact content digest/length后对物化正文验证，再与RunValue原子提交。consumer仍在自己的trust boundary可见正文时
  重新验证；不得把ArtifactRef metadata当成被引用正文来验证内容schema；
- 下游 NodeExecution 只有在所有 required control/data port 满足时从 Pending 进入 Ready；
- token/value 不通过 NATS 传递权威正文。

## 10. Controller 节点推进

Branch、Fork、Join、Map、Loop、ErrorBoundary、ModelLoop 和 Return/Raise 是 Scheduler 驱动的 controller。
每次 drive：

1. 读取固定 Plan 与已提交 execution facts；
2. 在纯 domain 函数中生成 deterministic commands；
3. 用 expected projection version 原子提交 commands；
4. 重复 drive 在同一事实下产生相同 logical command keys。

Controller 不执行网络 I/O，不持有跨 await transaction，不依赖进程内 continuation。

Branch/Map/Loop/Compute的`ControllerObservation`不是authority或可调用API参数，而是05 closed evaluator对exact Plan程序与
immutable RunValue的派生结果。执行分两段：Scheduler可在事务外物化/验证Plan和RunValue正文并纯计算；提交事务必须重新锁定
Job/Run/Node current version，重验所有input `ValueRef` identity/schema/content digest、expression digest与输出canonical digest，
然后原子写入Compute产生的immutable RunValue、Scope environment CAS、Node/Job转换、Receipt/Event/Outbox。任一输入已改变、缺失、跨run/tenant、
Artifact正文不匹配或fence丢失时整批不可见。RunValue不可变，因此成功提交后无需另建observation current projection；Receipt/Event
只保存bounded evidence digest与引用。

Compute output RunValue的classification由05 owner规则计算：external input classification的lattice join，空external closure固定
`Internal`；同一事务必须从重验后的input rows重算，禁止command、Worker或Artifact metadata选择更低等级。classification计算失败、
证据与行不一致或任何output尝试降级时，RunValue、Scope CAS及其余Node/Job/Event/Outbox mutation全部回滚。

首次Map求值原子冻结input value ref、item count、batch cursor与failure policy；后续批次只消费该冻结payload。Loop每次iteration
冻结loop-carried value refs和condition evidence。Branch只为winner创建NodeExecution；未选arm与`otherwise`之外不存在Skipped事实。
每个Map item admission用05 exact `item_port` schema创建独立immutable RunValue，并在新MapItem Scope环境绑定该port；item正文来自已验证
array element，classification继承本次expression effective classification。RunValue insert、Scope payload、item Node/Job及批次cursor同事务。

## 11. Parallel、Map 与 Join Settlement

- `AllSuccess`：任一 leg failure 触发其他未完成 leg cancel intent，drain 后 join failed；
- `AllSettled`：收集每个 leg 的 typed outcome；
- `Quorum(n)`：达到 quorum 后按 Plan policy cancel 或 drain 剩余 leg；
- Map 结果按 input index；
- fan-out 分批创建，不能一次事务插入无界 NodeExecution；
- 对可能提前停止的 Map policy，下一批 admission continuation 保持 Pending，直到当前已准入批次全部 terminal 且累计失败数
  仍允许继续；一旦停止条件成立，item terminal winner 必须在同一事务取消活动 sibling、把该 continuation 冻结为
  settlement 并使其 Ready，后续批次不得创建。并发 item terminal、continuation claim 与重放由 Node/Job CAS 裁决；
- parent cancellation 传播到所有已接纳 leg/item；
- join terminal 只有一个 first-winner commit。
- 并发修改Node/Job、伪造Branch target/Map count/Loop condition、错误RunValue digest或重复Compute output只允许一个winner且不会留下
  部分RunValue、Scope、Node、Job或Event；

## 12. Retry

```rust
struct RetryPolicy {
    max_attempts: u16,
    backoff: BackoffPolicy,
    retry_on: BTreeSet<FailureClass>,
    max_elapsed: Duration,
}
```

- 重试必须同时被 NodePolicy、Capability Effect、backend idempotency 和 Run deadline 允许；
- jitter 使用首次 schedule 时持久化的随机值，恢复不重新抽取；
- `retry_at` 是数据库权威；
- retry 不修改输入、binding、Effect 或 approval；
- NonIdempotentWrite/Irreversible 的未知结果进入 reconciliation/manual review，不走普通 retry；
- max attempts 统计所有 Started Attempt，不只统计返回 failure 的尝试。

## 13. Waiting 与 Durable Work

`Waiting`必须引用至少一个durable等待所有者。callback/timer/poll 等机器恢复点使用 Job 内的 pending WakeContract；human
input/approval 使用 Task；已创建但尚未被 controller 消费结果的 leaf 调用使用同 Node 的非终态 Invocation。不得复制另一套
等待当前状态。所有等待所有者终结时，Node 必须在同一事务离开 Waiting。

CapabilityCall的同步停驻顺序固定为：Orchestration Attempt在Node Running时创建Invocation，然后提交并把Node
`Running -> Waiting`、释放controller permit；Capability Attempt只能从该Waiting owner claim并增加Run active work；唯一
terminal winner关闭leaf permit、递减active work、写结果证据并把Node `Waiting -> Ready`。Approval拒绝不创建伪Attempt，
但必须与拒绝回执和相同节点唤醒原子提交。

需要外部恢复协议的等待只引用03唯一`WakeContract`/`WakeContractPayload`/`WakeState` machine contract，不在Run领域复制结构。
机器 wake kind固定为`timer | signal | remote_invocation | child_run | retry_deadline`；RunNode只保存关联Job identity并从其typed payload读取current
Wake，不能另存kind/state/generation/deadline或opaque state。HumanTask/Approval不放入WakeContract，它们由Task表达。

唯一转换是`Pending -> Consumed | Cancelled | TimedOut`。callback/signal以generation CAS first-winner；非法或
schema不合格response不消费 wake。terminal后不能重复恢复节点。opaque external task/interaction handle必须加密
或通过Artifact/ValueRef保存，不进入public projection。

### 13.1 Task

HumanTask节点、Capability BackendInputRequest、MCP Elicitation 和 Approval 统一创建 durable Task，以 `TaskKind`
区分业务输入与授权；授权规则仍由 04 拥有：

```rust
struct Task {
    interaction_id: InteractionId,
    tenant_id: TenantId,
    run_id: RunId,
    owner_node_execution_id: NodeExecutionId,
    kind: InteractionKind,
    state: InteractionState,
    safe_prompt_key: SafePromptKey,
    response_schema_digest: Option<Digest>,
    deadline: DateTime<Utc>,
    generation: u64,
    projection_version: u64,
}

enum InteractionKind { Form, UrlConsent, BusinessInput }
enum InteractionState { Pending, Responded, Declined, Cancelled, Expired }
```

InteractionKind machine wire固定为`form | url_consent | business_input`。

唯一转换是`Pending -> Responded | Declined | Cancelled | Expired`。cancel/expiry与response按generation/version
first-winner。response正文进入bounded ValueRef/Artifact，Task 和 public Event 只保存 schema digest 与 safe projection。
respond/decline由caller-owned serializable transaction固定拥有`interaction.respond`的 PrincipalSnapshot，并在同一事务提交
Task terminal、response Receipt 与 owner NodeExecution `Waiting -> Ready`。schema digest不匹配、权限generation漂移或
deadline已到时均不得完成 Task。

## 14. Cancel 与 Timeout

Cancel 是 durable intent，不是删除：

1. caller-owned serializable command固定`runtime.control` PrincipalSnapshot，写cancel generation、请求时间与closed
   reason code，并在同一事务把`Queued | Running | Waiting` Run推进为`Cancelling`；
2. Scheduler 停止新业务 work，但允许 cancellation/cleanup work；
3. 向 active Invocation、ChildRun 和 HumanTask 传播；
4. Worker 使用 backend cancellation 能力 best effort 取消；
5. 等待 drain deadline；
6. 以数据库 first-winner 提交 Run `Cancelled`，或在 cleanup failure policy 下 `Failed`。

Run deadline 到达产生 `TimedOut` intent。timeout command必须锁定Run/control、证明数据库观察时间不早于immutable
deadline，并冻结当时的Run state与projection version；intent提交后只允许该观察状态、`Cancelling`或`TimedOut`，禁止
普通成功/失败结果越过winning timeout。外部结果若先提交会让timeout的expected state/version CAS失败，因此两者仍由
数据库commit顺序决定唯一赢家。平台不从timeout推断副作用没有发生，迟到结果只记录safe audit。网络级exact replay
仍由03的CommandReceipt证明；内部deadline scanner对同一当前generation返回unchanged，不重复增加generation。

上述cancel/timeout“请求时间”都是repository在事务内取得的PostgreSQL `clock_timestamp()`；command payload不携带可成为
状态机authority的caller time。Run/control提交点closure拒绝未来intent time，因此typed API与直接SQL遵循同一时钟合同。

Run/control 事务不变量必须双向证明：`Cancelling | Cancelled`存在cancel intent；`Cancelled`的reason/time闭合该
intent；`TimedOut`存在不早于deadline的timeout intent；冻结的Run projection version不大于当前版本。任何直接把Run
写入`Cancelled | TimedOut`而没有对应typed intent的事务必须在提交点失败。

## 15. Run Terminal

- `Succeeded` 必须有通过 Agent output schema 的 final ValueRef；
- `Failed` 必须有 safe Failure；
- `Cancelled` 保存发起 principal/reason code，不保存自由文本正文；
- `TimedOut` 保存 deadline 与 unresolved effect summary；
- terminal transaction 同时写 Run snapshot、transition、Artifact references、public terminal outbox；
- terminal 后新 signal、callback、approval 和 Worker outcome 不能改变结果；
- final public delivery 重试不改变 Run terminal state。

Run 因 deadline/cancel 进入终态时，如果仍有不确定外部 Effect，必须在同一事务中把它们移交给独立
Reconciliation record。该安全工作可以在 Run terminal 后继续，但只能更新 reconciliation/audit，不能回写
Run、NodeExecution 或最终输出。

## 16. Failure Propagation

叶节点 failure 先进入最近的 matching ErrorBoundary；没有匹配时向 scope/Run 传播。Failure match 只使用
稳定 class/code。内部 panic、schema invariant violation 和 storage corruption 是 platform failure，不能被普通
Agent catch 隐藏为业务成功。

## 17. Run Snapshot 与事件

Run snapshot 最低包含：

```text
run_id / tenant_id / agent_deployment_id / bindings_digest
state / projection_version / created_at / started_at / terminal_at
active_work_count / waiting_reason summary
input_ref / output_ref / failure summary
control generations / deadline / budget usage
```

内部 transition ledger append-only，按所属聚合版本严格递增；它用于恢复、审计和一致性诊断，不是公共cursor或
用户可重放全部中间正文的API。03/17只在产生安全PublicRun投影时分配独立per-Run public sequence，公共cursor只
引用该sequence。

Node public terminal事件必须区分`node.completed`、`node.failed`、`node.cancelled`和`node.timed_out`；不能把
TimedOut压成failed。Run级事件由17冻结，并从本状态机的committed transition投影。

## 18. Persistence 映射

- Run 当前状态、RunBindingsSnapshot、control intent、public sequence 与 counters 在 `runs`；
- Node/Scope/control-token/current relation 在 `run_nodes`，typed values 在 `run_values`；
- leaf logical calls 在 `invocations`，物理执行与 WakeContract 在 `jobs`，human/approval 在 `tasks`；
- transition/outcome/audit 在 `events`，command/callback/commit 去重在 `receipts`，外发在 `outbox_events`。

数据库保证tenant/FK/unique/CAS/lease/transaction/outbox；Rust保证状态机、binding closure、schema、policy、retry、cancel与
reconcile。Run admission在同一事务冻结完整RunBindingsSnapshot，不在执行时读取dynamic head。child admission只继承parent冻结的exact
ResourceVersion/Deployment并重验current tenant security fence，不追随active head或GitOps rollout。

## 19. 可观测性

最低指标：

```text
runs_total{state}
runs_active{state}
run_duration_seconds{terminal_state}
node_executions_total{kind,state}
jobs_total{work_class,outcome}
wake_contracts_active{kind}
retry_scheduled_total{work_class,failure_class}
late_outcome_total{work_class,reason}
```

Run/node ID 不进入 metric label。Trace span 可以携带 opaque IDs，但不携带输入输出正文。

## 20. 验收标准

- 每种合法/非法状态转换都有 model-based test；
- kill -9 发生在 claim、start、external call、outcome、join、terminal 各窗口时最终收敛；
- Retry 保持 NodeExecution ID 与 Job ID；claim递增`lease_generation`，start递增`attempt_count`，并追加generation evidence；
- stale epoch 无法推进 Node 或 Run；
- pause 不阻止 cancel、timeout、signal 和 terminal commit；
- waiting Run 不占 execution permit；
- branch 未选择路径无 NodeExecution；
- parallel/map cancel 后父 scope 等待安全 drain；
- signal/callback/timeout 竞态只有一个 first-winner；
- terminal output schema failure 不能提交 Succeeded；
- Artifact output fixture证明Staging/Verified candidate与stage Receipt永远不满足ValueRef；只有owner terminal可以原子产生
  Ready Artifact、ready retention、唯一Output Reference和RunValue，crash/stale/cancel/timeout任一窗口都不存在这些事实的部分可见组合；
- root admission按Receipt→Tenant→Resource锁序冻结完整02 binding并验证全部exact Model bindings；任一binding失败时Run、bindings、quota、
  Event/Outbox及terminal成功Receipt整事务回滚；
- child admission逐字段继承parent historical bindings，完全不读current active head；它重验current security fence，缺失任一exact历史
  ResourceVersion/Deployment时fail closed；
- 所有 v2 Run 都能在不同 runtime 实例恢复，不存在 volatile 分支。

## 21. 明确推迟的工作

- continue-as-new；
- workflow migration 到新 Agent Revision；
- fork/redrive public API；
- compensation/saga；
- 跨区域 Run ownership；
- transition retention 数值。

## 22. 未决问题

CR-166已统一root current与child inherited exact binding合同，无installation/release中间层。本规范已Accepted；
durable state、崩溃恢复与parent/child fixture仍待实现。public status的精简映射与SSE schema由17定义，
不能改变这里的durable first-winner状态机。
