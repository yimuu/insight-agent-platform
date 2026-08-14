# Platform v2 Capability Invocation规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-09 |
| 依赖 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md)、[`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) |
| 直接下游 | 08、13、14、16、17、18 |

## 1. 决策摘要

每次Capability调用先创建一个durable Invocation aggregate，再创建或复用共享Job dispatch。Invocation只拥有逻辑调用、
frozen admission、当前Job引用和最终结果；Job拥有lease、attempt generation、backend handle、poll/callback wait和recovery。
Approval/Input复用Task，幂等/callback复用Receipt，transition/outcome/rejection/audit复用Event。

不再建立Capability专用policy-binding、transition、outcome、remote-task、resume或callback-rejection表。相同正确性由一个
Invocation current row、一个Job current row、typed immutable snapshots和原子Event/Receipt提交保证。

## 2. 目标与非目标

### 2.1 目标

- 所有native/http/gRPC/MCP/Sandbox backend使用同一逻辑状态机；
- 支持同步低延迟与durable waiting；
- admission冻结Deployment、input、Effect、Policy、deadline和attempt budget；
- duplicate dispatch/result/callback不会产生双结果；
- output在进入Plan/Model前完成schema、Artifact和Policy验证；
- 非幂等uncertain Effect进入reconciliation/manual review；
- Model tool与Plan node调用使用同一真实执行语义；
- persistence复杂度不随outcome或backend种类增加。

### 2.2 非目标

- 不把HTTP连接或Worker内存作为Invocation权威；
- 不保证所有backend支持callback/cancel/progress；
- 不承诺外部Effect exactly-once；
- 不允许backend修改Run/Node、选择新Deployment或扩大权限；
- 不持久化无限stdout/token/progress；
- 不允许Model伪造completed event；
- 不支持一次Invocation在retry时切换Deployment。

## 3. Invocation aggregate

```rust
struct CapabilityInvocation {
    invocation_id: InvocationId,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    origin: InvocationOrigin,
    state: InvocationState,
    version: u64,
    admission: AdmissionSnapshot,
    current_job_id: Option<JobId>,
    approval_task_id: Option<TaskId>,
    input_task_id: Option<TaskId>,
    result: Option<InvocationResult>,
    failure: Option<Failure>,
    reconciliation: Option<ReconciliationSnapshot>,
    deadline: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    terminal_at: Option<DateTime<Utc>>,
}

enum InvocationOrigin {
    PlanNode,
    ModelToolCall { model_turn_id: ModelTurnId, call_id: ModelCallId },
}
```

Invocation state、result和failure只在该aggregate保存一次。Event记录历史但不能直接编辑current state。Job result先作为
physical outcome提交，再由Invocation application service验证和归并；Worker/backend不能直接terminalize Invocation。

## 4. Admission snapshot

```rust
struct AdmissionSnapshot {
    schema_version: u32,
    origin_key: TypedOriginKey,
    slot_binding_digest: Digest,
    selection_policy: ExactVersionRef,
    selection_evidence: TypedSelectionEvidence,
    deployment: ExactDeploymentRef,
    interface: ExactVersionRef,
    implementation: ExactVersionRef,
    input: ExactValueRef,
    effect: Effect,
    idempotency: IdempotencyContract,
    cancellation: CancellationContract,
    input_schema_digest: Digest,
    output_schema_digest: Digest,
    error_schema_digest: Digest,
    artifact_contract: CapabilityArtifactContract,
    data_flow_policy: CapabilityDataFlowPolicy,
    interface_limits: CapabilityInterfaceLimits,
    policies: PolicyDecisionBundle,
    principal: PrincipalSnapshot,
    effect_key_digest: Digest,
    idempotency_key_digest: Digest,
    attempt_limit: u32,
    retry_backoff_milliseconds: u64,
    deadline: DateTime<Utc>,
    canonical_digest: Digest,
}
```

Snapshot由trusted application service从Run bindings和exact Deployment构造，不接受公共API、Model或backend直接提交完整对象。
它是closed、bounded、canonical typed JSONB value；高频的tenant/run/state/deadline保留为普通关系字段。任何input、candidate、
Deployment、Policy、schema、Artifact/DataFlow/Interface limits、deadline或principal变化必须创建新Invocation，不能复用旧approval
或idempotency identity。admission前trusted application service使用exact Interface完整input schema校验实际Value：Inline
值在admission transaction内再验证；Artifact-backed RunValue必须由trusted producer在核对exact content digest/length并
物化正文后验证，admission锁定该immutable RunValue的schema/content digest，执行adapter在读取真实字节后
再验证。result归并前使用同一exact Interface完整output schema校验结果。不得用ArtifactRef metadata
替代对被引用正文的验证；durable snapshot只冻结digest不是跳过本地validation的理由。

Effect key和idempotency key由固定Run/Node/ModelCall identity派生。裸`ModelCallId`只在exact ModelTurn内有效。

## 5. 状态机

```rust
enum InvocationState {
    Created,
    AwaitingApproval,
    Ready,
    InFlight,
    Deferred,
    AwaitingInput,
    RetryScheduled,
    Cancelling,
    ReconciliationRequired,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

```text
Created -> AwaitingApproval | Ready | Failed
AwaitingApproval -> Ready | Failed | Cancelled | TimedOut
Ready -> InFlight | Deferred | Cancelling | TimedOut
InFlight -> Succeeded | Failed | Deferred | AwaitingInput |
            RetryScheduled | ReconciliationRequired | Cancelling | TimedOut
Deferred -> InFlight | Succeeded | Failed | AwaitingInput |
            RetryScheduled | ReconciliationRequired | Cancelling | TimedOut
AwaitingInput -> Ready | Deferred | Failed | Cancelled | TimedOut
RetryScheduled -> Ready | Deferred | Cancelling | TimedOut
Cancelling -> Cancelled | Failed | ReconciliationRequired | TimedOut
ReconciliationRequired -> Succeeded | Failed | Cancelled
```

Terminal不可离开。每个command以expected state/version CAS并由Rust exhaustive transition function验证；数据库只检查closed state、
positive version、terminal shape和CAS affected-row。ReconciliationRequired不占execution permit，但占unresolved-effect quota。
Cancelled/TimedOut仅在没有未决Effect，或已原子记录Reconciliation snapshot时允许。

## 6. 创建与Approval

Admission command在一个事务中：

1. claim Command Receipt；
2. 锁定Run、Node与RunBindings snapshot；
3. 验证Capability slot、exact candidates和Selection Policy；
4. 通过trusted selector产生typed decision；
5. 读取并验证Deployment/Interface/Implementation/gates；
6. 锁定input ValueRef并验证tenant/schema/classification/digest；
7. 计算Effect/idempotency keys、deadline和attempt-limit intersection；
8. 构造AdmissionSnapshot；
9. 根据Approval Policy创建Task或推进Ready；
10. 与Invocation、Event、Outbox和Receipt原子提交。

Approval Task绑定Invocation version、admission digest、input digest、Effect、deadline和eligible principal rule。Response与Task terminal、
Invocation Ready/Failed和Event同事务first-winner。Backend不能返回`ApprovalRequired`决定平台审批。

## 7. Job dispatch

Ready Invocation只通过03共享Job执行：

```rust
struct CapabilityJobBinding {
    schema_version: u32,
    invocation_id: InvocationId,
    admission_digest: Digest,
    deployment: ExactDeploymentRef,
    effect_key_digest: Digest,
    idempotency_key_digest: Digest,
    input: ExactValueRef,
    deadline: DateTime<Utc>,
}
```

Job typed payload还保存同一binding、optional closed WakeContract、encrypted remote-state envelope、optional exact resume-input、
bounded progress counter/milestone、下一次start的closed `NewPhysicalAttempt | ResumePhysicalAttempt` disposition和physical outcome
evidence。它们都是Job当前执行事实，不复制Invocation result/current state；
generic Job repository必须按WorkClass解码对应nominal payload，不能只为Orchestration识别WakeContract。

Native/HTTP/gRPC的claim事务重新检查Run/control/Node、Invocation Ready/version、Deployment gate、quota和deadline；创建或推进
Job lease并将Invocation置InFlight。WorkClass由backend kind服务端派生。调用方不能提供binding digest、attempt limit、deadline或
next generation。

MCP Tool只有在exact MCP Deployment transport为`streamable_http`时，才由一个`CapabilityRemote` Capability Job经MCP Host与
独立Egress执行；`Mcp` WorkClass只用于Host自有discovery/subscription等工作。transport为`managed_stdio`时，逻辑
Implementation仍是`Mcp`，但物理WorkClass必须由exact Deployment closure服务端派生为`Sandbox`：Gateway直接创建
`work_class=sandbox`的同一个共享Job，禁止先创建或领取`CapabilityRemote`父Job，也禁止Capability/MCP Worker持有permit等待
microVM。该Sandbox request除Interface input/output外，还冻结exact MCP Deployment、Discovery Snapshot、Protocol/Auth closure、
operation/continuation和Managed Runner Package/Runtime/Profile/Policy；Executor claim后才绑定WorkerProcessGeneration与lease，调用方
不得在admission时自报物理fence。

普通Sandbox Implementation同样因独立isolation bulkhead直接创建`work_class=sandbox`的同一个共享Job，不得先创建
`CapabilityRemote`父Job。Managed stdio Resource Subscription保留一个`work_class=mcp`逻辑subscription Job，并为每个有界live
session创建一个独立`work_class=sandbox`物理session Job；两者分别拥有逻辑subscription恢复与microVM生命周期，不得复制session
generation、process lease或terminal authority。逻辑Job只有在exact Sandbox session进入已证明的Running/prepared状态后才能提交Ready；
Sandbox terminal或session loss必须使逻辑Job按同一generation重建，不能在未知旧进程状态下并行创建replacement。
Sandbox admission在一个事务中锁定Ready Invocation，验证其expected version和exact frozen
admission，创建共享Sandbox Job，并把Invocation推进为Deferred、`current_job_id`绑定该Job；因此等待Sandbox容量或执行时不持有
Capability Worker permit。`SandboxJobId`只是该Job的typed owner identity，必须与`JobId`使用相同UUID，不拥有第二条生命周期。
Sandbox terminal physical outcome由独立Capability owner controller依据当前Invocation、同一Job及Effect归并为逻辑outcome；
Sandbox Executor不得直接修改Invocation，也不得为归并创建第二个Job。
owner controller只消费精确terminal Event绑定并执行optimistic merge；它不得选择绝对`retry_at`或使用副本本地策略。
safe retry的相对退避在Invocation admission时冻结为`retry_backoff_milliseconds`，Sandbox request必须复制同一值，repository使用
PostgreSQL transaction clock计算`retry_at`并与剩余deadline求交。

Backend envelope包含Invocation ID、Job ID、lease generation、exact deployment refs、bounded input/Artifact grants、idempotency key、
deadline、optional callback binding和safe trace context。Endpoint、SecretBinding、network/isolation policy由adapter从Deployment解析；
Secret value仅在trusted adapter late resolve。

## 8. Dispatch outcome

```rust
enum DispatchOutcome {
    Completed(CompletedOutput),
    Deferred(RemoteWait),
    InputRequired(BackendInputRequest),
    RetryableFailure(SafeBackendFailure),
    PermanentFailure(SafeBackendFailure),
    Uncertain(SafeUncertainty),
}
```

Outcome首先以Job lease fence和JobCommit Receipt提交。Owner service随后在同一事务或可恢复的下一command中根据frozen admission
处理：Completed验证输出；Deferred保存encrypted backend state和WakeContract；InputRequired创建Task；RetryableFailure重新计算
retry intersection；Uncertain按Effect进入reconciliation。未知tag/schema fail closed。

## 9. 同步路径

```text
Invocation Ready
  -> Job Leased/Running + Invocation InFlight
  -> backend Completed
  -> validate
  -> Job Succeeded + Invocation Succeeded + owner wake
```

Invocation与Job在外部调用前durable。同步await不阻塞Tokio thread；客户端断开不取消Invocation。Terminal事务验证current lease、
output schema/size/depth、Artifact finalization/classification和Policy，写result、Job/Invocation state、Node/Model wake、Event、Outbox和
Receipt，exact replay不重复wake或Event。

## 10. Deferred与callback/poll

```text
Invocation InFlight + Job Running
  -> Invocation Deferred + Job Waiting(wake generation)
       ├─ callback
       ├─ bounded poll
       ├─ cancel
       └─ timeout/reconcile
```

- Deferred提交清除Job lease/permit，不占Worker连接；
- remote handle加密并受大小限制，不进入公共Event；
- callback binding由平台从tenant/Invocation/Job generation/Deployment/external identity派生；
- poll schedule由HardLimitProfile、bounded backend hint、attempt count和remaining budget派生；
- due scan按database clock和bounded keyset产生候选，claim重新CAS；
- callback、poll、cancel、timeout竞争同一Job state/wake generation；
- callback或poll winner推进Job generation并把Invocation恢复InFlight或terminal；继续已建立remote work时必须冻结
  `ResumePhysicalAttempt`，后续claim只增加lease generation，resume不增加`attempt_count`；
- loser使用同一Callback/Command Receipt记录稳定结果；
- current Waiting callback不能被通用Reject按钮丢弃；late reason由服务端从current Job/Invocation推导。

## 11. Retry与recovery

Retry intersection：

```text
Node retry policy
∩ Interface Effect/Idempotency
∩ Implementation conformance
∩ Security Policy
∩ remaining Invocation/Run deadline
∩ frozen attempt limit
```

同一Invocation所有generation使用相同admission、effect和idempotency key。Lease过期scanner只产生observation；owner command重新锁定
Invocation/Job并决定safe retry、cancel、timeout或reconcile。外部调用后结果未知且无safe replay proof时不能创建下一generation。
相对retry backoff是admission snapshot的一部分并受`1..=60000ms`硬边界约束；调用方、backend、Event hint和controller副本都不能提交
绝对`retry_at`。repository只在first-winner事务内用database clock派生时间，若已无剩余deadline则转为terminal failure。
通用backend的`RetryScheduled -> Ready -> Leased -> Running`必须使用`NewPhysicalAttempt`并在start时增加`attempt_count`；Sandbox
backend允许Gateway在due retry admission事务内直接执行`RetryScheduled -> Deferred`并创建下一条唯一物理Sandbox Job。两种路径都不能
复用Deferred/Input continuation disposition，也不能用新的lease generation或新Job绕过frozen attempt limit。

## 12. InputRequired与Task

BackendInputRequest只能包含closed request kind、safe prompt key、closed response schema、deadline hint和opaque bounded state。
平台Policy决定eligible principal、presentation和classification。Task response验证tenant/principal/generation/schema后唤醒同一Job。
不得索取Secret value、平台token、任意文件路径或扩大网络权限。

合法response winner把同一Job置Ready并冻结`ResumePhysicalAttempt`：后续claim重新取得lease generation，resume保留
`attempt_count`，因此`attempt_limit=1`的非幂等调用仍可完成同一次已Started物理attempt。缺少exact Task/owner/wake/state绑定时
不得退化为新attempt，必须整体拒绝或进入owner reconciliation。

Interaction Task必须冻结Invocation owner version、admission digest、Job ID与wake generation、opaque-state digest、eligible-principal
rule和response schema。Response first-winner同事务写Task terminal、exact RunValue、Job wake、Invocation state、Event/Outbox和Receipt；
任一owner/wake/schema漂移都整体回滚。

## 13. Progress

- progress绑定Invocation、Job和lease generation；
- Interface schema、size/rate和public policy先验证；
- LiveOnly可以丢弃；CoarseDurable作为bounded Event或Invocation milestone更新；
- progress不续租、不改变deadline/terminal/retry；
- stale generation progress丢弃并计数；
- stdout/stderr默认是private bounded diagnostic Artifact。

## 14. Failure与reconciliation

Raw backend error映射为safe stable Failure；长诊断使用encrypted short-retention Artifact。写Effect：before-dispatch failure可safe retry；
明确backend rejection按contract决定；dispatch后timeout/connection loss默认Uncertain。Reconciliation snapshot保存schema version、Effect、
external identity、last known Job generation、safe observations、Policy path和manual/automatic mode。Manual resolution必须通过授权command
追加Event并CAS Invocation，不能直接修改result。

## 15. Cancellation与timeout

- Run或owner cancel是durable Command Receipt + Event；
- Approval/Input/Ready/Retry且无active Effect时可直接cancel；
- InFlight/Deferred进入Cancelling，backend cancel是best effort/confirmed contract；
- backend确认停止不证明此前Effect未发生；
- write Effect在无no-effect proof时进入ReconciliationRequired；
- timeout使用database clock和immutable deadline竞争state/wake CAS；
- parent Run terminal后的callback不能覆盖Invocation，但uncertain Effect仍记录incident/reconciliation。

内部control Event payload是closed versioned合同：`schema_version=1`、`control_kind=cancel|timeout`、可空的exact
`job_id`、control winner后的`state`和可空的exact Interaction `task_id`。对仍有物理执行的backend，
`capability.cancelling`允许`control_kind=cancel|timeout`；物理终止证据归并后，`capability.cancelled`只允许`cancel`，
`capability.timed_out`只允许`timeout`；
`capability.reconciliation_required`必须保留触发它的原始control kind。Sandbox、MCP或remote adapter可以把该已提交Event
投影成精确终止信号，但不得建立第二个durable cancel current state；丢失wake由Event/Invocation safety scan重投。

## 16. ModelLoop集成

每个tool intent以`(ModelTurnId, ModelCallId)`创建一个Invocation；并行calls分别拥有身份。只有committed terminal Invocation result才能
返回Model；结果可见性与PublicRun可见性分别由Policy决定，不按tool name合并或接受Model自报completion。

## 17. 持久化边界

Capability domain只需要Invocation aggregate的current storage。Physical execution复用Job，Approval/Input复用Task，callback与
idempotency复用Receipt，历史/evidence/audit复用Event，delivery复用Outbox。Admission、backend state、result、failure和
reconciliation是versioned bounded typed snapshots；不得因新增backend/outcome/rejection reason增加Capability专用表。

## 18. 公共事件

closed event types：

```text
capability.started
capability.waiting
capability.input_required
capability.progress
capability.completed
capability.failed
capability.cancelled
capability.timed_out
```

Event payload由Interface和Agent publish Policy双重裁剪。External handle、endpoint、Secret、raw error和private diagnostics永不公开。

## 19. 可观测性

```text
capability_invocations_total{backend_class,effect,outcome}
capability_duration_seconds{backend_class,effect,outcome}
capability_waiting_active{backend_class}
capability_retry_total{backend_class,failure_class}
capability_reconciliation_active{effect}
capability_output_rejected_total{backend_class,reason}
capability_cancel_total{backend_class,outcome}
```

tenant、Capability名称、Invocation ID、endpoint和error body不进入label。

## 20. 验收标准

- 所有backend通过同一Invocation/Job state-machine fixture；
- external call前Invocation与Job durable；
- duplicate admission只产生一个Invocation；
- fast path Worker kill不会双terminal；
- Deferred清除permit且callback/poll/cancel/timeout只有一个winner；
- Deferred callback与Input response恢复同一物理attempt，不消耗新的attempt；RetryScheduled必定消耗新的attempt；
- late callback稳定Rejected但current Waiting callback不能丢弃；
- output schema/Artifact/Policy失败不进入Plan/Model；
- stale lease progress/outcome被拒绝；
- NonIdempotentWrite dispatch后断线进入reconciliation；
- Approval固定admission digest，参数变化不能复用；
- cancel/completed竞态保留Effect uncertainty；
- Model伪造completion无法成为结果；
- 新backend/outcome不新增专用持久化表；
- Secret/handle/raw error canary不出现在公共或默认observability面。

### 20.1 当前实施证据边界（非规范性）

generic Invocation admission/current-state与Capability execution domain切片已经交付：pure Rust aggregate冻结origin、slot/candidate
选择证据、exact Deployment/Interface/Implementation、input Value/Artifact reference、Effect/idempotency/cancellation、Policy、principal、
attempt limit与deadline；caller-owned PostgreSQL transaction覆盖prepare、quota bundle claim、start/resume、terminal/deferred/input outcome、
callback/poll wake、progress、control、cancellation outcome与manual reconciliation，并在同一事务提交current state、Task/RunValue/ArtifactLink、
Receipt、Event、Outbox与quota settle。

fresh PostgreSQL 16 fixture覆盖inline/Artifact input、exact replay/conflict、跨tenant/permission/candidate拒绝、审批first-winner、同步完成、
coarse progress且不续租、stale progress全事务回滚、Deferred释放permit/quota、callback/poll并发wake只有一个Event/Outbox winner、
`attempt_limit=1`下callback与Input response两次resume仍保持`attempt_count=1`、错误Task rule不留Receipt、uncertain write进入manual
reconciliation、cancel/completed并发first-winner以及全部quota归零。Text2SQL typed input还在同一事务证明exact
`database.query.readonly` Interface/Deployment/ReadOnly Effect与committed SqlCatalog Observation未漂移；错误引用不留下Invocation或Receipt。
pure Job fixture同时证明RetryScheduled start消耗新的物理attempt。

Phase 4的Capability Worker组合现也已交付：claim事务返回exact ExecutionContract/Input与Job fence，组合层重验Running state、
Invocation/Job/attempt/lease/Worker identity后生成credential-free adapter request；process-installed dispatcher执行adapter，结果只经
`CapabilityExecutionAuthority`提交同一fenced PostgreSQL transaction。NonIdempotentWrite的dispatch后失败不能被transport标成安全重试，
attempt耗尽也不能留下不可调度的RetryScheduled。reserve与settle quota ledger identity分离并在adapter I/O前拒绝复用。
durable control后，组合层用原claim与更新后的Cancelling Invocation/Job重新验证完整物理身份，只旋转Job version fence；Native/HTTP/gRPC
取消同一执行，transport observation从不充当no-effect proof。write Effect进入ReconciliationRequired；原execution deadline后仍可在
frozen backend timeout派生、平台hard limit封顶的cleanup window提交同一worker/lease/token fence。
12项adapter/worker unit、8项Invocation unit、29项Egress unit与fresh PostgreSQL 16端到端fixture实际通过，覆盖Native dispatch/cancel、
terminal/cancellation commit、Receipt/Event/Outbox、quota settle、幂等replay和cancel/completed first-winner；Egress其中8项覆盖Capability
HTTP/gRPC exact catalog、DNS/Secret、bounded framing/response、Effect/idempotency failure和stale exact cancel。

上述证据关闭Phase 3的Capability synchronous/deferred/input/progress/reconcile domain交付项并推进Phase 4，但不替代HTTP/gRPC/MCP
真实远端服务、Secret Manager/TLS/mTLS、callback ingress与部署资格；也不覆盖公共`/v1`、production topology、
capacity/soak/DR，因此本规范仍是Implementation In Progress而非Implemented/Verified。

## 21. 明确推迟

- server-to-client streaming final Value；
- batch Capability API；
- cross-Deployment failover；
- generic compensation；
- public manual reconciliation UI；
- backend-specific public wire extensions。

## 22. 未决问题

没有阻止下游cross-review的问题。Backend-specific transport只能由新版本typed adapter映射到本规范closed envelope/outcome，不能
通过generic extension bag改变Invocation状态机。
