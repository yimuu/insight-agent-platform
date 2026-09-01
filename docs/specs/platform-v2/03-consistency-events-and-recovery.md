# Platform v2 一致性、Event 与 Recovery 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 revision 3 |
| 日期 | 2026-09-02 |
| 依赖 | 01、02 |
| 直接下游 | 04～18 |

> CR-216 revision 1 impact：Sandbox create 与 Package activation 拆开。shared Job 保存 stable provisioning token、selected candidate、
> runner boot/activation 与 cleanup evidence；create 只生成 bounded inert candidates，PostgreSQL current-fence CAS 选唯一 candidate，
> fixed runner durable latch 最多启动一次 Package。OpenSandbox/Kubernetes state 不复制 Job state，terminal 仍以 current lease fence first-winner。

> CR-216 revision 2 closure：Job JSON 不复制 input/result body；exact immutable input RunValue 在 claim/recovery 时重建 request，terminal
> transaction 原子写 output RunValue。terminal 后业务 lease 已清除，Dispatcher 只能用 shared Job physical evidence 内独立、可过期、
> generation-fenced 的 cleanup claim 写 delete/absence evidence；该 CAS 不得改变任何 terminal business fact。

> CR-216 revision 3 closure：只有 owner payload 已证明同一 external physical attempt 可安全 observe/replay 时，expired
> `Running` Job 才可 `Running -> Ready` 并由新 lease continuation claim；转换保持 `attempt_count`，且不能 dispatch 新 effect。

> CR-206 impact：public Operation result是shared Job terminal safe result的closed typed projection。普通成功Job返回
> `digest`；ContextDatasetBuild成功返回`context_dataset_generation`并从同一frozen Job payload投影exact `dgen`与terminal
> result digest。projection不读取mutable Dataset head，不建立第二份result authority，非该kind不得携带generation identity。

> CR-198 impact：一个业务命令可拥有多个有顺序依赖的shared Job，但每个Job只有一个current-state owner。MCP discovery owner Job与其
> 预分配的`ArtifactScan`验证Job以typed payload互相引用；stage/verify完成只通过committed Event/Outbox wake恢复owner。最终owner事务必须
> 同时重验两个latest fence、Artifact/Blob verification evidence和quota closure，再原子创建业务结果并结算；RPC completion、内存future或
> message delivery均不能代替该事务，也不产生新的aggregate或当前状态投影。

> CR-202 impact：`RegistryValidation` success/failure使用shared Job的同一lease/fence和JobCommit Receipt；不能先generic
> `commit_job`再分开写Resource，也不能先写`ValidationSummary`后假定Job最终会终结。Resource validation、Job terminal、
> Event、Outbox与Receipt必须同一事务first-winner；崩溃、重复wake、过期lease、Draft CAS失败或validator/profile drift均不得留下
> “已验证但Operation仍运行”或“Operation成功但Draft未验证”的可见中间态。

> CR-197 impact：定义恢复安全的trace identity。Run admission或非Run command admission拥有一个`TraceIdentityV1`；由其创建的Job、Task、
> Event和Outbox snapshot复制同一trace ID。lease/attempt/Worker变化不改变trace ID，每个实际执行/RPC hop只生成新的span ID。trace字段不进入
> request/idempotency/payload/result/effect digest，不参与CAS、fence、first-winner或业务索引唯一性。

### Durable trace identity

```rust
struct TraceIdentityV1 {
    schema_version: u32, // exact 1
    trace_id: TraceId,   // 32 lowercase hex, 16 bytes, not all zero
}
```

首版W3C parent只接受exact `00-{trace_id}-{parent_span_id}-{flags}`：trace ID为32位小写hex且非全零，span ID为16位小写hex且非全零，
flags只能是`00|01`。`tracestate`和`baggage`不进入平台内部合同。公共入口缺少parent时生成新trace；格式错误时以`invalid_request`拒绝。
内部mTLS RPC必须携带合法parent；receiver只把它用于correlation并生成child span，不从中读取tenant、principal、Run、Job或其他业务identity。
durable recovery从owner snapshot恢复同一trace ID并生成新span，绝不复用已终止process的span ID。

## 1. 决策摘要

PostgreSQL是业务current state、tenant scope、CAS、lease、Receipt、Event和Outbox的交易权威。NATS只运输
wake hint或committed outbox message。外部I/O使用“prepare -> fenced attempt -> evidence -> transactional commit/reconcile”，
不在长事务中执行网络/对象存储/代码。

一个业务事实只有一个current-state authority。Event是append-only history，Job是physical work authority，
Operation是Job的public projection，不建立平行current aggregate。

## 2. Shared aggregates

| Shared concept | 拥有 | 不拥有 |
|---|---|---|
| Resource/Version/Deployment | definition、immutable version、active binding | Run execution |
| Run/Invocation | business execution state/result | physical lease/retry |
| Job | Ready/Leased/Running/Waiting/Retry/terminal、attempt、lease | owner business result |
| Task | approval/input/human current state | owner execution state |
| Receipt | command/callback idempotency result | aggregate current state |
| Event | transition/outcome/rejection/audit history | current projection |
| Outbox | committed delivery intent/status | business state |
| Artifact domain | metadata/blob/link/grant/storage lifecycle | Run/Invocation current state |

不建立transition/evidence/rejection/outcome/release/operation专用表。低频detail使用bounded typed JSONB或Artifact。

## 3. Tenant scope

所有业务aggregate、Receipt、Event和Outbox都包含non-null `tenant_id`。没有installation-scoped business row、fake
tenant或nullable authority scope。平台运维/发布审计存在GitOps/CI/Kubernetes日志，不与租户业务Event流混合。

repository查询/mutation同时使用tenant + nominal object ID，不先按object ID查找再在应用层补tenant check。
FK/unique/index在可行时包含tenant维度，跨tenant reference在DB或repository typed verifier层fail closed。

## 4. Typed owner registry

```rust
enum TypedOwnerRef {
    Resource(ResourceId),
    ResourceVersion(ResourceVersionId),
    Deployment(DeploymentId),
    Run(RunId),
    NodeExecution(NodeExecutionId),
    Invocation(InvocationId),
    Job(JobId),
    Task(TaskId),
    RunValue(RunValueId),
    Artifact(ArtifactId),
    Blob(BlobId),
}
```

JobKind、ReceiptKind、EventKind、ArtifactRelation与owner-kind pair来自一个closed machine registry。每次创建/解析必须验证
nominal ID prefix、tenant、source row kind/state和合法pair。不使用generic `(owner_type: String, owner_id: UUID)`作为信任边界。
Job可作为Artifact/Receipt等关联的typed owner，但首版Job kind-owner registry不允许Job拥有child Job。

r328实现上述既有合同的internal nominal `JobKind` machine registry：18个closed kind及25个合法
`JobKind × WorkClass × OwnerKind`三元组由Rust单一owner生成到`registries.json`，每个三元组必须投影到既有execution work-owner pair。
该批只闭合上游machine contract；baseline `jobs.job_kind`、repository读写和JSON hot-predicate替换仍待下一实现批次，不能将r328视为持久化完成证据。

r329把该合同落实到clean baseline `jobs.job_kind`与全部repository读写。创建和读取均重验closed三元组；Artifact/Context claim的lane选择只读typed
relational kind，JSONB只保留冻结闭包与低频证据。managed MCP physical session继续复用共享Job，合法owner为其logical Job ID，且以
`SandboxManagedMcpSession + Sandbox + Job`与Capability Sandbox execution区分，不再发明`sandbox_job` owner kind。独立schema checker逐个扫描
production/test Job INSERT并拒绝JSON kind热谓词或未注册SQL owner。该证据闭合仓库内持久化合同，但没有替代fresh PostgreSQL concurrency/recovery gate。

`WorkClass::Context`的合法owner为`ContextQuery`、`ContextDataset`，以及仅用于MCP Resource subscription refresh/reconcile的
`McpOperation`。最后一种pair必须由Context application owner transaction重载同tenant `invocation_kind=mcp_subscription` row、exact
Deployment/Discovery/authorization/session/event evidence后创建；MCP Host不能凭该pair直接创建Job或执行Context backend。它复用已有
subscription aggregate identity，不产生新的Context current-state aggregate。

当前实施证据：r269已把该pair写入Rust closed machine authority、generated registry/root manifest与独立合同checker，并以正反unit fixture
证明只开放目标方向。PostgreSQL source-row/payload验证和跨WorkClass claim仍待后续L2/L3证据。

Sandbox execution只有shared Job owner/fence，无SandboxJob ID/aggregate。Job的bounded physical evidence可以保存
`provisioning_token_digest + selected_sandbox_id + physical_attempt + runner_boot_id + activation_state + cleanup evidence`，
但不得复制OpenSandbox/Kubernetes lifecycle state。MCP首版无stdio
session child。Operation无owner variant；它直接投影Job的typed owner。

## 5. Optimistic concurrency 与fence

所有current aggregate有从1开始、单调递增的`projection_version`。mutation必须携带expected version，SQL update
必须在where中同时复核tenant、ID、state和version。零行更新后重读只judgment为not found/forbidden/conflict/
already applied，不盲目重试业务command。

Job还有`lease_generation`、`lease_expires_at`、`worker_process_generation_id`和opaque lease token digest。start/heartbeat/
terminal必须用数据库时间复核current lease。旧generation/process/token/version任一不匹配都不能commit。

ArtifactLink保存的owner projection version只是create-time CAS evidence，不是持久授权fence。read不与owner current
version比较；release command另携带owner current expected version和Link expected version。

## 6. 事务边界与锁序

一个command的默认锁序：

```text
Receipt
  -> Tenant / Principal / Policy / Quota authority
  -> typed parent aggregate
  -> Job / Task / Artifact child aggregate
  -> quota reservation / link / grant
  -> Event
  -> Outbox
  -> Receipt completion
```

同级对象按nominal ID canonical bytes排序。所有锁序在repository helper中一次定义，不由每个handler自由选择。
遇serialization/deadlock只在command的Effect/idempotency合同允许时做bounded事务级retry，不重复external I/O。

必须同事务的关键闭包：

- Resource active switch + Event/Outbox + Receipt；
- Run admission + frozen bindings + initial Node/Job + quota + Event/Outbox + Receipt；
- expression controller commit + immutable RunValue rows + current Scope data-environment CAS + Node/Job + quota + Event/Outbox + Receipt；
- parent/child Run link + child frozen bindings + initial work；
- Job terminal + owner terminal/wake + quota settle + Event/Outbox；
- Task terminal + owner wake + Receipt/Event/Outbox；
- Artifact Ready + Link/RunValue + owner terminal + Job/quota/Event/Outbox；
- Artifact delete/quarantine commit + link/grant/quota/Event/Outbox。

## 7. Job state 与public Operation projection

```rust
enum JobState {
    Ready,
    Leased,
    Running,
    Waiting,
    RetryScheduled,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    UnknownOutcome,
}
```

Job保存kind、typed owner、WorkClass、immutable binding snapshot、state、attempt count/limit、lease、retry_at、deadline、
bounded progress、terminal safe result/failure和projection version。typed owner保存`current_job_id`只作为当前工作指针，不复制
Job state。新generation/retry/reconcile在owner + Job事务中更新。

public `OperationView`直接从Job的safe fields投影，`operation_id == job_id`。它没有独立state/version/table。
public kind/target只是Job kind-owner registry的安全子集，非法pair不能创建或投影。

## 8. Receipt

Receipt key是`tenant + principal/workload + operation scope + idempotency key`，并保存request schema ID/version/digest、
owner-generated typed result、status class、ETag/Location projection inputs、claimed/completed time和expiry。

claim结果：

- 同key + 同digest + completed：从stored typed result重建原response；
- 同key + 异digest：`idempotency_conflict`；
- 同key + in-progress：bounded wait/poll或stable conflict，不并发执行；
- expired abandoned claim：只有在证明没有committed owner effect后才reclaim；
- callback/poll/approval使用current owner/Job/Task fence做first-winner classification。

Receipt schema/registry从owner types生成。不为普通validation result、CI gate或release状态创建长期Receipt。

## 9. Event 与Outbox

Event是immutable tenant-scoped envelope，包含EventId、typed event kind/version、aggregate kind/ID/version、sequence、occurred/committed
time、principal/workload evidence、trace ID、bounded typed payload和payload digest。不包含Secret/token、prompt/body、object locator或raw error。

Outbox与Event/业务mutation同事务创建，包含topic kind、EventId、tenant、partition key、attempt/next delivery、
lease和delivery state。publisher少读有界claim并至少一次发送，consumer以EventId/Receipt幂等。NATS ack不改写业务state。

Event retention和Outbox delivery retention不同。Outbox terminal后可按policy回收，Event按审计retention保留；两者都不存大正文。

## 10. Retry、cancel 与reconciliation

retry只由owner Effect、idempotency、failure class、attempt budget、deadline和published policy决定，并将`retry_at`持久化。
Worker不内存sleep。cancel/timeout先写durable intent，物理cancel是best effort，不把unknown external effect改写为“未发生”。

Sandbox 是更严格的 specialization：同一 `SandboxProvisioningTokenV1` 可以发现 bounded inert candidates，但 current Job CAS 只能
选择一个。Dispatcher 必须在外部 activate 前持久化 `ActivationAuthorized/PotentiallyStarted`；此后 response loss、crash 或 lease loss
只能查询或重放相同 sandbox/boot/activation token，不能创建新 token/candidate/sandbox/physical attempt。boot identity 变化且无完整 result
时进入 `UnknownOutcome` 并执行 absence reconcile。这个规则防止 Platform 制造重复 Package 启动，但不声称 workload 内部网络、数据库、
消息或第三方 API 具备幂等或 exactly-once。

该 durable external continuation 的 lease 恢复是 shared Job 状态机的窄转换：旧 `Running` lease 到期后，owner transaction 必须先验证
payload 已有 exact provisioning token/physical attempt 且 recovery decision 只会 observe 或 replay 同一 selected activation，才可将 Job
变为 `Ready`；随后 continuation claim 增加 lease generation、保持 attempt count。普通 Running Job、没有 physical evidence 的 Sandbox
Job 或任何会创建新 token/candidate/sandbox/Package start 的路径不得使用此转换。

`Context -> McpOperation` subscription refresh Job固定为ReadOnly physical attempt。Context owner以Job lease/fence调用协议adapter；
成功只允许保存closed terminal evidence（request/response/resource digest、item/byte count、observed time），不把remote body、session或
Secret写入Job/Event/Receipt，也不声称创建Context Observation、dataset generation或cache。owner按`JobCommit` Receipt原子提交
Job terminal、quota settlement、Event和Outbox；stale fence或字段漂移零写入。dispatch前validation/authorization失败为terminal failure，
可恢复dependency/capacity失败进入bounded retry；响应是否到达不改变ReadOnly属性，post-dispatch uncertain可作为新attempt安全重读并保留
attempt evidence。deadline/cancel/lease recovery继续使用同一Job generation/fence规则。

subscription refresh的remote evidence绑定不可变`execution_identity_digest`：tenant/subscription/Job、worker process generation、
lease generation/token、physical attempt与exact request closure。它明确排除heartbeat推进的Job `expected_version`、lease expiry和时间戳。
Host在dispatch前用收到的完整fence重验current running Job；Context Worker每次成功heartbeat后更新本地owner commit fence，但不改变已派发的
execution identity。最终JobCommit Receipt摘要最新commit attempt与response，response evidence仍验证同一不可变物理attempt。

reconciliation冻结exact owner/Job generation、backend idempotency/correlation identity、known evidence、deadline/budget和closed decision policy。
只能返回`ConfirmedSucceeded | ConfirmedFailed | StillUnknown | RetryableProbeFailure`。不确定保持Unknown/Reconciling并交由人/运维
处置，不伪造terminal success。

OpenSandbox cleanup reconcile只能按已持久化 selected sandbox ID或opaque Job/provisioning metadata做bounded observe/delete/absence proof。
`list(metadata)`只用于发现 inert candidate 和 orphan；唯一 selection 来自 PostgreSQL CAS。orphan sweeper删除physical sandbox前必须确认
对应 Job 的 selected/activation/terminal/cleanup evidence，
且它只改变provider physical state，不直接推进Run/Invocation。

业务 terminal transaction 清除标准 Job lease，并在同一 Job payload 原子写入 `cleanup_required + cleanup_generation`。Dispatcher 或
orphan sweeper必须先以 `FOR UPDATE SKIP LOCKED` 和数据库时间取得 `SandboxCleanupFenceV1(expected_job_version,
cleanup_generation, process_generation, expires_at)`；每个 delete/absence observation 都重新验证该 fence 并递增 Job version。过期 fence
只能由一个新 generation 接管；absence 完成后清除 cleanup owner/expiry。cleanup CAS 不可修改 Job state/result/terminal time、Invocation、
RunValue、quota、Event、Outbox 或 Receipt，因而不是第二业务 authority。

## 11. Recovery scan

safety scan按WorkClass/对象种类使用有界batch、shard和high-water cursor，只读候选。实际mutation事务重新
复核tenant、ID、state/version、lease/deadline、owner、cancel/timeout intent和budget。同一expired generation的多个recovery worker
只有一个winner。

最低扫描类别：expired Job lease、due retry/timer、stuck Receipt、undelivered Outbox、expired Task、Artifact staging/delete/orphan、
MCP callback/subscription和Sandbox process cleanup。不为每个扫描类别创建新current-state表。

## 12. Error classification

stable顶层类别：`Validation | Authorization | Conflict | Capacity | Dependency | Timeout | Cancelled | UnsafeEffect |
UnknownOutcome | InternalInvariant`。retryable是由owner policy决定的属性，不等于HTTP 5xx。raw SQL/provider/runtime error
映射为stable code + safe bounded diagnostic，敏感detail只进受控diagnostic Artifact。

## 13. 验收标准

- 并发CAS/Receipt/Job claim/Task terminal/Artifact Ready只有一个winner；
- 旧aggregate/lease/process/token fence全部fail closed；
- 所有Receipt/Event/Outbox为tenant-scoped，无installation/fake tenant scope；
- Operation与Job共享ID/state/version，不存在并行aggregate；
- NATS全丢/重复时Outbox/safety scan恢复并且consumer幂等；
- external timeout按Effect进入retry/reconcile/Unknown，不伪造安全failure；Sandbox exec可能已开始时不自动重发；
- ArtifactLink在owner正常version推进后仍有效，release使用current owner fence；
- transaction kill/deadlock/serialization fixture不留半成品owner/Event/Outbox/Receipt；
- JSONB/registry/codegen/conformance与size limits全部fail closed。

`run_values`行拥有immutable值identity/schema/content/storage；Scope aggregate的bounded typed JSONB只保存
`ExactDataPortRef -> ExactRunValueRef` current binding。不得在RunValue行增加可变“current port”列，也不得以
`run_nodes.output_value_id`表达多port环境。Inline JSONB结构guard必须至少容纳HardLimitProfile的
`run_scheduler.inline_value_bytes.hard_max`且仍由Rust按effective Q1/deployment值先行收紧；两者漂移时startup/schema qualification fail closed。

## 14. 明确推迟

- 跨region consensus/event replication；
- 外部副作用exactly-once；
- 为每个domain建独立event store/projection database。

## 15. 未决问题

首版一致性、Event/Receipt/Outbox和recovery合同无未决设计问题。
