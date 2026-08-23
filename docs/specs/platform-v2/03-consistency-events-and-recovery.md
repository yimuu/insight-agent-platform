# Platform v2 一致性、Event 与 Recovery 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-176 |
| 日期 | 2026-08-20 |
| 依赖 | 01、02 |
| 直接下游 | 04～18 |

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

Sandbox execution只有shared Job owner/fence，无SandboxJob ID/aggregate。MCP首版无stdio session child。Operation无owner variant；
它直接投影Job的typed owner。

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

reconciliation冻结exact owner/Job generation、backend idempotency/correlation identity、known evidence、deadline/budget和closed decision policy。
只能返回`ConfirmedSucceeded | ConfirmedFailed | StillUnknown | RetryableProbeFailure`。不确定保持Unknown/Reconciling并交由人/运维
处置，不伪造terminal success。

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
- external timeout按Effect进入retry/reconcile/Unknown，不伪造安全failure；
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
