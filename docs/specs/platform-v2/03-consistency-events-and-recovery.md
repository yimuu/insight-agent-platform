# Platform v2 数据一致性、事件与恢复规范

| 属性 | 值 |
|---|---|
| 状态 | Draft / Architecture Revision |
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
| Installation current Release/Candidate | PostgreSQL aggregate | 18唯一installation-scoped current authority |
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

tenant aggregate与唯一installation aggregate共享同一版本/CAS语义，但scope必须是closed union，不能用nullable tenant、fake tenant或
字符串kind让任意domain取得installation权限：

```rust
enum AggregateScope {
    Tenant { tenant_id: TenantId },
    Installation { installation_id: InstallationId },
}

struct AggregateHeader {
    scope: AggregateScope,
    aggregate_id: ResourceId,
    state: ClosedState,
    version: u64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}
```

`Installation`只允许18的`InstallationReleaseStateV1`及本节明确列出的installation command/event/receipt；其aggregate ID必须是
02 `InstallationId`。其他current aggregate、Job、Task、Quota、Run和Invocation仍必须是tenant-scoped。加入新的installation-scoped
owner必须先修改本规范、ADR和closed machine registry，不能仅把数据库列改成nullable。

每个command只由一个application service拥有；它可以在同一事务组合多个aggregate，例如Invocation terminal同时唤醒Node，
但Worker不能拼接任意SQL。每次mutation必须携带expected version/state，成功推进version恰好一次。

一个事实只保存在一个current owner：

- Run保存Run状态、control intent、bindings snapshot和next public sequence；
- Node保存节点状态和bounded local execution data；
- Invocation保存逻辑调用状态、frozen admission、exact current Job pointer和result/failure；pointer只拥有当前关系，不复制Job state；
- Job保存物理state/attempt、lease、remote wait、retry、terminal winner和backend state；
- Task保存approval/interaction/human work；
- Receipt保存去重key与稳定结果；
- Event保存不可变历史；
- Outbox保存尚待或已经发送的Event envelope。
- InstallationReleaseState保存唯一current Release/Candidate binding、active Model Deployment计数与compatibility generation；
  Candidate/Release document、Event或进程cache不能成为第二current pointer。

Projection或generated column只能是读取优化，不能拥有独立写API。

`Job.owner`是创建后immutable的typed back-reference，不能单独推导“current”；typed父aggregate的`current_job_id`是当前关系的唯一authority。
Job创建/替换/清除必须与父pointer及业务state在同一事务提交：terminal Job在owner尚未归并前仍可保持current，归并后才清空或原子切到下一
Job。父aggregate不得复制Job state、lease、attempt、retry time或terminal result，Job也不得改写父业务state/Effect；任何pointer/back-reference
不匹配都按invariant failure整体回滚。

当前machine registry的closed owner-to-pointer mapping固定为：`run -> Run`、`node_execution -> RunNode`、`interaction -> Task`、
`internal_blob -> ArtifactBlob`、`job -> parent Job.current_child_job_id`；`management_operation | model_turn | capability_invocation | context_query | mcp_operation`
均映射到其唯一shared Invocation owner。各owner的现有row或bounded typed
payload必须持有exact `current_job_id`，不新增pointer表。create/switch必须同时写Job与pointer并验证immutable back-reference；pointer未清除或未按
owner closed state消费前禁止创建另一current Job，live-owner unique只作结构防线。Job terminal不自动清pointer；owner merge/consume事务才可清除
或替换。新增owner pair必须同时扩展该mapping、07 machine registry、ADR映射与并发fixture，不能退回按Job查询猜current。

## 5. 事务边界

每次authoritative aggregate mutation和terminal success winner都在一个caller-owned PostgreSQL transaction/savepoint中：

1. 解析closed authority scope与PrincipalSnapshot；tenant command解析tenant，installation command只接受installation principal；
2. claim或读取Receipt；
3. 按共享lock order锁定current aggregates；
4. 验证expected version/state、Policy与typed snapshot；
5. 修改aggregate；
6. 追加Event；
7. 必要时追加Outbox；
8. terminalize Receipt；
9. commit。

普通短command在这一个事务内完成Receipt claim到terminal；03明确注册的可恢复长preflight可先用独立短事务提交Processing Receipt
lease/capture，但最终业务mutation、success Receipt、Event与Outbox仍必须在同一个final winner事务提交，不能出现“状态已变但Receipt未终结”窗口。
确定性rejection可在不修改业务aggregate/Event的短事务terminalize同一Receipt。

Principal/Command Receipt要求的唯一例外是04首个installation operator部署期bootstrap：尚无Principal时不能伪造`PrincipalSnapshot`或Command Receipt。它只接受server-side
configured `InstallationId`，以bootstrap advisory lock + `RequestId` +完整输入digest执行exact replay，并在同一事务写
`installation.bootstrap` installation-scoped audit Event；Gateway/Worker不可调用。bootstrap完成后所有installation command都必须使用上述模板。

任一后置CAS失败必须回滚本command的全部修改。Repository不替caller提交外层事务。外部I/O、用户代码、容器启动、NATS ack、
无界正文解析均在事务外。

共享lock rank：Receipt；InstallationReleaseState；Tenant security aggregate；tenant quota/scheduler state；parent aggregate；按kind/id排序的
child aggregate；Job/Task；Artifact；Run event sequence；最后append Event/Outbox。一次命令的同rank对象先排序再锁；不得边遍历用户输入边
加锁。读取04 encryption-domain current fence的Artifact/Model command必须在quota、Run/Model/Job和Artifact之前锁Tenant并在锁后复验；
rebind/revoke使用同一rank，因此不能与terminal Ready提交交叉穿透。

影响Model Deployment bindability的activate/deactivate/suspend/resume/archive/retire，以及04任一Tenant encryption-domain
add/rebind/revoke，都必须先锁InstallationReleaseState，再按rank锁Tenant security与目标Resource/Deployment。Model active-set mutation在同一
事务更新count和compatibility generation；encryption-domain mutation保持count不变但同样推进compatibility generation/state digest，使锁外
Release preflight与并发root admission稳定失效。Release preflight scan不能持有该锁。root Run admission按同一顺序先锁installation再锁
Tenant/Resource并在提交前复验；child Run只消费parent冻结的历史binding并重验current security fence。由此不会形成tenant Resource/Tenant
先锁、全局installation后锁的反向死锁。

## 6. 统一Job

Job表示一个逻辑后台工作及其物理generation，不为Attempt、RemoteTask、poll或recovery建立第二current row：

```rust
const MAX_VERSIONED_SNAPSHOT_CANONICAL_BYTES: u32 = 262_144;

struct ClosedPayloadSchemaId(String);
struct ContractRelativePath(String);

struct VersionedSnapshot {
    schema_id: ClosedPayloadSchemaId,
    schema_version: u32,
    canonical_payload: BoundedBytes,
    canonical_payload_digest: Digest,
}

enum JobKind {
    RegistryValidation,
    Orchestration,
    Model,
    Capability,
    Context,
    Mcp,
    Sandbox,
    Interaction,
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

enum TypedOwnerKind {
    ManagementOperation,
    NodeExecution,
    ModelTurn,
    CapabilityInvocation,
    McpOperation,
    ContextQuery,
    Job,
    Interaction,
    InternalBlob,
    Run,
}

#[serde(tag = "owner_kind", rename_all = "snake_case", deny_unknown_fields)]
enum TypedOwnerRef {
    ManagementOperation { operation_id: OperationId },
    NodeExecution { node_execution_id: NodeExecutionId },
    ModelTurn { model_turn_id: ModelTurnId },
    CapabilityInvocation { invocation_id: InvocationId },
    McpOperation { mcp_operation_id: McpOperationId },
    ContextQuery { context_query_id: ContextQueryId },
    Job { parent_job_id: JobId },
    Interaction { interaction_id: InteractionId },
    InternalBlob { blob_id: InternalBlobId },
    Run { run_id: RunId },
}

struct Job {
    header: AggregateHeader,
    kind: JobKind,
    work_class: WorkClass,
    owner: TypedOwnerRef,
    current_child_job_id: Option<JobId>,
    binding_snapshot: VersionedSnapshot,
    current_attempt_snapshot: Option<VersionedSnapshot>,
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

#[serde(tag = "binding_use", rename_all = "snake_case", deny_unknown_fields)]
enum BindingSnapshotUseV1 {
    Job {
        job_kind: JobKind,
        owner_kind: TypedOwnerKind,
        work_class: WorkClass,
    },
    JobAttempt {
        job_kind: JobKind,
        owner_kind: TypedOwnerKind,
        work_class: WorkClass,
    },
    ManagementOperation {
        operation_kind: ClosedBindingOperationKind,
        target_kind: ClosedBindingTargetKind,
    },
}

struct ClosedBindingOperationKind(String);
struct ClosedBindingTargetKind(String);

struct BindingSnapshotContractV1 {
    schema_version: u32, // const 1
    binding_use: BindingSnapshotUseV1,
    payload_schema_id: ClosedPayloadSchemaId,
    payload_schema_version: u32,
    payload_schema_path: ContractRelativePath,
    payload_canonical_bytes_maximum: u32,
}
```

`ClosedPayloadSchemaId`、`ClosedBindingOperationKind`与`ClosedBindingTargetKind`均为1～128 ASCII bytes并匹配
`^[a-z][a-z0-9_.-]{0,127}$`；`ContractRelativePath`为1～512 ASCII bytes、
使用`/`分隔且不得含absolute prefix、empty/`.`/`..` segment、反斜线、query或fragment；`schema_version`为正。
`canonical_payload`必须是strict canonical JCS object、非空且不超过上述hard max，digest exact为
`SHA-256(canonical_payload bytes)`；payload自身必须含exact同值`schema_version`，unknown/null/重复key、非canonical bytes、版本或digest漂移全部拒绝。
`VersionedSnapshot`只是bounded immutable envelope，不授权开放JSON：根machine registry按完整tagged `BindingSnapshotUseV1`值唯一映射到
schema ID/version和closed validator；`Job`、`JobAttempt`即使三元组相同也是两个不同registry key，不能丢弃`binding_use` discriminator后碰撞；
`ManagementOperation`同样以完整variant值为key。缺失、重复、错误pair或未消费payload字段fail closed。Job/Operation repository保存完整bytes与digest，
reader必须先按registry验证再投影typed struct，不能只信digest。首批Artifact verify Operation映射到15
`ArtifactVerifyOperationBindingV1`；Registry/Capability/Context/MCP ordinary output的current attempt映射到15 `ArtifactStageAttemptBindingV1`；
Sandbox current attempt统一映射到14 `SandboxJobAttemptBindingV1`，需要Artifact output时由其closed variant嵌入15 stage binding；
`JobKind::Model + ModelTurn + WorkClass::Model`的每个current attempt必须映射到16 `ModelJobAttemptBindingV1`，其`InlineOnly`与
`ArtifactCapable`是同一closed schema的两个variant。其他Job/Operation payload仍各由其owner规范定义，但都使用这一唯一envelope。
binding registry与schema目标路径固定为`contracts/platform-v1/bindings/snapshot-contracts.json`和
`contracts/platform-v1/schemas/bindings/snapshot-contracts.schema.json`。entry按`binding_use` tag及其closed enum ordinal/ID raw bytes严格排序，
use tuple与payload schema ID均唯一；schema version/byte maximum为正且maximum不超过全局snapshot hard max，relative schema path必须位于root
contract tree并由manifest固定raw SHA/length。目标`registries.json.execution_work_owner_pairs`必须原子增加`job_kind`并与全部Job entry的三元组
exact相等；Operation entry则与17 kind↔target registry exact相等。任一漏项、额外项、重复、path/digest漂移或validator未消费字段使Candidate/server
启动失败。上述目标文件与扩展当前尚未checked in，完成前不能把此Draft envelope声明为当前machine behavior。

`TypedOwnerRef`只允许上述tag与对应nominal ID/prefix，variant字段不可交换，并由tag唯一投影同名`TypedOwnerKind`；JobKind、07 WorkClass与owner kind三元组必须存在于唯一
`execution_work_owner_pairs` machine registry，尤其Capability Native/Remote不能只靠JobKind区分。Binding snapshot冻结Deployment、idempotency/effect key、policy和
hard-limit digest。Attempt history进入Event，current lease只在Job保存一次。

`TypedOwnerRef::Job`只用于13/14 Managed MCP subscription的`Mcp`父Job拥有一个`Sandbox`物理session child；普通Sandbox Capability或managed-stdio
Tool的Sandbox Job owner exact为`CapabilityInvocation`。只有注册的parent/child kind+work-class组合允许
`current_child_job_id=Some`，parent pointer与child immutable owner必须同事务双向建立/切换/清除；其他Job一律为None。child不得再拥有child，parent不得
指向自身/ancestor，深度exact为1；同一parent最多一个未消费child。目标registry以`JobKind::Sandbox + owner=Job + WorkClass::Sandbox`表达该例外，
不再存在`sandbox_job` owner kind、`SandboxJobId`或同UUID typed alias。当前checked-in registry中的`sandbox_job/sjob`将在clean-cut contract slice删除，
完成前不得宣称新mapping是当前行为。

`current_attempt_snapshot`是唯一current physical-attempt payload，不建立Attempt row。它只冻结跨合法continuation稳定的attempt identity、
admission binding、预分配资源与恢复合同，不得包含`lease_generation`、lease token/digest、Worker process generation或其他每次claim都会旋转的fence；
这些volatile事实只保存在Job current lease并在每次外部I/O request中显式提交和重验。未start的Job为None；`NewPhysicalAttempt` start在递增
`attempt_count`与提交Running fence的同一事务安装一份新snapshot，`ResumePhysicalAttempt`必须逐字节复用Some且不得替换。Waiting continuation保留；
RetryScheduled只有在旧attempt已用Receipt/Event保存terminal/retry disposition且所有可能object写入已有cleanup owner后才可清除，并由下一次new start安装
不同digest/identity；logical terminal事务清除。claim/heartbeat不修改snapshot，stale lease不能清除或替换；recovery按registry重新验证完整bytes，不能从
digest、backend object或环境重建。

`ResumePhysicalAttempt`只恢复owner明确声明可继续的logical continuation；它取得新的lease/Worker fence，但复用同一snapshot中的稳定attempt identity。
任何已经向不支持durable continuation的外部stream提交request header、可能继续写对象或可能产生新的付费dispatch的执行，在lease loss后都不得以Resume
复用旧I/O session；owner必须先用Receipt/Event与cleanup owner收敛不确定side effect，再走`NewPhysicalAttempt`。需要跨Waiting继续的Provider handle、
cursor或stream recovery state只存入Job的bounded encrypted `backend_state`，按snapshot内closed recovery contract验证；它不是snapshot字段，也不能替换
current lease fence。

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

Remote callback、bounded poll、signal、child completion或retry deadline使用Job中的唯一WakeContract；human response只使用Task：

```rust
struct WakeContract {
    state: WakeState,
    generation: u64,
    deadline: DateTime<Utc>,
    payload: WakeContractPayload,
}

enum WakeState {
    Pending,
    Consumed,
    Cancelled,
    TimedOut,
}

#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WakeContractPayload {
    Timer {
        fire_at: DateTime<Utc>,
    },
    Signal {
        accepted_sources: WakeSourceSet,
        expected_response_schema_digest: Digest,
    },
    RemoteInvocation {
        accepted_sources: WakeSourceSet,
        next_poll_at: Option<DateTime<Utc>>,
        poll_count: u32,
        poll_limit: u32,
        callback_binding_digest: Option<Digest>,
        encrypted_opaque_state_digest: Digest,
    },
    ChildRun {
        child_run_id: RunId,
    },
    RetryDeadline {
        retry_at: DateTime<Utc>,
    },
}
```

`WakeContract`、`WakeState`与`WakeContractPayload`只由03拥有；其他规范只能引用。`generation`必须为正，初始state为`Pending`，唯一转换为
`Consumed | Cancelled | TimedOut`且终态不可离开。payload variant不能带另一variant字段；source set非空，poll count不超过正数limit，所有
fire/poll/retry时间不晚于deadline。opaque backend state本体必须加密保存在typed Job payload，Wake只冻结其digest，不引用06 ValueRef或新增资源。

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
enum TaskKind {
    Approval,
    InteractionForm,
    InteractionUrlConsent,
    InteractionBusinessInput,
    ExternalAuthorization,
    HumanWork,
}

enum TaskState { Pending, Responded, Declined, Approved, Rejected, Cancelled, Expired }
```

Task保存typed owner、schema、safe presentation、eligible principal rule、generation和deadline。`Approval`只允许
`Pending -> Approved | Rejected | Cancelled | Expired`；interaction/human variants按各自schema只进入`Responded | Declined | Cancelled | Expired`，
不能用通用`Completed`抹掉语义。Response必须匹配tenant、principal、kind、generation和Pending state。Task terminal与该variant规定的owner
wake/Event在一个事务中提交；17 encryption-domain显式apply工作流的Approval resolution只terminalize Task并追加safe Event，不直接执行owner
mutation。迟到response只追加safe Event。

共享Task aggregate直接使用kind对应的公开nominal ID：Approval必须是02 registry `approval_task/apr`的`ApprovalTaskId`，其余公开human
interaction variants使用`interaction/int`的`InteractionId`；public DTO不得接受internal `task/tsk` ResourceId或自由`TaskId`。04
`ApprovalTask`只是同一shared Task current row的closed safe projection，不是第二aggregate/state；其同名state与这里逐值相等。

## 8. 统一Receipt与幂等

```rust
enum ReceiptKind { Command, Callback, JobCommit }
enum ReceiptState { Processing, Succeeded, Failed, Rejected }
enum ClosedAuthorityScopeKind { Tenant, Installation }

struct ClosedOperation(String);

struct TypedDedupeOwner(VersionedSnapshot);

struct BoundedReceiptResult(VersionedSnapshot);

#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ReceiptRequestCommitmentModeV1 {
    CompleteAtClaim,
    StreamingCoreThenTerminal {
        terminal_commitment_schema_id: ClosedPayloadSchemaId,
        terminal_commitment_schema_version: u32,
        terminal_commitment_schema_path: ContractRelativePath,
        terminal_commitment_canonical_bytes_maximum: u32,
        stream_bytes_hard_maximum: u64,
    },
}

struct ReceiptOperationContractV1 {
    schema_version: u32, // const 1
    receipt_kind: ReceiptKind,
    operation: ClosedOperation,
    authority_scope_kind: ClosedAuthorityScopeKind,
    dedupe_owner_schema_id: ClosedPayloadSchemaId,
    dedupe_owner_schema_version: u32,
    dedupe_owner_schema_path: ContractRelativePath,
    request_schema_id: ClosedPayloadSchemaId,
    request_schema_version: u32,
    request_schema_path: ContractRelativePath,
    request_canonical_bytes_maximum: u32,
    request_commitment_mode: ReceiptRequestCommitmentModeV1,
    result_schema_id: ClosedPayloadSchemaId,
    result_schema_version: u32,
    result_schema_path: ContractRelativePath,
    result_canonical_bytes_maximum: u32,
}

enum AuthorityScope {
    Tenant { tenant_id: TenantId, scope: TypedAggregateRef },
    Installation { installation_id: InstallationId },
}

struct Receipt {
    receipt_id: ReceiptId,
    kind: ReceiptKind,
    scope: AuthorityScope,
    principal_or_binding: TypedDedupeOwner,
    operation: ClosedOperation,
    idempotency_key: BoundedKey,
    request_digest: Digest,
    state: ReceiptState,
    claim_generation: u64,
    lease: Option<Lease>,
    result: Option<BoundedReceiptResult>,
}
```

`ClosedOperation`为1～128 ASCII bytes并匹配`^[a-z][a-z0-9_.-]{0,127}$`。唯一machine registry按
`ReceiptKind + ClosedOperation`保存exact `ReceiptOperationContractV1`；同一operation只能出现一次，三个schema version与两个byte maximum均为正，
maximum不得超过`MAX_VERSIONED_SNAPSHOT_CANONICAL_BYTES`。route/worker adapter必须用registered request schema验证canonical request bytes后计算
`CompleteAtClaim`要求`request_digest = SHA-256(request JCS bytes)`；`StreamingCoreThenTerminal`要求该digest只覆盖registered canonical
Header/core schema，terminal commitment schema/version和stream hard maximum均为正，并由domain stream limit进一步收紧。`TypedDedupeOwner`与terminal `BoundedReceiptResult`分别是contract指定schema的完整
`VersionedSnapshot`，不得只保存裸ID、digest或自由JSON。authority scope kind、dedupe owner、request与result任一schema/version不匹配都在claim前
fail closed。`CompleteAtClaim` terminal replay只读取Receipt result bytes并重新验证其schema/digest；streaming mode还必须有界读取调用方重放的
request Data/Terminal，按registered commitment schema重算final digest并与terminal result比较，same才返回结果、different为conflict。该replay禁止
访问object store/KMS、修改Receipt/current aggregate或重新执行业务side effect，且两种mode都不依赖current aggregate。

该registry与schema路径固定为`contracts/platform-v1/receipts/operations.json`和
`contracts/platform-v1/schemas/receipts/operations.schema.json`；entry中的payload schema path必须位于同一root contract tree并由manifest的raw
SHA/length固定。各domain只注册自己拥有的payload。15必须逐项注册
`artifact.upload.prepare.v1 | artifact.upload.complete.v1 | artifact.download_grant.issue.v1 | artifact.rescan.v1 | artifact.delete.v1`及其§9表中
exact request/result schema；prepare/download的terminal result保存完整safe response preimage与deterministic token claims/key version，complete/rescan/delete
保存Artifact ETag/version与Operation reference，因此terminal replay只读Receipt result即可重建同一logical HTTP response。五项均为
`CompleteAtClaim`，不得用generic `ArtifactGrantIssuanceResultV1`、当前aggregate读取或自由JSON替代。15还必须为五个
ordinary stage operation注册只含Succeeded/Failed的`StageWorkloadArtifactTerminalResultV1`；外层RPC `Deferred`不得进入Receipt。16
`model_output.stage`只注册closed tagged `StageModelOutputTerminalResultV1`，不能用Receipt state猜测两个无tag struct。五个ordinary stage
operation必须使用`StreamingCoreThenTerminal`并注册15 `StageWorkloadArtifactRequestCommitmentV1`；其他首批operation包括
`model_output.stage`均为`CompleteAtClaim`，不得自行启用stream replay例外。ordinary stage的registered request schema exact为15
`StageWorkloadArtifactRequestCoreV1`；wire Header wrapper不参与request digest，必须先验证其core digest逐值相等。
registry、其closed JSON Schema与所有referenced result schema均进入root contract digest；缺entry、重复operation、unknown result variant、nullable
terminal result或超过registered maximum的claims/evidence使Candidate与server启动失败。

`receipt_id`使用02唯一registry的`rcp_<uuidv7>`并全局唯一。新claim固定为`Processing`、正数`claim_generation`、`lease=Some`、
`result=None`；只有Processing且lease已过期才可由same key/digest接管并严格增加generation。terminal
`Succeeded | Failed | Rejected`必须`lease=None,result=Some`且不可离开；terminal result是bounded closed tagged value，不能用缺失result或
`lease=None`推导另一个隐式状态。所有状态CAS同时比较current state、request digest与claim generation；不存在`InProgress` ReceiptState。

不同ReceiptKind使用closed dedupe key shape：

- Command：scope + principal + operation + idempotency key；
- Installation Command只允许18的promote/rollback及其精确重放，scope ID必须为`InstallationId`；04 bootstrap明确不创建Receipt；
- Callback：tenant + verified binding + external task identity + callback ID；
- JobCommit：tenant + Job + lease generation + commit request ID。

Callback与JobCommit永远不能使用Installation scope。持久化可以让tenant discriminator为空，但只有closed
`ReceiptKind::Command + InstallationId + allowlisted operation`组合合法；唯一键必须把NULL作为同一installation scope处理，不能让重复NULL
绕过幂等。Receipt ID全局唯一，tenant composite key只作为tenant查询/外键保护，不是假installation tenant。

相同key/digest重放返回同一terminal result；不同digest返回`idempotency_conflict`。Callback先做body size/media type和认证，再创建
Receipt；payload正文使用bounded encrypted value或ArtifactRef。Receipt result可包含typed rejection reason与observation snapshot，
不为late rejection另建表。Processing receipt使用短lease并可恢复；bounded外部I/O可以跨越Receipt lease，但不能持有数据库事务或
行锁，长操作必须续租且所有最终mutation回绑current `claim_generation`。terminal Receipt的同key/digest重放先于current aggregate
authorization并且不重做I/O；只有新建或Processing lease过期接管才重新授权，接管必须递增claim generation。

18 Release preflight是该模式的规范实例：第一个Receipt-only短事务先返回terminal replay/conflict，或提交Processing lease；随后capture短事务先
锁Receipt再锁InstallationReleaseState，manifest resolve与bounded catalog scan才在事务外执行，续租也只能使用Receipt-only短事务。capture、
transient classification和最终事务都必须先锁同一Receipt并复验
`claim_generation`，再锁InstallationReleaseState；最终事务不能先锁installation后补写Receipt。public If-Match在capture后因任何
compatibility-generation mutation而失配时必须在该锁序下terminalize稳定412，不能内部重试成503；无terminal winner的transient/serialization
耗尽可以保留Processing供同key恢复。

## 9. Event与Outbox

Event是统一append-only历史：

```rust
struct Event {
    event_id: EventId,
    scope: AggregateScope,
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

tenant Event必须携带Tenant scope；只有04 `installation.bootstrap` audit及installation Release/compatibility Event可以携带Installation scope，
且不得携带Run ID/public sequence。
Outbox逐字段继承其Event scope，Event ID与Outbox ID全局唯一。只有指向合法installation Event的Outbox可以没有tenant discriminator；
dispatcher、ACL与consumer仍按closed scope授权。不得用一个tenant-owned Outbox外发installation Event，也不得因NULL SQL unique语义产生重复
envelope。

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
`Receipt -> Tenant security -> quota -> parent/child aggregate -> Job -> Artifact -> Run sequence -> Event/Outbox`锁序提交；Producer不得在对象I/O期间持有
数据库事务，也不得修改ModelTurn/Job、quota余额、Event或Outbox。Inline/cancel/timeout/first-winner loser必须让非Ready candidate进入
Artifact GC，不能用bucket/object事实反推业务成功；quota服从04双bundle：未Ready Artifact count/logical bundle可Close，candidate Blob/
PUT不可能时upload/staging/physical bundle也可Close，已有candidate/object或dedupe race loser时Blob bundle保持Open到exact
deletion/absence evidence，不能先释放再GC。

Model output stage是明确的physical sub-protocol例外：Blob bind以及Artifact/Blob到Verified的内部物理transition不逐步追加
Event/Outbox，durable审计由claim-generation-bound JobCommit Receipt与Verified evidence承担；最终Ready或cleanup/incident owner事务
必须在自己的Event中回绑该evidence digest。Producer的Processing claim、Blob-bind、Uploaded/Verifying checkpoint与final-Verified短事务
都要按上述锁序先锁stage Receipt，再锁Tenant security aggregate并重验current encryption-domain fence，随后按04 canonical顺序对冻结的
两个quota bundle header/line取得
`FOR SHARE`并锁后重验exact generation，然后对current ModelTurn/Job取得会阻塞cancel/lease/terminal更新的共享serialization guard，最后锁
Artifact/Blob；quota Close/Expiry/
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
- shared Task fixture覆盖Approval `apr_`与interaction `int_` kind/ID exact pairing，拒绝public `tsk_`；approve/deny response只产生
  Approved/Rejected，interaction respond/decline只产生Responded/Declined，系统Cancel/Expiry对两类仍合法；tenant encryption-domain Approval
  owner不要求或伪造Run/Invocation；
- Receipt fixture覆盖required `rcp_` ID/state、Processing lease/result组合、expired takeover generation和三个terminal state/lease/result组合，
  unknown或`InProgress` state及terminal后迁移全部拒绝；
- 非幂等uncertain Effect不自动retry；
- Event history与aggregate current state不形成双写入口；
- installation command的Receipt/Event/Outbox在没有fake tenant的情况下保持exact replay、唯一性与scope ACL；Callback/JobCommit及
  任意tenant aggregate不能伪装为installation scope；
- Model bindability mutation、Release切换与root Run admission在Receipt之后都以InstallationReleaseState作为第一个current aggregate lock/CAS，
  且不存在反向锁序；
- Tenant encryption-domain Add/Rebind/Revoke固定按Receipt→InstallationReleaseState→Tenant security加锁，active Model count保持不变而
  installation compatibility generation/state digest严格推进；并发root preflight失效，child不锁current installation、只重验current security fence；
- Release长preflight只持有可接管Processing Receipt lease而不持有事务/行锁；terminal replay先于resolver/If-Match，final mutation严格按
  Receipt→InstallationReleaseState加锁，response loss或lease takeover不产生第二scan winner/Event；
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

本次architecture revision必须与02/04/05/06/07/08/09/12/15/16/17/18及ADR共同关闭installation scope、Run binding和region合同。
物理baseline与本规范分离审查；任何为了数据库便利引入的projection都不得获得domain写权限。
