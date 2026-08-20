# Platform v2 Capability Invocation 规范

| 属性 | 值 |
|---|---|
| 状态 | Reviewed / Awaiting Acceptance |
| 日期 | 2026-08-20 |
| 依赖 | 03、04、06、07、09 |
| 直接下游 | 13、14、15、17、18 |

## 1. 决策摘要

CapabilityInvocation是业务调用authority，shared Job是物理attempt、lease、retry和wake authority。
Native、HTTP/gRPC、MCP Tool与Sandbox code共享同一Invocation合同，它们只是typed backend。

一次Invocation同时最多指向一个current Job。Sandbox backend的Job就是shared Job，没有SandboxJob ID、别名或child row。
首版MCP只使用remote Streamable HTTP，没有Managed stdio物理session child。

## 2. 模型

```rust
struct CapabilityInvocation {
    invocation_id: InvocationId,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    interface_version_id: ResourceVersionId,
    deployment_id: DeploymentId,
    backend: CapabilityBackendKind,
    effect: Effect,
    state: InvocationState,
    input: RunValueId,
    output: Option<RunValueId>,
    current_job_id: Option<JobId>,
    deadline_at: Timestamp,
    projection_version: u64,
}

enum CapabilityBackendKind { Native, RemoteHttp, RemoteGrpc, McpTool, Sandbox }

enum InvocationState {
    PendingApproval,
    Ready,
    Running,
    Waiting,
    InputRequired,
    ApprovalRequired,
    Reconciling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    UnknownOutcome,
}
```

Invocation不复制Job的lease、attempt count、retry time或Worker identity。Job也不复制Invocation的Effect、
input/output、approval与terminal business result。

## 3. Admission snapshot

创建Invocation时必须冻结：

- tenant、Run、Node、Interface与exact Deployment identity；
- input schema/output schema digest与validated RunValue identity；
- backend kind、protocol、endpoint/runtime digest与Credential reference identity；
- Effect、idempotency、approval、retry、cancel、timeout与policy digest；
- Artifact ports、network、Secret和Sandbox profile要求；
- deadline、quota、HardLimitProfile version和trace identity。

snapshot是closed、canonical、有size limit的immutable JSONB合同。执行时active head变化不改写它。

## 4. 创建与审批

创建事务必须：

1. 验证Run/Node expected version与冻结binding；
2. 执行authorization、policy、quota、suspension与schema admission；
3. 以Receipt处理idempotency；
4. 创建Invocation；
5. 若需审批，创建shared Task并进入`PendingApproval`；否则创建首个Job并进入`Ready`；
6. 追加Event与Outbox。

approval winner与Invocation transition同事务提交。拒绝或过期不创建可执行Job。

## 5. Job dispatch

| Backend | WorkClass | 执行者 |
|---|---|---|
| Native | CapabilityNative | Capability Worker |
| HTTP/gRPC | CapabilityRemote | Capability Worker + Egress Broker |
| MCP Tool | CapabilityRemote | Capability Worker + MCP Host |
| Sandbox | Sandbox | Sandbox Controller/Executor |

Job payload只携带immutable Invocation snapshot identity和expected owner version，不接受自由URL、header、shell command、
runtime installer或Secret value。

Sandbox调用直接以Invocation为typed owner创建`work_class=Sandbox`的Job。Controller通过带
`JobId + lease_generation + worker_process_generation`的closed RPC与Executor交互；Executor不直接写数据库。

## 6. Outcome

```rust
enum InvocationOutcome {
    Completed { output: RunValueId },
    Deferred { wake: WakeContract },
    InputRequired { task: TaskSpec },
    ApprovalRequired { task: TaskSpec },
    Failed { failure: InvocationFailure },
    Unknown { evidence: UnknownOutcomeEvidence },
}
```

terminal winner必须在一个事务中验证Job fence、验证output schema、写RunValue、推进Invocation、关闭Job、
释放quota并写Event/Outbox。重复或旧generation outcome只能返回已有结果或stable conflict。

Inline value验证canonical bytes与digest。显式Artifact port只保存nominal ArtifactRef；Capability/Sandbox产生文件时由
15的Artifact Data Worker形成Ready Artifact后再提交output。

## 7. Deferred、callback 与poll

`Deferred` 将Job进入`Waiting`并保存closed WakeContract：

- remote task identity、callback nonce digest或poll cursor；
- exact backend/session/auth binding digest；
- next poll time、deadline、attempt/reconcile budget；
- callback/poll result schema。

Worker在提交Deferred后释放execution permit和lease。callback、poll、cancel与timeout通过Receipt争用
同一current WakeContract，first-winner推进。不为每个waiting invocation保留常驻future。

## 8. Retry、timeout、cancel 与reconciliation

- retry只由冻结Effect、idempotency、failure class、attempt budget和deadline决定；
- retry delay持久化，不占用Worker；
- cancel是durable intent，backend cancel是最佳努力，不改写已发生外部副作用；
- timeout不把unknown external outcome归类为safe failure；
- 非幂等或返回不确定的backend进入`Reconciling`或`UnknownOutcome`；
- recovery只在验证current owner/Job fence后创建新generation。

## 9. Model loop 集成

Model只能从09的Capability Interface获取tool schema。tool intent先被正规化和验证，再创建Invocation。
Model Worker不直接调用backend，也不绕过approval、policy、quota或Receipt。工具结果以typed
RunValue返回Model loop。

## 10. 安全、可观测性与限制

- tenant、Run、Node、Invocation、Job与backend identity在每个跨进程边界重新绑定；
- Egress Broker只接受catalog endpoint identity，不接受调用方URL；
- Secret只在最后一跳解析，不进入DB、Event、trace或error body；
- input、output、progress、callback、poll、attempt、deadline与Artifact都有hard limit；
- metric不使用tenant/backend高基数ID，log不记录正文。

## 11. 验收标准

- 同一idempotency key并发创建只有一个Invocation；
- approval通过前没有可claim Job；
- 旧lease generation、错tenant/owner/backend的outcome全部fail closed；
- Deferred释放Worker容量，callback/poll/cancel只有一个winner；
- Sandbox Invocation只有一个shared Job和JobId；
- MCP Tool首版只走remote Streamable HTTP Host，不产生stdio session child；
- NATS丢失和Worker崩溃后可从PostgreSQL恢复；
- non-idempotent timeout不被伪造为安全重试。

## 12. 分层证据

domain state/property tests、PostgreSQL transaction/lease tests、backend adapter contract tests和production-equivalent
fault/isolated-capacity tests分层运行。一个低层fixture不同时声明发布资格。

## 13. 明确推迟

- Managed MCP stdio、persistent sandbox session与parent/child Job例外；
- microVM backend；
- 自动cross-backend failover；
- 对外部副作用的exactly-once保证。

## 14. 未决问题

首版Invocation合同无未决设计问题。
