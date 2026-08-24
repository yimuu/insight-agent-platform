# Platform v2 Subagent 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-181 |
| 日期 | 2026-08-20 |
| 依赖 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md)、[`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md)、[`10-capability-invocation.md`](10-capability-invocation.md) |
| 直接下游 | 17、18 |

> Persistence ruling：ChildRunLink 是 Run/Node 的 typed relation 与 snapshot，不建立独立 lifecycle、transition、budget
> 或 interaction 表族；历史关系写入共享 Run/Node/Event/Task/Quota 聚合。

> CR-181：ChildAgentCall由05 Plan v4冻结slot、input/output、candidate route、budget limit、cancel/retry与resume；Scheduler
> 不能从测试fixture或caller body补充这些字段。04 exact selector决定候选，创建事务重验全部evidence。

## 1. 决策摘要

Subagent 是父 Agent 通过 ChildAgentCall 创建的独立 durable child Run，不是 Capability、Skill、线程或
聊天 session。父 Agent 保留最终结果责任；child 只通过固定 Agent Interface 接收 typed input、返回 typed
output/failure，并以 durable ChildRunLink 传播 progress、interaction 和 cancellation。所有候选 child
Deployment 在父 Deployment/RunBindings 中精确固定。

## 2. 目标与非目标

### 2.1 目标

- 使用现有 Run 状态机表达可恢复、可重试、可取消的子 Agent；
- 分离父子输入输出、预算、状态、trace 和 terminal authority；
- 支持并行 child Runs 和 typed aggregation；
- 静态检测 deployment dependency cycle，并限制运行深度/数量；
- 让 child interaction 能安全路由给父 Run/用户；
- 让父取消、deadline、tenant policy 和 data classification 向 child 收紧传播；
- 让 child 失败成为父 Plan 可捕获的稳定 Failure。

### 2.2 非目标

- 不允许 Agent 间自由聊天或共享可变 memory；
- 不把 child Run 当作普通 tool JSON 调用；
- 不允许跨 tenant child Run；
- 不允许 child 动态选择未绑定 Agent、Revision 或 Deployment；
- 不自动共享父 Conversation、完整 trace、Secret 或 Capability 权限；
- 不允许 child 直接提交父 Run final result；
- 不实现无界递归、自复制 Agent 或动态组织结构。

## 3. Agent Requirement 与 Binding

父 Plan 的 ChildAgent slot 声明：

```rust
struct AgentRequirement {
    required_interface_revision_id: ResourceVersionId,
    allowed_agent_ids: BTreeSet<AgentId>,
    selection_mode: ChildSelectionMode,
    data_policy: DataFlowPolicy,
    budget_policy: ChildBudgetPolicy,
}
```

父Agent Deployment将候选解析为exact child Agent Deployment，并验证输入输出兼容、安全策略、
Capability/Model/Context 闭包、区域和 suspension。RunBindings 固定候选集合。运行时只可使用05 node的可选
`candidate_route` RunValue作为04 exact Selection Policy输入；没有route时policy必须仅从冻结snapshot唯一决定。
不能使用模型临时字符串、discovery、名称`latest`或扩大集合。

## 4. ChildRunLink

```rust
struct ChildRunLink {
    child_link_id: ChildRunLinkId,
    tenant_id: TenantId,
    parent_run_id: RunId,
    parent_node_execution_id: NodeExecutionId,
    parent_attempt_ordinal: u16,
    child_run_id: RunId,
    child_agent_deployment_id: DeploymentId,
    state: ChildLinkState,
    input_digest: Digest,
    deadline: DateTime<Utc>,
    cancellation_policy: ChildCancellationPolicy,
    projection_version: u64,
}
```

`ChildLinkState`是闭合枚举：`Running | Waiting | Cancelling | Succeeded | Failed | Cancelled | TimedOut`。
由于Link与child Run在一个事务创建，`Creating`不是可观察durable状态；事务失败时二者都不存在。
转换：

```text
Running -> Waiting | Cancelling | Succeeded | Failed | Cancelled | TimedOut
Waiting -> Running | Cancelling | Succeeded | Failed | Cancelled | TimedOut
Cancelling -> Succeeded | Cancelled | Failed | TimedOut
```

Child Run 自己使用 06 的完整 RunState；Link 是父节点对 child 的 durable projection，不替代 child Run。

## 5. 原子创建

ChildAgentCall drive 在一个 PostgreSQL transaction 中：

1. 验证父 Run/Node、tenant、binding、depth、quota、deadline 和 policy；
2. 从父 typed values 构造 child input 并通过 child Interface schema；
3. 创建稳定 ChildRunLink；
4. 从parent RunBindings逐字段继承同一exact ResourceVersion/Deployment closure，再创建child Run；
5. 写 parent NodeExecution Waiting continuation；
6. 写 parent/child transitions 和 outbox。

事务还必须重新加载Plan v4 node，确认slot/input/output/budget/cancel/retry/resume完全一致，按Scope词法链解析input与route，并重验04
`CandidateSelectionEvidence`。child input正文和classification复制自已解析parent RunValue，command不得降低classification或提交另一份
自由JSON。`logical child key`固定由`parent_node_execution_id + attempt_ordinal + selected_deployment_digest + input_content_digest`
规范计算；caller不能选择。child entry node/key/interface来自selected exact Deployment closure，不能由Scheduler声明。

同一 parent node/attempt/logical child key 并发重放只返回同一个 child Run。不得先创建 child 再异步补 link，
也不得让 parent 在 link 未提交时等待进程内 future。

child不得读取current active head或把parent binding替换成更新Deployment。admission必须使用parent冻结closure验证child的全部Model候选，
并重验04 current security fences；失败时整个child/link/parent-wait事务回滚，不能fallback或删减候选。

## 6. 输入与数据隔离

- 只传 ChildAgentCall 声明的 typed fields/ArtifactRefs；
- child 不继承父全部 Value Store、scratchpad、Model messages 或 Context results；
- Artifact grant 缩小为 child 所需对象、closed capability、port/purpose、audience 和 TTL；
- classification/egress policy 只能等于或严于父约束；
- Secret 通过 child 自己的 Deployment binding late resolve，不从父 input 传 value；
- 默认不传 Conversation；需要对话上下文时使用明确、安全、有限的 Message input；
- child output 经 child schema 与父 node response schema 双重验证。

## 7. 权限与能力

Child 的能力集合来自 child Deployment，不从父权限做并集。创建 child 必须同时满足：

```text
parent allowed-child binding
∩ caller/run policy
∩ child deployment policy
∩ tenant quota/data policy
```

child 不能因被高权限父调用就获得父的额外 Capability/Secret；父也不能通过 child 绕过自己被禁止的数据
出口。Policy compiler 必须检查已绑定 child dependency closure。

## 8. 预算与并发

父为 child 委派预算：

```rust
struct ChildBudget {
    deadline: DateTime<Utc>,
    max_model_tokens: u64,
    max_capability_calls: u32,
    max_artifact_bytes: u64,
    max_descendant_runs: u32,
}
```

- child deadline 不得晚于父 remaining deadline；
- budget 从父 reservation 扣除，child 未使用部分按 policy 释放；
- descendant usage 汇总到 root Run/tenant quota；
- waiting child 不占 execution permit，但占 active child quota；
- parallel child creation 分批且受 fan-out 上限；
- 单 parent/root/tenant 都有 child 并发与总数限制。

## 9. 深度与循环

发布/Deployment 时构建 Agent dependency graph：

- 固定 child edges 必须无环；
- 候选集合形成的潜在 edge 也参与 cycle detection；
- 同一 Agent 不允许通过别名/不同 Deployment 绕过 cycle detection；
- runtime 保存 root run ID、ancestry Agent IDs/Deployment IDs 和 depth；
- 即使错误配置漏过静态检查，runtime hard depth/descendant limit 仍 fail closed；
- 不允许 child 动态发布或激活新 Agent 来继续递归。

## 10. 结果与失败

Child terminal projection：

- Succeeded：读取 child final ValueRef，验证 child Interface 和 parent response type，提交 parent node result；
- Failed/TimedOut：映射为 `child_agent_failed`，保留 safe child failure code、child run ID 和 retryability；
- Cancelled：如果由父 cancel 导致，父节点继续 cancel convergence；否则按 node policy 处理；
- ArtifactRefs 重新验证 tenant、classification、media 和 parent port policy；
- child raw trace/model/tool 内容不自动进入父 output。

父可以用 ErrorBoundary 捕获稳定 child failure，但不能把 platform corruption/fence violation 当普通业务错误。

## 11. Retry

Child Run 内部节点 retry 由 child 自己处理。只有 child 已进入 terminal failure，且 parent NodePolicy、failure
class、Effect uncertainty、deadline 和 budget 都允许时，父 ChildAgentCall 才能 retry。

父级 retry：

- 保持 parent NodeExecution ID；
- 增加 parent Attempt ordinal；
- 创建新的 ChildRunLink 和新的 child Run ID；
- 使用相同 typed input digest 与 exact child Deployment；
- 不复活或改写旧 child Run；
- 如果旧 child 有 unresolved non-idempotent effect，先 reconciliation，不能直接创建新 child。

## 12. Cancellation

默认策略 `CascadeAndWait`：

```rust
enum ChildCancellationPolicy {
    CascadeAndWait,
    CascadeWithDeadline,
}
```

`DetachOnParentTerminal`不属于首版wire enum；输入该值按unknown variant拒绝。未来只有在独立后台Agent合同、
budget/ownership和安全审批全部规范化后才能通过breaking profile新增。

父 cancel：

1. Link 进入 Cancelling；
2. 对 child Run 提交 cancel intent；
3. 等待 child terminal/drain 或 cancellation deadline；
4. parent node/run 收敛。

kill 进程、删除 link 或断开 stream 都不等于取消。child terminal 与父 cancel 并发由数据库 first-winner 和
Link generation 决定。

## 13. Interaction 路由

Child HumanTask/InputRequired/Approval 可以：

- 由 child 内部预绑定 approver 处理；或
- 投影为 root Run interaction，携带安全 ancestry path 和 child interaction ID。

响应先验证 root principal/policy，再路由到 exact child continuation generation。父 Agent 不能修改 child
response schema，也不能把一个响应广播给多个 child。child interaction 期间父节点和 Link Waiting，不占
execution permit。

## 14. Progress 与公开事件

Child progress 默认私有。Parent node 可以配置安全 summary projection：

```text
child.started
child.waiting
child.progress
child.completed
child.failed
child.cancelled
child.timed_out
```

投影只包含 child Agent 的公开 display label、safe milestone 和 opaque child Run ID；不包含 input、tool
arguments、Prompt、Secret、内部 node graph。root/parent publish policy 与 child public policy 双重限制。

## 15. 状态传播与恢复

- child terminal transaction 写 child outbox；parent linker/recovery worker 以 child Run ID claim Link；
- 同库部署可以在一个后续 transaction 原子 settle Link + wake parent，不依赖同步函数返回；
- 事件丢失由 Link safety scan 恢复；
- parent/child 可以由不同 runtime/Worker 推进；
- parent process 重启不重建 child；读取 ChildRunLink 即可恢复；
- child active head 后续切换不改变绑定。

## 16. Persistence 映射

ChildRunLink 作为 parent Node 的 typed relation/snapshot 保存；child 本身仍是普通 Run。parent logical child key、child
Run ID、ancestry、预算 reservation reference、interaction route 与 link generation 是 bounded typed payload 和普通索引列。
状态变化进入共享 Event，interaction 使用 Task，预算使用 quota ledger；不建立 Subagent 专用表族。

## 17. 可观测性

```text
child_runs_total{outcome,depth_bucket}
child_runs_active{depth_bucket}
child_run_duration_seconds{outcome}
child_run_retry_total{failure_class}
child_run_cancel_total{outcome}
child_run_interactions_active{kind}
child_run_cycle_rejected_total{stage}
```

Agent/tenant/Run ID 不进入 label。Trace 使用 parent-child span link，不把 child 全部 span 强制嵌套在一个长父
span 中。

## 18. 验收标准

- parent/link/child 创建在任一故障窗口重放时只有一个逻辑 child；
- parent runtime kill 后可从 Link 恢复，不重复创建 child；
- child active head或GitOps rollout不影响运行中绑定；child不读取current head、不得fallback或逐字段混合，只继承parent完整historical binding；
- child全部Model candidates针对inherited exact Deployment验证并重验current security fence；historical ResourceVersion/Deployment缺失时整个
  child/link创建事务回滚，不留下partial child、quota、Event/Outbox或成功Receipt；
- 静态 cycle、候选 cycle、runtime depth/descendant overflow 全部被拒绝；
- child 无法读取未显式传递的父 value、Artifact、Conversation、Secret 或 Capability；
- parent cancel 与 child terminal 竞态只有一个 Link outcome；
- parent retry 创建新 child Run，不改写旧 terminal child；
- child interaction 精确路由到一个 continuation generation；
- parallel child 饱和受独立 quota，不阻塞 Scheduler/Model/Sandbox 其他类；
- child output/failure/public progress 通过双重 schema/policy；
- unresolved child side effect 阻止不安全 parent retry。

## 19. 明确推迟的工作

- 跨 tenant delegation；
- 跨平台/跨信任域 Agent federation；
- detached background Agent 的完整产品合同；
- Agent-to-Agent discovery protocol；
- shared long-term memory；
- 动态组织重规划。

## 20. 未决问题

CR-166已确认child Run只继承parent允许的exact Deployment/ResourceVersion closure，不读取installation或release candidate。
CR-181 cross-review已确认Plan v4 selection/dispatch/terminal-link闭合并恢复Accepted；parent/child transaction、quota、
cancel/recovery和schema fixture仍待实现。Detached background Agent
尚未进入本合同，也没有隐藏发布开关。
