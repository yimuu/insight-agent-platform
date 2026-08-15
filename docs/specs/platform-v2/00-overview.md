# Platform v2 规范索引与实施路线

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-15 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |

> `Platform v2` 是架构代号，不是公共 API 版本。目标系统会在 clean replacement 后直接占用 `/v1` 和
> `insight.platform/v1`；它不兼容当前 `insight.agent/v1`。这里出现的类型、API、数据库表、配置和容量要求在实现、
> conformance tests 与资格验收完成前都不是当前平台合同。

> 2026-08-09 persistence reset：此前 migration 1～35 及 177 表候选把行为不变量过度绑定为专用表、evidence 表和
> deferred trigger，已经停止继续实施。00～18 曾全部退回 Architecture Revision；在共享
> Resource/Aggregate/Job/Task/Event/Receipt 模型完成全量 cross-review 后已重新 Accepted。旧候选不得作为新实现兼容基线。

## 1. 决策摘要

Platform v2 采用以下不可逆的架构决定：

1. Agent 负责目标、流程和最终结果；Subagent 是具有独立状态的 child Run；
2. Skill 是版本化的方法包，只包含指令、引用、资产和 Capability 需求，不拥有执行状态；
3. Capability 是唯一通用可调用合同，原生代码、远程服务、MCP Tool 和脚本只是实现后端；
4. ContextSource 独立于 Capability，保留检索、引用、分页、来源和数据权限语义；
5. MCP 是独立协议 Host。Tool、Resource、Prompt、Task 分别投影到 Capability、Context、供 Agent/Skill Revision
   引用的候选 Prompt Artifact 与远程 Invocation，而不是把整个 MCP 降格为某一种 Action；
6. Model Provider、Profile、Deployment 与 ModelTurn 是独立合同；模型 intent 不等于真实 Tool 执行；
7. 所有跨进程调用先创建 durable CapabilityInvocation，允许立即完成，也允许暂停后由事件恢复；
8. 脚本只能在独立 Sandbox Execution Plane 中运行，API、Scheduler 和普通 Worker 不创建脚本进程；
9. PostgreSQL 是唯一事务与执行状态权威；消息总线只传 wake hint 和已提交 outbox 的投影；
10. 管理面可以动态变化，但每个 Run 必须固定 Agent、Skill、Capability、Model 和 Context 的精确版本；
11. 安全、配额、审批、取消、Artifact 和审计是平台合同，不交给模型或 Skill 自行实现。
12. 新架构完成资格验收后原位替换旧 `/v1`；不提供双栈、旧 wire 兼容、数据兼容或运行时 fallback。

## 2. 文档集合

完整实现由 18 份实现规范和本索引组成。

| 编号 | 文件 | 状态 | 负责合同 |
|---|---|---|---|
| 00 | `00-overview.md` | Accepted / In Progress | 总体路线、规范模板、依赖和完成定义 |
| 01 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) | Accepted / In Progress | 系统架构、领域对象和所有权边界 |
| 02 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) | Accepted / In Progress | ID、Resource、Version、Deployment、Binding |
| 03 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) | Accepted / In Progress | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) | Accepted / In Progress | 多租户、授权、Secret、Effect、Quota、Approval |
| 05 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) | Accepted / In Progress | Agent Interface、Typed Plan、Model Loop |
| 06 | [`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) | Accepted / In Progress | Run、NodeExecution、暂停、重试、取消 |
| 07 | [`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md) | Accepted / In Progress | Scheduler、Worker、Lease、背压和隔舱并发 |
| 08 | [`08-subagent.md`](08-subagent.md) | Accepted / In Progress | Child Run、父子通信、取消传播和循环限制 |
| 09 | [`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) | Accepted / In Progress | Capability Interface、Implementation、Registry |
| 10 | [`10-capability-invocation.md`](10-capability-invocation.md) | Accepted / In Progress | 调用协议、幂等、同步快路径、异步恢复 |
| 11 | [`11-skill-system.md`](11-skill-system.md) | Accepted / In Progress | Skill Package、发现、选择、绑定和依赖 |
| 12 | [`12-context-and-retrieval.md`](12-context-and-retrieval.md) | Accepted / In Progress | ContextSource、检索、引用和数据权限 |
| 13 | [`13-mcp-host.md`](13-mcp-host.md) | Accepted / In Progress | MCP Transport、OAuth、投影、Task 和 Subscription |
| 14 | [`14-sandbox-execution-plane.md`](14-sandbox-execution-plane.md) | Accepted / In Progress | Python、Node、WASM、受信任 Shell、隔离和扩缩容 |
| 15 | [`15-artifacts-and-files.md`](15-artifacts-and-files.md) | Accepted / In Progress | S3、内容寻址、上传、生命周期和内容安全 |
| 16 | [`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) | Accepted / In Progress | Provider、Model Profile、ModelTurn、流式响应和预算 |
| 17 | [`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) | Accepted / In Progress | 管理 API、Run API、事件流和错误模型 |
| 18 | [`18-deployment-observability-and-qualification.md`](18-deployment-observability-and-qualification.md) | Accepted / In Progress | Kubernetes、指标、Tracing、压测、故障注入和验收 |

Planned 文件不得被实现或其他规范作为已确定合同引用。一个文件只有进入 Draft 并给出完整状态机、
不变量和验收条款后，才能成为实施输入。

## 3. 实施依赖

```text
00 -> 01 -> 02 -> 03 -> 04
04 -> 05
05 -> 06 -> 07
05 -> 09
03 + 06 + 07 + 09 -> 10
05 + 06 + 07 + 10 -> 08
02 + 04 + 05 + 09 -> 11
02 + 04 + 05 + 07 + 11 -> 12
03 + 04 + 06 + 09 + 12 -> 15
02 + 04 + 05 + 06 + 07 + 10 + 15 -> 16
04 + 09 + 10 + 12 + 16 -> 13
04 + 07 + 09 + 10 + 13 + 15 -> 14
02～16 all domain contracts -> 17 API/Events -> 18 Qualification
```

这是有向无环依赖；下游可以实现上游port，但上游domain不能为了某个下游adapter反向依赖。例如Artifact Scanner
可以用Sandbox实现，Artifact contract仍不依赖Sandbox；MCP Sampling可以调用Model port，Model domain不依赖MCP。

后续规范可以收紧上游合同，但不能隐式改变已经 Accepted 的上游不变量。需要改变时必须先更新上游
规范、记录理由，并把所有下游规范退回 Draft。

## 4. 规范状态

每份规范只能使用以下状态：

```text
Draft
  -> Reviewed
  -> Accepted
  -> Implementing
  -> Implemented
  -> Verified
  -> Archived
```

- **Draft**：合同可变，不能据此声明功能存在；
- **Reviewed**：跨模块冲突已经检查，仍允许非破坏性修订；
- **Accepted**：目标合同冻结，可以开始实现；
- **Implementing**：至少一个实现任务已开始；
- **Implemented**：代码和 schema 已交付，但资格证据尚未完整；
- **Verified**：全部验收门槛已有可复现证据；
- **Archived**：合同已经进入 `docs/current`，本文件只保留决策历史。

## 5. 规范写作模板

每份实现规范必须包含下列章节；不适用时也必须说明原因，不能静默省略。

1. 决策摘要；
2. 目标与非目标；
3. 术语与信任边界；
4. 领域模型；
5. Rust 所有权接口；
6. 数据库与 Artifact Schema；
7. HTTP、gRPC 或 Event 机器合同；
8. 状态机；
9. 全局与局部不变量；
10. 幂等、并发和背压；
11. 超时、重试、取消和恢复；
12. 安全、租户和 Secret；
13. 可观测性与隐私；
14. 配置与部署；
15. 测试矩阵；
16. 验收标准；
17. 明确推迟的工作；
18. 未决问题。

规范中的 **MUST / MUST NOT / SHOULD / MAY** 为规范性要求。示例代码和示例 JSON 只有在正文明确
标记为 normative 时才构成机器合同。

## 6. 统一完成定义

一份规范进入 Verified 必须同时满足：

- 公开 Rust API、JSON Schema/OpenAPI、数据库约束和文档语义一致；
- PostgreSQL real-process integration tests 覆盖正常、重复、乱序、超时、取消和崩溃恢复；
- 未知字段、重复 JSON key、越界集合、非法 ID 和跨租户引用被拒绝；
- 所有外部写操作具有明确 Effect、idempotency 和 approval 语义；
- Secret value 不出现在数据库业务列、API 回读、错误、日志、trace、metric label 或 outbox；
- 所有无界队列、集合、正文、Artifact、并发和等待都有硬限制；
- 进程退出、消息丢失和迟到执行者不能破坏 durable authority；
- 关键指标、告警、runbook、容量基线和故障注入证据已经提交；
- `docs/current` 已更新，活动规范已归档。

## 7. 全平台验收门槛

全部 v2 工作完成时至少需要以下端到端证据：

1. 50 个并发 active Run 下，Sandbox 饱和不降低 API 和 Model Worker 的准入能力；
2. Runtime、MCP Host、Sandbox Executor 任一进程被终止后，已提交状态可恢复且无越权重放；
3. 丢失或重复全部 wake hint 时，安全扫描最终收敛；
4. Agent、Skill、Capability 或 Provider active head 在 Run 中途切换，不改变该 Run 的绑定；
5. 同一个 idempotency key 的并发提交只产生一个逻辑 Invocation；
6. 跨租户 ID、Artifact、Secret、Context 和 callback 均无法读取或关联；
7. 非幂等副作用在不确定结果下进入人工处置，不自动伪装为安全重试；
8. 动态代码只能进入策略允许的 Sandbox 后端，不能在控制面进程执行；
9. MCP Tool、Resource、Prompt 与 Task 分别保持各自语义，不通过通用 JSON 丢失安全元数据；
10. 版本、状态机、事件和公开错误码均通过 machine-readable conformance suite。

## 8. 本批次结论与下一步

00～18 的 persistence architecture cross-review 已关闭，目标合同推进为 Accepted / Implementation In Progress。旧的专用
表族、migration 1～35、177 表 catalog、checksum 和资格结论全部退出活动基线。物理模型由
[`ADR-0001`](../../adr/0001-platform-v2-postgres-baseline.md)冻结为 23 张表，并已经形成单一 `0001`、共享 repository 与
真实 PostgreSQL 16 foundation fixture。

这只表示 persistence foundation 已实现，不表示 00～18 的全部 API、Worker、Sandbox、MCP、SLO 或部署拓扑已经实现。
后续按新的[实施计划](implementation-plan.md)继续 domain service、execution integration、public `/v1` 和 qualification。

### 8.1 当前实施与证据边界（非规范性）

旧 migration 1～35、177 表 catalog、专用表族及其 checksum、fixture 和资格结论已经全部撤销；详细演变只保留在
Git 历史，不再复制到活动规范。它们不能证明当前 schema、API、Worker、部署或容量行为。

当前 persistence baseline 只有 ADR-0001 冻结的 23 张表和单一 `0001_platform_baseline.sql`。Phase 1、Phase 2 与 Phase 3
functional exit 已关闭；Phase 3 的 Artifact transaction/worker、generic Invocation、Capability execution、ModelTurn、Context 与
Text2SQL domain/repository 已在 fresh PostgreSQL 16 上作为同一全量 fixture suite 实际执行。Text2SQL admission 还在同一事务锁定
committed SqlCatalog Observation 与 exact `database.query.readonly` Capability Interface/Deployment/ReadOnly Effect，不建立专用表。
CR-165 已将 Model Artifact-backed output 的目标合同完成 cross-review并冻结为Accepted：写路径使用独立Model Artifact Producer，
与只读Model Artifact Broker分离，且只有Model terminal PostgreSQL事务能够把Verified Artifact原子推进为Ready并提交Output Link、
RunValue、usage/quota、Event与Outbox；pre-header transport timeout与storage write-quiescence barrier分别阻断slowloris容量占用和
absence后迟到PUT。该结论只表示目标合同可实施；对应domain/schema/protobuf、Producer进程与权限、部署及
real-process/故障/容量资格仍全部Open。当前Model output materializer仍为Inline-only，超过Inline能力时仍走开发期
`model_output_artifact_required`防护；不得据此关闭Phase 4～6、任一Qualification Gate，或把Artifact-backed output声明为当前行为。
精确完成度和下一门禁只以 [`implementation-plan.md`](implementation-plan.md) 为准。Phase 4～6 尚未完成，Phase 7 还要求
用户对 clean replacement 单独明确授权。

Accepted 只表示目标合同可作为实施输入，不表示任一新 API、数据库结构、部署拓扑、SLO 或容量数字已经成为当前行为。
cutover 前当前行为继续以 [`docs/current`](../../current/README.md) 为准。
