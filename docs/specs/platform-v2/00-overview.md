# Platform v2 规范索引

| 属性 | 值 |
|---|---|
| 状态 | Reviewed / CR-220 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |
| 当前行为 | OpenSandbox-only；以 [`docs/current`](../../current/README.md) 为准 |
| 项目阶段 | Development；生产级 L4～L6 deferred |

本文件只保存稳定架构决定、规范所有权和完成边界。逐提交修订、cross-review 流水账、实施过程和历史资格报告
不在工作树重复保存，需要时从 Git 历史查看。当前各领域的完整行为由 01～18 的 owning specification 定义。

## 1. 当前边界

- 公共合同保持 `/v1` 与 `insight.platform/v1`；“Platform v2”只是架构代号；
- 首版 Sandbox 使用 OpenSandbox Kubernetes、BatchSandbox、containerd/runc 和每次 attempt 的临时 sandbox；
- PostgreSQL 是业务状态、事务、lease fence、quota、Event/Outbox 和恢复的唯一持久权威；
- shared Job 是唯一业务 work authority，OpenSandbox 和消息系统不保存第二份业务当前状态；
- Sandbox Dispatcher 独占 candidate 创建、选择、activation、terminal fence 和清理；
- runner 与 Package 使用不同 UID，activation 签名绑定 exact sandbox、boot、request、schema 和 input；
- 本地仓库门禁与 L1～L3 已验证；本机 Kind L4 mechanics 曾通过 12/12 动态检查；
- 项目处于开发阶段，生产级多故障域、容量、24 小时 soak、restore 和 GitOps promotion 当前不执行，
  不声明 production-ready、生产容量或 SLO。

## 2. 架构决定

1. Agent 负责目标、流程和最终结果；Subagent 是具有独立状态的 child Run。
2. Skill 是版本化方法包，只包含指令、引用、资产和 Capability 需求，不拥有执行状态。
3. Capability 是唯一通用可调用合同；原生代码、远程服务、MCP Tool 和脚本只是实现后端。
4. ContextSource 独立于 Capability，保留检索、引用、分页、来源和数据权限语义。
5. MCP 是独立协议 Host；Tool、Resource、Prompt 和 Task 保持各自语义。
6. Model Provider、Profile、Deployment 与 ModelTurn 是独立合同；模型 intent 不等于 Tool 执行。
7. 跨进程调用先创建 durable CapabilityInvocation，允许立即完成或暂停后恢复。
8. 动态脚本只能在 Sandbox Execution Plane 运行，控制面、Scheduler 和普通 Worker 不创建脚本进程。
9. 消息只传 wake hint 或已提交 projection，不承载当前执行状态。
10. 管理面可以变化，但 Run 固定 Agent、Skill、Capability、Model 和 Context 的精确版本。
11. 安全、配额、审批、取消、Artifact 和审计由平台合同拥有。
12. 新架构原位占用 `/v1`，不提供旧 wire、双栈、双写或运行时 fallback。
13. 应用发布、promotion 和 rollback 由部署/GitOps 系统拥有，不进入业务数据库或公共管理 API。
14. 首版 Model 输出只允许 Inline；文件和大输出由 Capability/Sandbox 经 Artifact Data Worker 产生。

## 3. 规范集合

| 编号 | 文件 | 负责合同 |
|---|---|---|
| 00 | 本文件 | 总体边界、索引、依赖和完成定义 |
| 01 | [架构与领域边界](01-architecture-and-domain-boundaries.md) | 系统架构、领域对象和所有权 |
| 02 | [身份、版本与部署](02-identity-revision-and-deployment.md) | ID、Resource、Version、Deployment、Binding |
| 03 | [一致性、事件与恢复](03-consistency-events-and-recovery.md) | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [租户、安全与策略](04-tenancy-security-and-policy.md) | 授权、Secret、Effect、Quota、Approval |
| 05 | [Agent 与 Typed Plan](05-agent-and-typed-plan.md) | Agent Interface、Typed Plan、Model Loop |
| 06 | [Durable Run 状态机](06-durable-run-state-machine.md) | Run、NodeExecution、暂停、重试、取消 |
| 07 | [Scheduler、Worker 与并发](07-scheduler-workers-and-concurrency.md) | Lease、背压、隔舱与公平性 |
| 08 | [Subagent](08-subagent.md) | Child Run、父子通信、取消和循环限制 |
| 09 | [Capability 与 Registry](09-capability-model-and-registry.md) | Interface、Implementation、Registry |
| 10 | [Capability Invocation](10-capability-invocation.md) | 调用协议、幂等、异步恢复 |
| 11 | [Skill System](11-skill-system.md) | Skill Package、发现、选择和绑定 |
| 12 | [Context 与 Retrieval](12-context-and-retrieval.md) | ContextSource、检索、引用和权限 |
| 13 | [MCP Host](13-mcp-host.md) | Transport、OAuth、投影、Task、Subscription |
| 14 | [Sandbox Execution Plane](14-sandbox-execution-plane.md) | OpenSandbox、activation、恢复和清理 |
| 15 | [Artifact 与文件](15-artifacts-and-files.md) | S3、内容寻址、上传、安全和生命周期 |
| 16 | [Model Provider 与调用](16-model-provider-and-invocation.md) | Provider、Profile、ModelTurn、预算 |
| 17 | [管理与运行 API](17-management-and-runtime-api.md) | 公共 API、事件流和错误模型 |
| 18 | [部署、可观测性与验证](18-deployment-observability-and-qualification.md) | 部署、指标、故障注入和资格边界 |

规范内容进入实现前必须达到 Reviewed 或 Accepted。Architecture Revision 必须先修改上游 owning spec，再复核
ownership、identity、schema、error、transaction、event、security、capacity、recovery 和 test evidence。

## 4. 依赖关系

```text
00 -> 01 -> 02 -> 03 -> 04
04 -> 05
05 -> 06 -> 07
05 -> 09
03 + 06 + 07 + 09 -> 10
05 + 06 + 07 + 10 -> 08
02 + 04 + 05 + 09 -> 11
02 + 04 + 05 + 07 + 11 -> 12
04 + 09 + 10 + 12 -> 13
03 + 04 + 06 + 09 + 12 + 13 -> 15
02 + 04 + 05 + 06 + 07 + 10 + 15 -> 16
04 + 07 + 09 + 10 + 13 + 15 -> 14
02～16 -> 17 -> 18
```

下游可以实现上游 port，但上游 domain 不能为了 adapter 反向依赖。后续规范可以收紧上游合同；需要改变
Accepted 不变量时，必须先更新上游规范并复核受影响下游。

## 5. 规范状态

```text
Draft -> Reviewed -> Accepted -> Implementing -> Implemented -> Verified -> Archived
```

- **Draft**：合同可变，不能作为实现授权；
- **Reviewed**：跨模块冲突已检查，仍允许非破坏性修订；
- **Accepted**：目标合同冻结，可以开始实现；
- **Implementing**：实现已开始；
- **Implemented**：代码和 schema 已交付，但仓库证据未完整；
- **Verified**：合同、实现与仓库门禁有可复现证据，不自动表示生产环境已通过；
- **Archived**：已退出活动合同；工作树不保留正文，历史由 Git 提供。

## 6. 仓库完成定义

一份规范进入 Verified，至少需要：

- 真实边界的机器合同、生成投影、数据库约束和文档语义一致；
- PostgreSQL real-process tests 覆盖正常、重复、乱序、超时、取消和崩溃恢复；
- 未知字段、重复 JSON key、越界集合、非法 ID 和跨租户引用被拒绝；
- 外部写操作具有明确 Effect、幂等和审批语义；
- Secret 不进入业务列、API 回读、错误、日志、trace、metric label 或 outbox；
- 队列、集合、正文、Artifact、并发和等待具有硬限制；
- 进程退出、消息丢失和迟到执行者不能破坏 durable authority；
- 指标、告警、runbook、部署 preflight 和相关负向测试已提交；
- 未执行的真实集群、容量、故障、soak、restore 和 promotion 明确标为未运行。

`docs/current` 在 clean cut 后同步当前行为。仓库范围 Verified 不表示 production-ready，也不生成容量或 SLO 声明。

## 7. 当前开发阶段验证策略

日常开发只要求与变更风险相称的仓库门禁：格式、编译、Clippy、单元/集成测试、机器合同和静态部署检查。
涉及部署、网络、Sandbox 或恢复的重大修改，可以按需重建 Kind 环境执行本机 mechanics。

生产级 L4～L6 是未来生产准备里程碑的发布门禁，目前延期而非失败：

- L4：独立故障域、真实身份/隔离/监控和完整故障矩阵；
- L5：批准的 CapacityProfile、mixed load、饱和点和持续 soak；
- L6：签名供应链、backup/restore、upgrade/rollback 和 GitOps promotion。

机器可读资格 profile 可以保留，但没有目标环境证据时不得产生 passed release evidence。
