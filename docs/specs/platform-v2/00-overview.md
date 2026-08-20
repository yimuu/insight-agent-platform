# Platform v2 规范索引与实施路线

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation Authorized |
| 日期 | 2026-08-21 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |

> `Platform v2` 是架构代号，不是公共 API 版本。目标系统会在 clean replacement 后直接占用 `/v1` 和
> `insight.platform/v1`；它不兼容当前 `insight.agent/v1`。这里出现的类型、API、数据库表、配置和容量要求在实现、
> conformance tests 与资格验收完成前都不是当前平台合同。

> 2026-08-09 persistence reset：此前 migration 1～35 及 177 表候选把行为不变量过度绑定为专用表、evidence 表和
> deferred trigger，已经停止继续实施。共享 Resource/Job/Task/Event/Receipt 模型的首轮cross-review曾完成；2026-08-15因
> CR-165曾把Installation Release、Model Artifact Producer和八类Artifact角色引入首版；2026-08-20的CR-166确认该闭包过度设计，
> 改由GitOps发布、Inline-only Model、三类Artifact角色、WASI+gVisor和remote-only MCP收敛首版。CR-166已完成全量cross-review，
> 2026-08-21的CR-169进一步确认editable Draft只由Resource aggregate拥有，publication才创建immutable ResourceVersion；
> Deployment是immutable exact closure，Resource active binding + gate是未来Run admission的唯一current authority，并完成
> Run admission闭包。CR-170在此基础上冻结public Artifact DTO、服务端identity/policy ownership与Public Gateway到Artifact Gateway的
> mTLS/current-principal rebinding。CR-171进一步以tenant current config的exact Retention/ArtifactIo Policy slot消除default policy歧义，
> 并完成00～18全量复核。相关规范已完成Acceptance并进入实施授权。旧候选不得作为新实现兼容基线。

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
13. 应用发布由Kubernetes/GitOps拥有；业务数据库不实现Installation Release状态机。
14. 首版Model输出只允许Inline；文件和大输出由Capability/Sandbox经共享Artifact Data Worker产生。

## 2. 文档集合

完整实现由 18 份实现规范和本索引组成。

| 编号 | 文件 | 状态 | 负责合同 |
|---|---|---|---|
| 00 | `00-overview.md` | Accepted / Implementation Authorized | 总体路线、规范模板、依赖和完成定义 |
| 01 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) | Accepted | 系统架构、领域对象和所有权边界 |
| 02 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) | Accepted | ID、Resource、Version、Deployment、Binding |
| 03 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) | Accepted | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) | Accepted | 多租户、授权、Secret、Effect、Quota、Approval |
| 05 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) | Accepted | Agent Interface、Typed Plan、Model Loop |
| 06 | [`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) | Accepted | Run、NodeExecution、暂停、重试、取消 |
| 07 | [`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md) | Accepted | Scheduler、Worker、Lease、背压和隔舱并发 |
| 08 | [`08-subagent.md`](08-subagent.md) | Accepted | Child Run、父子通信、取消传播和循环限制 |
| 09 | [`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) | Accepted | Capability Interface、Implementation、Registry |
| 10 | [`10-capability-invocation.md`](10-capability-invocation.md) | Accepted | 调用协议、幂等、同步快路径、异步恢复 |
| 11 | [`11-skill-system.md`](11-skill-system.md) | Accepted / Implementation In Progress | Skill Package、发现、选择、绑定和依赖 |
| 12 | [`12-context-and-retrieval.md`](12-context-and-retrieval.md) | Accepted | ContextSource、检索、引用和数据权限 |
| 13 | [`13-mcp-host.md`](13-mcp-host.md) | Accepted | MCP Transport、OAuth、投影、Task 和 Subscription |
| 14 | [`14-sandbox-execution-plane.md`](14-sandbox-execution-plane.md) | Accepted | Python、Node、WASM、受信任 Shell、隔离和扩缩容 |
| 15 | [`15-artifacts-and-files.md`](15-artifacts-and-files.md) | Accepted | S3、内容寻址、上传、生命周期和内容安全 |
| 16 | [`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) | Accepted | Provider、Model Profile、ModelTurn、流式响应和预算 |
| 17 | [`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) | Accepted | 管理 API、Run API、事件流和错误模型 |
| 18 | [`18-deployment-observability-and-qualification.md`](18-deployment-observability-and-qualification.md) | Accepted | Kubernetes、指标、Tracing、压测、故障注入和验收 |

Planned文件不得被实现或其他规范作为已确定合同引用。一个文件进入Draft并给出完整状态机、不变量和验收条款后，只能进入
cross-review；至少达到Reviewed，且破坏性目标合同通常达到Accepted后，才能成为实现输入。任何Architecture Revision期间新增的合同都不得
由既有Accepted状态旁路生成代码。

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
04 + 09 + 10 + 12 -> 13
03 + 04 + 06 + 09 + 12 + 13 -> 15
02 + 04 + 05 + 06 + 07 + 10 + 15 -> 16
04 + 07 + 09 + 10 + 13 + 15 -> 14
02～16 all domain contracts -> 17 API/Events -> 18 deployment/qualification
```

这是按合同章节而不是文件编号排序的有向无环依赖。GitOps发布输入不属于业务API；18只消费领域与17的API/Event合同定义部署和qualification。
下游可以实现上游port，但上游domain不能为了某个下游adapter反向依赖。例如Artifact Scanner
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

- 每个真实边界的权威机器合同、生成投影、数据库约束和文档语义一致；不要求未跨边界对象重复拥有Rust/protobuf/JSON Schema；
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

1. 在已资格CapacityProfile的混合并发负载下，Sandbox饱和不降低API、Model Worker和critical-control的准入能力；
2. Runtime、MCP Host、Sandbox Executor 任一进程被终止后，已提交状态可恢复且无越权重放；
3. 丢失或重复全部 wake hint 时，安全扫描最终收敛；
4. Agent、Skill、Capability 或 Provider active head 在 Run 中途切换，不改变该 Run 的绑定；
5. 同一个 idempotency key 的并发提交只产生一个逻辑 Invocation；
6. 跨租户 ID、Artifact、Secret、Context 和 callback 均无法读取或关联；
7. 非幂等副作用在不确定结果下进入人工处置，不自动伪装为安全重试；
8. 动态代码只能进入策略允许的 Sandbox 后端，不能在控制面进程执行；
9. MCP Tool、Resource、Prompt 与 Task 分别保持各自语义，不通过通用 JSON 丢失安全元数据；
10. 版本、状态机、事件和公开错误码均通过 machine-readable conformance suite。

## 8. CR-166～CR-171 简化结论与下一步

2026-08-20的CR-166撤销CR-165中超出首版需要的最终形态设计，并已完成受影响规范的全量cross-review：

- 发布、promotion和rollback由Kubernetes/GitOps拥有；Candidate和qualification报告是CI/CD内容寻址产物，不是数据库或公共API状态；
- 数据库不新增`InstallationReleaseState`，目标仍为23张总表/22张业务表；clean-cut ID/owner约束完成后schema contract从当前v6升级为v7；
- root Run在tenant事务中解析并冻结exact ResourceVersion/Deployment binding；后续部署变化不修改既有Run；
- 首版Sandbox backend闭集为restricted WASI与single-Job gVisor；microVM、Firecracker、KVM和plain runc不在目标闭集；
- 首版MCP只支持远程Streamable HTTP；Managed stdio及其持久Sandbox session、parent/child Job例外和Provider recovery全部推迟；
- Model output保持Inline-only；文件和大输出由Capability/Sandbox调用共享Artifact Data Worker生成，不建设Model Artifact Producer；
- Artifact物理角色收敛为Gateway、Data Worker、Maintenance三类；不同调用方使用closed method、identity和capacity lane，但共享一套staging、
  verification、dedupe、quota和cleanup权威；
- 公共Operation只是shared Job的safe projection，不建立ManagementOperation aggregate、状态机或表；
- public HTTP、internal protobuf、persisted Rust JSONB各自只在真实边界拥有机器合同；registry/schema从owner type生成，禁止无边界的三份手写复制；
- 首版公共`/v1`只包含Agent/Skill/Capability管理、Run、Task、Artifact、MCP HTTP binding和Run SSE；
- qualification按开发门禁与发布门禁分层，A～G不持久化为运行时GateResult/ReleaseManifest。

2026-08-21的CR-167在上述闭包内消解Draft authority歧义：Resource拥有唯一current editable Draft及validation fence，
publication才创建immutable ResourceVersion；public management API因此使用`/draft` update/validate/publish，不公开mutable Version identity。

2026-08-21的CR-168消解Deployment authority歧义：Deployment一经创建即为immutable exact closure；activate/suspend以Resource ETag
做CAS，只改Resource active binding与AdministrativeGate，不建立第二Deployment current-state projection。

2026-08-21的CR-169补齐root Run admission authority：public request显式选择`agent_id`，immutable Agent Deployment冻结validated
Plan entry ID/kind；admission不接受调用方提供的内部entry/binding，也不在事务外读取Plan Artifact猜测入口。

上述决策减少目标服务、状态机、Schema和资格组合，但不降低PostgreSQL durable Job、Receipt幂等、Event/Outbox原子性、Run冻结binding、
tenant/permission/quota、lease fence、Artifact content integrity及Sandbox物理隔离。

### 8.1 当前证据边界（非规范性）

当前checked-in persistence baseline是23张总表/22张业务表、schema contract v7和单一`0001_platform_baseline.sql`。仓库有
CR-171之前候选架构的多类functional fixture；只有已按CR-171重新对照且通过适用门禁的批次可计为实现证据，尚不能据此宣称全部phase完成。

CR-170进一步确认public Artifact调用方只提交业务意图或opaque completion proof，Blob/Grant/Job/Task/Receipt/Event/Outbox、policy、quota、
storage与audit closure全部由服务端拥有；upload target是唯一显式Secret-bearing响应例外。Public Gateway不取得storage authority，Artifact Gateway
不信任自由principal header，两者以exact audience mTLS连接并由Artifact Gateway从PostgreSQL重绑定current principal。

CR-171把public Artifact使用的Retention与ArtifactIo default revision加入tenant current config exact slot；多条active Policy不再通过排序或
隐式安装默认选择，绑定更新沿用Tenant CAS/Receipt/Event/Outbox且保留其他slot。

仓库中已有的microVM/Firecracker、Managed stdio session和Model Artifact Producer候选代码不再构成首版目标证据；后续实现批次应先从
registry、runtime manifest、RPC、Helm和测试入口中删除或隔离这些非目标路径，再补齐gVisor、三角色Artifact和最小`/v1`。切除旧候选不得
恢复host execution、plain runc或第二持久状态权威。

精确实施顺序只以[implementation-plan.md](implementation-plan.md)为准。本次受影响规范已经CR-171复核并Accepted，但Accepted
本身不声明新的API、部署拓扑、容量数字或qualification结果是当前行为。cutover前当前行为继续以
[docs/current](../../current/README.md)为准。
