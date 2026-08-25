# Platform v2 规范索引与实施路线

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-187 |
| 日期 | 2026-08-25 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |

> 2026-08-24 implementation feedback（CR-181）：production Scheduler接入外部叶节点时确认Plan v3仅为
> `ModelLoop/CapabilityCall/ContextQuery/ChildAgentCall/HumanTask/TimerWait/SignalWait`保存`resume`，无法从冻结Plan
> 推导exact slot、输入输出port、预算、deadline或durable wait合同。CR-181按05→06～18顺序补齐closed node payload并将
> 未发布Typed Plan wire提升为version 4；候选选择同时重开04，在00～18 cross-review关闭前暂停Implementation Authorization。

> 2026-08-24 implementation feedback（CR-182）：CR-181冻结了CandidateSelectionEvidence却没有冻结可执行selector program，
> `PolicyKind::Selection`仍允许空document。CR-182补齐首版closed selector mode并暂停selection/leaf dispatch实现，直到影响复核关闭。

> CR-183 implementation feedback：CR-182示例误把`semantic_digest`放进自身canonical document。Selection Policy沿用02/09
> `PolicyResourceSpec.rules_digest`作为唯一外部digest，document不保存自引用digest。

> CR-184 implementation feedback：Model/Capability/Context terminal若重新Ready同一leaf Node，`None` observation会再次dispatch。
> 05→06→07→10/12/16→18已复核为terminal owner原子终结leaf Node并激活exact Plan `resume`目标；不新增observation wire或current authority。

> 2026-08-25 implementation feedback（CR-185）：spec11只冻结了Skill逻辑目录与manifest，没有冻结package Artifact物理字节。
> 11→15/17/18复核后固定首版无压缩`insight.skill-package/1` frame、专用media type与逐entry验证；不接受运行时ZIP/TAR猜测。

> 2026-08-25 implementation feedback（CR-186）：spec11/16只规定Prompt assembly顺序，却未冻结每块进入canonical Model request
> 后的source-map wire、信任角色和预算失败语义。11→16→18复核后固定七阶段block、owner-scoped source ID、source/content digest、
> classification、ordinal及byte/token budget；首版不隐式截断，Skill/Context/User永不获得platform role。

> 2026-08-25 implementation feedback（CR-187）：production Model admission发现Model Deployment虽引用Safety/Budget/
> PublicProjection Policy，`PolicyResourceSpec`却没有对应nominal document，导致attempt、token/cost ceiling、平台安全指令和overflow
> 语义只能由测试provider自由填写。04→16→17/18复核后冻结三类closed Policy，并给Model Deployment增加exact Safety binding。
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |

> `Platform v2` 是架构代号，不是公共 API 版本。目标系统会在 clean replacement 后直接占用 `/v1` 和
> `insight.platform/v1`；它不兼容当前 `insight.agent/v1`。这里出现的类型、API、数据库表、配置和容量要求在实现、
> conformance tests 与资格验收完成前都不是当前平台合同。

> 2026-08-21 implementation feedback（CR-173）：owner registry仍允许Skill、Policy与Sandbox Profile直接激活
> ResourceVersion，且public generic deployment route对这些kind实际不可用；这与本索引及工程规则要求的统一
> `Resource -> ResourceVersion -> Deployment -> Binding`生命周期冲突。02为上游authority，00～18受影响合同已退回
> Architecture Revision；完成closure matrix、Run binding、API与资格fixture的全量复核前，不再授权继续生成完整Management API。

> 2026-08-09 persistence reset：此前 migration 1～35 及 177 表候选把行为不变量过度绑定为专用表、evidence 表和
> deferred trigger，已经停止继续实施。共享 Resource/Job/Task/Event/Receipt 模型的首轮cross-review曾完成；2026-08-15因
> CR-165曾把Installation Release、Model Artifact Producer和八类Artifact角色引入首版；2026-08-20的CR-166确认该闭包过度设计，
> 改由GitOps发布、Inline-only Model、三类Artifact角色、WASI+gVisor和remote-only MCP收敛首版。CR-166已完成全量cross-review，
> 2026-08-21的CR-169进一步确认editable Draft只由Resource aggregate拥有，publication才创建immutable ResourceVersion；
> Deployment是immutable exact closure，Resource active binding + gate是未来Run admission的唯一current authority，并完成
> Run admission闭包。CR-170在此基础上冻结public Artifact DTO、服务端identity/policy ownership与Public Gateway到Artifact Gateway的
> mTLS/current-principal rebinding。CR-171进一步以tenant current config的exact Retention/ArtifactIo Policy slot消除default policy歧义。
> 2026-08-21实施反馈发现Dataset build缺少首个root identity且Context Deployment未冻结chunker/embedding，以及MCP discover
> 未冻结public authorization input；CR-172按12→13→17→18完成上游到下游修订与00～18影响复核。相关规范已完成Acceptance并进入
> 实施授权。旧候选不得作为新实现兼容基线。

> 2026-08-23 implementation feedback（CR-174）：production Scheduler接入时确认candidate `RuntimePlan`只保存
> Branch/Map/Loop目标与上限，却没有冻结可执行表达式或exact RunValue observation evidence；repository fixture因此能手工注入
> `ControllerObservation`，production handler无法从“exact Plan + committed facts”自行推进。05先冻结closed typed expression IR与
> node input binding，06冻结派生observation/RunValue原子提交，07冻结Scheduler materialization/evaluator边界，17禁止public注入，
> 18补资格矩阵；完成00～18影响复核后恢复实施授权。

> 2026-08-23 implementation feedback（CR-175）：实现closed expression IR时确认05要求`HardLimitProfile`进一步收紧
> instruction/input/stack上限，但18拥有的profile registry没有对应typed字段。CR-175在18补齐三个closed limit及profile version，
> 05明确绝对上限与deployment profile的双重约束；00～18完成容量、schema、错误和fixture影响复核后恢复实施授权。

> 2026-08-23 implementation feedback（CR-176）：PostgreSQL production Plan driver接线时确认现有`run_values`只保存immutable
> value，`run_nodes.output_value_id`只能表达单一最终输出，无法作为多个exact Plan data port的绑定权威；动态Map/Loop scope也会产生同一
> Plan port的不同实例。CR-176把port→RunValue current environment收敛到既有Scope aggregate typed payload，冻结词法scope解析与原子
> CAS，并对齐Inline RunValue数据库结构上限；不新增表或第二value authority。

> 2026-08-23 implementation feedback（CR-177）：Compute owner transaction接线时确认Typed Plan没有output classification字段，
> 而RunValue必须持有classification；若由caller自由提供将允许派生值降级。CR-177冻结表达式classification传播：同一expression
> controller的全部external input classification取lattice join，Compute全部输出继承该结果；无external input的常量闭包默认为
> `Internal`。该规则由pure owner计算并在提交事务重验，不增加Plan字段、profile字段、表或public输入。

> 2026-08-23 implementation feedback（CR-178）：Map item Scope binding接线时确认`item_port`只有名字，缺少producer与item
> schema digest，无法形成`ExactDataPortRef`或验证item RunValue。CR-178把Map `item_port`收紧为exact NodeOutput ref，Compiler/
> publication验证items array element schema与port schema一致，并将未发布Typed Plan wire提升到version 2；不增加表、profile或public字段。

> 2026-08-24 implementation feedback（CR-179）：Loop carried rollover接线时确认规范没有冻结下一iteration Scope在condition、body与
> settlement之间的生命周期，直接实现会读取已关闭Scope、把carried值写回父Scope或形成Scope父链自环。CR-179规定body settlement
> 预建下一iteration的open Scope并绑定复制后的immutable carried RunValue，continuation在该Scope求值且condition为true时body复用同一
> Scope；condition为false时原子关闭它并从词法父Scope激活exit。所有iteration Scope保持同一root Loop controller owner，不形成跨轮父链。

> 2026-08-24 implementation feedback（CR-180）：Run terminal接线时确认无字段`Return`/`Raise`无法满足Succeeded final ValueRef与
> Failed safe Failure不变量。CR-180把二者收紧为引用exact data port的Plan v3 terminal consumer；Compiler/publication对齐Agent
> output/error schema，Scheduler只物化已提交RunValue，owner transaction重验Scope、value、schema/content/classification及正文。

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
| 00 | `00-overview.md` | Accepted / CR-185 | 总体路线、规范模板、依赖和完成定义 |
| 01 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) | Accepted / CR-173 | 系统架构、领域对象和所有权边界 |
| 02 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) | Accepted / CR-173 | ID、Resource、Version、Deployment、Binding |
| 03 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) | Accepted / CR-176 | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) | Accepted / CR-183 | 多租户、授权、Secret、Effect、Quota、Approval |
| 05 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) | Accepted / CR-184 | Agent Interface、Typed Plan、Model Loop |
| 06 | [`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) | Accepted / CR-184 | Run、NodeExecution、暂停、重试、取消 |
| 07 | [`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md) | Accepted / CR-184 | Scheduler、Worker、Lease、背压和隔舱并发 |
| 08 | [`08-subagent.md`](08-subagent.md) | Accepted / CR-182 | Child Run、父子通信、取消传播和循环限制 |
| 09 | [`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) | Accepted / CR-182 | Capability Interface、Implementation、Registry |
| 10 | [`10-capability-invocation.md`](10-capability-invocation.md) | Accepted / CR-184 | 调用协议、幂等、同步快路径、异步恢复 |
| 11 | [`11-skill-system.md`](11-skill-system.md) | Accepted / CR-185 | Skill Package、发现、选择、绑定和依赖 |
| 12 | [`12-context-and-retrieval.md`](12-context-and-retrieval.md) | Accepted / CR-184 | ContextSource、检索、引用和数据权限 |
| 13 | [`13-mcp-host.md`](13-mcp-host.md) | Accepted / CR-181 | MCP Transport、OAuth、投影、Task 和 Subscription |
| 14 | [`14-sandbox-execution-plane.md`](14-sandbox-execution-plane.md) | Accepted / CR-181 | Python、Node、WASM、受信任 Shell、隔离和扩缩容 |
| 15 | [`15-artifacts-and-files.md`](15-artifacts-and-files.md) | Accepted / CR-185 | S3、内容寻址、上传、生命周期和内容安全 |
| 16 | [`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) | Accepted / CR-187 | Provider、Model Profile、ModelTurn、流式响应和预算 |
| 17 | [`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) | Accepted / CR-185 | 管理 API、Run API、事件流和错误模型 |
| 18 | [`18-deployment-observability-and-qualification.md`](18-deployment-observability-and-qualification.md) | Accepted / CR-185 | Kubernetes、指标、Tracing、压测、故障注入和验收 |

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
4. Agent、Skill、Capability或Provider active Deployment在Run中途切换，不改变该Run的冻结绑定；
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

截至2026-08-25，Plan v4的ChildAgent、HumanTask、TimerWait与SignalWait已接入PostgreSQL owner transaction和durable Plan store。
SignalWait的exact key、可选payload schema/摘要、immutable RunValue、当前Scope绑定、首次胜出和Receipt重放，以及Timer due/Signal
timeout的typed Job scheduling与critical-control bounded scan已通过fresh PostgreSQL 16 r88 L2 fixture；Timer另已在fresh PostgreSQL 16
r113完成claim、durable park、独立safety到期唤醒、continuation claim、Return物化与Run终态的真实协调器L3链路；r181进一步完成
durable park后强制终止首个Worker、由替代Worker及独立safety scanner恢复且finish Node唯一的Timer多进程kill-window。独立认证Signal
ingress已接入generated OpenAPI、Gateway和同事务Principal/Signal owner重验，并在fresh PostgreSQL 16 r187通过权限、owner与Receipt replay门禁。
fresh PostgreSQL 16 r189又完成Timer到期恢复后进入SignalWait、强制终止第二个Worker、认证Signal owner事务唤醒、第三个Worker仅凭durable
state完成Return且finish Node唯一的多进程链路。Child/Task的多进程kill/recovery仍未完成，ModelLoop、CapabilityCall和ContextQuery仍缺完整production多进程L3，因此不能据此宣称
Phase 2完成。

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
