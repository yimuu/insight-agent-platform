# Platform v2 规范索引与实施路线

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-197 |
| 日期 | 2026-08-26 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |
| 当前行为 | 不变；仍以 [`docs/current`](../../current/README.md) 为准 |

> 2026-08-26 implementation feedback（CR-197）：最终observability审计确认规范要求全组件传播trace identity，但machine/runtime合同没有
> 一个可在Job lease、进程终止和恢复后重建的durable owner；仅转发自由`traceparent`会允许调用方伪造关联，也会在新Worker恢复时断链。
> CR-197按03→04/06/07→08/10/12～16→17/18冻结`TraceIdentityV1`：Run或非Run command admission生成/接受一个合法W3C trace ID，
> durable owner snapshot只保存该低敏opaque ID；每个进程/RPC hop生成新span ID。trace不参与tenant、principal、owner、Receipt、request digest、
> fence、policy或业务状态判断，首版Egress不向第三方转发内部trace header。无新表、aggregate、public route、WorkClass或Secret路径。

> 2026-08-26 implementation feedback（CR-196）：真实OAuth token exchange审计发现CR-195只为MCP Streamable HTTP endpoint安装显式
> trust bundle，`ReqwestMcpOAuthCredentialBroker`仍读取默认CA集合。CR-196将同一exact Trust Policy编译结果加入OAuth verification/startup
> binding，绑定auth profile与token endpoint；HTTPS client只使用该bounded PEM bundle和canonical hostname。无新业务表、Resource字段、
> public route、Secret路径或current-state authority。

> 2026-08-26 implementation feedback（CR-191）：CR-190要求subscription refresh/reconcile创建`Context` Job且不新增aggregate，
> 但03 closed owner registry只允许`ContextQuery/ContextDataset`，没有合法owner可绑定已有MCP subscription。CR-191增加唯一
> `Context -> McpOperation` pair，并限定为Context owner transaction从exact `mcp_subscription` source row创建的refresh/reconcile Job；
> 不新增WorkClass、表、aggregate、route或MCP执行Context backend的权限。

> 2026-08-26 implementation feedback（CR-192）：CR-191只关闭了subscription refresh Context Job的合法owner pair，尚未冻结
> 该Job如何执行、何时成功以及commit-window如何恢复。CR-192把它定义为Context Worker拥有lease/retry/terminal authority的有界只读
> refresh attempt；Worker通过typed internal MCP Resource Refresh RPC请求MCP Host执行exact protocol I/O，Host重载Job fence与MCP closure，
> 只返回bounded digest/count evidence。首版不物化subscription cache/Observation，不新增表、aggregate、WorkClass、public route或Secret路径。

> 2026-08-26 implementation feedback（CR-193）：CR-192成功evidence若摘要整个attempt，会把heartbeat推进的Job `expected_version`
> 错当成远端业务identity，导致合法长调用在terminal commit失配。CR-193把`execution_identity_digest`限定为tenant/subscription/Job、
> worker generation、lease generation/token、physical attempt和exact request的不可变闭包；Host在dispatch时验证当时fence，Context Worker
> 续租后只更新owner commit fence。evidence不绑定可变Job version/lease expiry。

> 2026-08-26 implementation feedback（CR-194）：CR-192要求full reconcile按冻结profile执行有界`resources/list` +
> `resources/read`，但published MCP method machine registry只有`resources/read`，导致Host要么跳过list、要么使用未登记的自由method。
> CR-194将`resources/list`加入同一closed ReadOnly method registry及per-method limits；refresh transport仍由Host从cause与published profile
> 选择，Context Worker不得传method，且不增加Capability Invocation、public route、current-state authority或持久化正文。

> 2026-08-26 implementation feedback（CR-195）：真实MCP HTTPS last-hop接线发现process-installed endpoint catalog只保存
> exact Trust Policy ref，没有可执行的显式trust bundle/pin material；HTTP client只能落回默认trust store，违反04的Egress合同。
> CR-195要求MCP Egress启动目录为每个exact Deployment安装bounded显式PEM trust bundle并纳入startup config digest，TLS client只使用该
> bundle与canonical endpoint hostname；缺失、无效、错Deployment/Policy或默认trust fallback必须在发送HTTP bytes前拒绝。无新业务表、
> Resource字段、public route、Secret路径或current-state authority。

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

> 2026-08-25 implementation feedback（CR-188）：production Capability Worker接线发现HTTP/gRPC/MCP Implementation只冻结
> mapping digest，未冻结可由Worker执行的installed codec identity；Remote Deployment也未冻结required Worker manifest。仅凭digest无法
> 构造codec或证明claim落到资格镜像。09→07/10/13/17/18复核后冻结installed protocol codec manifest与remote Worker manifest binding；
> 运行时不解释自由模板或下载代码。

> 2026-08-25 implementation feedback（CR-189）：RemoteSearch正文要求canonical endpoint，但machine binding只保存digest/region，
> Context Deployment也缺少exact TLS/Trust Policy与required Worker manifest。02→04→07→12→17→18补齐immutable execution closure；
> 00～18复核确认无新aggregate/表/Job/WorkClass/route/role，禁止进程配置自由URL、默认trust store或明文Secret补全缺失事实。

> 2026-08-26 implementation feedback（CR-190）：production MCP subscription接线发现13虽要求通知触发Context invalidation/reconcile，
> 但12没有定义接收该命令的durable owner transaction，代码中`McpSubscriptionInvalidationTarget`也只有测试实现。CR-190冻结subscription
> refresh/reconcile admission为Context owner创建的shared `Context` Job + Receipt/Event/Outbox；MCP Host只提交exact request并在durable acceptance
> 后结算自身MCP Job，不得伪造work digest或以内存回调替代。

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
| 00 | `00-overview.md` | Accepted / CR-197 | 总体路线、规范模板、依赖和完成定义 |
| 01 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) | Accepted / CR-197 | 系统架构、领域对象和所有权边界 |
| 02 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) | Accepted / CR-196 | ID、Resource、Version、Deployment、Binding |
| 03 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) | Accepted / CR-197 | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) | Accepted / CR-197 | 多租户、授权、Secret、Effect、Quota、Approval |
| 05 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) | Accepted / CR-186 | Agent Interface、Typed Plan、Model Loop |
| 06 | [`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) | Accepted / CR-197 | Run、NodeExecution、暂停、重试、取消 |
| 07 | [`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md) | Accepted / CR-197 | Scheduler、Worker、Lease、背压和隔舱并发 |
| 08 | [`08-subagent.md`](08-subagent.md) | Accepted / CR-197 | Child Run、父子通信、取消传播和循环限制 |
| 09 | [`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) | Accepted / CR-188 | Capability Interface、Implementation、Registry |
| 10 | [`10-capability-invocation.md`](10-capability-invocation.md) | Accepted / CR-197 | 调用协议、幂等、同步快路径、异步恢复 |
| 11 | [`11-skill-system.md`](11-skill-system.md) | Accepted / CR-186 | Skill Package、发现、选择、绑定和依赖 |
| 12 | [`12-context-and-retrieval.md`](12-context-and-retrieval.md) | Accepted / CR-197 | ContextSource、检索、引用和数据权限 |
| 13 | [`13-mcp-host.md`](13-mcp-host.md) | Accepted / CR-197 | MCP Transport、OAuth、投影、Task 和 Subscription |
| 14 | [`14-sandbox-execution-plane.md`](14-sandbox-execution-plane.md) | Accepted / CR-197 | Python、Node、WASM、受信任 Shell、隔离和扩缩容 |
| 15 | [`15-artifacts-and-files.md`](15-artifacts-and-files.md) | Accepted / CR-197 | S3、内容寻址、上传、生命周期和内容安全 |
| 16 | [`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) | Accepted / CR-197 | Provider、Model Profile、ModelTurn、流式响应和预算 |
| 17 | [`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) | Accepted / CR-197 | 管理 API、Run API、事件流和错误模型 |
| 18 | [`18-deployment-observability-and-qualification.md`](18-deployment-observability-and-qualification.md) | Accepted / CR-197 | Kubernetes、指标、Tracing、压测、故障注入和验收 |

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
fresh PostgreSQL 16 r199又完成Timer到期恢复后进入SignalWait、强制终止第二个Worker、认证Signal owner事务唤醒，再进入HumanTask并强制终止
第三个Worker、由Task owner事务提交typed response，随后创建exact-binding durable child Run并在child Timer停车后强制终止第四个Worker；第五个Worker
恢复child、由critical-control scanner结算terminal child link与typed output、恢复parent并完成Return，parent finish Node唯一。Timer/Signal/Task/Child的
多进程kill/recovery至此闭合。后续r208/r217/r221已分别关闭Native、Remote HTTP/gRPC与Remote MCP ToolsCall的Capability process L3；
r233又以production Model Worker、mTLS Egress/NATS、错manifest零调用、Provider响应后commit-window强杀、expired-lease保守结算与安全重放
关闭Model provider process L3。r234之后又新增独立`platform-context-worker`与仅允许DNS/PostgreSQL出站的NativeCatalog部署角色；其
read-only候选扫描在claim前精确匹配冻结adapter contract与installed adapter digest，claim/heartbeat/recovery/terminal仍复用durable
ContextQuery/Job owner合同。fresh PostgreSQL 16 r240进一步通过错digest零claim、terminal commit窗口强杀、替代进程expired-lease恢复、
物理attempt 2和唯一Observation，关闭NativeCatalog process L3。Model tool-result整链、Context remote backend及Sandbox-backed Capability仍缺完整production多进程L3，因此不能据此
宣称Phase 2完成。

截至2026-08-26，r279又以fresh PostgreSQL 16、production Resource Host与subscription Context Worker进程及真实mTLS测试Egress service
覆盖subscription refresh的dispatch后Host/Worker强杀、response后/terminal commit前Worker强杀、expired-lease恢复和唯一completed Event。
Egress尚运行在测试进程内且未接真实Streamable HTTP fake server，因此该证据只关闭Host/Context进程恢复切片，不改变完整subscription L3、
隔舱容量与L4～L6仍未完成的边界。

r280随后实现CR-195：MCP installed endpoint携带显式bounded PEM trust bundle，实际POST/SSE client只信任该bundle；独立CA/SAN真实TLS
fixture跑通full reconcile list/read，错CA在零HTTP业务request时失败。该证据不等于Egress已进入独立OS进程恢复矩阵，也不推进L4～L6状态。

r281进一步以独立Egress测试进程、production Resource Refresh RPC/connector、production Host/Context Worker及真实TLS fake MCP server完成
initialize后Egress/Host/Worker强杀、list/read响应后commit-window Worker强杀和第三次expired-lease恢复；3次initialize、2次list/read只产生
一个completed Event。subscription protocol/crash component L3由此闭合，容量饱和、真实scrape及L4～L6仍保持未完成。

r286实现CR-196并以fresh PostgreSQL 16、真实独立CA HTTPS token endpoint、production OAuth reqwest broker、mTLS Egress RPC及Callback owner
完成token-store后/数据库commit前的多进程恢复。第一次exchange后同时强杀Callback/Egress，第二组进程从prepared token metadata恢复且不重发
one-time authorization code；endpoint调用严格为1，最终只有一个responded Task、Receipt和completion Event。OAuth callback/exchange component
L3由此闭合；Secret Manager rotation、容量饱和、真实scrape及L4～L6仍未完成。

r287从shared PostgreSQL Outbox authority以数据库时间导出fixed due/expired-claim/dead count与oldest lag；采样不读取Event payload，也不输出
tenant、Outbox/Event、claim owner或失败文本。fresh PostgreSQL 16、strict Clippy、13-panel dashboard、12条symptom-first alert及逐alert runbook
通过。该证据关闭shared Outbox backlog/recovery的L1接线；其他role authority、动态payload审计、真实scrape及L4～L6仍未完成。

r290完成CR-197 machine/runtime projection：公共HTTP严格校验W3C `traceparent`，Run、Invocation、Job、Task、Event与Outbox持久化同一trace ID；
首版实际MCP、Egress、Artifact、Sandbox与Security mTLS/UDS RPC在workload identity授权后校验trace，跨hop生成新span，durable reclaim/restart
恢复原trace ID。Egress provider及gVisor guest/storage边界保持零平台trace header。合同/schema、workspace strict Clippy、真实mTLS/UDS与fresh
PostgreSQL 16恢复测试通过。该证据关闭CR-197 trace implementation与component L3连续性；动态payload审计、真实scrape、telemetry
RBAC/retention及L4～L6仍未完成。

r291以真实loopback TCP listener启动shared production observability Router，发送payload/identity、`tracestate`和`baggage` canary后再抓取
`/metrics`；采集结果只出现fixed `other/rejected` series，全部canary及header名称为零。该证据关闭metrics adapter的component real-socket
scrape与动态metric payload负向切片，不替代Prometheus deployment scrape、log/trace动态采集、telemetry RBAC/retention或L4～L6。

r292为公共HTTP及内部RPC correlation安装fixed tracing spans；动态采集证明公共parent trace ID、per-hop span ID、accepted/rejected outcome与
internal same-trace/new-span字段存在。真实loopback provider路径注入prompt、response、token、query、tenant identity、`tracestate`和`baggage`
canary，production tracing只采集bounded request/response metadata且全部canary为零；公共扩展header拒绝span与RPC采集也为零。连同r291，仓库
component L3动态metric/log/trace payload canary闭合；production telemetry backend、RBAC/retention及L4～L6仍未完成。

r293从Sandbox Controller实际Artifact-response semaphore接入closed operational capacity series，scrape时读取available/used而非配置推断；
现有owner tests覆盖permit持有/释放。dashboard增至14 panel，并增加持续capacity exhaustion alert与runbook。动态capacity coverage达到10/19
pool；其余9个pool、production Prometheus scrape、telemetry backend/RBAC/retention及L4～L6仍未完成。

r294将Artifact Gateway的`download`、Data Worker的scanner/三类Scheduler/Sandbox五个read bulkhead及Maintenance的`delete`从各自实际
Artifact Broker semaphore接入capacity scrape。owner测试证明response lease持有、并发拒绝与drop恢复。三个Artifact pool闭合后动态capacity
coverage达到13/19；Gateway双pool、MCP双Host及Security/Egress六个pool、production scrape与L4～L6仍未完成。

r295从Management API与Runtime API各自真实SQLx PostgreSQL pool导出`postgresql_connections` available/used；真实PostgreSQL 16验证
checkout与异步归还会使series 0→1→0。两个Gateway pool闭合后动态capacity coverage达到15/19；MCP双Host及Security/Egress四个pool、
production Prometheus scrape与L4～L6仍未完成。

r296为MCP Tool Host与MCP Resource Host各自的真实RPC admission semaphore导出fixed `rpc_requests` available/used；capacity在构造时强制
注入，身份与trace授权之后、业务解码之前获取，饱和返回`ResourceExhausted`且permit释放后available恢复。closed配置与hard max、owner/config
tests、真实mTLS、受影响fixtures编译、strict Clippy及部署/observability门禁通过。动态capacity coverage达到17/19；仅剩Security Authority与
Egress Broker两个pool、production Prometheus scrape、telemetry backend/RBAC/retention及L4～L6未完成。

r297从Security Authority唯一实际SQLx PostgreSQL pool导出fixed `postgresql_connections` available/used；capacity是配置上限，used由
established减idle计算，available包含idle与尚可合法建立的槽位，不新增第二admission authority。fresh PostgreSQL 16验证checkout/drop使used
0→1→0；unit tests、strict Clippy及Security/Egress、observability门禁通过。动态capacity coverage达到18/19；仅剩Egress Broker、production
scrape、telemetry backend/RBAC/retention及L4～L6未完成。

r298从Egress Broker的11个真实隔舱owner导出capacity：Secret resolution/store、Model、HTTP/gRPC Capability、Remote Context、MCP OAuth、
普通/订阅MCP与subscription bridge pending/active；scrape只读取对应Semaphore maximum/available。OAuth与bridge owner tests验证permit持有、
饱和拒绝和释放恢复，真实HTTPS/mTLS、strict workspace Clippy及部署/observability门禁通过。动态capacity L1至此覆盖19/19 pool；production
Prometheus scrape、完整dependency health、L5 mixed-load/saturation profile、telemetry backend/RBAC/retention及L4～L6仍未完成。

r299在两个全新PG16 baseline（共享主authority与独立Model conformance authority）、真实NATS及当前production process binaries上完成串行
workspace all-target/all-feature L1～L3回归，退出码为0；两个外部S3测试保持显式ignored。该批修复Scheduling JSON-null候选污染、terminal
transaction serialization重试、MCP RPC trace、OAuth callback/cleanup exact binding与aggregate kind、数据库时钟timer边界及多进程fixture的tenant
scoping，并通过workspace strict Clippy、format、doc tests和OAuth 8/8真实TLS/kill-recovery复验。本轮未配置Model TLS NATS process fixture，且未运行
外部S3/KMS、production Prometheus、production-equivalent Kubernetes/runsc或L4～L6，因此这些release gate仍保持Pending。

r314为共享Egress RPC client建立closed transport observation port，覆盖Model streaming、Capability HTTP/gRPC、Remote Context及MCP
OAuth/cleanup/Tool/Resource/subscription的实际tonic返回边界；observer只接收success/failure，不接收业务身份、endpoint、payload或error，本地拒绝不污染计数。
真实mTLS成功与不可达端点失败测试及strict Clippy通过。各production process尚未注入该port，故role Egress series、production scrape/fault及L4～L5仍Pending。

r315把该port注入production Model Worker并接入既有PostgreSQL/NATS process metrics surface；实际Model建连、stream read与cancel仅导出固定
`model-worker + egress + outcome`，不改变readiness或业务语义。目标测试、strict Clippy和部署/observability/redaction门禁通过；production scrape、真实
Egress fault、其他client role及L4～L5仍Pending。

r316只在production Capability Remote Worker注入Egress observer，HTTP/gRPC调用与取消仅导出固定role/dependency/outcome并复用既有process surface；
Native继续保持PostgreSQL-only。目标测试、strict Clippy和双角色部署/observability/redaction门禁通过；production scrape、真实fault、其他Egress/MCP client
及L4～L5仍Pending。

r317只在production Remote Context Worker注入Egress observer并复用其PostgreSQL metrics surface；Native/Subscription保持PostgreSQL-only，实际查询
RPC仅导出固定role/dependency/outcome。目标测试、strict Clippy和Context部署/observability/redaction门禁通过；production scrape、真实fault、其他
Egress/MCP client及L4～L5仍Pending。

r318为production MCP Tool/Resource Host与OAuth Cleanup Worker注入Egress observer；Tool为Egress-only，Resource/Cleanup为PostgreSQL+Egress，
各实际transport只导出固定role/dependency/outcome。目标测试、strict Clippy及MCP部署/observability/redaction门禁通过；production scrape、真实fault、
Callback/Sandbox Egress client及L4～L5仍Pending。

r319为production Callback API OAuth exchange client注入Egress observer并与PostgreSQL sampler同surface；实际RPC只导出固定role/dependency/outcome。
目标测试、strict Clippy及Callback部署/observability/redaction门禁通过；production scrape、真实fault、Sandbox Egress client及L4～L5仍Pending。

r320以observability静态清单锁定七个first-release production Egress client全部注入observer；余下no-op仅限测试/fixture及release明确排除的
Firecracker/microVM provider，首发WASI/gVisor Sandbox没有Egress client。门禁通过；production scrape、真实fault及L4～L5仍Pending。

r321修复完整workspace门禁发现的rolling-summary测试时序耦合：18轮串行SQLite summary fixture使用30秒测试专用owner lease，production owner逻辑不变。
修复后全workspace all-target/all-feature tests、strict Clippy、format与doc tests通过；两个外部S3 fixture仍ignored，L4～L6无新增证据。

r322为首发Sandbox WASI/gVisor Executor补齐Core NATS dependency health；实际TLS connect、subscribe/flush、request/reply、stream closure与unsubscribe
仅导出固定role/nats/outcome，本地校验与业务字段不进入指标。目标测试、strict Clippy及Sandbox部署/observability/redaction门禁通过；本轮无真实NATS或
production scrape新证据。

r323以observability checker锁定全部first-release dependency owner及AWS/NATS adapter inventory；移除observer、sampler或production client注入会fail
closed。六类external dependency仓库内L1接线闭合；production scrape/fault、其他domain backlog/recovery series及L4～L5仍Pending。

r324修复Orchestration process scrape中的重复Prometheus标签集：PostgreSQL dependency transport outcome继续只由共享
`insight_platform_dependency_observations_total`拥有，durable queue查询本身的成功/失败改由
`insight_platform_durable_observations_total`表达。组合render测试锁定同一dependency series只出现一次；目标测试、strict Clippy、
observability/redaction、format与diff门禁通过。该修复不新增production scrape、fault injection或L4～L5证据。

r325抽取共享durable Job queue metrics owner并由Orchestration复用；Model Worker新增按typed `WorkClass::Model`的PostgreSQL只读sampler，固定导出
`due`/`expired_lease` count与oldest lag，失败保留上一有效snapshot。dashboard、三条Model/observation symptom alerts及runbook/closed threshold
门禁同步接线。相关目标26/26、baseline编译、strict Clippy及部署/observability/redaction门禁通过；本轮未配置fresh PostgreSQL或production scrape，
因此只关闭Model backlog/recovery仓库内L1接线，L2/L4～L5仍Pending。

r326为Capability Native/Remote两个production binary复用crate内共享sampler，分别按唯一typed `WorkClass::CapabilityNative`与
`WorkClass::CapabilityRemote`导出durable backlog/recovery lag；双角色共享固定告警但保留`component_role`隔离。目标13/13、strict Clippy、双部署、
observability/redaction/format/diff门禁通过；本轮无fresh PostgreSQL或production scrape，只关闭两条Capability queue的仓库内L1接线。

r327新增closed `DurableJobOwnerKind` selector，并让Sandbox Controller只观察`WorkClass::Sandbox + owner_kind=job`的execution queue，明确排除同
WorkClass下MCP-owned `sandbox_job`。固定due/expired alert与runbook接线；lib tests 14/14、strict Clippy、Sandbox部署、observability/redaction门禁通过。
本轮无fresh PostgreSQL、production scrape或runsc证据，只关闭Sandbox execution backlog/recovery的仓库内L1接线。

r328修复剩余Artifact/Context/MCP队列审计暴露的上游合同缺口：按spec03既有要求建立18项internal `JobKind`及25项合法
kind/work-class/owner三元组，生成registry与Python checker fail closed。contracts全目标、生成漂移与strict Clippy门禁通过；baseline typed column、
31个production INSERT/读取及claim JSON hot predicate替换仍待下一批，因此不宣称JobKind persistence或剩余queue metrics闭合。

r288新增独立production-candidate CI workflow：所有action固定commit SHA，且必须先以40位commit SHA只读checkout GitOps environment closure；
以两个Docker target构建exact-digest runtime与gVisor guest，生成并
签名SPDX SBOM、BuildKit/GitHub provenance、CandidateManifest和传递闭合的release-bundle index；Candidate冻结15个ComponentRole、7个实际
WorkerManifest、唯一baseline migration、contract/config/limit/policy/qualification摘要和可复现commit timestamp。CI静态门禁、生成器负向测试及
Rust `validate-production-candidate`通过。该批实现可执行的signed candidate producer，但尚无实际外部registry/GitOps运行产物、人工审批
或production-equivalent L4～L6运行证据，因此`signed_supply_chain`仍不得标记passed。

r289最终静态复核确认subscription Context Worker与MCP Resource Host加入后，15个ComponentRole当前映射为19个隔离workload pool，
其中9个LocalWorkerPools具备动态permit指标；早期r248～r267记录的17-pool是当时历史证据，不再代表当前拓扑。Security/Egress全局门禁同步
纳入CR-192 `RefreshMcpResources` closed RPC，继续要求Egress只暴露13个reviewed remote-only method。全局render、observability、redaction和
strict workspace Clippy通过；该更正不新增role、RPC或authority，也不替代live L4。

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
