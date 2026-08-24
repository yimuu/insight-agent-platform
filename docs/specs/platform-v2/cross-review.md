# Platform v2 00～18 Cross-review（CR-182）

| 属性 | 值 |
|---|---|
| 状态 | Closed / CR-182 Accepted |
| 日期 | 2026-08-24 |
| 输入 | 00～18 live tree、ADR-0001、ADR-0002、AGENTS.md、CR-182 selection program implementation feedback |

> CR-182重新打开04 selection program及05～11/16/18影响复核；CR-181历史结论保留，但在closed selector owner schema、
> deterministic evaluator、evidence重验和L1～L3 fixture闭合前不授权selection/leaf dispatch实现。

### CR-182 closed selection program impact review

- **Authority/schema**：04 Selection Policy Revision唯一拥有schema v1 document；evidence仍是immutable proof，不新增current aggregate/table。
- **Determinism**：only-candidate、ordered-first和canonical route-hash是closed纯函数；排序、hash、modulo由共享owner library实现。
- **Transactions/security**：06 owner transaction重算而非信任evidence；无health/time/random/fallback，wrong document/route/candidate整批回滚。
- **Downstream**：05 route presence与mode对齐；08/10/16冻结一次selected Deployment；09/11 publication拒绝不兼容mode；12～15/17无字段或
  authority变化；18增加L1～L3矩阵。Plan仍为v4，HardLimitProfile v5、23/22表、`/v1`与物理拓扑不变。
- **Recovery/capacity**：重放相同inputs得到相同selection，不新增queue/permit；selector bounded为既有slot candidate与route JSON limits。

CR-182逐份00～18影响复核没有未关闭P0/P1，形成Acceptance 25：Selection Policy document schema v1非空且closed，三个mode与route
presence/schema严格匹配，Scheduler与repository共享纯evaluator并由owner transaction重算；unknown/漂移/fallback全部fail closed。

> CR-181重新打开本cross-review。CR-180的历史结论保留为审计记录，但不再授权实现；需按05→06～16→17→18顺序
> 复核04 candidate selection及Plan v4 external leaf payload的state ownership、IDs、schema、transaction、event、permission、capacity、recovery与fixture后
> 才能关闭。
| 目的 | 验证简化后的状态、ID、schema、错误、事务、事件、权限、容量、恢复、Draft/Deployment/Run admission authority和fixture闭包 |

## 1. 结论

CR-180发现的terminal source P1已按05→06→07→17→18关闭：Plan v3用exact data port冻结Return final value与Raise safe
Failure，publication对齐exact Agent Interface output/error schema，Scheduler只物化immutable RunValue，owner transaction重新解析
Scope并重验value/schema/content/classification及正文后原子提交terminal。Plan v1/v2 clean-cut拒绝；不增加表、current projection、
WorkClass、public字段或兼容层。

CR-179发现的Loop carried Scope lifecycle P1已按05→06→07→18关闭：body settlement复制immutable carried RunValue并预建下一
iteration open Scope，continuation condition与同轮body复用该Scope，false exit原子关闭并回到固定词法父Scope；所有iteration Scope
由首次Loop controller直接拥有，不读取terminal Scope、不形成跨轮父链或自环。该修订沿用Plan v2、Scope payload、RunValue与既有
mutation/Event/Outbox合同，不新增表、profile、public字段、current projection或WorkClass。

CR-178发现的Map item port P1已按05→06→07→18关闭：Map冻结exact NodeOutput item port及element schema，publication验证array
element schema，runtime为每个item创建独立RunValue并绑定MapItem Scope。未发布Plan wire提升到version 2；不保留version 1兼容，
不新增表、profile、public字段、current projection或WorkClass。

CR-177发现的expression-derived classification P1已按05→06→07→18关闭：classification不是Plan/caller自由字段，repository从
提交事务内重验的external input RunValue取lattice join，Compute全部output继承该结果，空input closure固定`Internal`。该修订
不新增schema字段、profile字段、表、current projection、WorkClass或public surface，并消除了caller降级与Artifact metadata替代
RunValue authority的路径。

CR-176发现的data-port binding P1已按03→05→06→07关闭：immutable value仍由`run_values`唯一拥有，既有Scope aggregate payload
拥有bounded exact port→RunValue current environment；root/Compute/Map/Loop以同事务Scope CAS更新，Scheduler按bounded词法scope链解析并
在commit重验。Inline JSONB结构guard与profile hard max对齐。不新增表、current projection、trigger或WorkClass。

CR-175发现的expression profile P1已按05→18关闭：`HardLimitProfile v5`为instruction/input/stack增加独立typed limit，
代码绝对上限、profile hard max、deployment effective value和overflow outcome形成单一闭包；旧version、缺字段、错误unit、零值或放大
绝对上限均fail closed。00～04、06～17的state、ID、事务、权限、API和表预算经影响复核不变，不增加表、aggregate或WorkClass。

CR-174发现的controller observation P1已按05→06→07→17→18关闭：Plan Revision冻结closed typed expression IR与
exact input ports；Branch/Map/Loop/Compute结果只能由Scheduler从immutable RunValue派生，owner transaction重验
expression/RunValue/Node/Job evidence并原子提交；public API与generic internal proxy禁止注入observation；qualification加入
opcode、digest、跨tenant/run、原子回滚和多进程负向矩阵。该修订不增加表、current projection、WorkClass或外部执行后端。

CR-173发现的definition-only Deployment P0已按
02→04/11/14→05/06/09/10/12/13/15/16→17→18顺序关闭。Skill、Policy、Sandbox现在与其余public noun一样使用
immutable exact Deployment closure、Resource active binding和AdministrativeGate；owner registry、Run/Sandbox binding、PostgreSQL
lifecycle、generated owner schema与八类public route保持同一authority。Acceptance 13～17的正负、并发、Receipt/Event/Outbox与
machine-contract门禁通过，00～18全量复核未发现新的P0/P1合同冲突。

因此CR-180恢复Implementation Authorization并将受影响合同推进为Accepted。Accepted只表示target合同闭合；它不表示target已成为
current production behavior，也不替代18要求的L4～L6、CapacityProfile、restore/soak、signed supply-chain或GitOps cutover证据。

CR-171继承CR-170的public Artifact DTO与可信服务交接结论，并消解实施反馈发现的tenant Artifact default Policy authority缺口。全量审查确认首版目标收敛为：

- Sandbox = restricted WASI + single-Job gVisor；microVM/Firecracker/KVM推迟；
- MCP = remote Streamable HTTP；Managed stdio和persistent Sandbox session推迟；
- Model = bounded Inline request/response；无Model Artifact Producer/Broker；
- Artifact = Gateway、Data Worker、Maintenance三个物理role；
- Operation = shared Job的safe projection；无ManagementOperation aggregate/table；
- release/promotion/rollback = GitOps/CI/CD/Kubernetes；无Installation Release/Gate runtime state/API；
- storage/KMS = deployment-time static binding；无tenant self-service dynamic binding API；
- public protocol = `insight.platform/v1` + `/v1`；clean cut，无`/v2`或兼容层；
- target persistence = schema contract v7、23张总表/22张业务表，不增表；
- editable Draft = Resource aggregate的唯一current snapshot；publication才创建immutable ResourceVersion；
- Deployment = immutable exact closure；Resource active binding + AdministrativeGate是未来Run admission的唯一current authority；
- root Run request显式选择Agent Resource；immutable Agent Deployment冻结validated Plan entry，admission不接受内部entry/binding或读取Artifact猜测；
- implementation plan = 四阶段，证据按层级归属，不复制proof。

本次实施反馈发现并消解一个P1合同缺口：17只写“tenant active Agent Deployment”，无法在多Agent tenant中确定目标；05的Typed Plan
拥有entry node，但02的Agent Deployment closure未冻结入口，而durable admission命令又需要entry ID/kind。若让调用方提交内部入口或让
Gateway在事务外读取Plan Artifact，都会破坏exact closure与单事务admission。05先把validated entry纳入immutable Agent Deployment
closure，06明确root admission从所选`agent_id`的enabled active binding解析入口，17随后固定closed public request并禁止内部ID/closure；
00～18在复核期间退回Architecture Revision，并在本次00～18全量复核无新增P0/P1后作为同一批次重新推进为Accepted。Accepted仍不表示target已经成为
current behavior；只有通过实现与资格门禁后才能推进状态。

后续Artifact public API审计又发现一个P1合同缺口：candidate Artifact Gateway把Blob/Job/Receipt/Event/Outbox、policy/fence和grant token
作为所谓public request字段，并仅以自由principal header承接Public Gateway身份；15/17虽然禁止公开内部authority，却未冻结可替代的public DTO与
内部hop认证。CR-170先在15冻结业务意图、Secret-bearing upload target例外、server-owned identity/policy与mTLS + current principal rebinding，
再在17冻结exact public DTO、header/status/Location/ETag语义。01～14、16、18经00～18全量影响复核无需语义变更；该修订不新增aggregate、表、
Artifact role或第二current authority。

实现resolver时确认tenant可同时存在多个active Retention或ArtifactIo Policy Resource，而原`TenantConfig`只有scheduling slot；“服务端选择policy”
因此不具确定authority。CR-171在04现有tenant current config内增加两个exact revision slot，15/17要求public Artifact prepare只读取这些slot并验证
kind/digest/current gate。绑定沿用Tenant CAS/Receipt/Event/Outbox，不新增表、aggregate、head projection或安装级fallback；其余00～18影响复核无变化。

Context public build实施审计又发现两个P1合同缺口：首次build没有可引用的ContextDataset root，且DatasetGeneration所需chunker/embedding未在
Context Deployment闭包冻结；MCP discover route也未说明authorization binding authority。CR-172先在12冻结完整build closure、预留ID与
成功时物化规则，在13冻结显式authorization input及同事务重验，再由17固定两个closed DTO。03的shared Job、02的Resource authority、18的
隔舱与资格语义不变；失败时预留ID不成为Resource，因而不产生空root、第二current state或新表。00～18影响复核未发现新增P0/P1。

## 2. 文档状态与依赖

| 范围 | 状态 | Cross-review ruling |
|---|---|---|
| 00 | Accepted / CR-182 | target协议、authority与current-behavior边界闭合 |
| 01～03 | Accepted / CR-173/176（CR-181影响复核） | owner、ID、Resource与persistence authority不变 |
| 04 | Accepted / CR-182 | exact candidate selection program/evidence与owner重验闭合 |
| 05～07 | Accepted / CR-182 | Plan v4 leaf/terminal ports、Scope binding与Scheduler边界闭合 |
| 08～16 | Accepted / CR-182 impact review | domain owner只消费exact dispatch/result snapshot，无第二authority |
| 17 | Accepted / CR-181 | public/internal generic proxy禁止leaf与terminal authority注入 |
| 18 | Accepted / CR-182 | profile v5不变；Plan v4 L1～L6资格矩阵闭合 |
| ADR-0001 | Accepted | target v7/23/22与GitOps/Job/Artifact简化对齐 |
| ADR-0002 | Accepted | gVisor改为受限Launcher + admission-locked single-Job Pod；Job authority不变 |
| implementation-plan | In Progress | L1～L3与public contract已恢复；L4～L6、CapacityProfile和GitOps cutover仍待外部资格环境 |

依赖图为`00 -> 01 -> 02/03/04 -> 05～16 -> 17 -> 18 -> cross-review -> implementation-plan`。
18不再是17的Release state上游，因而不存在17→18→17的循环。

### 2.1 CR-174 controller observation cross-review

- **State ownership**：expression program属于immutable Plan Revision；input正文唯一authority仍是immutable RunValue；
  Run/Node/Job拥有current execution。Observation是command派生evidence，不建表、不成为第二current state。
- **IDs与schema**：程序只引用exact data port与RunValue ID；closed opcode/unknown-field、stack/type/output bounds进入05 owner schema；
  public DTO不新增字段。增加opcode提升`expression_version`。
- **事务与并发**：事务外纯求值允许降低锁时长，但commit必须重验Plan/expression digest、RunValue identity/schema/content、
  Node version及Job lease fence，并把RunValue/Scope/Node/Job/Receipt/Event/Outbox作为一个first-winner原子批次。
- **错误**：parse/type/stack/output/digest错误稳定映射Plan/schema/integrity failure；fence/version漂移是first-winner loss；原始表达式正文
  与值正文不进入public problem、metric label或默认log。
- **权限与隔舱**：只有Scheduler workload可调用Typed Plan Data RPC和提交controller command；evaluator无Provider/MCP/Context/Secret/
  Sandbox egress，使用Orchestration permit且不占critical-control reserve。
- **恢复**：Map首次提交冻结item source/count/cursor，Loop iteration冻结carried refs；重启从这些durable payload恢复，不重复读取active
  Resource或进程内observation。NATS仍只负责wake。
- **fixtures**：L1 opcode/property，L2 real PostgreSQL digest/fence/cross-tenant/atomicity，L3多进程自行导出Branch/Map/Loop/Compute；
  手工构造`ControllerObservation`只保留为repository负向单元输入，不计production handler证据。

00～18其余合同已按state ownership、IDs、JSON schema、errors、transactions、events、permissions、capacity、failure recovery和fixture
逐项复核：CR-174不改变Artifact/Model Inline、remote MCP、WASI+gVisor、GitOps release、23/22表预算或`/v1` clean-cut结论。

### 2.2 CR-175 expression hard-limit cross-review

- **State ownership**：18的versioned deployment file仍是HardLimit authority；Plan/program只冻结并验证effective bounds，不复制可变profile。
- **IDs与schema**：profile version提升到5，`registry_plan`增加三个required closed字段；machine schema和所有checked-in profile fixture同源更新。
- **错误与事务**：profile startup/publication validation在任何Run提交前fail closed；evaluation超限仍由既有owner transaction映射稳定
  content/plan failure，不新增Receipt/Event种类或current projection。
- **权限与容量**：tenant/policy只能收紧effective value，不能超过profile hard max或代码绝对上限；instruction/input/stack各自计量，禁止借用
  node/edge/branch limit，使Scheduler expression permit可被独立压测和观测。
- **恢复与fixtures**：重启读取同一startup manifest/profile digest；L1覆盖旧version、missing/unknown field、wrong unit、zero、放大和边界值，
  L3覆盖Scheduler使用profile有效值拒绝Plan。没有DB migration、表或外部网络变化。

CR-175全量影响复核确认01～04、06～17无需字段或语义修改；Artifact/Model Inline、remote MCP、WASI+gVisor、GitOps release、
23/22表预算与`/v1` clean-cut结论保持不变。

### 2.3 CR-176 data-port environment cross-review

- **State ownership**：RunValue row拥有immutable value；Scope payload只拥有current port binding。Node单一`output_value_id`继续只表示
  leaf/final output，不承担多port环境；Event/Receipt只保存evidence digest。
- **IDs与schema**：binding使用Plan `ExactDataPortRef`和`ExactRunValueRef`，同时重验tenant/run/schema/content；Scope payload schema version、
  size、binding count和canonical digest closed，unknown field fail closed。
- **事务与并发**：root input、Compute outputs、Map item和Loop carried binding与RunValue insert、Scope version CAS、Node/Job、quota、
  Receipt/Event/Outbox同事务；duplicate port、stale Scope或Job fence只有一个winner且无孤立RunValue。
- **权限与隔舱**：只有Scheduler owner transaction读写environment；public/API、NATS或Artifact RPC不能提交port binding。Artifact正文仍按15授权读取。
- **容量与恢复**：词法lookup复用`registry_plan.plan_nodes`、binding count复用`run_scheduler.value_refs_per_run`，不新增隐式profile字段；
  Inline DB结构guard容纳hard max，runtime按更小effective值拒绝；重启从
  Scope payload和immutable RunValue恢复，不依赖进程缓存。
- **fixtures**：L1覆盖digest/shadow/duplicate/depth，L2 real PostgreSQL覆盖root/Compute/Map/Loop原子CAS与跨scope/tenant负向，L3证明
  两个Map item的同Plan port不串值以及crash后重放一致。

CR-176全量影响复核确认不改变public DTO、外部协议、Artifact/Model/MCP/Sandbox拓扑、23/22表预算或GitOps发布权威。

### 2.4 CR-177 expression classification cross-review

- **State ownership**：immutable RunValue row继续唯一拥有classification；Plan不复制或开放output classification，Evidence/Event不成为current authority。
- **IDs与schema**：不增加ID、wire字段、profile字段或schema version；现有`DataClassification` closed lattice与RunValue字段足够表达结果。
- **事务与并发**：commit重验exact input identities/schema/content/classification后计算join；Compute outputs、Scope CAS、Node/Job、Receipt/Event/Outbox
  同一first-winner事务，stale fence或任一漂移整批不可见。
- **错误与权限**：caller/Worker提供classification不参与决策；任何降级或Artifact metadata不一致映射integrity/contract failure，正文与等级证据不进入public problem。
- **容量与恢复**：join只扫描05已界定的bounded external input closure，不增加I/O、queue、permit或恢复状态；重启从immutable rows重新计算同一结果。
- **fixtures**：L1覆盖四级lattice、空input与多assignment；L2覆盖caller降级、input/classification漂移和原子回滚；L3验证production driver不接受外部等级。

CR-177已逐份复核00～18的state ownership、IDs、JSON schema、errors、transactions、events、permissions、capacity、failure recovery和fixtures；
03 persistence、08～16 domain protocol、17 public DTO、18 profile字段、23/22表预算与GitOps clean cut均无需新增语义。

### 2.5 CR-178 Map item port cross-review

- **State ownership**：Plan v2拥有exact item port/schema；item正文唯一authority是immutable RunValue，MapItem Scope只拥有current binding。
- **IDs与schema**：`item_port`使用既有ExactDataPortRef NodeOutput variant；producer必须是当前Map node，element schema由Compiler验证；无自由kind/annotation。
- **事务与并发**：每批item RunValue、MapItem Scope/Node/Job、cursor、Receipt/Event/Outbox同事务；wrong ID/schema、stale fence或重放无部分item。
- **错误与权限**：version 1、wrong producer/schema、array element validation失败在mutation前fail closed；caller/public不能提供port或item value authority。
- **容量与恢复**：item数仍受`map_items`与fan-out batch限制，RunValue引用受既有limit；重启从immutable inputs和pending cursor确定性重算批次。
- **fixtures**：L1 Plan v1/producer/schema负向；L2 fresh PostgreSQL多item、批次、dynamic Scope隔离与回滚；L3 crash/replay不重复或串值。

CR-178全量影响复核确认不改变03表结构、08～16外部协议、17 public DTO、18 profile字段、23/22表预算或GitOps发布权威。

### 2.6 CR-179 Loop carried Scope lifecycle cross-review

- **State ownership**：carried正文/classification仍由immutable RunValue拥有；每个iteration Scope只拥有该轮exact port binding，Loop Node/Job拥有推进状态。
- **IDs与schema**：沿用Plan v2 `LoopCarriedPort`与既有Scope/RunValue ID；pair两端必须是NodeOutput、schema相同，next producer为Loop、body producer属于body region。
- **事务与并发**：body settlement重验current Scope/Node/Job及body output evidence，复制RunValue、关闭当前Scope、创建下一Scope、切换pending continuation scope、wake、Receipt/Event/Outbox同事务；任一ID冲突整批回滚。
- **错误与权限**：缺失/跨region/schema不等/漂移映射closed plan或integrity failure；caller、Worker和public API不能提供carried正文、port、classification或iteration。
- **容量与恢复**：每轮只增加声明数量的bounded RunValue与一个Scope，iteration Scope直接指向固定root controller owner，不形成随iteration增长的词法深度；重启从pending wait、open Scope和immutable refs恢复。
- **fixtures**：L1覆盖pair schema/producer/duplicate；L2覆盖两轮不串值、Scope复用、false close、stale fence/ID冲突/Receipt replay；L3覆盖settlement与wake间crash恢复。

CR-179全量影响复核确认不改变baseline表、Plan wire/profile版本、08～17外部协议/public DTO、Artifact/Model/MCP/Sandbox拓扑或GitOps发布权威。

### 2.7 CR-180 Return/Raise terminal authority cross-review

- **State ownership**：final正文/Failure唯一authority仍是immutable RunValue；Return/Raise只冻结exact port，Run拥有唯一terminal state，
  Receipt/Event只保存bounded evidence，不建立terminal value projection。
- **IDs与schema**：沿用ExactDataPortRef、ExactRunValueRef与Agent Interface Revision；Plan wire提升到version 3且不接受v1/v2。
  Return schema必须等于output schema，Raise schema必须等于error schema并解码为safe Failure。
- **事务与并发**：terminal owner锁定Job/Run/Node/root Scope，按词法环境解析port并重验tenant/run/value/schema/content/classification；
  RunValue/output link、Scope/Node/Job/Run、quota、Receipt/Event/Outbox同一first-winner事务，stale fence或任一漂移整批不可见。
- **错误与权限**：missing/cross-run/closed Scope、wrong producer/schema、正文schema失败映射closed plan/integrity failure；public、Worker、
  NATS和generic internal proxy均不能提交terminal正文、Failure、schema或classification。
- **容量与恢复**：lookup与正文上限复用既有Scope/value-ref/Inline/Artifact profile；Artifact读取沿用Data Worker exact lease授权；重启从
  Plan、Scope和RunValue恢复，不依赖进程缓存或第二queue。
- **fixtures**：L1 Plan v1/v2与port/schema负向；L2 fresh PostgreSQL覆盖Inline/Artifact、shadow、stale fence、ID冲突、原子回滚/replay；
  L3覆盖Data RPC物化与terminal窗口kill/restart。L4～L6沿用既有topology/capacity/release门禁。

CR-180逐份复核00～18的state ownership、IDs、JSON schema、errors、transactions、events、permissions、capacity、failure recovery和fixtures；
01～04、08～16无需新字段，baseline仍为23/22表，HardLimitProfile仍为v5，Artifact/Model/MCP/Sandbox/GitOps与`/v1` clean-cut不变。

### 2.8 CR-181 executable external leaf cross-review

- **State ownership**：Plan v4只拥有immutable leaf intent；Capability/Context/Model/Task/ChildRunLink/Wake各自拥有business current state，
  shared Job拥有物理work，RunValue/Scope拥有typed data；Selection Evidence、Receipt/Event不成为第二current authority。
- **IDs与schema**：external node冻结exact slot、input/output/route port、budget/deadline/retry/wait字段；HumanTask使用shared
  `TaskDefinition::Interaction | HumanWork`子集与唯一TaskKind wire；Plan提升v4并拒绝v1/v2/v3。04 evidence绑定exact Policy Revision、
  ordered candidate digests、route ExactRunValueRef和selected ExactDeploymentRef。
- **事务与并发**：Scheduler可执行closed selector但owner transaction重放Plan/Policy/Scope验证；dispatch原子关闭Orchestration Job并创建
  一个Invocation/Query/ModelTurn/Task/Wake/Child，terminal owner原子写声明output RunValue和唯一resume Job。stale fence、ID冲突、并发
  callback/terminal/recovery均first-winner，无partial owner/value/resume。
- **错误与权限**：零candidate、ambiguous selector、集合外selection、wrong route/input/output/schema/classification、caller-supplied child
  entry/Task/Wake/result/resume均fail closed；public/internal generic proxy、Worker、MCP、Sandbox、Artifact和Model provider不能解释Plan或扩大bindings。
- **容量与恢复**：dispatch不占leaf I/O lane；各owner Job仍进入既有WorkClass隔舱，waiting释放permit；Outbox/NATS丢失和进程kill由bounded
  safety scan从owner current state恢复，不增加表、queue authority或HardLimitProfile版本。
- **fixtures**：L1覆盖Plan旧版和closed payload/schema/selector；L2 fresh PostgreSQL覆盖selection/dispatch/result first-winner与回滚；L3独立
  Scheduler/leaf/Artifact Data/critical-control进程覆盖全部leaf和kill window；L4～L6继续覆盖identity、topology、saturation、chaos/soak/restore。

逐份影响结论：00恢复实现授权；01～03 owner/persistence不变；04增加closed selection evidence；05使用Plan v4；06～08冻结dispatch/
subagent事务；09～16只允许domain owner消费exact snapshot且无新authority；17拒绝内部字段；18增加分层资格。baseline保持23/22表，
公开协议仍为`insight.platform/v1`与`/v1`，无兼容层、dynamic installer、managed stdio、microVM或Model Artifact路径。

## 3. 状态所有权

| 业务事实 | 唯一current-state authority | 历史/调度映射 |
|---|---|---|
| Resource Draft/definition/head | Resource current Draft / immutable ResourceVersion / immutable Deployment / Resource active binding + gate | Event/Outbox |
| Run/Node control | Run/NodeExecution | Job/Event/Outbox |
| Capability business call | CapabilityInvocation | Job/Event/Receipt |
| Child agent | child Run + typed parent link | Job/Event/Receipt |
| Model business turn | ModelTurn/invocation aggregate | Job/Event/Receipt |
| Context query | Context query owner | Job/Event/Receipt |
| MCP remote continuation | Invocation WakeContract/Task | Job/Event/Receipt |
| physical attempt/lease/retry | shared Job | Event/Outbox |
| human/approval/input | shared Task | Event/Receipt |
| idempotency/callback | shared Receipt | Event |
| Artifact metadata/blob/link/grant | Artifact domain aggregates | Job/Event/Receipt |
| release/current environment | GitOps + Kubernetes rollout | signed CI artifacts/rollout history |

Event是append-only history，不是第二current projection。NATS是wake/committed delivery，不是execution authority。
Job只拥有physical work state，typed owner只拥有business state，不复制lease或terminal outcome。

## 4. ID 与typed owner

复核后的ID规则：

- 一个实体一个nominal ID/prefix，不用裸UUID推断kind；
- Sandbox physical work只有`JobId`，不存在`SandboxJobId`或同UUID alias；
- public Operation ID就是`JobId`，DTO字段为`operation_id: JobId`，不定义独立`OperationId`；
- `RunValueId`与Job/Artifact ID独立，不使用同UUID typed projection；
- 无`InstallationId`业务scope或Release singleton identity；
- TypedOwnerRef的variant与合法`JobKind x OwnerKind`对由03 machine registry唯一生成；
- ArtifactLink owner只使用closed typed owner，不包含ManagementOperation。

Job owner正向矩阵至少覆盖ResourceVersion validation、MCP Deployment discovery、ContextDataset build、Invocation execution、
Artifact verify/delete与recovery。unknown kind、unknown owner、错ID prefix、跨tenant或非法kind-owner组合fail closed。

## 5. Schema 与machine contract

每个业务schema有一个owner nominal Rust type/closed registry。OpenAPI、protobuf、JSON Schema、Receipt result和DB JSONB投影
从owner type生成或由conformance test逐字段对照，不要求人工同时编辑多份对等schema。

JSONB统一要求：`schema_version`、closed validation、nominal IDs、hard size/depth/items/string/bytes limits、
canonical serialization、digest和immutable-after-publication。低频evidence/detail进JSONB，tenant、owner、state、version、lease、
scheduling time与hot predicate保留typed relational columns。

Draft只存在于Resource current payload并由Resource projection version + draft generation CAS；validation evidence绑定exact generation与
document digest。publication在同一事务创建immutable ResourceVersion batch并推进Resource projection。`resource_versions`不保存mutable
Draft row，Event也不成为Draft current projection。

checked-in schema contract v7只承载clean-cut ID/owner/snapshot收敛，不新增表，也不为Draft增加第二authority。

## 6. 错误与结果

HTTP/gRPC只表示当前command/transport结果，domain terminal failure由typed resource state + stable failure code + Event evidence
表示。不以自由字符串扩展状态机。

统一错误属性：stable code、retryable、bounded retry-after、safe detail、field errors、trace ID。Secret/token、prompt/body、
object locator、backend raw error和内部SQL不进public problem/Event/log。timeout不把不确定外部Effect伪造为安全failure。

Operation view的error只是Job safe failure projection，没有第二Operation terminal authority。Model超出Inline hard limit固定为
`model_output_too_large`，不自动Artifact fallback。

## 7. 事务、锁序与fence

通用mutation顺序：

1. Receipt claim/replay；
2. tenant/principal/security/quota authority；
3. typed parent/current aggregate；
4. Job/Task/Artifact等child aggregate；
5. quota/link/grant/other subordinate fact；
6. Event；
7. Outbox；
8. Receipt completion。

所有current mutation需要expected projection version；Job commit还需要lease generation、worker process generation和database-time lease有效。
external I/O不在长PostgreSQL transaction内执行，使用prepare/attempt/evidence/commit与reconcile。

ArtifactLink的`created_with_owner_projection_version`只是create-time CAS evidence。后续read/authorize只验证Link Active、
tenant、owner identity/relation和current authorization，不要求其等于owner current version。release command另携操作时
owner current expected version和Link expected version。

## 8. Event、Receipt 与Outbox

- Event记录committed business transition/outcome/rejection/audit evidence，不存current state；
- Receipt以tenant + principal/workload + operation scope + idempotency key去重，保存owner-generated typed result；
- Outbox与业务mutation同事务，publisher至少一次投递，consumer幂等；
- callback/poll/approval/input使用Receipt争用current fence，first-winner推进；
- NATS丢失或重复不改变业务结果，safety scan从PostgreSQL恢复。

Receipt replay必须从stored typed result重建原status/body/ETag/Location，不重读current aggregate。内部schema ID/registry
从owner types生成，不在17人工维护一组平行入口。

## 9. 权限与信任边界

| Role | 允许 | 禁止 |
|---|---|---|
| Management API | Resource lifecycle command | Run drive、code execution、storage admin |
| Runtime API | Run/Task/Artifact public command | Scheduler decision、Secret resolution |
| Scheduler/Worker | exact WorkClass claim/commit | arbitrary owner/table、active head mutation |
| MCP Host | remote Streamable HTTP protocol | stdio spawn、raw token、Sandbox execution |
| Sandbox Controller/Executor | fenced WASI execution；admission-locked gVisor single-Job Pod lifecycle | API process spawn、Executor DB write、runc fallback、通用Kubernetes管理 |
| Artifact Gateway | principal upload/download | internal workload authority、maintenance |
| Artifact Data Worker | closed workload stage/read/verify/derive | public principal API、Ready owner commit |
| Artifact Maintenance | closed retention/delete/GC transition | 新business reference、public upload |
| Egress/Secret Broker | catalog egress、last-hop Secret resolution | business current state、returning raw Secret |

所有跨进程边界mTLS认证exact workload audience并重绑定tenant/owner/Job fence。plain URL/header、object key、shell command、
Secret value和任意JSON owner不是可信请求输入。

## 10. 容量与拓扑

最低隔舱：API、Orchestration/Recovery、Model、Native/Remote Capability、Context、MCP、Sandbox Controller、
WASI Executor、gVisor Pod Launcher/guest pool、Artifact Gateway、Artifact Data Worker、Artifact Maintenance、Egress/Secret Broker。

隔舱意味独立queue、permit、DB/storage/client pool、ServiceAccount与autoscaling signal，不意味每个domain都拆成服务。
Sandbox或Artifact饱和时API、Scheduler、Model和critical-control仍必须准入。Model没有output materialization专用池；
MCP没有stdio/microVM session pool。

容量数值只由18已资格CapacityProfile拥有。Draft期间的数字不是current capacity。

## 11. 失败恢复

| 失败 | 恢复合同 |
|---|---|
| API/Scheduler/Worker restart | 从Receipt/Run/Job/Event/Outbox恢复 |
| lease loss | 新generation接管，旧generation fail closed |
| NATS loss/duplicate | safety scan/outbox replay + idempotent consumer |
| external timeout | Effect-aware retry/reconcile/UnknownOutcome |
| MCP disconnect | durable cursor/task/wake contract恢复或stable failure |
| Sandbox process kill | termination/absence evidence + cleanup + new Job generation |
| S3/KMS uncertainty | exact object generation reconcile，不伪造Ready/Delete success |
| rollout | drain、handoff、lease expiry和compatible worker pool |
| backup restore | PITR + Artifact consistency + fence invalidation + outbox/recovery scan |

延迟工作不持有常驿future或business permit。不对外部副作用声称exactly-once。

## 12. 资格fixture矩阵

| 层级 | 必须证明 |
|---|---|
| Domain | closed schema/state/policy/determinism/property |
| PostgreSQL | tenant/CAS/lease/Receipt/Event/Outbox/migration transaction |
| Component | adapter/protocol/runtime、crash/restart、bounded I/O |
| Topology | mTLS/RBAC/NetworkPolicy/role permission matrix |
| Capacity | mixed load、single-lane saturation、critical-control reserve、soak |
| Release | supply chain、GitOps rollout/rollback、backup/restore |

必须显式包含的负向fixture：

- ID/owner kind/prefix/tenant mismatch；
- unknown schema field/enum/version、oversize/depth/items和digest mismatch；
- duplicate Receipt/callback/outbox、old projection/lease/process generation；
- Job public kind-target非法组合；
- ArtifactLink owner正常version推进后仍可读取，错current release fence失败；
- API中spawn runtime、MCP stdio、runc fallback、microVM、Model Artifact与dynamic storage route不存在；
- three Artifact roles的mutual-deny权限矩阵；
- Sandbox/Artifact/MCP/Model分别饱和时的cross-lane可用性；
- Secret/prompt/body/object locator在log/metric/Event/problem中无泄漏；
- GitOps回滚不需读写Installation Release row。

证据存在CI artifact store/Git/container registry/Kubernetes rollout history，不存运行时GateResult/ReleaseManifest表。

## 13. Persistence 预算复核

ADR-0001的23张总表/22张业务表目标符合以下规则：

- shared Resource/Version/Deployment/Run/Invocation/Job/Task/Event/Receipt/Outbox代替每领域副本；
- Artifact/Blob/Link/Grant因独立存储和安全生命周期保留domain persistence；
- 无Installation Release、ManagementOperation、SandboxJob、MCP session、Model Artifact producer、Gate/evidence/transition专用表；
- 第24张表及以上必须在ADR中证明independent lifecycle/concurrency/core query，并说明shared aggregate为何不能表示。

表数是设计预算，不是test oracle或feature-completion evidence。

## 14. 本次冲突消解

| 原冲突 | reviewed resolution/input |
|---|---|
| `SandboxJobId`/同UUID alias与shared Job冲突 | 只保留JobId，RunValueId独立；无Sandbox child aggregate |
| ArtifactLink stored owner fence会随owner正常推进而失效 | stored version只是create-time CAS evidence；read不与current version比较；release另携current expected version |
| ManagementOperation只允许Artifact target，但MCP/Context也创建Operation | 删除aggregate：Operation是Job projection，target从Job kind-owner registry投影 |
| Installation Release把GitOps事实复制到DB | GitOps/Kubernetes是release authority，DB不新增state/API/table |
| Model output导致Producer/Broker/容量/Artifact状态爆炸 | 首版Inline-only，文件由Capability/Sandbox Artifact port产生 |
| Artifact八role权限与容量矩阵过度分裂 | 收敛为Gateway/Data Worker/Maintenance三role，内部用closed caller capability区分 |
| microVM/Managed stdio在首版引入Provider/session/child Job恢复 | Sandbox只WASI+gVisor，MCP只remote HTTP，全部推迟 |
| direct runsc嵌套Pod同时要求无host/cgroup/runtime权限，拓扑不可启动 | ADR-0002：受限Launcher创建admission-locked `RuntimeClass=runsc` single-Job Pod；guest无Kubernetes API，Job仍是唯一physical-work authority |
| 02把Draft写成mutable ResourceVersion、17又要求Version validate/publish route | Draft由Resource aggregate唯一拥有；17使用`/draft` update/validate/publish，publication后才有immutable Version GET identity |
| 02给Deployment可变state/version却又要求immutable，17的`suspend`未指定authority | Deployment是immutable closure；activate/suspend以Resource ETag做CAS，只改Resource active binding/gate |
| public Run未选择Agent且admission entry无durable authority | request显式携带`agent_id`；Agent Deployment冻结validated entry ID/kind，admission不接受内部入口或临时读Artifact |
| Artifact candidate把内部ID/policy/grant token当作public request并只信自由principal header | public DTO只含业务意图与opaque completion proof；服务端生成内部identity/closure；Public Gateway到Artifact Gateway使用exact audience mTLS并在DB重绑定current principal |
| 多个active Artifact Policy使“服务端选择”没有唯一authority | Tenant current config新增exact Retention与ArtifactIo revision slot；Artifact prepare验证slot kind/digest/gate，禁止任取active row或fallback |
| 02要求统一Deployment binding，但Skill/Policy/Sandbox仍直接激活Version | CR-173目标：增加closed `skdep`/`pdep`/`sxdep` closure；definition-only Deployment不执行代码，但冻结requirement/applicability/qualification；ContextDataset generation data head是唯一Version-head例外 |
| 17注册八类deployment route而Rust closure只支持六类 | CR-173目标：owner ID/kind/closure/allowed-edge先闭合，再开放完整OpenAPI；unsupported kind不得以永久shape error冒充已实现route |

## 15. Acceptance 记录

以下1～12项是CR-172历史Acceptance记录，不代表CR-173已关闭；CR-173必须在它们之上补齐13～17并重新签署Acceptance：

1. `rg` stale-contract scan确认microVM、Managed stdio、Installation Release、ManagementOperation、
   Model Artifact Producer和八role只出现在历史/否定/明确推迟语境，不再是首版正向requirement；
2. 文档链接、编号、状态、术语、ID、owner、schema version和table budget对齐；
3. `git diff --check`通过；
4. implementation-plan的每个phase都只从Reviewed合同引用可观测行为和分层证据；
5. 批准前不对外声明target API、topology、capacity、schema v7或runtime已经上线。
6. gVisor topology复核确认Launcher Kubernetes权限不传播给Controller/WASI/guest，admission与RBAC是结构性双闸，Pod status不成为第二Job authority。
   Launcher process generation由同Pod非特权attestor经shared PID namespace与Pod-local UDS封装；只有Launcher container持有
   scoped projected Kubernetes token，attestor/guest均无API或host authority。
7. 逐份复核03～16对ResourceVersion的引用只指published immutable revision；05与11已有“current Draft + validation + atomic publish”语义，
   12的Dataset Generation仍由owner Job原子创建immutable version，均不需要mutable Version row或新表。
8. 逐份复核03～16的Run admission和exact deployment reference只读immutable closure + Resource current binding/gate，无下游需要
   mutable Deployment state/projection；23表baseline不新增Deployment transition或current-head副本。
9. 逐份复核05/06/08/17的root/child admission：root只从request `agent_id`解析enabled active Deployment及其entry，child继续继承
   parent允许的exact closure；公共请求不暴露Deployment/Node/Job identity，且无需新增表或Plan Artifact事务外读取。
10. 逐份复核00～18的Artifact边界：只有15/17拥有public DTO字段；03/04的Receipt/permission、07的隔舱、10的Effect恢复、12/13/14/16的
    Artifact port以及18的三role/mTLS资格均与server-owned identity、exact generation和current principal rebinding一致，不需新增表、role或队列。
11. stale surface scan确认public Artifact request不再把Blob/Job/Task/Receipt/Event/Outbox、tenant/principal、storage/object locator、policy revision、
    retry或audit identity作为调用方字段；upload target/proof是唯一显式Secret-bearing响应例外，并被no-store/redaction/非明文Receipt约束。
12. TenantConfig slot复核确认Scheduling/Retention/ArtifactIo各有一个exact optional current binding；mutation保留未修改slot，Artifact prepare要求后两者，
    不扫描或排序多个active Policy，不新增tenant-policy join/head表。
13. owner registry新增Skill/Policy/Sandbox Deployment nominal kinds、prefix与allowed-kind matrix，禁止definition Version成为普通active binding；
14. `DeploymentClosure`为八个public noun提供closed variant，每个variant引用唯一owner Revision并冻结bounded requirement/policy/qualification closure；
15. Agent/Run/child/Invocation/Artifact/Sandbox snapshots逐字段改为exact Deployment + Revision，不从active binding重建历史；
16. fresh PostgreSQL验证八类create/activate/suspend并发CAS、wrong owner/tenant/kind/digest、Receipt replay与Event/Outbox原子性，且不新增表；
17. OpenAPI/owner schema/protobuf/fixtures逐字段一致，八类route均有真实handler与positive/negative conformance，ContextDataset仍只有build/generation API。
18. HardLimitProfile v5的三个expression字段在Rust owner、generated schema、checked-in profile和Scheduler Plan validation中逐字段一致；旧version、
    缺字段、wrong unit、zero、profile放大与超过代码绝对上限的fixture全部fail closed。
19. Scope data environment owner type、baseline JSONB结构guard与repository transaction一致；fresh PostgreSQL证明多output、词法shadow、
    stale Scope/Job fence、跨tenant/run/digest及Map/Loop动态实例隔离，且schema仍为23/22表。
20. expression classification只由exact input RunValue lattice join导出；空input为`Internal`，caller/Worker/Artifact metadata不能降级，
    L1/L2/L3 fixture覆盖四级join、漂移、回滚与production boundary，且不新增Plan/profile/public字段或持久化对象。
21. Typed Plan version 2的Map item port冻结exact producer/element schema；version 1与wrong producer/schema fail closed，每item RunValue与
    MapItem Scope/Node/Job/cursor原子提交并覆盖batch crash/replay与动态Scope隔离，不新增表/profile/public字段。
22. Loop carried pair冻结exact body/next NodeOutput与相同schema；body settlement预建下一open Scope并原子复制binding，continuation/body
    复用该Scope，false exit关闭并返回固定词法父Scope；两轮不串值、不读terminal Scope、不形成父链自环，冲突与crash/replay无部分rollover。
23. Typed Plan version 3的Return/Raise冻结exact terminal port并分别对齐Agent output/error schema；v1/v2、wrong producer/schema、
    missing/cross-run/closed Scope value、正文或Artifact digest漂移、stale fence与ID冲突fail closed，terminal提交/replay无部分状态。
24. Typed Plan version 4冻结全部external leaf payload，04 selector与owner transaction双重重验exact candidate/input/route；各leaf terminal只写
    node声明output并创建唯一resume Job，Plan v1/v2/v3及caller-supplied dispatch/result字段fail closed，L1～L3覆盖crash/replay。
25. Selection Policy document schema v1非空且closed；`only_candidate | ordered_first | route_hash`使用唯一canonical排序/hash算法，
    Scheduler与repository共享纯evaluator并由owner transaction重算，unknown/route漂移/集合外结果/health fallback全部fail closed。

## 16. 未决项

CR-182合同范围没有未关闭P0/P1。Acceptance 25与既有13～24形成单一闭包，00～18状态为Accepted。

实现计划仍有明确的发布资格未完成项：production-equivalent Kubernetes与真实`RuntimeClass=runsc`、L4拓扑安全矩阵、L5容量/持续
soak与首个CapacityProfile、L6签名供应链/backup-restore/rollout-rollback以及经人工审批的GitOps clean cut。这些是18的外部证据门禁，
不回退已闭合合同，但在实际通过前禁止宣称Phase 4、production capacity或Platform v2总体完成。
