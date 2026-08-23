# Platform v2 00～18 Cross-review（CR-176）

| 属性 | 值 |
|---|---|
| 状态 | Closed / CR-176 Accepted |
| 日期 | 2026-08-23 |
| 输入 | 00～18 live tree、ADR-0001、ADR-0002、AGENTS.md、CR-176 implementation feedback |
| 目的 | 验证简化后的状态、ID、schema、错误、事务、事件、权限、容量、恢复、Draft/Deployment/Run admission authority和fixture闭包 |

## 1. 结论

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

因此CR-176恢复Implementation Authorization并将受影响合同推进为Accepted。Accepted只表示target合同闭合；它不表示target已成为
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
| 00 | Accepted / CR-176 | target协议、authority与current-behavior边界闭合 |
| 01～02、04 | Accepted / CR-173（CR-176影响复核） | owner、ID、Resource与Policy authority不变 |
| 03 | Accepted / CR-176 | Scope binding/RunValue/Inline结构与原子事务闭合 |
| 05～07 | Accepted / CR-176 | typed expression、scope解析、derived observation与Scheduler边界闭合 |
| 08～16 | Accepted / CR-173（CR-176影响复核） | Subagent/Capability/Skill/Context/MCP/Sandbox/Artifact/Model owner合同不变 |
| 17 | Accepted / CR-174（CR-176影响复核） | public禁止observation注入与错误投影不变 |
| 18 | Accepted / CR-175（CR-176影响复核） | profile v5、物理identity/permit与分层资格不变 |
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

## 16. 未决项

CR-176合同范围没有未关闭P0/P1。Acceptance 19与既有13～18形成单一闭包，00～18状态为Accepted。

实现计划仍有明确的发布资格未完成项：production-equivalent Kubernetes与真实`RuntimeClass=runsc`、L4拓扑安全矩阵、L5容量/持续
soak与首个CapacityProfile、L6签名供应链/backup-restore/rollout-rollback以及经人工审批的GitOps clean cut。这些是18的外部证据门禁，
不回退已闭合合同，但在实际通过前禁止宣称Phase 4、production capacity或Platform v2总体完成。
