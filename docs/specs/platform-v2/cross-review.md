# Platform v2 00～18 Cross-review（CR-220）

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-220 Sandbox activation and runner-boundary review |
| 日期 | 2026-09-04 |
| 输入 | 00～18 live tree、product-experience 00～06、ADR-0001～0007、AGENTS.md |

### CR-220 Sandbox activation/runner/readiness cross-review

整体review的P1-03～05共享一个边界问题：已持久化activation secret作为bearer通过OpenSandbox Server
proxy，而runner与不可信Package同UID且共享可写state path；同时Dispatcher只用metadata list判定Ready。这使
Server可对另一candidate重组activation，Package可signal runner/改写latch或留下daemon，且controller/runner/delete
链路失效仍可以假Ready。

ownership/identity/schema：PostgreSQL shared Job、Invocation、RunValue和terminal authority不变，不新增表、aggregate、
JobKind、WorkClass、ComponentRole或public route。Job physical evidence中现有opaque 256-bit value改为Dispatcher-only
Ed25519 signing seed，它与public-key digest继续受current Job fence保护。runner config只公开verifying key；activation
frame增加exact sandbox ID和signature，签名preimage绑定boot/request/schema/input digest、declared bytes及正文。
OpenSandbox Server被承认为signed-byte relay，但不获得signing authority或Platform credential。

security/process/files：runner/Package固定为UID 65532/65533；container先drop all，仅向trusted runner增加`SETUID`与
`KILL`。child切换UID时不保留capability，在独立process group运行，并以no-new-privileges/seccomp拒绝信号、
session/process-group与namespace逃逸。runner在任何terminal前kill整组并bounded等待quiescence。latch/result只位于
runner-owned `0700`子目录，用`0600`/`O_NOFOLLOW`建立和读取。Package不取得seed、OpenSandbox/API/DB
credential、runner state权限或父进程信号权限。这不把containerd/runc说成强多租户隔离；该声明仍需正式L4。

readiness/capacity/recovery：无业务Job的低频探针用唯一synthetic identity和不可变Platform image执行inert
`create -> token-scoped list -> Armed state -> delete -> absence`，不activate Package，不写Platform DB。full probe严格有界且不并发；
成功只缓存短TTL，每次dependency sample仍执行authenticated list，任一观测失败立即撤销readiness。response-loss
按唯一token bounded list回收，delete/absence失败本身使readiness fail closed，不能影响Job/Invocation/quota。

证据：L1覆盖签名篡改/cross-candidate/cross-boot、state no-follow/mode、Package signal/session/daemon/fill失败和
readiness各阶段失败清理；L3使用真实OpenSandbox/Kubernetes/containerd-runc验证cross-candidate零激活、恶意Package
terminal后无存活后代与state不可写，并在controller/runner/delete故障下撤销readiness。本机Kind可生成
`production:false`机制证据；正式production-equivalent L4、L5、L6仍须独立运行。

### CR-219 revision 1 Sandbox cancel/timeout cross-review

整体review确认现有Sandbox控制路径存在authority断点：Capability owner事务只把Invocation推进为`Cancelling`，shared Job、lease、payload和
quota均不变；Dispatcher没有消费该Event的分支，claim又排除deadline-past Job。因此cancel后旧worker仍可能create/activate/commit，timeout
后Job、Invocation与四维quota可能永久悬挂。已有L2直接构造`Cancelled/TimedOut` outcome只证明terminal helper，不证明生产控制入口。

本revision把控制事实收敛到既有shared Job。`SandboxControlIntentV1`以closed kind、database requested time、target Invocation version及
tenant/invocation/Job/request identity的domain-separated digest绑定。显式命令按Invocation → Job锁序同时写intent并推进二者
`Cancelling`；Job状态图只为带owner intent的Ready/Leased/Running/Waiting/RetryScheduled开放该转换。Dispatcher在reserved
critical-control capacity中执行bounded scan；每个终态事务按quota → Invocation → Job锁序重验并提交Job/Invocation、quota、Event/Outbox
和cleanup intent。deadline由数据库时间物化timeout，消息仅是wake hint。scan不调用provider、不建立第二lease或current-state表。

ownership/identity/schema复核：PostgreSQL shared Job仍是cancel/attempt/lease/terminal唯一authority，Invocation仍是业务调用projection，
OpenSandbox仍只拥有physical lifecycle。optional intent不新增表、aggregate、JobKind、WorkClass、ComponentRole、route、credential或queue；
无intent payload保持既有canonical shape。pre-claim control保持attempt 0且没有physical evidence；started control保留原physical identity。
terminal first-winner使late result、第二control与scanner race零写入，四维quota只在terminal事务结算一次。

安全/容量/恢复复核：intent不含正文、activation signing seed、provider URL或credential，不进入metric label。控制scan使用critical-control保留容量，
Sandbox普通lane饱和不能阻止cancel/timeout；provider不可达不能阻塞terminal/quota，只使既有cleanup backlog重试并持续告警。外部
terminate/delete/absence始终发生在terminal之后并由`SandboxCleanupFenceV1`保护。L1必须覆盖intent digest/second-intent/pre-claim零provider，
L2必须覆盖public cancel、database-time timeout、result/control race、quota exact-once和provider-independent terminal，L3必须覆盖started
workload cancel/timeout、Dispatcher kill恢复与provider unavailable cleanup。该revision本身不产生L4～L6 passed evidence。

实现复核（2026-09-04）：`a5aceabb`与`58a84199`已按上述ownership、锁序和错误边界实现，并通过L1～L3。
真实L3在started cancel intent落库后对测试Dispatcher进程发出SIGABRT，随后把OpenSandbox Server缩容为零；新进程仍完成
Job/Invocation/quota/Event/Outbox终态，Server恢复后完成DELETE 404重放与absence proof。started timeout与最终零残留也已通过。
该实现证据不改变正式L4～L6的Not run状态。

### CR-218 revision 1 Sandbox boot-rollover cross-review

整体review确认CR-216的目标规则正确，但实现缺少可在observation与terminal之间崩溃后恢复的boot-rollover evidence：
`ActivationAuthorized`分支会先对任何`Armed`状态调用activate，`Started`分支会对任何latched/started状态直接等待，两处都未先比较
已授权boot。这使旧activation frame可能到达controller重建后的新runner，且repository把不同boot仅映射为普通frame错误，无法进入
既有`UnknownOutcome + cleanup`路径。

本revision不改变业务语义，只补足已有裁决的持久证据与顺序。03先把optional、validated
`runner_boot_rollover`纳入shared Job bounded physical evidence；它保存observed boot、runner state frame digest与domain-separated evidence digest，
回读必须重算摘要。10/14规定其preimage绑定tenant/job/physical attempt、selected
sandbox、request digest、original/observed boot与observed runner state frame digest。Dispatcher观察到不同boot时必须先用current Job fence
CAS该摘要并转为physical `UnknownOutcome`，随后terminal transaction把同一摘要写入既有`SandboxTerminalOutcomeV1::UnknownOutcome`。
相同observation可重放；不同第二observation、旧fence或已有完整result均fail closed。Dispatcher在该CAS前后均不得调用activate，且不能创建新
token/candidate/sandbox/physical attempt。

ownership/identity/schema复核：PostgreSQL shared Job仍是唯一attempt/lease/terminal与physical-evidence authority；Invocation/RunValue、
OpenSandbox/Kubernetes与cleanup所有权不变。optional evidence不新增表、aggregate、ID、JobKind、WorkClass、ComponentRole、public/internal route、
credential或第二状态投影；未发生rollover的canonical payload保持原shape。terminal仍按quota → Invocation → Job锁序first-winner，rollover
observation只按current Job fence更新同一payload/version，不直接推进Invocation、quota或RunValue。

安全/错误/容量/恢复复核：摘要不进入metric label且不包含正文、token、URL或credential；Event只允许safe phase/digest。
`sandbox_runner_boot_changed`映射既有safe `UnknownOutcome/ReconciliationRequired`，cleanup继续按独立generation fence取得absence proof。
没有额外provider调用或容量lane；反而把新boot上的activate调用数收紧为零。L1必须覆盖same-observation replay与different-observation拒绝，
Dispatcher回归必须证明new-boot `activate_calls=0`；真实provider门禁必须覆盖Pod/runner boot rollover。该revision不构成L4～L6 passed evidence。

2026-09-04 closure evidence：L1以两个Dispatcher phase证明different-boot observation先于任何activate/await且
`activate_calls=0`；fresh PostgreSQL L2证明同一摘要的持久化、重放和Job/Invocation原子`ReconciliationRequired`；真实
Kind/OpenSandbox L3在`Started`长任务期间删除workload Pod，证明Controller重建产生新Pod UID与新runner boot，且新runner因
emptyDir丢失回到`Armed`。该危险状态由前两层门禁收敛为`UnknownOutcome + cleanup`，不发送旧activation frame。L4～L6仍Not run。

### CR-217 qualification fail-closed cross-review

本轮只收紧CI/GitOps资格证据边界，不改变Platform业务状态机。ownership保持：PostgreSQL仍是业务current state authority；
live Kubernetes/CRD/NetworkPolicy只形成部署观测，CI artifact store保存qualification evidence，GitHub/OCI/GitOps仍是release authority。
不新增表、aggregate、public/internal API、ID、Event/Receipt/Job kind、WorkClass、ComponentRole或第二current-state projection。

身份与schema复核：`mcp-callback-api`和`mcp-cleanup-worker`是既有`mcp_host`下独立pool，Context dataset是既有
`context_worker` pool；Candidate/Capacity closed key set不变。Evidence wire version不变，但Rust语义新增production至少86,400秒、
manifest elapsed time、每gate专属digest及artifact exact closure；JSON Schema继续表达可静态描述的边界，跨字段专属/时间约束由Rust owner执行。

安全与恢复复核：L4从带Platform workload namespace标签的Namespace建立完整Deployment/DaemonSet闭包；未标注或unknown role fail closed。
live BatchSandbox CRD对完整规范化spec计算并比对reviewed digest；Sandbox ServiceAccount、Role/ClusterRole binding与三组fail-closed
ValidatingAdmissionPolicy/Binding逐项核验；NetworkPolicy摘要纳入全部策略并拒绝namespace-wide/unbounded allow，
因此额外对象不能被过滤掉。结构validator仍不声称真实执行，production promotion还必须验证受保护producer、artifact bytes/signature和
GitOps history。starter release DAG改为candidate build/sign → offline exact-cache qualification → 含资格evidence的final bundle重签 →
release tag/GitHub Release，失败只保留诊断candidate，不存在不可变release。事务、错误、业务Event、安全credential、capacity authority和
Sandbox recovery语义均不变。

测试证据要求覆盖：1秒production soak拒绝、跨gate复用单artifact拒绝、CRD schema drift、标记namespace中的无role workload、额外
allow-all NetworkPolicy、callback/cleanup/dataset ComponentRole闭包，以及publish依赖qualification的workflow静态负向。该修复本身不执行
真实multi-node、24小时soak、restore或promotion，故L4～L6继续Not run。

### CR-216 OpenSandbox-only execution cross-review

CR-216 revision 1 将首版 Sandbox physical implementation 从 restricted WASI + per-Job gVisor clean-cut 为 OpenSandbox
Kubernetes provider + BatchSandbox Controller + containerd/runc，并以 inert candidate + Armed runner activation 取代对 OpenSandbox
源码 atomic provisioning extension 的要求。复核确认“完全替换”只适用于 physical runtime：Capability Invocation 与 shared Job
仍是业务 authority，Sandbox Dispatcher 是唯一 provider caller、candidate selector 与 terminal commit 入口；OpenSandbox 只拥有
BatchSandbox/Pod/runner physical lifecycle。ADR-0007 取代 ADR-0002；实现完成前 `docs/current` 不变。

revision 2关闭实现接线发现的两个P1：`jobs.payload`与RunValue inline authority的byte ceiling冲突，以及terminal清除标准Job lease后
absence evidence缺少写fence。裁决是不把input/result body放入Job；claim从exact input RunValue重建，terminal同事务写唯一output
RunValue；terminal后的delete/absence只使用同一Job physical evidence内独立cleanup generation fence。它不新增业务表、aggregate、
JobKind、Event/Receipt kind或第二terminal authority。

revision 3关闭实现测试发现的reclaim状态机P1：shared Job原状态图不允许`Running -> Ready`，无法在不产生新attempt的前提下让新
Dispatcher lease接管可能已启动的physical attempt。裁决是仅当owner payload证明durable external continuation时允许该窄转换；
continuation claim只增加lease generation，保持attempt count，禁止新token/candidate/sandbox/activation。普通Job状态语义不变。

revision 4关闭L2夹具发现的candidate binding P1：metadata已有Runtime/Profile digest，但Plan/Request没有可比较字段会使其形同未绑定。
裁决是把两者纳入semantic request closure并逐字段验证；错误digest candidate零Job mutation，不从mutable head补值。

revision 5关闭orphan L2设计发现的lookup P1：candidate metadata缺少tenant/job/physical attempt时，bounded orphan page无法无歧义、
高效地定位shared Job，只能全表扫描JSON payload。裁决是增加这三个operator-only identity字段，repository以tenant/job point-read后重验
current attempt/token与全部candidate closure，返回只读closed retain/delete decision；corrupt/ambiguous/unavailable必须retain。

revision 6关闭lease rollover L2发现的reclaim P1：若expired Running仅持久化为continuation Leased，observation/terminal不接受该状态，
而下一次reclaim也拒绝带physical evidence的Leased。裁决是在同一claim transaction内逻辑执行
`Running -> Ready -> Leased -> Running`，只提交最终Running；lease generation增加一次，attempt与全部physical identity保持。

revision 7关闭Dispatcher create loop设计发现的response-loss P1：进程内count/quiescence在restart后会丢失，metadata list也不能证明
一个无响应create是否已执行。裁决是每次provider create前以current Job fence在PostgreSQL CAS授权exact ordinal，并持久化database-time
provisioning start、authorization count与last authorization time；相同ordinal重放，下一ordinal受durable count/quiescence/total-time
hard limit约束。provider I/O仍在事务外，OpenSandbox不改源码且不成为业务authority。

revision 8关闭authorization replay P1：若旧version重放authorization后仍调用provider，同ordinal仍可产生无界调用。裁决是repository
返回`Applied | Replayed`，只有`Applied`唯一caller恰好调用一次provider；`Replayed`不调用。授权后、调用前崩溃会burn ordinal，恢复只可
在durable quiescence后申请下一ordinal。因此每个ordinal至多发起一次create，全部调用数由Profile count hard limit约束。

revision 9修复03 machine registry与既有CR-216裁决的矛盾：MCP首版只有remote Streamable HTTP，managed-stdio physical session及
`SandboxManagedMcpSession` JobKind/三元组全部删除。既有shared Job表和`SandboxCapabilityExecution`不变，不增加替代kind、兼容映射或fallback。

final implementation review对实现、schema contract 8、OpenSandbox client/runner/Dispatcher、Helm/Profile/CLI、persistent physical
evidence、L1～L3 fixture和active residual scan逐项复核。实现没有修改OpenSandbox源码，没有新增业务aggregate/table/route/JobKind/
WorkClass，没有OpenSandbox业务写权限、兼容层、双写或旧backend fallback。shared Job/RunValue/Invocation authority、exact-ordinal create
authorization、current lease terminal fence、PotentiallyStarted后的no-replacement和workload external-effect边界均与下表Accepted ruling一致。

| 维度 | Cross-review ruling |
|---|---|
| state ownership | Invocation拥有业务调用，shared Job拥有attempt/lease/fence/cancel/terminal、selected candidate与cleanup intent；RunValue拥有input/output正文；OpenSandbox/Kubernetes只拥有physical lifecycle；Job只保存bounded reference/digest/evidence |
| IDs | 不新增 SandboxJob/Operation/business ID；provisioning token、OpenSandbox ID、runner boot ID和activation signing seed都是physical evidence；public Operation仍等于JobId |
| schemas | provider是closed `OpenSandboxKubernetes` singleton；Runtime/Profile/Plan/Request/ProvisioningToken/CreateAuthorization/CandidateMetadata/Activation/RunnerResult/CleanupFence均为canonical、bounded v1并有digest；CandidateMetadata以operator-only tenant/job/physical attempt/create ordinal支持point lookup与authorization校验，Plan/Request逐字节冻结Runtime contract/Profile Deployment digest并绑定其余metadata；Inline正文effective ceiling为`min(Profile, 1_048_576)`；unknown backend/fallback拒绝 |
| idempotency | Platform保证command Receipt、PostgreSQL exact-ordinal create authorization/candidate-selection first-winner、每个Applied ordinal至多发起一次provider create、runner signed activation replay-safe/最多一次Package start、Job terminal first-winner；不保证一个token历史上只有一个inert object，也不拥有workload外部副作用幂等 |
| transactions | current fence先持久化provisioning intent/database-time start；每次provider create前CAS exact ordinal/count/last time，仅`Applied` caller在事务外调用一次provider，`Replayed`不调用且crash可burn ordinal；candidate selection与activation authorization分别CAS；`PotentiallyStarted`先于外部activate；terminal重验latest Job fence并原子写Job/Invocation/RunValue/quota/Event/Outbox/cleanup intent；之后cleanup只用独立generation fence CAS physical evidence |
| errors/events | candidate limit/selection conflict、activation conflict、boot rollover、provider unavailable、unknown outcome、invalid/oversized result 映射 safe Job failure/Event；不公开 ID、endpoint、API body、diagnostics 或 workload 正文 |
| permissions | Dispatcher 只有 Sandbox Job repository、OpenSandbox lifecycle client 和 fixed runner protocol；官方 execd init 只监督 fixed runner 且 Platform 不调用 general exec/file API；OpenSandbox/Controller/runner 无 Platform DB/NATS/Artifact/Run/Invocation 权限；任何组件都无 Docker/CRI socket |
| network/Secret | Profile 只允许 `Disabled | Direct` operator policy；默认 deny ingress，无 public exposure；Direct 拒绝 internal/metadata CIDR，Disabled 零 egress；Secret injection disabled |
| capacity | `WorkClass::Sandbox` 不变；candidate count/quiescence/time、Dispatcher permits、OpenSandbox API、BatchSandbox/Pod、result bytes 与 cleanup backlog 独立有界，饱和不得消耗其他 lane |
| recovery | create response loss先list inert candidates；下一create必须由durable ordinal/count/quiescence/total-time重新授权，restart不重置预算；PostgreSQL只选一个；`ActivationAuthorized/PotentiallyStarted`后只查询/重放同runner/boot的逐字节相同signed activation；expired Running lease只可用owner-proven continuation在一个claim transaction内逻辑执行`Running -> Ready -> Leased -> Running`并只提交最终Running，attempt不变；boot变化则UnknownOutcome；terminal后cleanup claim/reclaim由database time与generation first-winner；orphan以tenant/job point-read并在corrupt/ambiguous/unavailable时retain；delete/TTL/orphan sweep不推进业务state |
| fixtures | L1 closed contract/digest/runner/result/limits；L2 real PostgreSQL claim/create authorization/CAS/fence/terminal/cancel/quota/orphan；L3 real OpenSandbox+Kubernetes/containerd-runc create/restart/kill/activation/network/cleanup；L4～L6 `Not run` |

完整00～18影响复核结论：01/02收敛plane与ComponentRole；03收敛provisioning/terminal幂等及recovery；04收敛Direct network和Secret边界；
07保留WorkClass并替换worker pools；09/10收敛Capability Deployment/Invocation；14重写physical protocol；15将Sandbox Artifact port推迟；
17保持public surface不变；18替换deployment/qualification；product-experience把`wasi` feature clean-cut为`sandbox`。05/06/08/11～13/16
的Plan、Run、Subagent、Skill、Context、MCP和Model authority不变。

该修订不增加业务表、aggregate、ResourceKind、JobKind、WorkClass、public route、Task/Event/Receipt kind或compatibility layer。
Kubernetes physical store 不是 Platform business database。该 revision 不新增业务表、aggregate、ResourceKind、JobKind、WorkClass、
public route、Task/Event/Receipt kind 或 compatibility layer；candidate selection/activation evidence 使用 shared Job 同一 row/version CAS。
final review重新复核state/ID/schema/error/transaction/event/permission/capacity/recovery/fixture后无未关闭P0/P1。L1、真实PostgreSQL L2与
真实OpenSandbox/Kubernetes/containerd-runc L3均有独立通过证据；workspace、contract、CLI/profile、deployment与docs gates通过。
L4～L6仍Not run且不作production-ready声明。旧WASI/gVisor证据没有抵扣任何CR-216门禁。

### CR-215 browser authoring profile authority cross-review

Phase 4 Console接线确认：browser不能读取CLI私有runtime config，而new-Agent compiler必须冻结exact Policy revision/Deployment binding。
把generated ID写入bundle、localStorage或synthetic默认会与shared compiler分叉，并在fresh tenant产生无效Deployment closure。

CR-215在Management surface增加一个read-only、bounded、tenant/principal-scoped authoring-profile projection。Tenant config仍选择当前
default Policy Deployment；immutable ResourceVersion/Deployment仍拥有exact digest；Gateway只在repeatable read中组合safe DTO，Console不拥有
任何business fact。缺失、suspended、digest mismatch、permission denied或feature disabled均fail closed。响应private/no-store且排除Secret、
provider endpoint、signed URL、raw config与worker identity。

复核覆盖DTO/schema、tenant/permission、transaction snapshot、digest、Problem、Console memory-only消费与negative fixture。该修订不新增表、
aggregate、Event projection、cache、mutation、Job/Task/Receipt、role、migration或compatibility fallback；影响按17→18→00、Product
01/03/04/06→00与implementation plan同步后授权Phase 4继续。

### CR-214 Agent Run input defaults authority cross-review

Phase 3 CLI接线确认：manifest的`spec.input.classification`与物化后的`spec.limits.deadlineSeconds`必须在`insight.lock`丢失、跨设备读取或
显式`agent adopt`后仍可从服务端恢复；两项此前只存在于compiler `AgentResourceIntent/DeploymentBindingIntent`，未进入Resource、Revision
或Deployment closure。让CLI/Console使用本地profile、lock或隐藏默认会造成第二authority，并使adopt无法安全构造`POST /v1/runs`。

CR-214选择把closed `DataClassification`与1～3600秒的bounded duration加入既有
`AgentResourceSpec.input_classification/default_deadline_seconds`。compiler materializer逐字节复制normalized manifest值；Resource Draft CAS、
validation、publish与immutable Revision digest继续复用既有authority。Run create仍使用absolute `UtcTimestamp`，客户端只在命令执行时从
exact Resource值计算；服务端不新增clock/default逻辑。

影响按05→17→18→00、Product 01/02/03/04→00与implementation plan复核。该修订不新增表、aggregate、ResourceKind、route、
Job/Task/Event/Receipt、role、migration、Deployment字段或compatibility fallback；permission、tenant、Run冻结与执行状态机不变。

### CR-213 Agent required feature authority cross-review

Phase 2 direct-authority list实现确认：`AgentSummaryV1.required_features`必须在Agent仍为Draft、正在验证或尚未激活Deployment时可读，
但Phase 1 compiler的closed结果此前只存在于client intent/corpus；既有`AgentResourceSpec`没有对应字段。由active Deployment slot、
Plan Artifact、project lock或Event推断都会造成状态缺失或第二authority。

CR-213选择把最多16项、按wire value严格排序且不重复的closed `AgentRequiredFeature`集合加入既有
`AgentResourceSpec.required_features`。compiler materializer逐字节复制其结果；Resource Draft CAS、validation、publish与immutable
Revision digest继续复用既有authority。Agent list只投影current Resource document中的集合，不读取client或执行侧数据补值。

影响按05→17→18→00、Product 01/04/06→00与implementation plan复核。该修订不新增表、aggregate、ResourceKind、route、
Job/Task/Event/Receipt、role、migration、Deployment requirement或compatibility fallback；Run冻结与执行行为不变。

### CR-212 Agent authoring name authority cross-review

Phase 2 direct-authority list审计确认：`AgentSummaryV1.name`与`RunSummaryV1.agent_name`要求来自Agent Resource authority，但
normalized `metadata.name`此前只存在于本地manifest与project lock；既有`AgentResourceSpec`只有display name外层字段和语义内容。
服务端若继续实现只能把display name当稳定key、读取Artifact正文或信任client lock，都会产生猜测或第二authority。

CR-212选择把bounded normalized name加入既有`AgentResourceSpec.authoring_name`。compiler逐字节物化manifest name；Resource创建后
Draft update必须保持同一值，publish后随immutable Agent Revision冻结并参与canonical digest。name只需project-local唯一，不增加
tenant-wide unique index、查名route或新的identity。Agent list从current Resource document投影，Run list通过frozen Agent Deployment
精确关联同一owner Resource后投影该字段；历史Run不使用current active Deployment改写绑定。

复核覆盖closed schema、digest/CAS、Resource update、Revision publication、Run frozen binding、tenant/permission、cursor filter与CLI lock恢复。
CR-212不新增表、aggregate、ResourceKind、route、Job/Task/Event/Receipt、role、migration或compatibility fallback；05→17→18→00、
Product 00/01/plan与cross-review已同步并恢复Accepted/Implementing，授权Phase 2继续。

### CR-211 Product compiler digest preimage cross-review

Phase 1输出审计确认Interface `contract_digest`与model slot `requirement_digest`此前没有唯一preimage；Rust/TypeScript、首次publish与
crash recovery若各自digest不同对象，会让相同manifest产生不同Plan/Receipt/binding，或在Artifact ID物化后发生漂移。

CR-211冻结两个最小closed v1 document：Interface preimage只含三个ClosedJsonSchema canonical digest与schema_version；model requirement
只含schema_version、kind=`model`与normalized manifest ref。两者复用02 RFC 8785/SHA-256，均不含Artifact、Version、Deployment、Receipt、
ETag或其他server identity。exact Model Deployment与selection Policy只属于Deployment binding intent。

复核确认Resource/Version/Deployment、Run、Artifact、Receipt与runtime authority不变；不新增field/table/route/Job/role/fallback。05、18、00、
Product 00/01/plan与cross-review已同步并恢复Accepted/Implementing，授权Phase 1继续。

### CR-210 Deterministic template terminal schema cross-review

Phase 1 Typed Plan生成审计确认：产品`deterministic`模板只有`start -> return`，而05/06要求RunInput exact schema与terminal output schema
同时匹配。若manifest允许不同input/output schema，compiler只能伪造一个port digest、加入未声明转换或生成首次Run必失败的Plan。

CR-210把约束放在最窄的product authoring surface：`deterministic`要求两个ClosedJsonSchema canonical digest相同，Return消费该exact
RunInput；需要转换的Agent使用`model_chat`或高级Typed Plan。05/06现有wire与runtime invariant不变，错误在任何HTTP、Artifact或lock I/O前
返回。复核确认不新增node、route、表、Job/Task/Event/Receipt、role、authority或fallback；Product 00/01/plan、18、00与cross-review已同步，
恢复Accepted/Implementing并授权Phase 1继续。

### CR-209 Agent author instruction trust cross-review

Phase 1实现审计确认`model_chat.spec.instructions`没有合法的persisted/runtime落点：现有`AgentResourceSpec`不携带作者正文，
而初始model assembly只有platform safety、Agent contract、Plan node instruction三个trusted platform block和Skill/Context/user输入。
忽略正文会使manifest行为失效；把正文拼入前三者会把tenant作者内容提升为platform policy，并破坏CR-186信任合同。

CR-209选择由既有immutable Agent Revision拥有nullable bounded `author_instructions`，并在canonical assembly中增加独立
`AgentInstruction`位置。正文随Agent document canonical digest、Draft CAS、publish与Run exact binding冻结；block固定为`user` role、
`trusted_instruction=false`，位于Plan node instruction之后、required Skill之前。Run/Event/Problem/list summary不复制正文，runtime不从
active head、caller metadata、local lock或browser state补取。

| Spec | CR-209 结论 |
|---|---|
| 00～04、06～10、12～15 | identity、Resource lifecycle、Run/Job/Task/Event/Receipt与执行边界不变 |
| 05 | `AgentResourceSpec`增加nullable bounded作者正文；Revision/digest/CAS继续由既有Resource authority拥有 |
| 11/16 | 增加独立AgentInstruction assembly位置；作者正文为user/untrusted且不能扩大任何binding/policy |
| 17 | 复用既有Agent Draft document route；list/Run/Event/Problem不公开正文，不增加自由prompt endpoint |
| 18/Product 00～01 | 增加compiler、role、顺序、digest、漂移与泄漏门禁；`model_chat`行为闭合 |

复核覆盖state ownership、ID/closed schema、canonical digest、CAS、permission、prompt role、budget、recovery与泄漏。CR-209不新增表、
aggregate、ResourceKind、route、Job/Task/Event/Receipt kind、ComponentRole、Secret路径或compatibility fallback；05→11/16→17→18→00与
product-experience 00/01/plan已同步并恢复Accepted/Implementing，授权实现。

### CR-208 Agent compiler Artifact identity cross-review

实现前类型审计确认`ResourceDocument::Agent`中的`authoring_package.artifact.artifact_id`和`typed_plan_artifact_id`都是required
server-owned identity。Spec 01原先又把完整ResourceDocument列为无网络compiler输出，合法客户端既不能预知ID，也不能向Artifact prepare请求
指定ID；继续实现只能伪造ID、发送placeholder或在upload后悄悄重新编译，都会破坏digest/recovery合同。

CR-208把边界拆为两个closed纯类型：compiler产出无server ID的`AgentResourceIntent`和有logical dependency的lifecycle plan；publish
executor完成authoring/plan upload并观察Ready authority后，以返回的exact ID/digest调用纯`materialize`构造既有`AgentResourceSpec`。
materializer重验purpose、digest、classification与intent，HTTP Receipt digest绑定最终实际request body，logical reference不出客户端。

复核确认Artifact仍是ID/Ready/storage authority，Resource仍是Draft/current authority，compiler/lock/journal均不是authority；crash恢复从
Receipt/Artifact read重建相同步骤，不生成新随机ID假装第一次未发生。CR-208不修改Platform 02～18 machine contract，不增加route、table、
aggregate、ResourceKind、ArtifactPurpose、Job/Task/Event/Receipt kind、role、Secret路径或compatibility fallback。product-experience 01与00、
implementation plan已按此修订并恢复Accepted/Implementing，授权Phase 1实现。

### CR-207 Agent product experience cross-review

产品体验spec要求普通用户只理解Agent、Publish、Run与Result，同时Console需要tenant Agent/Run历史。现有exact-ID read无法支持远端列表，
而以browser storage或Event重建会产生第二current-state authority。CR-207选择在现有Agent noun和Run collection上增加两个只读projection：
直接查询Resource/Deployment/Run/Task authority，使用signed/AEAD opaque keyset cursor，不增加list表、cache或current head。

分页与“默认DTO不显示cursor”的表面冲突收敛为两层合同：`AgentSummaryV1/RunSummaryV1`不含cursor；closed page envelope可以携带
`next_cursor`作为客户端代管的protocol metadata，默认CLI text/Console DOM不得渲染。cursor绑定route purpose、tenant、principal scope、
canonical filter、page size、snapshot/boundary与expiry；错误绑定在repository读取前拒绝。

| Spec | CR-207 结论 |
|---|---|
| 00～02 | Resource/Version/Deployment、ID与active binding authority不变；Agent list只投影既有Agent Resource及active Deployment |
| 03～04 | list是只读操作，不创建Receipt/Event/Outbox；tenant/principal permission与cursor key是唯一安全边界，不把cursor当业务state |
| 05～16 | Typed Plan v5、Run frozen binding、Job/Task/Artifact/Capability/Context/MCP/Sandbox/Model合同不变；manifest只编译两个评审模板 |
| 17 | 增加Agent/Run list DTO、closed filters、stable keyset与cursor binding；不新增generic CRUD或内部字段 |
| 18 | 增加compiler/API/CLI/Console/release/profile L1～L3与distribution门禁；starter保持non-production，L4～L6状态不变 |
| Product 00～06 | ADR前置与17→18→00复核关闭，状态恢复Accepted；实现顺序固定为01→02/03→04及05→06 |

ADR-0003 clean-cut `base/full`为`starter + closed features`并要求prebuilt exact release；ADR-0004把`apply`保留为advanced authority入口，
默认增加确定性Agent manifest/CLI；ADR-0005保持static `/v1`/无BFF边界并把Console扩展为authoring与Run产品入口。三项都不改变
PostgreSQL durable authority、Run exact binding、Receipt/CAS、public route version或production GitOps ownership。

复核覆盖state ownership、ID/closed schema、errors、transaction/Event/Receipt、permissions、cursor confidentiality/integrity、capacity、
crash/reload recovery、browser/CLI leakage、distribution supply chain和test fixture。CR-207不新增table、aggregate、ResourceKind、JobKind、
WorkClass、Task/Event/Receipt kind、ComponentRole、Secret路径、BFF、数据库或兼容fallback；受影响合同按ADR-0003～0005、17→18→00及
product-experience 00～06完成复核并授权clean-cut实现。

### CR-206 Context Dataset generation discovery cross-review

fresh Context golden scenario审计确认：build admission把预留`dset`放入Operation target，但成功Operation只有opaque
`result_digest`；唯一generation read route还要求调用方提供`dgen`。没有list/head/read-result入口时，合法public客户端无法
发现成功事务创建的Version，因而会被迫查询业务数据库。这与17的公开journey和12的exact citation证据合同冲突。

CR-206不增加mutable Dataset head/list API，而把`SafeJobResult`升级为closed tagged union。普通成功Job使用`digest`；
`ContextDatasetBuild`成功使用`context_dataset_generation { result_digest, generation_id }`。`generation_id`只从同一个Job
admission冻结的artifact preallocation投影，成功事务已重验并以它创建Version；projection不得扫描active head或另建result row。

| Spec | CR-206 结论 |
|---|---|
| 00～02 | 路线、plane、Resource/Version authority不变；00保持In Progress |
| 03 | Operation继续是Job projection；typed result只是terminal safe payload，不是第二aggregate |
| 04～11 | tenant/policy/Run/worker/Capability/Skill合同不变，不增加permission、quota或work kind |
| 12 | 成功build公开frozen `dgen`，客户端再读取exact immutable generation；失败不暴露预分配 |
| 13～16 | MCP/Sandbox/Artifact/Model合同不变；Artifact identity/locator仍不进入Operation result |
| 17 | result union按Job kind-target-state闭合；generic digest与generation variant不可互换 |
| 18 | 增加positive generation discovery/read与wrong kind/state/ID/payload drift负向矩阵；L4～L6不变 |

复核覆盖state ownership、ID/schema、errors、transaction/Event/Receipt、permission、capacity、recovery与fixtures。CR-206不增加
table、aggregate、route、ResourceKind、JobKind、WorkClass、Secret或兼容fallback；受影响合同按03→12→17→18复核并恢复Accepted，
授权clean-cut实现。

### CR-205 internal definition authoring cross-review

full-profile产品化审计发现，现有public lifecycle只暴露八类可调用owner，Capability/Context Implementation、Model Provider及
Sandbox Runtime/Package虽已拥有nominal ResourceDocument、ResourceKind、permission与repository validation，却没有合法authoring
surface。Interface/Profile Deployment要求其exact Version/Deployment，因此fresh客户端只能预写数据库或无法继续，五条full场景共同受阻。

CR-205将五类既有kind加入closed domain noun matrix，不增加generic Resource JSON route。Capability/Context Implementation与
Sandbox Runtime/Package是definition-only，只执行create/draft/validate/publish/read Version；Model Provider保持Deployment/activate/
suspend。Definition-only的deployment path在kind dispatch时fail closed，不能为统一CLI流程伪造空Deployment。

| Spec | CR-205 结论 |
|---|---|
| 00～01 | 00保持In Progress；plane与authority边界不变，不升级Verified |
| 02 | closed noun覆盖可author shared Resource kind；Version/Deployment ownership与table budget不变 |
| 03～08 | Job/Receipt/Event、tenant/policy、Run/worker/Subagent合同不变；validation继续使用shared Job |
| 09 | Capability Implementation取得domain noun，仍绑定exact Interface Revision且没有独立Deployment |
| 10～11 | Invocation/Skill只消费exact closure，不追随新增noun或active head |
| 12～13 | Context Implementation取得domain noun；Dataset/MCP owner与query transaction不变 |
| 14～15 | Sandbox Runtime/Package取得definition-only noun；Artifact扫描、Sandbox Profile与execution plane不变 |
| 16 | Model Provider取得完整Resource→Version→Deployment authoring surface；Model Profile仍只绑定exact Provider Deployment |
| 17 | OpenAPI/CLI使用十三类closed noun；四类definition-only流程不得生成Deployment/activation请求 |
| 18 | 增加十三类矩阵、definition-only负向和fresh full场景证据；L4～L6状态不变 |

复核覆盖state ownership、ID/schema、errors、transaction/Event/Receipt、permission、capacity、recovery和fixture。CR-205不增加
table、aggregate、ResourceKind、JobKind、WorkClass、ComponentRole、Secret路径、internal RPC或兼容fallback；它只让既有domain
kind可经共享authority合法发布。受影响合同按02→09/12/14/16→17→18复核并恢复Accepted，授权clean-cut实现。

### CR-204 Agent Context binding identity cross-review

public Agent Deployment create曾复用persisted `DeploymentClosure`，要求客户端提交只有本次command才能生成的owner `adep`、
每个Context slot的`xcb`及两层binding digest。route没有reserve-ID操作，Gateway又只能在解析body后生成Deployment ID，因此合法
public客户端无法创建带Context slot的Agent。这是02/05/12持久化快照与17 create intent混用造成的P0身份环。

CR-204保留完整persisted/read closure，只拆分create DTO：调用方提交exact Context binding intent；Gateway按slot规范顺序预留
一个`adep`和每个Context slot一个`xcb`，由nominal Rust owner构造Context snapshot并规范计算内外digest，再把完整closure交给
repository transaction。Receipt request digest仍绑定原始intent，winner返回第一次提交的完整Deployment，重放不得泄漏新预留identity。

| Spec | CR-204 结论 |
|---|---|
| 00 | 保持In Progress；记录P0身份环及修复，不升级Verified或改变L4～L6状态 |
| 01 | control/durable/execution plane不变；Gateway只做typed materialization，不执行用户代码或成为durable authority |
| 02 | 明确create intent与persisted closure边界；服务端生成嵌套identity/digest，Deployment仍是唯一immutable authority |
| 03～04 | Receipt/Event/Outbox、tenant/permission/lock/CAS不变；重放返回原winner且新生成的候选ID不产生业务写入 |
| 05 | Agent slot input省略server fields；外层digest绑定完整`slot_id + requirement_digest + target` |
| 06～11 | Run/worker/Subagent/Capability/Skill消费的完整RunBindings和Deployment closure不变 |
| 12 | Context snapshot digest、tenant/policy/deployment validation不变；`xcb`由Agent resolution transaction生成 |
| 13～16 | MCP/Sandbox/Artifact/Model contract与capacity/security boundary不变 |
| 17 | OpenAPI create-input union与read closure分离；unknown/server-owned fields在写入前fail closed |
| 18 | 增加DTO closedness、materialization、owner/digest与Receipt replay测试；production L4～L6仍未执行 |

复核覆盖state ownership、IDs与closed JSON、errors、transaction/Event/Receipt、permission、capacity、recovery和fixtures。CR-204
不增加table、aggregate、ResourceKind、JobKind、WorkClass、ComponentRole、Secret路径、route或兼容fallback；PostgreSQL仍是
Deployment/Receipt authority，NATS仍只携带wake。受影响合同按02→05→12→17→18完成复核并恢复Accepted，授权clean-cut实现。

### CR-203 Agent publication identity cycle cross-review

productization fresh first-Run探针发现：public Agent Draft必须在`draft:publish`前已有immutable typed Plan Artifact；现有Plan v4
又要求该Artifact嵌入publish时才由服务端生成的Agent Interface Revision UUIDv7。public route没有reserve-ID或caller-selected
Version ID，因而任何只走`/v1`的客户端都无法构造首个合法Agent。这是05 owner contract与02/17 lifecycle之间的P0环。

CR-203选择内容寻址消环：Typed Plan v5绑定Draft已知的`interface_contract_digest`。Interface/Plan Revision仍由同一publish
command生成不同UUIDv7；Deployment与Run仍冻结exact Revision/Deployment ID。digest只证明Plan针对哪个Interface contract编译，
不参与active-head查找、compatibility selection或跨Resource拼接。

| Spec | CR-203 结论 |
|---|---|
| 00 | 保持In Progress；记录first-Run P0与clean-cut只接受Plan v5，不升级为Verified |
| 01～04 | plane、Resource/Version/Deployment、Job/Receipt/Event、tenant/permission authority不变；不新增表、route、ID kind或current projection |
| 05 | Typed Plan wire升为v5并绑定`interface_contract_digest`；publish后Plan Revision仍引用exact Interface Revision ID |
| 06～07 | admission/materialization先验证exact Interface/Plan同owner、同publish batch，再比较contract digest；失败时Node/Job零写入 |
| 08～16 | external leaf、Subagent、Capability、Skill、Context、MCP、Sandbox、Artifact、Model payload与owner transaction不变，只消费current Plan v5 |
| 17 | public DTO不接受Version ID；publish响应仍返回server-generated Version matrix；Deployment create加强same-owner/batch验证 |
| 18 | 增加fresh public authoring正向fixture及wrong digest、cross-Agent拼接、Plan v4负向fixture；L4～L6状态不变 |

复核覆盖state ownership、ID与closed JSON、错误、transaction/Event/Receipt、permission、capacity、recovery和fixture。CR-203
不增加table、aggregate、ResourceKind、JobKind、WorkClass、ComponentRole、Secret路径、public/internal route或兼容fallback；NATS仍
只携带wake，PostgreSQL仍是Version/Deployment/Run authority。受影响合同按05→06～16→17→18完成复核并恢复Accepted，授权实现
Plan v5、same-owner/batch repository guard和fresh first-Run evidence；它不授权伪造production L4～L6 passed状态。

### CR-202 Registry Validation closure impact review

实现检查发现 Management Gateway 已能创建`RegistryValidation` Job，但15个现有`ComponentRole`没有任何一个合法claim
该WorkClass，且已有generic `commit_job`不能同一事务写入Resource validation。这不是CLI或本地profile可以填补的缺口：
否则会出现由CLI伪造summary、Gateway内联执行，或Job/Resource只完成一半的状态。

| Spec | CR-202 结论 |
|---|---|
| 00 | 撤回`Verified`状态；00～18保持Accepted/In Progress，L4～L6与production宣称仍未通过 |
| 01 | control/durable/untrusted plane边界不变；新增的是trusted durable Registry Validation physical role，不执行代码或外部I/O |
| 02 | 新增closed `RegistryValidationWorker` ComponentRole（16个实际部署role），Resource仍是Draft/summary唯一current-state authority |
| 03 | validation summary、Job terminal、Event/Outbox/Receipt必须同一fenced transaction，不新增table、aggregate或projection |
| 04 | 用已有`ServiceIdentity` tenant binding作受限completion actor；不能用InstallationOperator、原author冒充workload或跨tenant查找权限 |
| 05～06、08～16 | Plan、Run、Capability、Skill、Context、MCP、Sandbox、Artifact、Model数据/公开合同不变；它们不取得RegistryValidation owner权限 |
| 07 | WorkClass映射补齐为RegistryValidation -> dedicated worker，独立pool/manifest/claim/recovery，不复用Scheduler或API |
| 17 | validate继续返回同一个Job Operation；handler/CLI不提供或提交summary，terminal语义绑定atomic owner commit |
| 18 | candidate/profile/Helm/GitOps必须登记第16个role；增加L1-L3 worker transaction/process evidence，L4-L6仍为未执行外部门禁 |

复核覆盖state ownership、IDs/JSON schema、errors、transactions/events、permissions、capacity、failure recovery和fixtures。
CR-202不新增table、ResourceVersion、Deployment variant、public route、JobKind、WorkClass、Secret路径或runtime fallback。受影响
合同已由Architecture Revision回到Accepted，因而可以生成实现；实现、fresh PostgreSQL transaction tests、独立进程L3和
manifest/preflight仍是当前产品化工作，未执行L4～L6也不因该交叉复核变为passed。

### CR-201 repository completion scope impact review

项目owner决定不以采购或维护真实多节点Kubernetes环境作为00～18关闭条件。该决定只改变资格证据的归属与状态语义，不放宽运行时
安全合同：production topology仍要求exact digest、独立role/pool、`RuntimeClass=runsc`、admission/RBAC/NetworkPolicy、无runc fallback、
容量隔舱、restore和GitOps回滚。真实环境门禁由“spec完成前置”改为“部署方声明production-ready前置”。

| Spec | CR-201结论 |
|---|---|
| 00 | `Verified`定义收敛为仓库范围合同/实现证据；Archived仍只在clean cut与`docs/current`更新后使用 |
| 01～17 | domain、authority、machine schema、error、transaction、event、permission与recovery合同不变；已有L1～L3/静态部署证据用于关闭 |
| 18 | L4～L6实现保留为profile、validator、preflight、workflow和runbook；真实节点、soak、restore和promotion记录为未执行的release gate |
| implementation-plan | Phase 1～4的仓库交付关闭；目标环境执行和clean cut移出实现backlog |
| qualification docs | 保留可选production运行手册，状态明确为Not run，禁止伪造passed Evidence或CapacityProfile |

复核覆盖state ownership、IDs/JSON/protobuf/OpenAPI schema、errors、transactions/events、permissions、capacity、failure recovery和fixtures。
CR-201不新增或删除table、aggregate、route、ComponentRole、WorkClass、JobKind、Secret路径或runtime fallback；也不把OrbStack、Docker、
静态Helm或CI candidate冒充为live L4～L6。基于commit `1efcbabc17af73bef9f21237eee65a5e6af78f19`，GitHub CI run
`33182282744`与production-candidate run `33183969085`均成功；当时00～18曾推进Verified / CR-201并关闭仓库范围，
该状态已由CR-202状态纠正与CR-203实现反馈撤回，不代表当前状态。

### CR-200 Artifact storage authority closure impact review

Data Worker stage RPC接线确认producer DTO正确排除了storage authority，但tenant exact closure没有write binding或encryption domain，继续实现只能
使用process global default。CR-200把两者收敛到既有`PolicyKind::ArtifactIo` closed document v3。

| Spec | CR-200结论 |
|---|---|
| 00～01 | clean `/v1`与plane边界不变；修复tenant storage选择缺少单一authority |
| 02、04 | ArtifactIo仍走Resource→immutable Version→Deployment→TenantConfig slot；v3新增binding digest与EncryptionDomain ID |
| 03、05～12、14、16 | ID、Job、Receipt、Event、Run/Capability/Context/Sandbox/Model owner不变 |
| 13、15 | owner admission冻结v3字段；producer RPC不携带storage authority，Data Worker内部生成locator/evidence |
| 17 | public/internal producer DTO不暴露bucket/key/binding/encryption domain |
| 18 | installed catalog登记supported binding digests；增加unsupported/drift/injection zero-I/O矩阵 |

复核覆盖state ownership、IDs/schema、errors、transactions/events、permissions、capacity、failure recovery和fixtures。CR-200不新增table、aggregate、
PolicyKind、Resource、Deployment variant、public route、ComponentRole、WorkClass、JobKind或Secret路径；受影响规范恢复Accepted / CR-200，
Implementation Authorization恢复有效。

### CR-199 Artifact verification policy closure impact review

CR-198 waiting verification Job实现要求admission冻结scanner contract、evidence TTL和retry backoff，但三者此前仅由Artifact Gateway进程配置
提供，MCP owner既不能读取该配置，也不能合法使用默认值。CR-199把它们收敛到既有`PolicyKind::ArtifactIo` closed document v2。

| Spec | CR-199结论 |
|---|---|
| 00～01 | clean `/v1`、plane/service边界不变；该变更修复跨role配置漂移，不增加authority |
| 02 | ArtifactIo Policy仍走Resource→immutable Version→Deployment→TenantConfig slot；document wire升级v2 |
| 03 | Job/Receipt/Event/Outbox、CAS与恢复合同不变；已存Job继续使用admission冻结值 |
| 04 | v2新增scanner digest、bounded evidence TTL/retry；`rules_digest`仍摘要完整document |
| 05～12、14、16 | Plan/Run/Capability/Context/Skill/Sandbox/Model无schema或owner变化 |
| 13 | MCP discovery只消费owner transaction从exact slot冻结的verification closure，caller/Worker不得覆盖 |
| 15 | public/internal Artifact admission统一复制v2字段；Data Worker installed manifest验证scanner support |
| 17 | public DTO不新增字段；缺失/错version/unsupported closure映射safe server configuration failure |
| 18 | L1～L4增加v1/缺字段/超限、policy drift、unsupported scanner和rollout readiness/zero-I/O矩阵 |

复核覆盖state ownership、IDs/schema、errors、transactions/events、permissions、capacity、failure recovery和fixtures。CR-199不新增table、
aggregate、PolicyKind、Resource、Deployment variant、public route、ComponentRole、WorkClass、JobKind或Secret路径；process config只声明支持集合和
硬上限，不再拥有tenant业务默认。受影响00、02、04、13、15、17、18恢复Accepted / CR-199，Implementation Authorization恢复有效。

### CR-198 MCP discovery Artifact handoff impact review

production discovery driver接线确认13的直接Discovery Snapshot commit假设结果Artifact已经Ready，但15要求internal producer在owner Job开始前
预分配Artifact closure，并禁止Data Worker推进Ready。若MCP Worker自行写object/验证，或先Ready再分别写Link/Snapshot，都会绕过Artifact
authority并产生部分提交。CR-198使用既有MCP discovery Job与一个预分配的shared `ArtifactScan` Job形成durable两段交接。

| Spec | CR-198结论 |
|---|---|
| 00～02 | clean `/v1`、plane、Resource lifecycle与exact Deployment binding不变；登记两段交接，不新增resource noun |
| 03 | 两个Job各自拥有lease/current state，以typed payload、Event/Outbox wake连接；最终owner事务重验双方fence并原子结算 |
| 04 | admission在外部I/O前冻结stage/verify quota、classification、retention与Policy；Secret仍只在Egress最后一跳解析 |
| 05～12 | Plan/Run/Subagent/Skill/Capability/Context contracts不变；discovery不是Run leaf、Invocation或Context Observation |
| 13 | MCP Job拥有远端descriptor attempt与最终Snapshot；Egress只返回bounded bytes/evidence，Worker经closed Artifact stage后durable park |
| 14 | Sandbox无变化；不得借Sandbox或脚本执行discovery/scan |
| 15 | Data Worker拥有stage与`ArtifactScan` Job，最多推进Verified；MCP owner事务推进Ready并创建Evidence Link/Snapshot |
| 16 | Model Inline-only与ModelTurn不变；descriptor正文不进入Model输出路径 |
| 17 | public command/Operation仍只投影MCP Job；内部Artifact/Blob/verify Job与storage evidence不公开 |
| 18 | `mcp_host` role新增独立discovery workload pool；Artifact仍三role，增加stage/verify/wake/finalize kill与capacity矩阵 |

复核覆盖state ownership、IDs、closed JSON/protobuf schema、errors、transactions、events、permissions、capacity、failure recovery与fixtures：
Artifact/Blob/verify Job identity由admission生成并冻结，Egress和public caller不能覆盖；stage/verify/owner finalize分别使用exact latest fence；
Rejected/Quarantined/timeout/cancel有closed settlement；message丢失由PostgreSQL scan恢复。CR-198不新增table、aggregate、public route、WorkClass、
JobKind、ComponentRole、Artifact role或Secret路径。受影响00、03、13、15、17、18恢复Accepted / CR-198，Implementation Authorization恢复有效。

### CR-197 durable trace identity and transport impact review

实现审计确认01/18要求跨进程trace，但现有合同只有部分Model/Sandbox digest，无法在通用Job恢复后重建，也未规定第三方header边界。
受影响规范先进入Draft / Architecture Revision；按03→04/06/07→08/10/12～16→17/18完成以下复核后恢复Accepted：

| Spec | CR-197结论 |
|---|---|
| 00/01 | trace是跨plane correlation concern，不是新plane、service或business authority |
| 02/05/09/11 | Resource lifecycle、Plan、Capability registry和Skill package不保存或选择trace，合同不变 |
| 03 | 唯一定义`TraceIdentityV1`与exact W3C v00 parent；durable owner复制trace ID，span ID只属于物理hop |
| 04 | trace不授予tenant/principal权限；禁止baggage、敏感/高基数attribute和第三方header传播 |
| 06～08 | root/child Run、Job、wait、recovery保持trace ID，但state/fence/first-winner语义不依赖trace |
| 10、12～16 | 各execution owner复制trace ID；内部mTLS hop生成child span，Egress/storage/guest边界剥离header |
| 17 | public absent/good/bad parent、response/problem/Event correlation冻结；无新route |
| 18 | L1～L4增加格式、跨进程、kill/reclaim、动态canary和第三方零header证据 |

复核覆盖state ownership、ID、JSON schema、errors、transactions、events、permissions、capacity、failure recovery与fixtures：trace ID可作为
bounded typed snapshot字段和Event correlation字段，但不得进入Receipt request digest、owner CAS、Job lease/fence、quota、selection或业务索引；
span不持久化。CR-197不新增table、aggregate、route、WorkClass、Secret路径或外部header。受影响规范恢复Accepted / CR-197，Implementation
Authorization恢复有效。

### CR-196 OAuth token endpoint explicit TLS trust impact review

真实OAuth Callback/token exchange准备确认CR-195只把MCP Streamable HTTP endpoint的exact Trust Policy编译为可执行PEM roots；OAuth token
reqwest client仍使用系统默认CA。04已禁止任何首版remote HTTPS adapter以默认trust补全closure，因此machine/runtime合同不一致。CR-196要求
OAuth installed verification binding同时冻结Deployment closure exact Trust Policy及bounded PEM roots，并绑定exact Auth Policy/profile/token endpoint。

| Spec | CR-196结论 |
|---|---|
| 00～02 | clean `/v1`、Resource/Deployment owner不变；OAuth startup binding增加content-addressed trust material |
| 03、05～12、14～17 | Job/Run/Task/Receipt/Callback/API/Sandbox/Artifact/Model schema与事务不变；调用方不携带PEM |
| 04 | OAuth token adapter只使用installed roots与canonical hostname；default roots、缺失/无效PEM、exact ref漂移均dispatch前拒绝 |
| 13 | OAuth binding分别冻结local JWT verification JWKS与token endpoint TLS roots，并重验Auth/Trust/endpoint闭包 |
| 18 | L1/L3/L4增加OAuth roots parse/digest、独立CA/SAN、zero-token-bytes负向及rollout config drift矩阵 |

00～18已按state ownership、IDs/schema、errors、transactions、events、permissions、capacity、failure recovery与fixtures全量复核。
CR-196不新增table、aggregate、route、role、WorkClass、Secret路径或第二current-state authority；PEM只存在于GitOps/startup config与Egress
bounded memory，不进入Callback/Host/RPC/Event/log。00、01、02、04、13、18恢复Accepted / CR-196，Implementation Authorization恢复有效。

### CR-195 MCP explicit TLS trust-material impact review

真实Streamable HTTP fixture接线确认MCP process-installed endpoint只保存exact Trust Policy ref，实际reqwest client没有对应可执行CA/pin
material，只能使用默认trust store。这违反04禁止运行时默认补全closure的要求，也使错Trust Policy无法在真实TLS last-hop被证明。
CR-195把显式bounded PEM trust bundle固定为Egress startup catalog material；bundle不进入业务DB、Deployment或RPC正文。

| Spec | CR-195结论 |
|---|---|
| 00～02 | clean `/v1`与Resource/Deployment owner不变；installed catalog将exact Policy编译为content-addressed运行时material |
| 03、05～12、14～17 | Job/Run/Context/Capability/Sandbox/Artifact/Model/API schema与事务不变；调用方不得携带或覆盖trust正文 |
| 04 | Egress只使用entry显式bundle与canonical hostname；默认trust、缺失/无效PEM和exact ref漂移均dispatch前拒绝 |
| 13 | MCP installed endpoint增加bounded trust bundle；Tools/Resources/Subscriptions共享同一TLS last-hop规则 |
| 18 | L1/L3/L4增加bundle解析/digest、独立CA/SAN、默认trust负向及rollout drift/readiness矩阵 |

00～18已按state ownership、IDs/schema、errors、transactions、events、permissions、capacity、failure recovery与fixtures全量复核。
CR-195不新增table、aggregate、route、role、WorkClass、Secret路径或第二current-state authority；PEM仅存在于GitOps/startup配置和Egress bounded
memory，不经Host/Worker/RPC/Event/log传播。00、01、02、04、13、18恢复Accepted / CR-195，Implementation Authorization恢复有效。

### CR-194 MCP Resource full-reconcile method-registry impact review

CR-192正文要求full reconcile执行有界`resources/list` + `resources/read`集合，但machine `PublishedMcpMethod`仅登记read。若Host直接发送
`resources/list`会绕过published profile的method limits与capability校验；若只read root则不能满足full reconcile。CR-194把list登记为与read
同属Resources capability的closed ReadOnly方法，Host根据已冻结cause/profile选择协议序列，Context Worker请求仍不携带method。

| Spec | CR-194结论 |
|---|---|
| 00～02、04～11、14～17 | clean `/v1`、plane/resource/Job/Run/Capability/API/Sandbox/Artifact/Model authority与schema不变 |
| 03 | refresh仍为同一ReadOnly attempt与JobCommit；list/read不产生第二Job、Receipt或current projection |
| 12 | root refresh只read；full reconcile以profile的独立list/read limits有界执行并仅返回聚合digest/count evidence |
| 13 | published method registry增加`resources/list`；Host选择method，Egress逐method执行limits且不返回body |
| 18 | L1覆盖list registry/capability/effect，L3覆盖full reconcile实际list/read及任一步骤不确定后的安全重读 |

00～18已按state ownership、IDs/schema、errors、transactions、events、permissions、capacity、failure recovery和fixtures全量复核。
CR-194不新增table、aggregate、route、role、WorkClass、Secret路径、Capability Invocation或第二current-state authority；wire list结果仅在Host/Egress
调用内归一化为bounded evidence。受影响00、03、12、13、18恢复Accepted / CR-194，Implementation Authorization保持有效。

### CR-193 subscription heartbeat/evidence identity impact review

CR-192把Host evidence绑定到整个`ContextSubscriptionRefreshAttempt`，而其中Job fence的`expected_version`会被每次合法heartbeat推进。
长调用若不heartbeat会失去lease；若heartbeat，Host按dispatch attempt生成的evidence又无法用latest fence提交，形成P1不可实现合同。CR-193将
remote evidence改为绑定不可变execution identity，owner terminal Receipt仍绑定包含latest fence的完整commit attempt。

| Spec | CR-193结论 |
|---|---|
| 00～02、04～06、08～11、14～16 | clean `/v1`、plane/resource/policy/Run/Capability/Sandbox/Artifact/Model authority与schema不变 |
| 03 | execution identity冻结tenant/subscription/Job、worker generation、lease generation/token、attempt number与exact request；排除version/expiry |
| 07 | heartbeat只推进owner terminal fence；不创建新physical attempt，不改变execution identity |
| 12 | Context Worker保存dispatch identity并以latest fence提交；新lease/attempt必须生成不同identity |
| 13 | Host以dispatch fence做I/O前重验，response只绑定immutable identity/request，不能延长或改写lease |
| 17 | internal RPC携带完整dispatch fence与immutable identity；无public schema/route变化 |
| 18 | 增加跨至少一次heartbeat成功提交、旧version零写入与新attempt evidence不可复用fixture |

00～18已再次按state ownership、IDs/schema、errors、transactions、events、permissions、capacity、failure recovery与fixtures全量复核。
CR-193不新增table、aggregate、route、role、WorkClass、Secret路径或第二current-state authority；`expected_version`仍是PostgreSQL CAS fence，
只是明确不属于remote业务evidence identity。受影响00、03、07、12、13、17、18恢复Accepted / CR-193；Acceptance 35增加heartbeat窗口。

### CR-192 MCP subscription refresh execution/outcome impact review

CR-191允许Context owner创建`Context -> McpOperation` Job，但现有Context Worker只执行`Context -> ContextQuery`，且合同没有定义刷新成功
evidence、Host调用边界或response后commit-window。若直接terminalize Job会伪造远端刷新；若让Host claim/提交则会把MCP协议adapter变成第二
Job authority。CR-192把refresh固定为Context Worker拥有的fenced ReadOnly attempt，并增加唯一typed internal Context→MCP Host Resource Refresh RPC。

| Spec | CR-192结论 |
|---|---|
| 00 | 登记feedback；clean `/v1`、无兼容层与current/target边界不变 |
| 01 | Context Worker拥有Job/lease/retry/terminal，MCP Host只拥有协议I/O；两个bulkhead与permit保持独立 |
| 02 | exact ResourceVersion/Deployment/Binding lifecycle不变；RPC只消费已冻结closure |
| 03 | ReadOnly attempt按JobCommit Receipt原子提交bounded terminal evidence/Event/Outbox；stale fence零写入 |
| 04 | request credential-free；Host重载Auth/session，Secret仍由Egress最后一跳解析；Context与MCP quota不混用 |
| 05～06、08～11 | refresh不是Run leaf/ContextQuery/Capability/Subagent/Skill，不创建Node、Invocation或RunValue |
| 07 | 只有Context Worker可claim；Host不claim/heartbeat/terminal，post-dispatch uncertain按bounded attempt安全重读 |
| 12 | 定义closed attempt/outcome、无subscription cache/Observation语义及retry/cancel/recovery |
| 13 | 定义Host Resource Refresh RPC、I/O前重验、bounded safe evidence和零current-state mutation |
| 14～16 | Sandbox、Artifact、Model合同不变；remote body不经Artifact或Model路径 |
| 17 | RPC只进入internal protobuf，无public route/Operation；mTLS audience与owner/fence重绑定 |
| 18 | L1～L4增加claim/commit/kill-window、零I/O、零cache及pool/topology隔离矩阵 |

00～18已按state ownership、IDs、closed JSON/protobuf schemas、errors、transactions、events、permissions、capacity、failure recovery和fixtures
全量复核。CR-192不新增aggregate、table、WorkClass、ComponentRole、public route或Secret传播；successful Job只证明一次exact bounded remote
read/reconcile，不声明Context cache、Observation、Dataset或后续query可重读。Acceptance 35：独立Context Worker→MCP Host→Egress链在任意
claim/dispatch/response/commit崩溃窗口中保持唯一Job terminal、fenced retry、zero stale commit与相互隔离的permit；wrong identity/closure/fence在
外部I/O计数为零时拒绝。00、01、03、04、07、12、13、17、18恢复Accepted / CR-192。

### CR-191 MCP subscription refresh Context Job owner-pair impact review

CR-190要求Context owner为subscription refresh/reconcile创建shared `Context` Job且不新增aggregate；实现接线发现03/machine closed registry仅允许
`Context -> ContextQuery/ContextDataset`，而该工作不是Run leaf、没有Dataset identity。以自由owner string插入会绕过typed registry，虚构
ContextQuery/Dataset又会制造第二current-state authority。CR-191增加`Context -> McpOperation`这一pair，并将使用范围限定为Context owner从
同tenant `mcp_subscription` source row与exact CR-190 request创建的refresh/reconcile Job。

| Spec | CR-191结论 |
|---|---|
| 00～02 | 登记feedback；plane、resource lifecycle、clean `/v1`不变 |
| 03 | closed owner registry增加`Context -> McpOperation`；source row kind/state与typed payload仍须事务内验证 |
| 04～06 | tenant/permission、Plan/Run/Node authority不变；refresh仍不是Run leaf |
| 07 | MCP/Context各自WorkClass、scanner与permit；wrong class/kind/payload零claim |
| 08～11 | Subagent、Capability、Skill合同不变 |
| 12 | Context owner transaction锁定subscription identity并创建该pair的Job，不新建Context aggregate |
| 13 | Host自有Job仍为`Mcp -> McpOperation`，不得创建/claim Context Job |
| 14～17 | Sandbox/Artifact/Model/API无新route、owner或projection |
| 18 | L2/L3增加同owner跨WorkClass claim负向矩阵 |

00～18已按state ownership、IDs、JSON schemas、errors、transactions、events、permissions、capacity、recovery与fixtures全量复核。该变更不新增
WorkClass、aggregate、table、role或public API；`McpOperation`继续是subscription current-state authority，Context Job只拥有物理work state。
Acceptance 34：Context owner只可从exact active subscription创建至多一个typed refresh/reconcile Job；MCP与Context worker即使看到同一owner ID也
只能claim自身WorkClass与payload。00、03、07、12、13、18恢复Accepted / CR-191。

### CR-190 MCP subscription→Context durable admission impact review

production MCP Host接线确认13要求notification触发Context invalidation/reconcile，但12未定义durable接收owner；现有port只有测试target，若由Host
生成work digest、直接写Job或使用内存回调，会绕过Context exact Deployment/authorization、Receipt与独立WorkClass capacity。CR-190将该边界
冻结为Context application owner transaction：exact subscription evidence输入，shared Context Job + Command Receipt/Event/Outbox原子提交，返回
绑定request的durable acceptance；MCP Worker随后只结算自身Job。

| Spec | CR-190结论 |
|---|---|
| 00 | 登记implementation feedback；clean-cut、`/v1`及完成定义不变 |
| 01～04 | plane、shared Job/Receipt/Event/Outbox、tenant/authorization authority不变；不新增表或current projection |
| 05～06、08～11 | Plan/Run/Subagent/Capability/Skill语义不变；subscription refresh不是Run leaf |
| 07 | MCP与Context使用独立WorkClass/permit；Context owner transaction创建Context Job，MCP Job不执行backend |
| 12 | 新增exact subscription invalidation/reconcile admission、idempotency、transaction、error和recovery合同 |
| 13 | Host构造closed request、验证durable acceptance并在kill/restart后Receipt replay；不得生成work digest |
| 14～16 | Sandbox/Artifact/Model与Inline-only不变 |
| 17 | 无新public route/body；subscription management DTO不暴露internal Job或digest override |
| 18 | L2/L3增加唯一Context Job、commit-window kill、stale fence与pool isolation证据 |

00～18已按state ownership、ID、JSON schema、error、transaction、Event、permission、capacity、failure recovery及fixture全量复核。CR-190不新增
aggregate、表、WorkClass、deployment role或public API；Context Job沿用shared Job，历史沿用Event/Outbox，幂等沿用Receipt。Acceptance 33：任一
MCP subscription invalidation只能由Context owner从exact committed evidence创建至多一个Context Job；Host/Context任一进程在acceptance窗口崩溃
后replay仍返回同一durable work，stale session/fence零创建且两个pool互不占用。00、07、12、13、18恢复Accepted / CR-190。

### CR-189 remote Context exact transport authority impact review

production RemoteSearch接线确认12正文要求Context Deployment冻结canonical endpoint，但machine binding此前只保存
`endpoint_identity_digest + region`，无法从Run closure唯一恢复目标；Context closure还缺少exact TLS/trust Policy，且所有Context
backend都没有required Worker manifest。若由进程配置补齐URL或默认信任，会绕过02 exact Deployment、04 Egress last-hop Policy和07
rollout drift门禁。CR-189补全已声明但漏入machine wire的执行闭包，不改变Context Interface、Query/Observation、Job或current-state owner。

| Spec | CR-189结论 |
|---|---|
| 00 | 登记remote Context实现反馈；clean-cut、protocol版本和current/target边界不变 |
| 01 | Control/Durable/Execution plane与Context独立Worker边界不变 |
| 02 | runnable Context Deployment必须冻结required Worker manifest；RemoteSearch同时冻结canonical endpoint及exact Network/TLS/Trust Policy |
| 03 | ContextQuery/Job/Receipt/Event/Outbox authority、锁序与恢复不变 |
| 04 | Context Worker只提交opaque exact policy/Secret binding；Egress最后一跳重验并解析Secret，Worker不取得Secret值 |
| 05～06 | Plan v4 slot、RunBindings与external leaf owner transaction不变，只复制补全后的exact Deployment closure |
| 07 | Context startup manifest和claim在lease/quota mutation前匹配required manifest；dispatcher在I/O前再次匹配endpoint/policy closure |
| 08～11 | Subagent、Capability、Skill合同不变；不把Context改成Capability |
| 12 | `RemoteSearch` machine binding补齐endpoint，Context Deployment closure补齐Network/TLS/Trust与required Worker manifest |
| 13 | MCP Resources仍由exact MCP Deployment/Host拥有transport/OAuth，不复用RemoteSearch自由URL |
| 14～16 | Sandbox、Artifact、Model authority与Inline-only不变 |
| 17 | publication/Deployment validation拒绝缺失、kind错误或digest漂移；public API无runtime URL/Policy/Worker override |
| 18 | L1～L4增加错endpoint digest、Policy kind/digest、Worker manifest、空registry和rollout drift零外部I/O证据 |

CR-189不新增表、aggregate、Job、WorkClass、queue、public route或deployment role。endpoint与Policy只存在于immutable Context
Deployment closure，ContextQuery admission复制其exact snapshot；PostgreSQL仍是claim/lease/quota authority，Egress仍是唯一网络与Secret
last hop。00～18按state ownership、IDs、JSON schemas、errors、transactions、events、permissions、capacity、recovery和fixture逐份复核，
无新增P0/P1。Acceptance 32：任一remote Context物理attempt都能仅从Run冻结的Context Deployment与已资格Worker镜像唯一解析
endpoint、Network/TLS/Trust/Secret和adapter mapping；任一缺失或漂移必须在lease/quota mutation或外部I/O前按所属门禁fail closed。

00～18恢复Accepted / CR-189并继续implementation-plan。

### CR-188 installed Capability codec authority impact review

production Capability Worker接线确认09的HTTP/gRPC/MCP contract只保存mapping/protocol digest。digest能验证相等性但不能创建codec；
仓库也只有测试codec，Remote Deployment没有required Worker manifest，因此空registry或任意资格外镜像仍可能claim Job后才失败。
CR-188把mapping authoring与runtime执行分开：publication/image build验证并编译静态codec，startup以closed installed codec manifest报告
exact identity/module/descriptor，Deployment冻结exact codec与Worker manifest，claim/dispatch在外部I/O前重验全部闭包。

| Spec | CR-188结论 |
|---|---|
| 00 | 登记实现反馈；clean-cut、协议和current/target边界不变 |
| 01～06 | aggregate、ID、Policy、Plan v4、Run/Node/Scope authority不变 |
| 07 | Worker startup/claim必须验证closed manifest；WorkClass、queue、lease与permit不变 |
| 08 | child Run冻结同一exact Deployment，无新递归或父子状态 |
| 09 | 新增installed protocol codec manifest及remote required Worker manifest binding；无运行时模板/代码下载 |
| 10 | Invocation admission冻结codec/descriptor/Worker manifest，claim和dispatch逐层重验 |
| 11～12 | Skill/Context不取得codec执行权威；Context独立Worker不变 |
| 13 | MCP Tool codec只负责Platform↔MCP typed mapping，Host仍拥有remote Streamable HTTP语义 |
| 14～16 | Sandbox/Artifact/Model plane、Inline-only与三Artifact role不变 |
| 17 | publication/Deployment validation拒绝缺失或资格外installed codec；无新public runtime override |
| 18 | 增加错codec/module/descriptor/Worker manifest、空registry及rollout drift的L1～L4负向证据 |

CR-188不新增表、aggregate、Job、WorkClass、queue、public route或deployment role；manifest属于immutable Deployment/worker startup
bounded JSON closure。state ownership、IDs、errors、transactions、events、permissions、capacity与failure recovery逐份复核无新增P0/P1。
Acceptance 31：任一remote Capability物理attempt都能从exact Deployment与已资格Worker镜像唯一解析静态codec，任何缺失或漂移在
Egress/MCP I/O前fail closed。00～18恢复Accepted / CR-188并继续implementation-plan。

### CR-187 Model Policy authority impact review

production Model admission接线确认16要求冻结safety/budget/truncation，但shared `PolicyResourceSpec`没有这些PolicyKind的typed document，
且Model Deployment没有Safety Revision角色；测试provider因而能自由填写attempt/cost/safety。CR-187在04冻结ModelSafety、ModelBudget和
ModelPublicProjection三个nominal document，在16把exact Safety Revision加入Model Deployment closure，并规定effective ceiling取全部冻结
上限的最小值。Policy `rules_digest`仍是唯一canonical digest authority。

00～03、05～15的aggregate、Plan v4、Run/Job/Invocation、数据库表与执行平面不变；17的generic Resource/Deployment payload schema新增
closed字段但不新增route或caller runtime override；18增加wrong kind/missing document/digest、zero/overflow、宽松projection及恢复漂移L1～L3。
无新表、aggregate、Job、WorkClass、queue或deployment role。Acceptance 30：ModelTurn admission的安全指令、attempt/token/cost和overflow
全部可从exact Run/Deployment/Policy closure确定，任何缺失或漂移fail closed。

00～18逐份复核无新增P0/P1，恢复Accepted / CR-187并继续implementation-plan。

### CR-186 canonical Model request assembly impact review

实现反馈确认11/16已有七阶段顺序与digest要求，但`ModelContentSource`尚未携带owner-scoped ID、实际content digest、phase/ordinal和
逐块预算，因而恢复重组、同phase排序、overflow与Skill role隔离无法逐字节验收。CR-186冻结closed source-map wire：初始四个authority/
input phase必选，Skill/Context可选；同phase ordinal唯一；Skill/Context/User恒为非特权`user` role；所有overflow fail closed且首版不隐式
truncation。assistant/tool continuation使用独立尾部phase，不改变初始七阶段顺序。

00～04、06～10、12～15与17的state authority、ID、transaction、public API、table和topology不变；05仅删除与Plan v4冲突的旧示例，
11/16冻结canonical wire，18增加重排、ordinal碰撞、role injection、budget/digest与恢复一致性L1～L3 fixture。无新表、aggregate、Job、
WorkClass、route或deployment role。Acceptance 29：相同exact blocks逐字节产生同一messages/source map digest，任何漂移或超限fail closed。

| Spec | CR-186结论 |
|---|---|
| 00 | 登记assembly实现反馈；clean-cut与current/target边界不变 |
| 01～04 | authority、tenant、Policy和persistence不变 |
| 05 | Plan继续version 4；删除旧`messages` node示例，exact source仍来自published closure |
| 06～10 | Run/Job/Invocation/selection owner事务不变 |
| 11 | 冻结phase、source-map、trust role与overflow语义 |
| 12～15 | Context/Artifact只提供exact bounded material，不取得assembler authority |
| 16 | 冻结`ModelContentSource`字段与canonical ordering |
| 17 | 无public caller-supplied prompt/source-map surface |
| 18 | L1～L3增加determinism、role isolation、budget和recovery矩阵 |

00～18复核无新增P0/P1，恢复Accepted / CR-186并继续implementation-plan。

### CR-185 canonical Skill package byte contract impact review

实现反馈确认11此前只定义逻辑目录、manifest和ArtifactSliceRef，没有定义上传Artifact的物理archive编码。若由实现猜测
ZIP/TAR或按本地目录展开，会让发布输入、内容扫描、digest、跨语言authoring与运行时materialization不确定。CR-185按
11→15→17→18完成上游到下游复核并形成Acceptance 28：首版只接受无压缩、长度前缀、无trailing bytes的
`insight.skill-package/1` frame和`application/vnd.insight.skill-package` verified media type。

| Spec | CR-185结论 |
|---|---|
| 00 | clean-cut `/v1`、plane authority与实现/目标边界不变；登记physical package contract |
| 01～04 | service ownership、tenant/permission/policy与persistence authority不变；无新表、Job或current-state projection |
| 05～10 | Plan v4、Run/Job/Invocation、selector与Capability语义不变；只消费exact Skill binding |
| 11 | 冻结frame magic、整数编码、entry顺序、EOF、专用media、双digest与无压缩规则 |
| 12～14 | Context/MCP/Sandbox不解析或执行Skill package；既有权限与execution-plane边界不变 |
| 15 | Artifact verification逐entry校验；generic read、ZIP/TAR猜测与storage credential暴露仍禁止 |
| 16 | assembler只消费已验证instruction slice；Model Worker不解析archive、不持有object locator |
| 17 | 上传API只接受exact Artifact/media，不提供服务器端格式转换或运行时协商 |
| 18 | L1～L3增加frame corruption、exact binding与mTLS materialization证据；L4～L6门槛不变 |

CR-185未引入新公开route、table、aggregate、worker class或deployment role；错误继续投影既有invalid input/integrity/denied类别。
00～18逐份复核没有新增P0/P1，11、15、17、18与00恢复Accepted / CR-185，授权按implementation-plan继续实现。

### CR-184 external leaf terminal continuation cross-review

实现反馈确认Model/Capability/Context若在terminal时把同一leaf Node从Waiting置回Ready，05现有`None` observation会再次dispatch，
形成重复副作用和不可闭合恢复窗口。05→06→07→10/12/16→18已统一为：terminal owner以自己的Job fence first-winner写声明output
RunValue，终结当前leaf Node，并按exact Plan `resume`原子创建目标NodeExecution与唯一Orchestration Job。retry/deferred不激活resume；
terminal failure进入既有typed failure/error-cancel convergence。Task/Signal仍在原wait Node消费typed resolution，不受此变更影响。

00～04、08～09、11、13～15、17的authority、ID、public schema、permission、table与topology无变化；shared Job、Invocation/ContextQuery/
ModelTurn、RunValue/Scope仍是唯一current authority，不新增表、WorkClass、queue、profile字段或兼容层。L2必须证明owner/value/resume原子
first-winner与错误schema整批回滚；L3必须在terminal前后kill并证明同一leaf不重派。Acceptance 27：external leaf terminal只激活exact
resume目标，永不重新Ready同一leaf Node。00～18影响复核无未关闭P0/P1，恢复实现授权。

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

### CR-183 selection document digest cross-review

CR-183删除Selection document内不可能闭合的自引用digest，复用Policy Resource唯一`rules_digest`与exact Revision semantic digest。
02/09 publication、04 selector、06/07 owner重验与18 fixture均已有对应digest边界；05、08、10～17、baseline、Plan v4、API与topology无字段或
authority变化。Acceptance 26：Selection document canonical bytes不含自身digest，publication要求`rules_digest == canonical(document)`，
runtime exact binding继续验证Revision/Deployment digest。00～18影响复核无未关闭P0/P1。

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

CR-171继承CR-170的public Artifact DTO与可信服务交接结论，并消解实施反馈发现的tenant Artifact default Policy authority缺口。
以下列表是CR-216之前的历史收敛结果，其中Sandbox项已被本文件顶部CR-216 ruling取代：

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
| 00、01～04、07、09～10、14～15、17～18 | Accepted / CR-216 revision 9 impact-reviewed | OpenSandbox Kubernetes-only provider、one-shot create authorization、RunValue正文authority、Runtime/Profile candidate binding、point-read orphan decision、atomic continuation reclaim、Job/cleanup fence、inert candidate + Armed runner activation、Direct/Disabled network、remote-only MCP 与资格闭合 |
| 12～13 | Accepted / CR-193（CR-216影响复核） | Context/MCP authority与remote-only协议不变 |
| 05～06 | Accepted / CR-184（CR-189影响复核） | Plan v4 external leaf与Run snapshot只复制补全后的exact closure |
| 08 | Accepted / CR-182（CR-216影响复核） | Subagent不创建persistent Sandbox session |
| 11 | Accepted / CR-185（CR-216影响复核） | Skill/Package authority不变 |
| 16 | Accepted / CR-187（CR-189影响复核） | Model provider/Inline authority不变 |
| ADR-0001 | Accepted | target v7/23/22与GitOps/Job/Artifact简化对齐 |
| ADR-0002 | Superseded | 保留gVisor历史决策，不再作为实现输入 |
| ADR-0007 | Accepted / CR-216 revision 8 | OpenSandbox Kubernetes + BatchSandbox；不修改上游；one-shot create authorization、Runtime/Profile-bound candidate、point-read orphan decision、atomic continuation/terminal/cleanup fencing 与 Armed runner activation |
| implementation-plan | Implemented / CR-216 | OpenSandbox实现与L1～L3 passed；L4～L6仍Not run |

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
  一个Invocation/Query/ModelTurn/Task/Wake/Child，external leaf terminal owner原子写声明output RunValue、终结leaf Node并创建exact resume
  目标Node及唯一Job。stale fence、ID冲突、并发
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
| Sandbox Dispatcher | Sandbox Job claim、OpenSandbox 调用、candidate CAS、runner activation、physical evidence校验、fenced terminal commit与cleanup | 在本进程执行用户代码、绕过 Job fence、candidate selected 后另选 sandbox、解释 workload 第三方 API 语义 |
| OpenSandbox Server/Controller | Kubernetes provider、BatchSandbox/Pod/runner lifecycle、TTL/delete 与 physical observation | Platform DB/Run/Invocation mutation、public API、业务 retry/terminal/candidate selection 决策 |
| Artifact Gateway | principal upload/download | internal workload authority、maintenance |
| Artifact Data Worker | closed workload stage/read/verify/derive | public principal API、Ready owner commit |
| Artifact Maintenance | closed retention/delete/GC transition | 新business reference、public upload |
| Egress/Secret Broker | catalog egress、last-hop Secret resolution | business current state、returning raw Secret |

所有跨进程边界mTLS认证exact workload audience并重绑定tenant/owner/Job fence。plain URL/header、object key、shell command、
Secret value和任意JSON owner不是可信请求输入。

## 10. 容量与拓扑

最低隔舱：API、Orchestration/Recovery、Model、Native/Remote Capability、Context、MCP、Sandbox Dispatcher、
OpenSandbox Server/BatchSandbox Controller/sandbox Pod pool、Artifact Gateway、Artifact Data Worker、Artifact Maintenance、Egress/Secret Broker。

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
| Sandbox Dispatcher/OpenSandbox/Controller kill | 先按 Job evidence 与 provisioning token 发现 inert candidates；只恢复 selected runner；`PotentiallyStarted` 后不创建 replacement；current Job fence 提交或 UnknownOutcome + cleanup |
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
- API中spawn runtime、MCP stdio、unapproved Sandbox provider fallback、microVM、Model Artifact与dynamic storage route不存在；
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
| 多Sandbox backend与强隔离资格阻塞首条流程 | ADR-0007：首版 OpenSandbox Kubernetes-only + explicit containerd/runc；WASI/gVisor/Docker provider/microVM 均推迟，shared Job authority 不变 |
| OpenSandbox标准create缺少可依赖client幂等 | create 只产生 inert Armed candidate；metadata list 只发现，PostgreSQL CAS 选唯一 candidate，durable activation latch 最多启动一次 Package；不修改上游 |
| workload外部写幂等被误归平台 | Platform 只保证 candidate selection、runner activation 与 Job terminal；workload 第三方 API 副作用由 Package/目标服务拥有，可能已开始时不自动重跑 |
| 02把Draft写成mutable ResourceVersion、17又要求Version validate/publish route | Draft由Resource aggregate唯一拥有；17使用`/draft` update/validate/publish，publication后才有immutable Version GET identity |
| 02给Deployment可变state/version却又要求immutable，17的`suspend`未指定authority | Deployment是immutable closure；activate/suspend以Resource ETag做CAS，只改Resource active binding/gate |
| public Run未选择Agent且admission entry无durable authority | request显式携带`agent_id`；Agent Deployment冻结validated entry ID/kind，admission不接受内部入口或临时读Artifact |
| Artifact candidate把内部ID/policy/grant token当作public request并只信自由principal header | public DTO只含业务意图与opaque completion proof；服务端生成内部identity/closure；Public Gateway到Artifact Gateway使用exact audience mTLS并在DB重绑定current principal |
| 多个active Artifact Policy使“服务端选择”没有唯一authority | Tenant current config新增exact Retention与ArtifactIo revision slot；Artifact prepare验证slot kind/digest/gate，禁止任取active row或fallback |
| 02要求统一Deployment binding，但Skill/Policy/Sandbox仍直接激活Version | CR-173目标：增加closed `skdep`/`pdep`/`sxdep` closure；definition-only Deployment不执行代码，但冻结requirement/applicability/qualification；ContextDataset generation data head是唯一Version-head例外 |
| 17注册八类deployment route而Rust closure只支持六类 | CR-173目标：owner ID/kind/closure/allowed-edge先闭合，再开放完整OpenAPI；unsupported kind不得以永久shape error冒充已实现route |

## 15. Acceptance 记录

以下1～12项是CR-172历史Acceptance记录，不代表CR-173已关闭；CR-173必须在它们之上补齐13～17并重新签署Acceptance：

1. `rg` stale-contract scan确认WASI/gVisor/runsc/microVM、Managed stdio、Installation Release、ManagementOperation、
   Model Artifact Producer和旧Sandbox role只出现在历史/否定/明确推迟语境，不再是CR-216首版正向requirement；
2. 文档链接、编号、状态、术语、ID、owner、schema version和table budget对齐；
3. `git diff --check`通过；
4. implementation-plan的每个phase都只从Reviewed合同引用可观测行为和分层证据；
5. 批准前不对外声明target API、topology、capacity、schema v8或runtime已经上线。
6. CR-216 revision 1 topology 复核确认所有 role 都没有 Docker/CRI socket；Dispatcher 只调用 internal OpenSandbox lifecycle 与
   fixed runner protocol。OpenSandbox/Kubernetes status 不成为第二 Job authority，Direct network 不授予 host network/socket/Platform
   credential，developer-preview 不得冒充 gVisor/microVM 隔离。
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
26. Selection document不保存自引用digest；Policy `rules_digest`唯一等于document canonical digest，exact Revision/Deployment binding冻结runtime identity。
27. r328将03已要求的Job kind落实为18项closed nominal registry及25项`JobKind × WorkClass × OwnerKind`machine mapping；该实施对齐不改变
    表预算、authority、public route、Plan/Profile版本或首发拓扑。PostgreSQL typed column与所有claim predicate贯通前仍是implementation gap。
28. r329以`jobs.job_kind`贯通baseline、全部Job写读与Artifact/Context热claim，并把managed MCP physical session收敛到合法共享Job owner；
    Sandbox capability与managed MCP session由closed JobKind区分。checker锁定INSERT完整性且禁止JSON kind热路由/未注册SQL owner，不新增表、
    aggregate、public字段或兼容路径；fresh PG16与production-equivalent资格仍按18独立取证。
29. Acceptance 36：CR-198 discovery admission预分配exact Artifact/Blob/`ArtifactScan` Job与quota/policy/retention closure；Egress仅返回bounded canonical bytes，
    Data Worker最多推进Verified，MCP owner以durable wake恢复并在一个事务中创建Ready Evidence Link、Discovery Snapshot及双Job结算。L1～L4
    覆盖wrong identity/fence/digest、message全丢、所有stage/verify/finalize kill窗口及独立pool饱和，且无新增表、JobKind、role或public DTO。
30. Acceptance 37：ArtifactIo Policy v3在一个closed canonical document中冻结scanner contract digest、verification evidence TTL/retry、
    write storage binding digest、encryption domain与既有media/file rules；所有Artifact admission从TenantConfig exact slot复制，v1/v2/缺失/
    超限/unsupported scanner或binding在object I/O与Job claim前fail closed，已存Job不随policy或rollout变化，且无新增PolicyKind、表、role或public字段。
31. Acceptance 38：CR-216 shared Job 是唯一 Sandbox work authority，RunValue是input/output正文authority；OpenSandbox Server/Controller/
    runner没有Platform DB、NATS、Run/Invocation mutation权限。Dispatcher terminal transaction重验latest Job lease fence并原子写
    Job/Invocation/RunValue/quota/Event/Outbox/cleanup intent；physical ID只作bounded internal evidence。
32. Acceptance 39：`SandboxProvisioningTokenV1` 不含 lease generation；create 可产生 bounded inert candidates，metadata list 只发现，
    current Job row CAS选择唯一candidate。fixed runner在`PotentiallyStarted`先持久化后只接受candidate/boot-bound signed activation，Package
    最多启动一次；不修改 OpenSandbox 源码，也不声称一个 token 历史上只有一个 object。
33. Acceptance 40：Profile 只允许 `Disabled | Direct` operator NetworkPolicy；Direct workload 外部副作用不在 Platform idempotency/
    exactly-once 边界，可能已开始后不创建 replacement。默认 Secret injection disabled，Artifact port 不可 activate。
34. Acceptance 41：ComponentRole、capacity、API、Artifact、deployment 与 qualification 全部改为 Dispatcher/OpenSandbox Kubernetes/
    BatchSandbox closure；不新增业务表、JobKind、WorkClass、public route、Receipt/Event kind 或 compatibility fallback，旧 WASI/gVisor/
    Docker evidence 只保留历史意义。
35. Acceptance 42：deployment source-pin OpenSandbox Server `v0.2.3`、Controller `v0.2.0`、execd `v1.0.22` 与审核 chart commit；
    Server/Controller developer Profile 单副本，`informer_enabled=false`，readiness 不能只信 `/health`；L3 必须用真实 CNI 证明
    Direct/Disabled/internal-deny/no-public-ingress，并覆盖 runner boot rollover、TTL/delete/absence 与 orphan cleanup。
36. Acceptance 43：Job payload、Event、Outbox与Receipt不复制input/result正文；CR-216 Inline request/result的effective hard ceiling为
    `min(frozen Profile, 1_048_576)`，claim/recovery从exact input RunValue重建，terminal first-winner写预分配output RunValue。terminal
    清除标准Job lease后，delete/absence只用same-row `SandboxCleanupFenceV1` generation/database-time CAS；stale cleanup零写入且不能改写
    terminal business columns。不新增业务表、aggregate、JobKind或第二lease authority。
37. Acceptance 44：只有Sandbox owner payload已持久化exact physical attempt并证明recovery只observe/replay同一effect时，expired
    `Running` Job才允许`Running -> Ready`；continuation claim增加lease generation但保持attempt count。无physical evidence、普通Job、
    新provisioning token/candidate/sandbox或Package activation一律不能复用该转换。
38. Acceptance 45：Execution Plan/Request semantic digest必须包含冻结的Runtime contract与Profile Deployment digest；candidate
    metadata任一digest不匹配时零Job mutation，不能从mutable head、provider默认值或仅格式校验补齐。
39. Acceptance 46：candidate metadata必须携带operator-only `tenant_id + job_id + physical_attempt`，orphan repository据此point-read
    唯一shared Job并重验current token、request/runtime/profile/network、selection/activation/terminal/cleanup。它只返回closed只读
    retain/delete decision；Provisioning或selected candidate retain，missing owner/stale attempt/selection后的late或unselected candidate才可
    delete；corrupt、ambiguous、数据库不可用一律retain。OpenSandbox不因此获得Platform credential或业务state权限。
40. Acceptance 47：expired Running且owner payload证明exact physical continuation时，claim必须在一个PostgreSQL transaction内逻辑完成
    `Running -> Ready -> Leased -> Running`并只提交最终Running projection；lease generation只增加一次，attempt count、token、candidate、
    activation与runner identity保持。中间Leased不得对其他事务可见，旧lease result/terminal必须stable stale-fence且零写入。
41. Acceptance 48：每次OpenSandbox create前必须由current Job fence在PostgreSQL CAS授权exact next ordinal并以database time持久化
    provisioning start、authorization count与last authorization time；同一lease/attempt的相同ordinal可重放，旧ordinal、跳号、limits漂移
    或stale lease零写入。下一ordinal必须满足durable count/quiescence/total-time hard limit，Dispatcher restart不能重置预算；candidate
    metadata ordinal未经授权时不得记录、选择或激活。
42. Acceptance 49：create authorization response必须是closed `Applied | Replayed`；只有本次CAS为`Applied`的唯一caller可恰好调用
    一次provider create，`Replayed`不得调用。authorization commit后、provider call前崩溃会burn该ordinal，恢复只能在durable
    quiescence后授权下一ordinal；每个ordinal至多一次外部create调用，全部调用数不超过Profile maximum candidates。

## 16. 最终结论与未运行环境门禁

CR-216 final implementation review没有未关闭P0/P1合同或实现偏差；00标记Implemented / repository L1～L3 passed，受影响
01～04、07、09、10、14、15、17、18与product-experience 00/06保持Accepted合同。OpenSandbox Kubernetes/BatchSandbox/Armed
runner、shared Job fencing、CLI/profile、部署与cleanup/recovery已经实现，当前产品文档已切换。

L4 production topology/strong isolation、L5 capacity/持续soak与首个CapacityProfile、L6 backup-restore/rollout-rollback和人工GitOps
promotion均Not run。它们是environment release gate，不回退已关闭仓库证据；实际通过前禁止声称production capacity、SLO、HA、
强隔离/restore或environment production-ready。
