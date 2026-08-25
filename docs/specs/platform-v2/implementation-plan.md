# Platform v2 四阶段实现计划（CR-188）

| 属性 | 值 |
|---|---|
| 状态 | In Progress / CR-189 contracts accepted; L3～L6 pending |
| 日期 | 2026-08-25 |
| 合同输入 | 00～18、cross-review CR-188/实现反馈复核、ADR-0001、ADR-0002、AGENTS.md |

> 2026-08-24：production external leaf接线发现Plan v3缺少可执行payload及candidate selection evidence，CR-181已重新打开04～18与cross-review。
> CR-181 cross-review已经关闭并恢复实现授权；Leaf/Task/Subagent dispatch必须直接实现Plan v4与exact selection/owner transaction，
> 不得恢复Plan v3或caller-supplied completion。已通过的CR-180 terminal authority实现和证据保留，不回退。

> CR-182 cross-review已关闭并恢复实现授权；candidate selection必须使用04 schema v1三种closed mode与共享纯evaluator。

> CR-183确认Selection document自身不含digest，publication只接受外层`rules_digest == canonical(document)`。

> CR-184关闭external leaf terminal重复dispatch缺口：Model/Capability/Context terminal owner必须原子终结当前leaf Node、写exact output
> RunValue并激活Plan `resume`目标Node/Job；不得重新Ready同一leaf Node。cross-review已恢复实现授权。

> CR-185已关闭：Skill package首版物理合同为11定义的无压缩`insight.skill-package/1` frame；实现必须先完成
> dedicated Artifact verification/materialization，再接入InstructionSection与deterministic Model request assembler。

> 2026-08-25 implementation evidence：fenced Skill package broker不再接受“候选集合成员”作为充分授权；它从Run snapshot的exact
> Skill slot加载enabled Selection Policy并重新执行共享selector，只允许确定性选中的Skill Deployment读取package。fresh PostgreSQL 16
> r179已通过正确读取、错误slot、未绑定Deployment、Artifact完整性与既有Run kernel first-winner回归。

> CR-186已关闭：deterministic Model request assembler使用七阶段canonical block和完整source map；Skill/Context/User固定为非特权
> role，ordinal冲突与byte/token overflow fail closed，首版没有隐式truncation或恢复时重排。

> CR-186 L1 pure assembler、Model/provider contract tests以及fresh PostgreSQL 16 ModelTurn持久化/tool continuation/first-winner
> 回归已通过；exact Agent/Plan/Skill材料与Capability投影已接入production Scheduler admission，Context observation尚待ContextQuery
> continuation接线；后续r233已关闭provider process kill/recovery，但tool-result整链仍未完成，不能把本证据宣称为完整ModelLoop L3。

> CR-187已关闭production Model admission的Policy来源缺口：Safety/Budget/PublicProjection均为closed nominal Policy document，
> Model Deployment冻结exact Safety Revision；production provider必须从这些exact documents推导安全指令、attempt/token/cost与overflow，
> 不接受测试默认值或caller自由字段。

> CR-187 exact Model Policy loader已在fresh PostgreSQL 16 r177验证Model Deployment digest、profile Revision、三个enabled published
> Policy的exact semantic/payload/rules digest与PolicyKind；wrong Deployment digest fail closed。后续r180已把这些facts接入canonical
> request与Skill/Capability投影；后续r233已补齐独立production provider进程与L3恢复证据，tool-result整链仍待完成。

> CR-188已关闭Capability remote codec authority缺口：HTTP/gRPC/MCP mapping authoring只在publication/build阶段编译为静态codec，
> Deployment冻结exact installed codec identity/module/descriptor及required Worker manifest。实现必须在claim/dispatch外部I/O前重验三者；
> 空registry、测试codec、资格外镜像或运行时模板解释器都不能计为production remote backend。

> 2026-08-25 implementation evidence：CR-188 Rust owner与HTTP/gRPC/MCP dispatcher已冻结并重验exact installed codec、完整backend
> descriptor及required Worker manifest；manifest/module/descriptor漂移在transport调用计数为零时fail closed。相关contract/adapter tests、
> strict Clippy及fresh PostgreSQL 16 r200完整Capability Invocation fixture通过。Native binary/startup manifest已在后续r208闭合；真实remote
> protocol L3与L4 rollout drift仍待完成，不能把本证据宣称为production remote backend闭环。

> 2026-08-25 implementation evidence：Capability claim command现携带进程Worker manifest digest，PostgreSQL owner transaction在启动
> attempt和预留quota前对照exact Deployment。fresh PostgreSQL 16 r201证明错镜像得到空claim、正确manifest仍通过完整Invocation闭环；
> dispatcher的codec/manifest I/O前二次校验保留。Native binary/startup及kill/recovery在后续r208闭合，Remote仍待完成。

> 2026-08-25 implementation evidence：credential-free Capability adapter request现携带Invocation已冻结的exact output schema digest，
> 消除production adapter硬编码result schema的测试缝隙。Capability Worker新增静态`builtin.echo` Native adapter：仅接受Inline、生成新
> nominal RunValue ID、保持content/classification并绑定exact output schema；Artifact输入fail closed。adapter/worker tests与strict Clippy
> 通过；它是Native真实进程L3使用的受限实现，不代表remote backend或Artifact materialization完成。

> 2026-08-25 implementation evidence：Native Capability Worker已有独立production binary与Helm role。进程以strict JSON加canonical
> config digest启动，要求配置枚举的唯一`builtin.echo` descriptor与镜像内静态registry逐字段一致，并把该清单摘要绑定到exact Worker
> manifest；claim/heartbeat使用分离且总额封闭的business/critical-control PostgreSQL pool，启动前重验baseline schema，SIGTERM进入bounded
> drain。Helm锁定digest image/config、双副本/PDB/HPA、restricted pod且只允许DNS/PostgreSQL egress；正向render与错digest、单副本、空
> PostgreSQL CIDR、错误HPA负向fixture，以及worker tests、strict Clippy、crate-boundary scan均通过。该批只关闭Native production
> composition与静态部署边界；真实多进程kill/recovery由随后r208证据关闭，但仍不关闭Phase 2 exit gate。

> 2026-08-25 implementation evidence：Capability owner新增bounded/sharded expired-lease scanner；它在quota→Run→Node→Invocation→Job
> 锁序内重验exact version/generation/token/expiry/payload/reservation，原子结算两条quota、清除旧lease，并只按冻结Effect、idempotency、
> attempt/deadline把安全工作放入新物理attempt或把未知写副作用放入`ReconciliationRequired`。fresh PostgreSQL 16 r204覆盖非幂等
> owner恢复与授权人工结算；r208使用两个真实`platform-capability-native-worker`进程、exact builtin/startup manifest与真实commit事务
> kill窗口，证明第一个进程强杀后第二个进程从PostgreSQL恢复，旧reservation恰有两条reserve/两条settle且Job不保留quota/lease。
> Native Capability process kill/recovery L3至此闭合；Remote HTTP/gRPC/MCP真实协议、错codec外部调用零计数的跨进程L3仍待完成，Phase 2
> Model/Context及整链exit gate仍未关闭。

> 2026-08-25 implementation evidence：Remote Capability已有独立HTTP/gRPC production binary与Helm role。进程只安装配置枚举且
> image内存在的`platform.json@1.0.0`静态codec，清单摘要绑定exact Worker manifest；请求使用JCS，响应经backend byte limit与全局
> JSON depth/property/item hard limit共同约束，错误状态保留给共享Effect/idempotency policy判定retry或uncertain。所有远程网络调用只经
> mTLS Egress Broker RPC，进程持有分离的business/critical-control PostgreSQL pool，不持有Secret、Sandbox或MCP transport client。
> 后续收口把该module实际执行的HTTP protocol、HTTP/gRPC request/response/error mapping及MCP output mapping分别固定为
> domain-separated compiled digest；startup对任一任意或漂移mapping/protocol digest均fail closed，不能再让同一raw-JCS实现替任意
> authoring contract背书。最终Inline实例仍由PostgreSQL owner transaction按冻结Interface schema与byte limit重验。
> Helm以digest image/config、双副本/PDB/HPA、restricted pod和仅DNS/PostgreSQL/Egress Broker egress部署，正负静态fixture、worker/
> adapter tests、strict Clippy与crate-boundary scan通过。后续fresh PostgreSQL 16 r217以真实mTLS Egress RPC、分别漂移HTTP/gRPC codec
> manifest的进程、正确Remote Worker commit-window强杀和第二进程expired-lease恢复关闭HTTP/gRPC非幂等路径L3：错manifest时Job保持
> ready且对应外部调用为零；已返回响应的进程被杀后Invocation只进入`reconciliation_required`且两种远端调用分别保持一次。
> 后续fresh PostgreSQL 16 r221以production Remote Worker→production MCP Host→mTLS Egress三进程链完成MCP ToolsCall同一矩阵：exact
> protocol/auth/discovery/Capability bindings由PostgreSQL解析，错MCP codec的Worker保持Job ready且远端调用为零；正确Worker在Host返回后
> commit-window强杀，第二Worker只收敛到`reconciliation_required`，Streamable HTTP调用保持一次。Remote HTTP/gRPC/MCP ToolsCall process
> L3至此闭合；Model/Context external leaf及Phase 2整链exit gate仍待完成。

> 2026-08-25 implementation evidence：Remote MCP现有独立`platform-mcp-host` production进程及closed protobuf RPC。Capability
> Worker只通过mTLS client调用Host，服务端在解码前要求唯一Capability Worker URI SAN；同一私有CA签发的Model Worker身份在真实
> loopback mTLS fixture中被拒绝。RPC envelope只接受bounded canonical JCS nominal contract/outcome并绑定operation+digest；Host进程组合
> `McpHostService`与Remote Streamable HTTP transport，所有网络/Secret最后一跳仍只经mTLS Egress Broker。Capability Worker已安装独立
> MCP port、从PostgreSQL解析exact execution contract，并把Host响应丢失按post-dispatch unknown交由共享Effect/idempotency policy，避免
> 非幂等写被误判为安全失败。Host Helm为双副本/PDB/HPA/restricted pod，仅允许Remote Worker入站及DNS/Egress Broker出站，无DB、
> stdio、Sandbox或host process权限；正负静态部署fixture、RPC/Host/adapter/worker tests与strict Clippy通过。production binary进程fixture
> 进一步以Capability Worker→Host和Host→Egress两段独立mTLS身份验证Host边界：ReadOnly ToolsCall到达Egress后强杀Host，调用方只能得到
> `CompletionUnknown`；重启同一binary后安全重放同一冻结contract/request成功，Egress调用总数严格为2。该证据关闭Host自身的进程恢复
> 基座。fresh PostgreSQL 16 r221随后通过PostgreSQL Remote Worker→Host→Egress三进程exact binding、错codec零调用、commit-window强杀、
> expired-lease恢复与非幂等不重放矩阵，关闭MCP ToolsCall process L3；OAuth/subscription真实协议、隔舱饱和及Phase 3 exit gate仍待完成。

> 2026-08-25 implementation evidence：ModelTurn admission现已把Plan列出的Skill/Capability slots逐项冻结进snapshot与Receipt replay，
> 并在owner transaction用各slot的exact Selection Policy重新执行共享selector；请求投影即使引用候选集合内的合法但未选中
> Capability Deployment也会整批回滚。fresh PostgreSQL 16 r178已覆盖两候选`ordered_first`正/负路径及既有tool continuation、quota和
> first-winner回归。该证据仍不等于production canonical request provider或完整ModelLoop L3。

> 2026-08-25 implementation evidence：production `PostgresControllerModelAdmissionProvider`已实现exact Agent/Profile/Provider/Model Policy
> facts读取、Plan node/input逐项绑定、Skill package materialization、Capability selector/tool schema投影、四方预算交集与Inline canonical
> request构造；fresh PostgreSQL 16 r180已验证assembly facts只返回`ordered_first`选中的Capability及ModelTurn全回归。后续r233已关闭
> provider进程组合与kill/recovery；tool-result整链仍未完成，因此不能宣称完整ModelLoop production链路。

> 2026-08-25 implementation evidence：Model continuation不再使用私有链式source-map hash；每轮从全部冻结message source重建并校验
> canonical source-map entries、content digest、ordinal、byte/token budget与classification，再派生完整digest和input token estimate。
> Model与runtime单元测试及`-D warnings` clippy已通过；跨进程tool-result kill/recovery仍归L3待办。

> 2026-08-25 implementation evidence：production Model claim现携带进程Worker manifest digest，PostgreSQL owner在attempt/quota
> mutation前对照冻结的installed adapter manifest。Model Worker同时新增bounded expired-lease recovery：在quota→ModelTurn→Job锁序
> 内重验version/generation/token/expiry/payload/reservation，按未知已dispatch请求的冻结ceiling保守结算旧reservation，原子写
> `model.lease_recovered` Event/Outbox并将同一Job置为`retry_scheduled`。恢复使用共享1 MiB Inline RunValue hard limit，不再错误截为
> 64 KiB；OpenAI fixture也按其只报告token usage、不报告authoritative monetary cost的真实合同冻结。fresh PostgreSQL 16 r233以真实
> `platform-model-worker`、mTLS Egress Broker RPC及mTLS NATS完成错manifest零Provider调用/Job保持ready、Provider响应后的commit-window
> 强杀、第二进程expired-lease恢复、安全重放及structured Inline terminal commit；Provider调用总数严格为2。Model provider process L3
> 至此闭合；Model tool-result整链、Context external leaf及Phase 2 exit gate仍待完成。

> 2026-08-25 implementation evidence：Context owner新增bounded/sharded expired-lease scanner。在quota→ContextQuery→Job锁序内重验
> exact version/generation/token/expiry/payload/reservation，原子结算已发出的read-only query、清除旧lease/reservation、写
> `context.lease_recovered` Event/Outbox并把同一Job置为`retry_scheduled`；Deferred后同attempt恢复时不会重复结算已经消费的query，下一
> 物理attempt才重新预留。fresh PostgreSQL 16 r234覆盖Deferred→resume→lease expiry→owner recovery→新attempt→唯一Observation terminal，
> quota query计数为2且result bytes只结算一次。该批关闭Context process L3的durable recovery前置门禁；独立production Context Worker、
> backend protocol与真实多进程kill窗口仍待完成。

> 2026-08-25 implementation evidence：新增独立`platform-context-worker`及NativeCatalog静态adapter。Worker在claim前通过只读扫描精确匹配
> 冻结`adapter_contract_digest`与`installed_adapter_digest`，命中后才使用现有exact-slot claim，并以独立Context permit、generic Job
> heartbeat、Context owner terminal commit和bounded expired-lease scanner组合执行。digest-bound process config会拒绝Worker manifest/runtime
> drift；独立Helm role仅开放DNS/PostgreSQL出站，不挂载Egress、Secret、NATS或Sandbox凭据，部署静态检查、crate boundary、单元测试及
> `-D warnings` clippy均通过。该批完成production process/deployment接线，但真实双进程commit-window kill/recovery fixture与remote backend
> protocol L3仍待完成，不能据此声明Context L3闭合。

> 2026-08-25 implementation evidence：fresh PostgreSQL 16 r240以真实`platform-context-worker`关闭NativeCatalog process L3。错
> installed-adapter digest的进程启动后零claim，Job保持ready且attempt/worker/quota均未变化；正确进程在Context terminal commit的数据库
> trigger窗口被强杀，测试到期其lease后由第二进程通过bounded owner scanner恢复同一Job并完成物理attempt 2。最终仅有一个
> `context_observation` RunValue、一个`context.lease_recovered` Event，terminal Job清除worker/lease/quota reservation。该证据不覆盖remote
> Context backend protocol、隔舱容量或L4 rollout。

> 2026-08-25 contract feedback：RemoteSearch machine binding只保存endpoint digest/region，无法满足12正文已经要求的canonical endpoint
> freeze，也没有exact TLS/trust Policy或required Context Worker manifest。CR-189按02→04→07→12→17→18补齐immutable Deployment closure，
> 并完成00～18 cross-review；不新增表、aggregate、Job、WorkClass、route或role。后续remote Context实现必须消费该Accepted合同，禁止以
> 进程本地自由URL、默认trust store或明文Secret绕过。

> 2026-08-25 implementation evidence：RemoteSearch新增独立`platform-remote-context-worker`，与Native Worker共享Context/Job owner但
> 只通过typed mTLS Egress RPC执行远端查询；进程启动、claim扫描和Egress端分别重验exact Worker manifest、canonical endpoint、protocol/
> result mapping及Network/TLS/Trust closure。独立Helm role仅允许DNS/PostgreSQL/Egress Broker出站。fresh PostgreSQL 16 r241以真实
> Remote Worker进程和mTLS Egress服务证明错manifest时Job保持ready、lease/quota不变且远端调用为零；正确进程在响应后的terminal commit
> 窗口被强杀，第二进程expired-lease恢复后以attempt 2安全重放，远端调用恰为2且最终只有一个Observation和一个lease-recovered Event。
> 同一owner transaction还使原Context leaf唯一`succeeded`、Run恢复`running`并只创建一个ready Return resume Node/Job，证明terminal leaf
> 不会被重新dispatch。该证据关闭Remote Worker→Egress RPC及Context external-leaf terminal/resume component L3；production HTTPS last-hop、
> resume后的Return进程执行、隔舱容量与L4 rollout仍分别归后续门禁，不能由本fixture替代。

> 2026-08-25 implementation evidence：RemoteSearch Egress connector现将canonical request编码和closed response normalization收敛为
> 同一production路径，并以真实TLS socket、独立CA、只含`search.example.test` SAN的server certificate、固定DNS结果和显式PEM trust
> bundle完成HTTPS last-hop protocol fixture；服务端逐字节验证POST path/body，客户端验证closed response、classification与evidence。
> loopback许可仅存在于`cfg(test)`且限定loopback，production仍执行public-destination SSRF deny。完整Egress test matrix 53项与strict Clippy
> 通过。该批关闭RemoteSearch HTTPS wire/protocol L3，但完整Run→Context→resume→Return、隔舱容量及L4 rollout仍未由此关闭。

> 2026-08-25 implementation evidence：runtime新增production orchestration composition，明确把business claim/heartbeat连接与
> critical-control Plan/RunValue/Skill读取、owner commit及Safety scanner连接分开，并用同一mTLS Artifact Scheduler client组合exact
> Plan materializer、Capability/Model admission、durable Plan driver、lease-fenced executor和coordinator。独立binary和Helm角色已在
> 后续批次补齐；真实process shutdown/kill fixture仍是下一门禁，不能仅凭composition函数声明production部署完成。

> 2026-08-25 implementation evidence：新增独立`platform-orchestration-worker` binary与Helm role。进程使用digest-bound closed config、
> Orchestration WorkerManifest、独立business/critical-control PostgreSQL pools、startup schema verification和mTLS Artifact Scheduler client；
> SIGTERM同时drain coordinator/safety后关闭连接池，内部driver提前退出会使进程fail closed。Helm固定image digest、双副本/PDB/HPA、
> restricted Pod、无ServiceAccount token、default-deny且只开放DNS/PostgreSQL/Artifact Data Worker。binary config test、clippy和Helm
> positive/negative static boundary已通过；真实多进程kill/recovery和容量资格尚未完成。

> 2026-08-26 implementation evidence：fresh PostgreSQL 16 r243把r241的Remote Context恢复证据继续贯通到真实
> `platform-orchestration-worker`。Context响应后的commit-window强杀先由第二个Remote Worker恢复；恢复事务释放旧Run active-work permit，
> terminal owner事务再释放当前permit并只创建一个Return continuation。随后独立Orchestration Worker以mTLS Artifact Scheduler RPC、exact
> Run/Job lease fence和冻结Agent Plan revision读取typed Plan，执行`Return`并使Run、Return Node与Job唯一终态`succeeded`。fixture同时证明
> 错manifest零claim、远端调用严格为2、Observation/Event唯一、Artifact读取实际发生且Run output已绑定。该批关闭
> Run→Remote Context→resume→Return component L3以及恢复期active-work permit泄漏；Model tool-result整链、隔舱容量与L4 rollout仍待完成，
> 因而不能据此关闭整个Phase 2 exit gate。

> 2026-08-26 implementation evidence：fresh PostgreSQL 16 r244以真实`platform-orchestration-worker`、production Model Worker、
> production Native Capability Worker、mTLS Artifact Scheduler、mTLS Egress与TLS NATS完成Run→Model tool intent→CapabilityInvocation→
> tool result→第二轮ModelTurn→Return整链。Provider调用严格为2，Model Job/Invocation严格为2，Capability Job/Invocation严格为1，唯一Return
> Node成功且无非terminal Job；Run以独立`model_structured_output` RunValue终态化，完整canonical response仍保留为独立Inline RunValue。
> 同批修复structured-output请求在tool-use轮误解析空正文、tool-result wire字节摘要、Tool/Agent schema夹具耦合及response envelope误绑定Plan
> output端口，并在fresh PostgreSQL L2全事务套件重验quota、first-winner、fan-out/result continuation。Model tool-result production component L3
> 至此关闭；隔舱容量、L4 rollout及Phase 3/4其余exit gate仍待完成。

> CR-183已实现ChildAgent exact input/route/Selection Policy facts、SERIALIZABLE owner事务重算及PostgreSQL durable Plan store dispatch；HumanTask exact Plan owner/store、response Scope binding及owner-derived resume/failure事实已接线；Timer与Signal wait均由exact Plan及数据库时间的owner事务派生。Signal owner验证exact key、可选payload schema/摘要，将payload写为immutable RunValue并绑定当前Scope；Timer due与Signal timeout使用Job typed scheduling列和critical-control bounded scanner，普通wake/timeout deadline窗口互斥。上述first-winner、Receipt replay与扫描恢复已在fresh PostgreSQL 16 r88通过；Timer在fresh PostgreSQL 16 r181完成真实多进程L3 kill-window。fresh PostgreSQL 16 r199进一步把同一链路扩展为Timer→Signal→HumanTask→ChildAgent→Return：四次durable park后分别强制终止Worker，认证Signal/Task owner恢复外部等待，exact-binding child Run在自身Timer后由第五个Worker恢复并终态化，critical-control scanner结算terminal child link、复制typed output、恢复parent，最终parent/child均成功且parent finish Node唯一。该过程还修复terminal-child误用64项claim limit而非专属recovery batch limit，以及一个scanner失败会阻断其余critical-control lane的问题。Timer/Signal/Task/Child的独立进程kill/recovery L3至此闭合；Native Capability已在fresh PostgreSQL 16 r208以真实双进程kill/recovery闭合，Model、Remote Capability与Context external leaf仍待完成。

> 2026-08-25 implementation evidence：public `/v1/runs/{run_id}/signals/{signal_key}`现已进入generated OpenAPI与Gateway；closed typed body、
> principal/run/signal scoped Idempotency-Key、stable signal key和204 no-store合同由API tests覆盖。Gateway只解析目标，Scheduler owner transaction仍会
> 在同一PostgreSQL事务重载当前Principal并要求`agent.run`，再重验Run/Job/Wake generation、exact key及可选payload schema/digest；权限撤销、
> wrong key、重复key不同request均fail closed。fresh PostgreSQL 16 r187已通过上述owner、权限及Receipt replay回归；r199已通过上述Signal、HumanTask及ChildAgent多进程kill/recovery链路。
| 公开协议 | `insight.platform/v1`、`/v1`，clean cut |

## 1. 计划原则

本计划只描述目标实现顺序，不宣称任一Draft API、schema、topology、capacity或runtime已是current behavior。
只有当phase的code、migration、manifests、tests、runbooks和evidence同时通过适用门禁，才能标记完成。

CR-173的恢复顺序已经完成到public route/schema与L1～L3开发门禁：closed Deployment matrix → exact binding/Run snapshot →
public route/schema。production qualification与cutover仍必须继续执行L4～L6，不能用开发fixture、静态manifest检查或合同Accepted状态替代。

CR-174进一步冻结Scheduler执行Branch/Map/Loop/Compute所需的closed typed expression IR、exact RunValue evidence与
materialize/evaluate/first-winner提交边界。此前手工注入`ControllerObservation`的fixture不能计为production handler证据；Phase 2
必须增加expression evaluator、owner transaction和不接受外部observation的多进程fixture。

CR-175补齐`HardLimitProfile v5`的expression instructions/input ports/stack depth三项typed limit。Phase 1 machine schema、fixture、
startup validation与Phase 2 Plan publication/evaluation必须消费同一profile字段；不得借用其他Plan limit或只依赖代码常量。

CR-176冻结Scope aggregate内的exact data-port environment。Phase 1 baseline须对齐Inline JSONB结构hard max；Phase 2必须实现
root input binding、bounded lexical lookup、Compute/Map/Loop原子RunValue+Scope CAS，并证明同一Plan port在动态Scope间不串值。

CR-177冻结expression-derived classification：Phase 2 repository必须从提交事务内重验的external input RunValue计算lattice join，
空input closure固定`Internal`，并禁止command/Worker/Artifact metadata降低Compute output classification。

CR-178把Map item port收紧为exact NodeOutput ref并提升未发布Plan wire version 2；Phase 2必须为每个item创建immutable RunValue并与
MapItem Scope/Node/Job/batch cursor原子提交，wrong producer/schema与批次重放必须fail closed。

CR-179冻结Loop carried Scope生命周期：body settlement原子复制carried RunValue并预建下一iteration Scope，continuation condition与
body复用该open Scope，false exit原子关闭并回到固定词法父Scope；iteration Scope不得串成父链或读取terminal Scope。

CR-180冻结Run terminal authority：Plan v3的Return/Raise分别引用exact final value/failure port，publication对齐Agent
output/error schema；Phase 2必须实现bounded Scope解析、Inline/Artifact正文物化与owner transaction重验，证明terminal RunValue/
Reference、Scope/Node/Job/Run、quota、Receipt/Event/Outbox为单一first-winner原子提交，并拒绝Plan v1/v2与外部terminal注入。

实现遵循以下顺序：

```text
Phase 1 合同/持久化基线
        ↓
Phase 2 durable orchestration 与可调用内核
        ↓
Phase 3 协议/数据/不受信任执行平面
        ↓
Phase 4 public /v1、部署、资格与clean cut
```

每个phase拆成可review、单一目的commit；一个批次通过比例化检查后先commit，再开始下一批。

## 2. 当前证据边界

当前仓库有模块化Rust workspace、shared Job/Run/Artifact/MCP/Sandbox等候选实现、一个23表schema contract v7 baseline与大量
开发fixture；部分代码源自CR-171之前的candidate，曾包含已推迟的microVM、
Managed stdio session、Model Artifact或过度Artifact role拆分。

因此：

- 可复用的领域与仓储代码必须通过CR-171对照审计后才计入phase evidence；
- 仅存在crate、test、Helm manifest或migration object不等于目标功能完成；
- 已推迟代码需从default build、runtime composition、deployment和公开contract移除，可在后续独立研究分支保留；
- 本批规范修订不改代码，不改变current runtime behavior。

## 3. Phase 1：合同、ID 与minimal persistence baseline

### 3.1 目标

建立后续代码唯一依赖的machine contract和schema v7基线，先删除过度authority，不立即实现所有runtime。

### 3.2 实现批次

1. **Owner types 与registries**

   - 冻结nominal IDs、ResourceKind、WorkClass、JobKind/OwnerKind、TaskKind、EventKind、ReceiptKind和problem codes；
   - 删除`SandboxJobId`、独立`OperationId`、Installation Release/Gate types、ManagementOperation和Model Artifact output types；
   - 保证RunValueId、JobId、ArtifactId等不共享同UUID alias；
   - 从owner types/registries生成或校验JSON Schema/OpenAPI/protobuf/Receipt projection。

2. **Minimal migration v7**

   - 确认candidate schema尚未生产发布；
   - 用一个reviewed baseline migration替换未发布的1～35候选集；
   - 保持23张总表/22张业务表，无Installation Release、ManagementOperation、SandboxJob或Gate表；
   - 仅保留PK/FK、tenant、unique、CAS、lease、structural check和outbox durability等DB authority；
   - 不用trigger重复Rust业务状态机。

3. **Shared repository foundation**

   - Resource/Version/Deployment、Run/Invocation、Job、Task、Event、Receipt、Outbox、quota与Artifact domain repositories；
   - typed JSONB decode/validate/canonical digest和size limits；
   - 统一Receipt claim/replay、CAS、lock order、Job lease、Event/Outbox transaction helper；
   - Operation safe projection直接来自Job，无第二current state。

4. **Remove deferred composition**

   - 从workspace default-members/binaries/feature graph、runtime composition和Helm manifests移除microVM/Firecracker/KVM；
   - 移除Managed stdio runner/session child与Model Artifact Producer/Broker composition；
   - 收敛Artifact server roles为Gateway/Data Worker/Maintenance；
   - 不必在本批物理删除所有历史源文件，但它们不得参与default build/release，也不得被目标代码依赖。

### 3.3 Exit gate

- owner/ID/kind registries的正负矩阵和codegen/conformance通过；
- fresh PostgreSQL 16/17执行唯一baseline成功，tenant/FK/CAS/lease/Receipt/Event/Outbox fixture通过；
- schema v7表数与ADR目标一致，但行为测试而非表数作为完成证据；
- default build/deployment不包含五类推迟authority；
- `cargo fmt --check`、workspace `cargo check`、相关Clippy/tests和migration smoke通过。

## 4. Phase 2：durable orchestration 与可调用内核

### 4.1 目标

交付Resource lifecycle、Run/Node/Subagent、Scheduler/Worker、CapabilityInvocation、Task与最小Model loop的端到端durable闭环。

### 4.2 实现批次

1. **Resource lifecycle**

   - Resource current Draft -> publication创建immutable ResourceVersion -> Deployment -> tenant active binding；
   - validation Job、publication、activation/suspension和exact RunBindingsSnapshot；
   - active变化只影响未来Run，无Installation Release dependency。

2. **Run、Node 与Subagent**

   - Run admission、typed Plan、Node/Scope/token、pause/resume/cancel/timeout/terminal convergence；
   - Subagent是child Run，具有exact binding、quota、deadline和typed parent link；
   - parent/child crash、cancel propagation、late result和result schema。

3. **Scheduler/Worker**

   - deterministic tenant fairness、claim前RAII slot、durable quota bundle、lease/heartbeat/fence和retry_at；
   - deferred WakeContract、bounded safety scan、critical-control reserve和drain/handoff；
   - WorkClass隔舱，不引入Model materialization、stdio session或microVM专用容量。

4. **Capability、Task 与Model Inline loop**

   - Native/HTTP/gRPC/MCP Tool/Sandbox typed backend映射，本phase先以fake/port驱动外部backend；
   - approval/input Task、callback/poll Receipt、Effect-aware retry/reconcile/UnknownOutcome；
   - ModelTurn、deterministic request、tool intent -> CapabilityInvocation、structured response和Inline hard limit；
   - 超限返回`model_output_too_large`，不创建Artifact。

### 4.3 Exit gate

- fresh DB上从Run admission -> Model -> Capability/Task/Subagent -> terminal result的多进程端到端fixture通过；
- 并发activation/admission不改写已存Run binding；
- 并发claim/Receipt/callback/Task terminal只有一个winner，旧fence全部拒绝；
- NATS全丢、Worker kill、DB connection饱和与drain后可恢复；
- domain/L2/L3证据通过，不将其声明为production capacity qualification。

## 5. Phase 3：协议、数据与Sandbox Execution Plane

### 5.1 目标

交付Context、remote MCP、Artifact三role、Egress/Secret、真实Model adapter以及WASI + gVisor的物理隔离实现。

### 5.2 实现批次

1. **Artifact三role**

   - Artifact/Blob/Link/Grant、static storage manifest、prepare/upload/verify/Ready/read/delete/GC；
   - Gateway/Data Worker/Maintenance三个binary/Deployment、closed caller capability和mutual-deny权限；
   - Operation=Job projection，Link create-time fence/current release fence语义；
   - Capability/Context/MCP/Sandbox共享Artifact port，无Model专用路径。

2. **Context 与Text2SQL**

   - Context Interface/Implementation/Binding、冻结parser/chunker/embedding/ranking闭包的Dataset Generation build Job、query/citation/cache；
   - 首次build预留Dataset ID、成功时物化root+generation、重建归属验证与active generation CAS；
   - Text2SQL按read-only planning/validation/execution/result-boundary拆分，SQL execution通过exact read-only Capability/remote service；
   - dataset/query saturation不占用Orchestration或Sandbox pool。

3. **Remote MCP Host**

   - Streamable HTTP negotiation、显式authorization binding的discovery、Tool/Resource/Prompt projection、Task/Elicitation、OAuth和subscription；
   - Egress Broker last-hop Secret resolution、catalog endpoint、SSRF/TLS/redirect/DNS/rate/byte/time limits；
   - 无stdio process、persistent Sandbox session或session child Job。

4. **WASI + gVisor Sandbox**

   - publication-time package/dependency/image/SBOM/provenance/scan freeze；
   - shared Job fenced Controller protocol、Executor无DB凭据；
   - real WASI ABI/fuel/memory/interrupt；
   - 真实`RuntimeClass=runsc`、admission-locked single-Job Pod、受限Launcher RBAC、no-runc-fallback、filesystem/network/Secret/Artifact grant与cleanup；
   - process kill、Controller restart、timeout/cancel和orphan reconciliation。

5. **Real Model/provider path**

   - provider catalog/deployment、adapter normalization、stream、usage/budget、safety、retry/cancel/unknown；
   - request/response Inline-only和tool loop端到端；
   - Model Worker与MCP/Sandbox/Artifact/Egress隔舱。

### 5.3 Exit gate

- real PostgreSQL + NATS + S3/KMS-compatible + fake/real protocol endpoints的端到端fixture通过；
- Artifact三role权限矩阵、wrong tenant/owner/fence/digest/storage generation全部fail closed；
- MCP protocol/OAuth/subscription、WASI ABI、gVisor escape/cleanup和Model adapter contract tests通过；
- Sandbox/Artifact/MCP/Model单lane饱和时其他lane与critical-control可用；
- default artifact/image/runtime不包含microVM、Managed stdio、Model Artifact或dynamic installer。

## 6. Phase 4：public `/v1`、部署、资格与clean cut

### 6.1 目标

交付minimal public API、production-equivalent topology、observability/runbooks、完整资格和GitOps clean replacement。

### 6.2 实现批次

1. **Public/API contracts**

   - `/v1` Resource lifecycle、Run/control/result/SSE、Task、Artifact和Job Operation projection；
   - OIDC/tenant/principal、If-Match、Receipt replay、problem、cursor、rate/quota/backpressure；
   - OpenAPI/owner schema conformance，无`/v2`、Release/Gate、dynamic storage、internal RPC proxy。

2. **Production topology**

   - 按18部署隔舱创建ServiceAccount、DB role/pool、NetworkPolicy、PDB/HPA、startup manifest和digest门禁；
   - WASI/gVisor节点/runtime、Artifact三role和Egress/Secret隔离；
   - PostgreSQL/NATS/S3/KMS/Secret的backup、restore、rotation和failure runbook。

3. **Observability 与qualification**

   - low-cardinality metrics、trace/log redaction、SLI/dashboard/alerts/runbooks；
   - L1～L6 fixture manifests、mixed load、fault injection、security matrix、soak、restore和rollout/rollback rehearsal；
   - 根据实测冻结首个production CapacityProfile，不使用Draft数字。

4. **GitOps cutover**

   - 生成signed image/SBOM/provenance/migration/test artifacts；
   - 提交exact digest到GitOps environment repository，人工审批promotion；
   - 执行clean `/v1` replacement，无dual write/fallback；
   - 失败时GitOps回滚到上一已资格闭包，不读写Installation Release row。

### 6.3 Exit gate

- 17的minimal OpenAPI与internal RPC正负conformance全部通过；
- 18的L1～L6资格矩阵、backup/restore、rollout/rollback和soak通过；
- 隔舱饱和、跨tenant、Secret/log、runc fallback、旧fence、重放和不确定outcome负向测试通过；
- production GitOps只引用exact已签名digest与已资格profile；
- 实现、规范、ADR、runbook、deployment和evidence对齐，无P0/P1遗留。

## 7. 跨Phase实现规则

### 7.1 Commit hygiene

- 每个commit只包含一个可review行为闭包；
- 合同、实现、migration/manifest与tests在同一commit；
- 已通过的完整批次立即commit，不长期混在working tree；
- 不提交known failing、half-wired或只有schema没有owner handler的状态。

### 7.2 不允许的快捷方式

- 不为了编译保留旧Operation/Release/SandboxJob/Model Artifact compatibility layer；
- 不在API/Worker中spawn code或运行package manager；
- 不用通用JSON owner、自由URL/header/shell或raw Secret穿透类型边界；
- 不把NATS、object store list、metric或Event当作current-state authority；
- 不通过新增表/服务/队列来规避已有shared model的owner设计；
- 不用表数、trigger数、route count或单进程fixture声明功能/资格完成。

### 7.3 变更规则

如实现发现规范冲突，停止代码，先按upstream -> downstream修改owner规范，将受影响文档退回Draft，
执行00～18 cross-review，再更新本计划。实现不得自行发明第二current-state authority。

## 8. 总体完成标准

Platform v2只在Phase 1～4全部exit gate通过、CR-171影响规范推进为Accepted、schema v7与migration实际发布、
production CapacityProfile经L4～L6证明、GitOps clean cut完成后，才能宣称完成。

本计划本身不完成任何phase；它只是Reviewed合同的执行顺序。
