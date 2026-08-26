# Platform v2 四阶段实现计划（CR-197）

| 属性 | 值 |
|---|---|
| 状态 | In Progress / CR-197 trace implementation complete; observability and external L4～L6 pending |
| 日期 | 2026-08-26 |
| 合同输入 | 00～18、cross-review CR-197、ADR-0001、ADR-0002、AGENTS.md |

> CR-197 implementation order：先生成03 `TraceId/TraceIdentityV1/W3cTraceParent` nominal contracts和17 public projections，再将trace ID
> 加入Run/Job/Event的owner snapshots与PostgreSQL baseline；随后接公共HTTP middleware、各internal mTLS RPC和durable reclaim，最后以第三方
> fake endpoint零header计数与动态canary关闭18。任何只在process task-local中转发、恢复后换trace ID、把span持久化或让trace参与业务digest的
> patch均不计实现证据。

> 2026-08-26 implementation feedback：OAuth Callback真实token endpoint L3审计发现OAuth reqwest adapter仍使用默认CA，CR-195只闭合了
> MCP Streamable HTTP catalog。CR-196按00～18复核后要求OAuth installed verification binding同时冻结exact Trust Policy与bounded PEM roots；
> token client关闭默认roots且RPC不携带trust正文。实现必须先交付machine/runtime正负合同，再执行真实token endpoint与Callback kill-window。

> 2026-08-26 implementation feedback：真实MCP HTTPS protocol fixture接线发现installed endpoint只有exact Trust Policy ref，没有显式CA/
> pin material，production reqwest会隐式使用默认trust store。CR-195按00～18复核后要求process-installed MCP endpoint携带bounded PEM trust
> bundle、由startup config digest保护并只按exact Deployment/Policy选择；RPC不携带trust正文。实现必须先补机器合同与TLS正负fixture，再继续
> 独立Egress subscription L3。

> 2026-08-26 implementation feedback：Resource Refresh Egress接线发现full reconcile所需`resources/list`未进入closed published method
> registry。CR-194按00～18完成影响复核：list/read共享Resources capability但各自拥有冻结limits，Host从cause/profile选择，Context Worker不能
> 指定method；不复用Capability Invocation，不新增public route、aggregate、表或正文持久化。下一实现批须先交付registry/contract fixture。

> 2026-08-26 contract feedback：CR-190 L2接线发现closed owner registry没有subscription refresh Context Job的合法pair。CR-191在03增加
> 唯一`Context -> McpOperation` pair，并在07/12/13/18冻结source row/payload/WorkClass claim约束；不新增WorkClass、aggregate、table或route。
> PostgreSQL实现必须先锁定同tenant `mcp_subscription` row，再创建该Context Job，MCP Host仍不得直接创建或claim它。

> 2026-08-26 implementation evidence：r269将CR-191 `Context -> McpOperation` pair加入Rust machine authority与generated
> `registries.json`，同步更新root contract manifest和独立合同checker；unit fixture同时证明该pair合法而反向`Mcp -> ContextQuery`仍被拒绝。
> generated-contract、fixture conformance与strict Clippy通过。该证据只闭合owner registry，不代表PostgreSQL admission或worker claim已实现。

> 2026-08-26 implementation evidence：r270实现CR-190/191 Context subscription owner transaction。它以数据库时间验证closed request，先按
> exact request digest claim `Command` Receipt，再锁定同tenant active `mcp_subscription` row并重验Context/MCP Deployment、Discovery、
> authorization/session/event、root URI/digest与当前published closure；同一事务创建`Context -> McpOperation` ready Job、Event、Outbox并保存
> versioned stable acceptance。fresh PostgreSQL 16 fixture证明notification接受与Receipt replay返回同一Job/digest，stale generation整批回滚且
> 只有一个Context Job/Event，MCP completion消费owner生成的durable work digest。reconcile正向fixture、Context worker handler与Host adapter/L3仍待实现。

> 2026-08-26 implementation evidence：r271新增typed MCP→Context production adapter；它从冻结的invalidation/reconcile request构造Context
> command，自身不接受Job ID或work digest，commit-uncertain保持可重放错误。MCP request补齐exact root URI/digest与subscription deadline；unit
> fixture覆盖notification/full reconcile映射及owner返回digest回传。fresh PostgreSQL 16 fixture进一步在前一Context Job已终态的分层安排下证明
> full reconcile acceptance/replay创建下一唯一Context Job，MCP reconcile completion消费其digest。Context Job真实handler/recovery、Host process
> composition与kill-window L3仍待实现。

> 2026-08-26 contract feedback：CR-191只允许创建subscription Context Job，却没有定义production handler、成功evidence或
> response后commit-window恢复。CR-192冻结Context Worker claim/lease/retry/terminal authority与typed Context→MCP Host Resource Refresh RPC；
> Host只执行credential-free ReadOnly协议I/O并返回bounded digest/count evidence。首版没有subscription cache/Observation，PostgreSQL outcome、
> Context Worker handler、Host RPC composition及三进程kill-window必须按此合同实现。

> 2026-08-26 implementation evidence：r272实现CR-192 PostgreSQL owner全路径。exact Worker manifest scan、subscription→Job锁序、
> successful admission Receipt/current session/auth/closure重验、Context concurrent quota、fenced attempt、JobCommit success/retry、expired running
> lease recovery及Event/Outbox均已接线。fresh PostgreSQL 16 fixture按MCP先清pending、Context后claim的真实时序覆盖wrong manifest零claim、
> replay、attempt 2/3与零Context Observation。Context Worker production handler、Host Resource RPC和三进程L3仍待实现。

> 2026-08-26 implementation feedback：handler接线发现CR-192的whole-attempt evidence digest包含heartbeat可变Job version，长调用续租后会
> 系统性拒绝合法结果。CR-193按03→07→12→13→17→18修复：Host evidence绑定排除version/expiry的immutable execution identity，Host仍以
> dispatch fence fail closed，Context Worker最终以heartbeat后的latest fence提交。00～18 cross-review已关闭；实现需增加跨heartbeat正向fixture。

> 2026-08-26 implementation evidence：r273把CR-193 execution port接入`platform-context-worker` durable driver。driver从exact manifest
> scan认领`Context -> McpOperation` Job，先绑定本地Context permit，再以同一lease identity heartbeat；Host backend返回后由Context owner以
> latest fence提交success/retry/permanent outcome及quota/Event/Outbox，expired lease由独立bounded recovery scan处理。Unavailable与
> completion-uncertain固定映射为ReadOnly dependency retry，Host拒绝或非法响应固定映射为terminal rejection。domain fixture证明heartbeat前后
> execution identity稳定而commit digest改变；fresh PostgreSQL 16 fixture证明旧version零写入且latest fence成功。Host Resource RPC与三进程L3仍待实现。

> 2026-08-26 implementation evidence：r274在现有MCP Host protobuf上增加独立`McpResourceRefreshService.RefreshResources`，沿用bounded
> canonical JCS + digest envelope但不复用Capability Execute method。client直接实现credential-free Context backend port；server使用独立exact
> Context Worker SPIFFE audience，在decode前拒绝Capability/Model身份，并以closed success/failure union传播全部CR-193错误。protobuf、unit、真实mTLS
> identity fixture及strict Clippy通过。production Host adapter/server composition与Egress resource protocol仍待下一批，故L3状态不变。

> 2026-08-26 implementation evidence：r275增加Host-side refresh resolver与composition service。PostgreSQL resolver在单个只读事务重载
> running Context Job/payload/latest fence、successful admission、active subscription/session、exact Context/MCP Deployment、Discovery与Auth
> closure；任一漂移返回closed rejection且协议port尚未调用。fresh PostgreSQL 16 fixture在heartbeat后用latest fence成功解析并继续完成原有
> terminal/replay矩阵，strict Clippy通过。Egress Resource wire adapter与production server composition仍待实现。

> 2026-08-26 implementation evidence：r276交付独立Host→Egress Resource Refresh wire及真实Streamable HTTP执行。closed request冻结
> exact endpoint/policy/auth/discovery、Context Job execution identity、root、cause、deadline与list/read独立limits；没有Invocation ID、自由method、
> header或Secret。full reconcile有界分页`resources/list`后只匹配冻结exact root，再`resources/read`；remote正文仅在Egress内计算聚合digest/count/
> byte evidence后丢弃。Egress RPC只接受MCP Host workload，post-dispatch失败保持uncertain。Host/Egress/RPC全套、真实mTLS及strict Clippy通过；
> production MCP Host/Context Worker process composition和kill-window仍待实现，L3状态不变。

> 2026-08-26 implementation evidence：r277增加两个同现有ComponentRole、独立pool的production entrypoint：
> `platform-mcp-resource-host`组合PostgreSQL resolver、Host protocol与MCP Host-only Egress client；
> `platform-subscription-context-worker`组合exact Context Worker manifest scan、独立permit/DB pool与Context Worker-only Host mTLS client。
> Egress Broker production service同时安装Resource Refresh connector。全部binary/all-target tests、既有Host process L3、strict Clippy与合同检查通过。
> Helm topology和subscription三进程kill-window仍待下一批，因此不提前关闭L3。

> 2026-08-26 implementation evidence：r278把r277两个entrypoint加入同一digest候选镜像，并在既有Context Worker/MCP Host chart内增加
> 独立subscription/resource pool。每个pool拥有独立ServiceAccount、PDB、HPA、config/TLS/DB input和精确NetworkPolicy；普通MCP Host仍无DB
> egress，只有Resource Host可接受Context subscription caller并访问PostgreSQL/Egress。Helm lint/render、两类deployment checker及全局
> ComponentRole closure通过（15 roles、19 isolated pools；Context=3、MCP Host=2）。真实多进程kill-window仍待执行，L3未关闭。

> 2026-08-26 implementation evidence：r279在fresh PostgreSQL 16上运行production `platform-mcp-resource-host`与
> `platform-subscription-context-worker`进程，并通过真实mTLS连接测试进程内的Egress Broker service。fixture在第一次Egress dispatch后终止
> Resource Host及首个Worker，过期其lease；第二个Worker取得response后由数据库trigger暂停terminal commit并被终止；第三个Worker恢复后成功。
> 三次refresh调用最终只产生一个`context.subscription_refresh.completed` Event。该批同时修复全局recovery default大于subscription 64项仓储
> 上限导致Worker启动即退出的问题，批次上限现在由单一共享常量约束。此证据关闭production Host/Context进程的dispatch/response-commit崩溃
> 窗口，但Egress仍是测试进程内service，且尚未接真实Streamable HTTP fake server；独立Egress OS进程、真实list/read wire、pre-dispatch零I/O
> 与pool saturation矩阵仍待完成，因此不把完整subscription L3标为关闭。

> 2026-08-26 implementation evidence：r280实现CR-195。`InstalledMcpStreamableHttpEndpoint`现在要求不超过256 KiB的非空可解析PEM
> trust bundle；POST与subscription SSE均以reqwest `tls_certs_only`构造每次连接，禁用内置/系统默认根，同时继续按canonical hostname和固定DNS
> 地址握手。真实TLS socket fixture使用独立CA/SAN跑通initialize、initialized、`resources/list`与exact-root `resources/read`，换成另一有效CA时
> TLS失败且HTTP业务request计数为零。Egress/Broker全套测试、strict Clippy与合同检查通过。该证据关闭CR-195 L1与真实HTTPS last-hop L3切片，
> 但尚未把Egress放入r279的独立OS进程恢复矩阵。

> 2026-08-26 implementation evidence：r281把r279/r280组合为fresh PostgreSQL 16独立协议进程L3。测试进程以单独OS进程组合production
> Egress Resource Refresh RPC与`ReqwestMcpStreamableHttpConnector`，并运行独立CA/SAN的真实TLS fake MCP server；production Resource Host与
> subscription Context Worker保持各自进程。第一次HTTPS initialize到达后强杀Egress、Host与Worker；重启Egress/Host后，第二次完整
> initialize/initialized/list/exact-root-read返回时暂停terminal commit并强杀Worker；第三个Worker以expired lease恢复成功。方法日志严格为3次
> initialize、2次initialized/list/read，最终唯一completed Event。该证据关闭subscription Resource Refresh的真实wire与dispatch/response-commit
> crash L3；test-only loopback feature不进入production binary。role saturation、真实Prometheus scrape和L4 rollout仍是独立门禁。

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

> 2026-08-26 contract feedback：production MCP subscription组合发现Context invalidation target缺少durable owner port；CR-190已按
> 12→13→07/18冻结Context owner transaction、shared Context Job、Receipt/Event/Outbox、exact acceptance replay与MCP/Context pool隔离，
> 并完成00～18 cross-review。后续实现不得由Host生成work digest、直接写Context结果或以内存callback冒充durable admission。

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

> 2026-08-26 implementation evidence：production qualification preflight新增fail-closed live workload inventory门禁。它从真实cluster
> 抓取Deployment、DaemonSet、NetworkPolicy、PDB与HPA，对照同一production CandidateManifest和CapacityProfile重验15个closed
> ComponentRole至少有一个独立workload pool、全部container使用exact image digest、rollout generation/ready replica无漂移、聚合
> replica/HPA范围与profile精确一致、
> ServiceAccount不复用且token不automount，并检查non-root、RuntimeDefault seccomp、只读root、drop ALL、CPU/memory/ephemeral-storage
> request/limit与namespace exact default-deny。r245的5个workload正负fixture连同既有4个node/runtime-class topology fixture全部通过，覆盖
> 缺role、mutable/wrong image、未就绪/越界副本、共享identity、缺default-deny、安全资源与HPA漂移。该证据只证明门禁实现与fail-closed
> fixture，不是production-equivalent cluster的L4通过证据；当前Helm的缺role/隔舱偏差会被明确拒绝，必须逐项修复后才能产生L4 evidence。

> 2026-08-26 implementation evidence：r246把此前同一`platform-gateway`进程/Deployment同时承载Management与Runtime API的拓扑偏差
> clean-cut为两个显式`management_api | runtime_api`启动角色。进程最外层closed path allowlist在认证/数据库前拒绝错role noun；Management
> 只组合Resource/Operation且不读取Artifact mTLS或Run event cursor Secret，Runtime只组合Run/Task/Artifact/Operation。Helm生成两个独立
> Deployment、ServiceAccount、DB Secret、Service、PDB/HPA、NetworkPolicy与ServiceMonitor，Ingress按closed `/v1` noun路由；Runtime独占
> Artifact出站和Secret mounts。5个Gateway tests（含双向401/404零越权路由矩阵）、Helm lint、角色/副本负向render及静态权限检查通过。
> 该批关闭15-role inventory中的Management/Runtime物理隔舱实现，不代表其余13 role或真实cluster L4已经通过。

> 2026-08-26 qualification correction：r247删除r245把一个ComponentRole误等同为“恰好一个Kubernetes workload”的过度约束。
> CandidateManifest按role冻结image，CapacityProfile按role冻结聚合副本范围；同一role可有Native/Remote等多个隔离pool，但每个pool必须使用
> 独立ServiceAccount/PDB/autoscaler且全部image匹配同一candidate digest，所有DaemonSet固定副本与Deployment HPA min/max求和后必须精确等于
> role CapacityProfile。新增多Context pool正向fixture，既有缺role、共享identity、rollout/image/security负向矩阵保留。

> 2026-08-26 implementation evidence：r248把全部15个Candidate ComponentRole接入Kubernetes workload inventory；当前静态拓扑为17个
> 隔离pool（Context Native/Remote与Egress Broker/Security Authority各为同role双pool）。所有主workload使用machine-readable role label、
> digest image、独立ServiceAccount/PDB；Deployment均有HPA，WASI DaemonSet按节点固定容量；所有container补齐CPU/memory/
> ephemeral-storage request/limit。Sandbox补齐Controller/gVisor HPA及WASI/gVisor PDB，Egress两个pool补齐独立HPA。新增跨11个chart的
> 全局render门禁，验证15-role精确闭包、pool数量、同role image一致、ServiceAccount不复用、PDB/HPA匹配和每namespace exact default-deny。
> 全局闭包及全部受影响role静态检查通过；这仍是manifest L1门禁，不替代live inventory、mTLS/RBAC或production-equivalent L4证据。

> 2026-08-26 implementation evidence：fresh PostgreSQL 16 r249在唯一baseline的新数据库上通过MCP OAuth与subscription全部L2 owner
> fixture。OAuth start并发只有一个winner、同Receipt重放相同authorization URL，Task/Receipt/Event/Outbox各唯一且PKCE state/nonce不进入
> PostgreSQL；PKCE cleanup outbox lease过期后由新owner reclaim，旧fence拒绝且新fence唯一结算。subscription覆盖public discovery、tenant/
> authorization binding、create/replay/conflict、session connecting→initializing→ready、notification coalescing、refresh/reconcile、stale termination
> 拒绝、transport loss、due recovery及Event唯一性。该证据关闭这两条路径的fresh-DB L2，不替代production Callback/Cleanup/MCP Host/Egress
> 多进程、真实OAuth token endpoint与SSE subscription的L3 kill/restart门禁；后者仍是Phase 3待办。

> 2026-08-26 implementation evidence：r250把CandidateManifest的单一`deployment_config_digest`写入全部11个chart、17个主workload
> PodTemplate的`insight.platform/deployment-config-digest`不可变注解。跨chart render门禁要求所有15个role/17个pool携带合法且完全相同的
> sha256摘要；production live inventory preflight再逐pool对照输入CandidateManifest并把该摘要纳入canonical workload evidence。新增配置漂移
> 负向fixture后workload矩阵7项全部通过。该批关闭“image相同但部署配置漂移仍可能通过”的门禁缺口；它仍是manifest L1与L4 preflight
> 实现证据，只有production-equivalent cluster真实抓取通过后才构成L4 rollout evidence，且不替代各进程启动时的typed config自校验。

> 2026-08-26 implementation evidence：r251把shared bounded-label process telemetry接入Scheduler/Recovery的独立
> `platform-orchestration-worker`。digest-bound closed config新增独立observability listen address；只有数据库schema、Artifact mTLS client、
> WorkerManifest和runtime成功组合后才置readiness，runtime或metrics server意外退出都会使进程fail closed。`/livez`、`/readyz`、`/metrics`
> 使用no-store且只输出静态component/operation/outcome维度。Helm改用HTTP probes，新增内部Service、精确Prometheus ingress NetworkPolicy和
> ServiceMonitor；单元、strict Clippy、Helm正负及全局17-pool闭包通过。该批只关闭Scheduler/Recovery role的process telemetry接线，其他
> 非HTTP role、dashboard/alerts、trace/redaction和production scrape仍待后续批次。

> 2026-08-26 implementation evidence：r252把worker health router收敛进shared observability owner并接入Model Worker。共享入口固定
> `/livez`、`/readyz`、`/metrics`与`live|ready|metrics|other`四个operation，未知path不进入label；readiness默认false。Model Worker只有在
> schema、exact adapter/WorkerManifest、Egress mTLS、NATS TLS与三个driver全部组合且observability listener绑定成功后才Ready，任一driver或
> listener提前退出会取消其余组件并fail closed。Helm新增内部Service、HTTP probes、exact Prometheus ingress和ServiceMonitor。shared/
> Model/Orchestration tests、strict Clippy、两chart正负及17-pool闭包通过。该批仍不代表provider可用性会错误影响readiness，也不替代L4 scrape。

> 2026-08-26 implementation evidence：r253把同一shared health/readiness/metrics owner接入Capability Native与Remote两个独立进程。
> Native在exact builtin registry、WorkerManifest、双PostgreSQL pool和driver组合后Ready；Remote还必须先完成HTTP/gRPC/MCP codec closure、
> Egress与MCP Host mTLS client组合。两者的worker或observability task任一提前退出都会取消另一方并使进程失败。两个chart新增内部Service、
> HTTP probes、Prometheus-only ingress和ServiceMonitor，且保持Native无外部client、Remote只访问Egress/MCP Host的边界。crate tests、strict
> Clippy、两chart正负及17-pool闭包通过；真实scrape、queue/permit/recovery SLI与L4仍待执行。

> 2026-08-26 implementation evidence：r254把shared process health/readiness/metrics接入Context Native/Remote。Native只在exact
> NativeCatalog adapter、WorkerManifest和PostgreSQL owner组合后Ready；Remote还要求exact RemoteSearch adapter与Egress mTLS client启动成功。
> worker或listener提前退出均取消同进程另一组件并fail closed。两个Context chart新增内部Service、HTTP probes、Prometheus-only ingress和
> ServiceMonitor；crate tests、strict Clippy、chart静态检查及17-pool闭包通过。Dataset/query业务SLI、真实scrape和L4仍待外部资格。

> 2026-08-26 implementation evidence：r255为MCP Host增加与业务mTLS gRPC端口完全分离的shared observability listener。只有closed
> config、Egress mTLS client、MCP transport/Host service、Capability Worker client-identity interceptor与TLS RPC server组合成功后才Ready；
> RPC或observability server任一提前退出均取消另一方并fail closed。Helm Service提供两个命名端口，NetworkPolicy分别仅允许Remote
> Capability Worker访问gRPC、Prometheus访问metrics，HTTP probes不再用未认证TCP开放判断ready。unit/process L3回归、strict Clippy、chart
> 正负及17-pool闭包通过；OAuth/subscription production多进程L3与真实scrape仍未由此关闭。

> 2026-08-26 implementation evidence：r256把shared process health/readiness/metrics接入Security Authority与Egress Broker两个隔离pool。
> Security Authority只在restricted PostgreSQL schema、exact authority、TLS gRPC service及observability listener组合成功后Ready；Egress还要求
> Security Authority mTLS client、secret-provider catalog、MCP state/codecs与全部closed connector完成启动。任一业务或observability server提前退出
> 都会取消同进程另一方并fail closed。Helm为两个namespace分别增加独立metrics端口、HTTP probes、ServiceMonitor和Prometheus-only ingress，
> 不放宽Egress调用者或Authority数据库边界。crate tests、strict Clippy、chart正负及17-pool闭包通过；真实provider、Prometheus scrape和L4仍待外部资格。

> 2026-08-26 implementation evidence：r257把shared process observability接入Sandbox Controller。Controller只在restricted PostgreSQL
> schema、Artifact Broker mTLS client、routed process-attestor authority、executor identity interceptor及TLS RPC service全部组合后Ready；RPC或
> observability server任一提前退出都会取消另一方并fail closed。Sandbox chart为Controller增加独立metrics端口、HTTP probes、ServiceMonitor和
> Prometheus-only ingress，不改变Executor、attestor、Artifact或数据库边界。crate tests、strict Clippy、chart正负及17-pool闭包通过；WASI/gVisor
> Executor与attestor观测接线、真实runsc及production scrape仍待后续资格。

> 2026-08-26 implementation evidence：r258把shared process observability接入同一production binary的WASI Executor与gVisor Launcher。
> 两个backend使用固定且互异的component label；只有exact WorkerManifest/backend、node-local process registration、Controller mTLS、backend
> registry、NATS TLS control listener与HTTP listener全部组合后才Ready。driver、control或observability任一提前退出都会取消并bounded drain其余任务。
> 两个隔离pool分别新增metrics Service、HTTP probes、ServiceMonitor和Prometheus-only ingress，未扩大WASI host authority或gVisor Kubernetes API
> 权限。crate tests、strict Clippy、chart正负及17-pool闭包通过；两个attestor进程、真实runsc与production scrape仍待后续资格。

> 2026-08-26 implementation evidence：r259把shared process observability接入node-local DaemonSet与gVisor Pod-local sidecar两种
> process attestor。只有persistent generation registry、procfs/node identity observer、UDS+mTLS registration service、Controller mTLS proof
> service及HTTP listener全部组合后才Ready；三个server任一提前退出都会取消并bounded drain其余server。node attestor获得独立metrics Service，
> Pod-local attestor复用gVisor Service的独立命名端口；两者只向精确Prometheus identity开放。crate tests、strict Clippy、chart正负及17-pool
> 闭包通过；真实runsc、node-loss与production scrape/L4仍未由此关闭。

> 2026-08-26 implementation evidence：r260把shared process observability接入Artifact Gateway。只有restricted PostgreSQL/schema、AWS
> provider catalog readiness、bounded broker、exact Public Gateway mTLS listener与独立HTTP observability listener组合后才Ready；业务或指标
> server提前退出会取消另一方并fail closed。Artifact chart为Gateway增加独立metrics端口、HTTP probes、ServiceMonitor和Prometheus-only ingress，
> Public Gateway仍只能访问业务mTLS端口，且未向Artifact Gateway增加Data Worker或Maintenance authority。binary tests、strict Clippy、chart正负及
> 17-pool闭包通过；真实S3/KMS、production scrape与Data Worker/Maintenance观测接线仍待后续资格。

> 2026-08-26 implementation evidence：r261把shared process observability接入Artifact Data Worker。只有独立read/work PostgreSQL schema、
> AWS provider readiness、bounded Scheduler/Sandbox/guest broker、exact双TLS RPC listener、scan worker与独立HTTP listener组合后才Ready；任一
> RPC、scan或observability组件提前退出都会使进程fail closed。chart新增独立metrics端口、HTTP probes、ServiceMonitor及Prometheus-only
> ingress，同时保持Scheduler/Controller、gVisor guest和监控源各自只访问对应端口。binary tests、strict Clippy、chart正负及17-pool闭包通过；
> 真实S3/KMS、production scrape与Maintenance接线仍待后续资格。

> 2026-08-26 implementation evidence：r262把Artifact Maintenance既有内部health listener替换为shared process observability owner，避免
> 为同一内部职责制造第二条监听。只有restricted PostgreSQL/schema、AWS provider readiness、bounded deletion backend、maintenance worker与
> HTTP listener组合后才Ready；worker或listener提前退出使进程fail closed。chart把该唯一端口命名为observability，新增HTTP probes、
> ServiceMonitor和Prometheus-only ingress，普通业务caller仍无Maintenance ingress。binary tests、strict Clippy、chart正负及17-pool闭包通过。
> 至此全部17个ComponentRole workload pool均有shared HTTP readiness/metrics接线；真实scrape、业务SLI、dashboard/alerts及L4仍待后续门禁。

> 2026-08-26 implementation evidence：r263新增独立`insight-platform-observability` chart，把现有真实process/HTTP series接入Grafana dashboard与
> 四条symptom-first PrometheusRule：telemetry全失、role持续NotReady、有效流量下failure ratio和p95 latency。阈值由closed chart values提供，
> 每条alert固定owner/severity、稳定HTTPS runbook URL及checked-in逐alert处置步骤；PromQL与dashboard不使用tenant/principal/resource/run/job、
> URL、token或Secret label。Helm正负、dashboard JSON、alert inventory/runbook静态门禁已进入CI。该批只运营已存在的series，不声称queue、
> dependency、recovery、permit业务指标或production scrape已经闭合。

> 2026-08-26 implementation evidence：r264收紧production telemetry的静态脱敏边界：LLM、SSE、MCP OAuth、conversation及各worker启动日志
> 不再记录Run/Task/Conversation/Server/Process Generation等高基数标识、manifest digest或原始编码错误。新的CI门禁扫描production Rust source，
> 拒绝tenant/principal/resource/run/job/task/request/invocation/artifact/process generation以及token/secret/prompt/response/object key/URL等字段进入
> structured tracing或插值日志。相关crate tests、strict Clippy和静态门禁通过；该证据是source-level负向合同，不替代端到端trace propagation、
> 动态payload采集审计、retention/RBAC或production telemetry验证。

> 2026-08-26 implementation evidence：r265把Orchestration现有Coordinator、Safety Recovery及LocalWorkerPools权威快照接入
> `scheduler-recovery` shared observability endpoint，导出fixed-label active jobs、claim outcome、recovery scan/mutation outcome以及business/
> critical-control available/used permit；不以process-local hint伪装durable queue age。dashboard扩展为8个panel，并新增critical-control permit持续耗尽和
> 有效scan volume下recovery failure ratio两条带runbook的symptom alert。owner tests、strict Clippy、Helm正负及alert/runbook门禁通过；其他role
> saturation、durable queue/outbox/recovery lag、dependency health和production scrape仍未闭合。

> 2026-08-26 implementation evidence：r266把shared fixed-lane permit sampler接入Model、Capability Native/Remote与Context Native/Remote
> 五个production worker pool；sampler从同一`LocalWorkerPools`物理authority周期读取business/critical-control capacity与available，导出available/
> used而不记录worker generation或Job identity，并随process cancellation退出。连同r265 Orchestration，现有6个pool具备动态permit saturation series；
> owner/worker tests与strict Clippy通过。其余11个pool的role-specific saturation、durable backlog与dependency metrics仍待接线。

> 2026-08-26 implementation evidence：r267将同一shared permit sampler接入Sandbox Executor production binary；WASI与gVisor两个隔离pool
> 均从自身exact `LocalWorkerPools`导出business/critical-control available/used，并由同一process cancellation停止采样。Sandbox executor/owner
> tests与strict Clippy通过；动态permit coverage现为8/17 pool。Controller admission、Artifact lane、Security/Egress等非LocalWorkerPools容量模型
> 仍必须从各自authority接线，不能由该series推断。

> 2026-08-26 implementation evidence：r282从PostgreSQL Job authority为Orchestration接入read-only durable queue observation，使用数据库时间
> 导出fixed `due`/`expired_lease` count与oldest lag；不读取payload且不暴露tenant、Job、Worker或错误文本。采样失败保留上一有效gauge并只累加
> fixed PostgreSQL success/failure counter。fresh PostgreSQL 16验证真实ready Job读取；observability owner、production Scheduler sampler、strict
> Clippy、11-panel dashboard、9条symptom-first alert及逐alert runbook门禁通过。该批关闭Orchestration durable backlog/recovery lag与其PostgreSQL
> observation health的L1接线；shared Outbox、其余role backlog/dependency/saturation、真实Prometheus scrape及L4仍未闭合。

> 2026-08-26 implementation evidence：r283为durable MCP OAuth PKCE Cleanup Worker增加独立shared HTTP observability listener；只有closed
> config、PostgreSQL/schema、mTLS Egress client与cleanup owner组合成功后才Ready，listener提前退出使process fail closed。Helm将PID exec探针
> 替换为HTTP live/ready，增加独立observability Service、ServiceMonitor及仅Prometheus可访问的NetworkPolicy ingress，不放宽PostgreSQL或Egress
> 边界。binary tests、strict Clippy和chart正负门禁通过。该批关闭Cleanup Worker process observability/readiness接线，不替代OAuth真实endpoint
> 多进程kill/restart、真实scrape或L4证据。

> 2026-08-26 implementation evidence：r284在fresh PostgreSQL 16以production Cleanup Worker、独立Egress测试进程、production
> `EgressBrokerGrpcService`与真实mTLS workload identity gate关闭PKCE cleanup crash component L3。第一次exact Secret delete RPC进入后同时强杀
> Egress/Worker；lease到期后第二组进程以claim epoch 2恢复并形成唯一`cleanup_completed`，旧claim fence不能结算。fixture使用完整canonical
> terminal Task payload与current exact SecretBinding authority，不以直接列漂移绕过owner校验。strict Clippy及相关process/RPC tests通过。OAuth
> Callback真实token endpoint/exchange、其故障窗口、role saturation、production scrape及L4仍是独立门禁。

> 2026-08-26 implementation evidence：r286实现CR-196并关闭OAuth Callback/token exchange crash component L3。OAuth installed binding同时
> 安装exact Trust Policy与bounded PEM roots，production reqwest关闭默认roots并按canonical hostname/DNS pin访问真实独立CA HTTPS token endpoint。
> fresh PostgreSQL 16三进程fixture在token已由Egress store、Callback尚未提交owner transaction的窗口同时强杀Callback与Egress；第二组进程
> 从持久化prepared token metadata恢复，同一authorization code未再次发送，token endpoint总调用数严格为1，最终只有一个responded Task、
> callback Receipt与completion Event。错/缺PEM、Auth/Trust漂移在DNS、Secret、transport和store前fail closed。该证据关闭OAuth callback/exchange
> component L3；真实Secret Manager rotation、lane saturation、production scrape及L4～L6仍保持独立门禁。

> 2026-08-26 implementation evidence：r287从shared PostgreSQL Outbox authority接入bounded只读operational observation，使用数据库时间导出
> fixed `due`、`expired_claim`、`dead`计数及适用的oldest lag，不读取Event payload且不暴露tenant、Outbox/Event/claim owner或失败文本。
> Orchestration critical-control sampler保留最后有效gauge并将查询结果计入fixed PostgreSQL observation outcome。fresh PostgreSQL 16、strict
> Clippy、13-panel dashboard、12条symptom-first alert及逐alert runbook门禁通过。该批关闭shared Outbox backlog/recovery L1接线；其他role
> authority/dependency/saturation、动态payload审计、真实Prometheus scrape及L4仍未闭合。

> 2026-08-26 implementation evidence：r288增加commit-SHA pinned production candidate workflow与确定性生成器；GitOps environment repository
> 也必须以40位commit SHA只读checkout，其闭包与application Helm/Docker closure共同生成`deployment_config_digest`。两个digest-pinned
> Docker target分别发布runtime和gVisor guest exact subject，生成并验证keyless image signature、SPDX SBOM及GitHub build provenance；生成器
> 冻结15个ComponentRole、7个实际WorkerManifest、contract/config/HardLimit/policy/QualificationProfile摘要、唯一baseline migration和commit
> timestamp。测试报告、migration、SBOM及Candidate signature由canonical release-bundle index传递闭合并再次签名。静态供应链门禁、负向fixture、
> Platform v1合同检查及Rust production candidate validator通过。该实现提供L6执行入口，不构成registry/GitOps/人工promotion或目标环境gate通过。

> 2026-08-26 implementation evidence：r289最终render复核确认CR-192后当前闭包为15-role/19-pool，Context subscription pool使动态permit
> coverage达到9/19；此前17-pool/8-pool描述只保留为历史批次证据。Security/Egress checker补入已实现的`RefreshMcpResources`并继续exact
> 验证13个remote-only RPC。全局workload/observability/redaction/deployment checker与workspace strict Clippy通过；不改变L4～L6 Pending边界。

> 2026-08-26 implementation evidence：r290完成CR-197 machine/runtime projection。公共HTTP严格校验W3C `traceparent`并生成或延续同一
> `TraceIdentityV1`；Run、Invocation、Job、Task、Event与Outbox持久化同一trace ID，reclaim/restart从durable owner恢复而不换trace。首版实际
> MCP、Egress、Artifact、Sandbox与Security mTLS/UDS RPC均在workload identity授权后、业务解码前校验trace，跨hop保留trace ID并生成新span；
> Egress provider及gVisor guest/storage边界不转发平台trace header。workspace strict Clippy、合同/schema门禁、真实mTLS/UDS RPC、fresh PostgreSQL
> 16 Sandbox/OAuth恢复测试通过。该证据关闭CR-197 trace implementation与component L3连续性，不替代动态payload采集审计、真实Prometheus
> scrape、telemetry RBAC/retention或L4～L6。

> 2026-08-26 implementation evidence：r291以真实loopback TCP listener启动shared production observability Router，由HTTP客户端先发送包含
> payload/identity、`tracestate`和`baggage` canary的未知请求，再从`/metrics`执行Prometheus text scrape。响应证明未知operation只进入fixed
> `other/rejected` series，采集正文中所有canary及header名称均为零，并验证真实content type与graceful shutdown。crate tests和strict Clippy
> 通过。该证据关闭shared metrics adapter的component real-socket scrape与动态metric payload负向切片，不替代Prometheus deployment scrape、
> log/trace动态采集审计或L4 NetworkPolicy/RBAC。

> 2026-08-26 implementation evidence：r292为公共HTTP与内部RPC task-local correlation安装fixed tracing spans，动态采集分别验证公共parent
> trace ID、每hop span ID、accepted/rejected context outcome与internal same-trace/new-span字段存在。真实loopback OpenAI-compatible provider测试把
> prompt、response、token、query、tenant identity、`tracestate`和`baggage` canary送过production reqwest/tracing路径，允许的request/response
> metadata events存在且全部canary为零；公共扩展header拒绝span和RPC canary采集同样为零。相关tests、strict Clippy与静态redaction门禁通过。
> 连同r291，该证据关闭仓库component L3动态metric/log/trace payload canary，不替代production telemetry backend、RBAC/retention或L4～L6。

> 2026-08-26 implementation evidence：r293增加closed operational-capacity metrics surface，并从Sandbox Controller实际
> `SandboxArtifactResponseCapacity` semaphore在scrape时读取configured/available，导出fixed `artifact_response` available/used；持有response
> permit的现有owner tests证明available下降并在释放后恢复。dashboard增至14 panel，新增持续capacity exhaustion symptom alert及checked-in
> runbook，chart正负门禁、真实TCP scrape、相关tests和strict Clippy通过。动态capacity coverage达到10/19 pool；其余9个pool必须继续从各自
> admission、broker或dependency authority接线，不能由配置副本推断。

> 2026-08-26 implementation evidence：r294从Artifact Broker每个实际audience semaphore导出capacity，而非把同一配置值镜像成状态。
> Gateway固定`download`，Data Worker固定`scan_read`、三类Scheduler read与`sandbox_read`五个独立bulkhead，Maintenance固定`delete`；scrape
> 直接读取available/used。owner测试证明持有exact response lease时available归零、并发读取被拒绝且drop后恢复；三process tests、strict
> Clippy、Artifact Helm、14-panel/13-alert observability及19-pool closure门禁通过。三个Artifact pool闭合后动态capacity coverage为13/19；
> 其余Gateway双pool、MCP双Host及Security/Egress六个pool仍待真实authority接线。

> 2026-08-26 implementation evidence：r295为Management API与Runtime API各自的SQLx PostgreSQL pool导出fixed
> `postgresql_connections` capacity。configured maximum是上限，used由实际established减idle计算，available同时包含idle与尚可合法建立的槽位；
> 不读取SQL、tenant或连接identity。真实PostgreSQL 16测试证明checkout使used从0变1，drop后由pool异步归还并在有界时间恢复0。Gateway tests、
> strict Clippy、Helm/observability/19-pool门禁通过。两个Gateway pool闭合后动态capacity coverage达到15/19；剩余MCP双Host及
> Security/Egress四个pool。

> 2026-08-26 implementation evidence：r296为MCP Tool Host与MCP Resource Host分别安装构造期必选、process-local的真实RPC admission
> semaphore；身份与trace interceptor通过后、业务envelope解码前获取permit，饱和稳定返回`ResourceExhausted`，drop后恢复available。两个
> process均从该owner实时导出fixed `rpc_requests` available/used，配置以closed `maximum_in_flight_requests`受hard max约束。owner/config tests、
> 真实mTLS测试、受影响PostgreSQL fixtures编译、strict Clippy及MCP/observability部署门禁通过。动态capacity coverage达到17/19；仅剩Security
> Authority与Egress Broker两个pool，production Prometheus scrape、telemetry backend/RBAC/retention及L4～L6仍保持Pending。

> 2026-08-26 implementation evidence：r297从Security Authority唯一实际SQLx PostgreSQL pool导出fixed `postgresql_connections`
> available/used；configured maximum是capacity，used由established减idle计算，available同时计入idle与尚未建立但可合法使用的槽位，没有复制
> 业务状态或新增admission authority。fresh PostgreSQL 16证明checkout使used 0→1，drop后有界恢复0；unit tests、strict Clippy及Security/Egress、
> observability部署门禁通过。动态capacity coverage达到18/19；仅剩Egress Broker，production scrape、telemetry backend/RBAC/retention及
> L4～L6仍保持Pending。

> 2026-08-26 implementation evidence：r298从Egress Broker的11个实际隔舱owner导出closed capacity：Secret resolution/store、Model、HTTP/gRPC
> Capability、Remote Context、MCP OAuth、普通/订阅MCP，以及subscription bridge pending/active。每条series在scrape时直接读取对应Semaphore的
> maximum/available；不聚合tenant、endpoint、provider或request identity。OAuth饱和测试证明permit持有时available归零、dispatch前拒绝且drop
> 后恢复，subscription bridge测试证明pending/active分别随reservation变化。owner/RPC/broker tests、真实HTTPS/mTLS、strict workspace Clippy及
> Security/Egress、observability门禁通过。至此19/19 workload pool均有动态capacity L1接线；production Prometheus scrape、完整dependency health、
> L5 mixed-load/saturation profile、telemetry backend/RBAC/retention及L4～L6仍保持Pending。

> 2026-08-27 implementation evidence：r299以两个全新PG16 baseline隔离共享主authority与Model conformance全局WDRR，连接真实NATS并使用
> 当前production process binaries，串行执行`cargo test --locked --workspace --all-targets --all-features`，最终退出码0；两个外部S3测试保持
> 显式ignored。收敛项包括Scheduling JSON null不再进入候选窗口、terminal PostgreSQL serialization/deadlock同命令有界重试、MCP process
> trace scope、OAuth callback→cleanup exact token binding与`mcp_authorization_binding`事件解析、数据库时钟timer等待、Q1子进程tenant-scoped
> drain及durable fixture命名空间。workspace format、strict Clippy、doc tests与fresh OAuth 8/8 TLS/kill-recovery均通过。本轮未提供Model TLS NATS
> process环境，也未运行外部S3/KMS、production scrape、Kubernetes/runsc或L4～L6；不得据此推进release/cutover状态。

> 2026-08-27 implementation evidence：r300修复最终QualificationEvidence门禁只验证manifest内部引用、未解析实际artifact bytes的缺口。
> `validate-release-evidence`现在强制接收artifact root，并对每个closed artifact name读取真实普通文件、流式重算byte length与SHA-256；缺失、
> symlink、长度或digest漂移全部fail closed。CLI/unit负向测试、target strict Clippy、Platform v1 contract与candidate pipeline检查通过。
> 该门禁强化不生成Candidate、Capacity或L4～L6 evidence，也不改变外部qualification与人工promotion的Pending状态。

> 2026-08-27 implementation evidence：r301建立共享、低基数dependency observation owner，仅允许PostgreSQL/NATS/S3/KMS/Secret/Egress
> 六种Rust nominal依赖和success/failure两种结果；空集、重复、超量及未安装依赖均fail closed。Security Authority已在真实
> `load_for_resolution`/`register_prepared` PostgreSQL repository结果边界接线，前置身份拒绝不污染数据库失败计数，并与实际SQLx pool capacity
> 同surface导出。owner/Authority tests、format与strict Clippy通过；其余生产role调用边界及对应alert仍待后续批次。

> 2026-08-27 implementation evidence：r302在Secret Broker AWS adapter的七种实际SDK请求返回边界记录Secret/KMS success/failure：
> Secrets Manager describe/get/delete/create与KMS describe/encrypt/decrypt。本地catalog/reference/policy/identity/permit拒绝零dependency observation；
> observer port不携带tenant、provider、endpoint、ARN、错误或Secret material。Egress Broker将其映射到shared fixed series并与11-lane capacity
> 同surface导出。Secret/Egress tests、strict Clippy、redaction与Security/Egress deployment门禁通过；production AWS fault仍待L4～L5实测。

> 2026-08-27 implementation evidence：r303为Artifact AWS adapter增加无业务标识observer port，在KMS encrypt/decrypt/describe及S3
> head-bucket/head/get/delete实际SDK返回边界记录success/failure；presign与任何本地授权/binding/key/generation/limit拒绝零dependency observation。
> Gateway、Data Worker、Maintenance各自映射到本role fixed `s3`/`kms` series，不共享process state或扩大存储权限。Broker/三binary tests、
> strict Clippy、redaction和Artifact/observability部署门禁通过；Artifact PostgreSQL health及production S3/KMS fault仍待后续批次/L4～L5。

> 2026-08-27 implementation evidence：r304增加共享15秒PostgreSQL health sampler，从role既有restricted SQLx pool执行只读
> `SELECT 1::bigint`并仅上报fixed success/failure；不改变readiness，不追赶missed tick，shutdown可中断probe。Artifact Gateway/Maintenance各一个、
> Data Worker read/work pool各一个sampler，并与现有HTTP/RPC/worker共同cancel/drain，sampler意外退出使process fail closed。不可用pool、pre-cancel及
> 三binary tests、strict Clippy通过；可选真实database成功test已checked-in，但本轮本地PG16未运行，因此没有新增真实成功fixture或production scrape证据。

> 2026-08-27 implementation evidence：r305将Model Worker既有restricted PostgreSQL pool接到共享15秒sampler，并在实际NATS TLS connect、
> 每批publish+flush和shutdown drain返回边界记录fixed success/failure；observer不接收server、subject、tenant/run、payload或error，本地校验与
> backpressure不污染依赖计数。两个observer进入既有JoinSet cancel/drain且不改变readiness。真实连接失败、adapter、library/binary tests及strict Clippy
> 通过；可选真实TLS NATS fixture已同时验证connect/publish观测，但本轮未配置外部fixture，因此没有新增真实NATS/PG或production scrape证据；Model Egress
> 流式RPC观测仍待后续独立批次。

> 2026-08-27 implementation evidence：r306把Capability Native/Remote各自独立business与critical-control PostgreSQL pool接到共享15秒
> sampler；四个probe只汇总为各process固定`component_role + postgresql + outcome`，不暴露database、pool、SQL或error。每个process把permit与两个DB
> sampler组成受监督任务，和worker/HTTP共用cancellation并在正常shutdown join，sampler意外退出使process fail closed且不改变readiness。shared adapter、
> 两个binary tests、strict Clippy及Native/Remote deployment、redaction和observability门禁通过；本轮没有新增真实PG或production scrape证据，Remote
> Egress/MCP observation仍待后续批次。

> 2026-08-27 implementation evidence：r307把Context Native、Remote与Subscription三个restricted PostgreSQL pool接到共享15秒sampler；
> 每个process只导出固定`component_role + postgresql + outcome`，不暴露database、pool、SQL或error。permit与DB sampler成为受监督任务，全部
> signal/worker/HTTP/sampler退出路径均cancel并join其余组件，且修复Subscription异常分支此前未等待peer的问题；readiness不变。shared adapter、三binary
> tests、strict Clippy、Context deployment、redaction与observability门禁通过；本轮无新增真实PG或production scrape证据，Remote Egress与Subscription MCP
> Host observation仍待后续批次。

> 2026-08-27 implementation evidence：r308把MCP Resource Host和OAuth Cleanup Worker各自restricted PostgreSQL pool接到共享15秒sampler，
> 仅导出固定`component_role + postgresql + outcome`。Resource Host sampler进入RPC/HTTP cancel及bounded drain，Cleanup sampler在signal、HTTP或自身退出时
> cancel并等待peer；readiness不变，且不预装尚未接线的Egress series。两个adapter/binary tests、strict Clippy、MCP Host/Cleanup deployment、redaction与
> observability门禁通过；本轮没有新增真实PG或production scrape证据，MCP Tool/Resource/Cleanup Egress observation仍待统一RPC observer批次。

> 2026-08-27 implementation evidence：r309把Sandbox Controller restricted PostgreSQL authority pool接到共享15秒sampler，仅导出固定
> `component_role + postgresql + outcome`；probe不占用Sandbox execution或Artifact response capacity。sampler与RPC/HTTP共用cancellation和原有shutdown
> deadline，任一组件异常退出都会cancel并等待peer；readiness不变且不预装Artifact/attestor RPC series。adapter/binary tests、strict Clippy、Sandbox
> deployment、redaction与observability门禁通过；本轮无新增真实PG或production scrape证据，Artifact Broker/node attestor observation仍待后续批次。

> 2026-08-27 implementation evidence：r310把Callback API restricted PostgreSQL command pool接到共享15秒sampler，并附加到既有OAuth callback
> process metrics；只导出固定`component_role + postgresql + outcome`，不暴露database、pool、SQL、state或error。signal、HTTP server与sampler互相监督，
> 正常shutdown共用既有grace，超时中止残余任务；readiness与callback outcome不变，也不预装Egress series。adapter/binary tests、strict Clippy、Callback
> deployment、redaction与observability门禁通过；本轮无新增真实PG或production scrape证据，OAuth Egress observation仍待统一RPC observer批次。

> 2026-08-27 implementation evidence：r311把Management/Runtime Gateway各自restricted PostgreSQL pool接到共享15秒sampler，并与已有
> connection capacity共用process metrics surface；每个deployment只导出自身固定`component_role + postgresql + outcome`，不暴露database、pool、SQL或
> error。signal、HTTP server与sampler互相监督，配置的完整shutdown grace用于实际bounded drain，超时中止残余任务；readiness与HTTP/API不变。
> adapter/8个binary tests、strict Clippy、Gateway deployment、redaction与observability门禁通过；本轮无新增真实PG或production scrape证据，Runtime
> Artifact RPC observation仍待统一RPC observer批次。

> 2026-08-27 implementation evidence：r312补齐反向审计发现的间接SQLx owner：Orchestration Worker通过`PostgresConnectionBulkheads`持有
> business/critical-control两个pool，现均接到共享15秒sampler并汇总为固定`component_role + postgresql + outcome`，不暴露pool、database、SQL或error；
> 既有Job/Outbox backlog/lag query保持独立。signal、HTTP、runtime-finished或sampler退出都会关闭runtime、HTTP、sampler和bulkheads，readiness不变且不预装
> Artifact RPC series。adapter/binary tests、strict Clippy、Orchestration deployment、redaction与observability门禁通过；本轮无新增真实PG或production scrape
> 证据，Artifact Scheduler observation仍待后续批次。

> 2026-08-27 implementation evidence：r313把14-panel dashboard中的scheduler-only PostgreSQL panel扩展为按固定
> `component_role + dependency + outcome`聚合的六依赖概览，并用通用`InsightPlatformDependencyFailureRatioHigh`替换scheduler-only alert。告警需同时
> 超过closed失败率与最小观测数，避免单次provider/tenant失败触发；runbook只按fixed role/dependency分诊且禁止endpoint、database、subject、object key、error或
> tenant字段。Helm负向阈值、13-alert inventory、panel expression、HTTPS runbook锚点与低基数checker通过；该批只闭合消费端合同，不提供production scrape、
> 真实fault或L5 profile证据。

> 2026-08-27 implementation evidence：r314为共享`EgressBrokerGrpcClient`增加closed transport observer，只接收success/failure，并在Model
> 建连/流读取/取消、Capability HTTP/gRPC调用与取消、Remote Context、MCP OAuth/cleanup/Tool/Resource，以及subscription建连、首帧和持续读取的实际tonic
> 返回边界记录结果。本地编码/校验拒绝零观测，成功transport承载业务`Failed`仍记录success；metadata、tenant、provider、endpoint、payload和error均不跨越
> observer port。真实mTLS成功、不可达端点失败测试及strict Clippy通过。本批尚未在production process composition注入observer，因此不宣称role Egress
> series、production scrape/fault或L4～L5闭合。

> 2026-08-27 implementation evidence：r315把共享Egress observer注入production Model Worker，与既有PostgreSQL/NATS dependency metrics
> 共用同一process surface；实际Model建连、stream read和cancel RPC结果只映射为固定`model-worker + egress + success|failure`，不输出provider、endpoint、
> tenant/run、payload或error，也不改变readiness和业务状态机。adapter/binary tests、strict Clippy、Model deployment、observability及redaction门禁通过；
> 本轮无production scrape/真实fault，其他Egress client role与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r316只在production Capability Remote Worker注入共享Egress observer；HTTP/gRPC调用与取消的实际
> transport结果映射到固定`capability-remote-worker + egress + outcome`，并与双PostgreSQL sampler共用process surface。Native仍只安装PostgreSQL且
> 显式断言无Egress observer。三个binary target tests、strict Clippy、Native/Remote deployment、observability及redaction门禁通过；无production scrape/
> 真实fault，剩余Egress/MCP client与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r317只在production Remote Context Worker注入Egress observer并复用其PostgreSQL process metrics；
> Native/Subscription仍为PostgreSQL-only，后者的MCP Host边界不冒充Egress。实际查询RPC只导出固定`context-remote-worker + egress + outcome`，不输出
> endpoint、tenant/query、payload或error。四组binary target tests、strict Clippy、Context/Remote deployment、observability及redaction门禁通过；无
> production scrape/真实fault，其余Egress/MCP client与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r318把共享Egress observer注入production MCP Tool Host、Resource Host与OAuth Cleanup Worker；Tool
> 只安装Egress，Resource/Cleanup为PostgreSQL+Egress。普通Tool、Resource Refresh、OAuth exchange/PKCE delete与subscription transport结果仅导出固定
> role/dependency/outcome，不输出server/endpoint、tenant/task/resource、payload或error。四组binary target tests、strict Clippy、MCP Host/Cleanup
> deployment、observability及redaction门禁通过；无production scrape/真实fault，Callback/Sandbox Egress client与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r319把共享Egress observer注入production Callback API OAuth exchange client并与PostgreSQL sampler
> 同surface；实际RPC只导出固定`mcp-callback-api + egress + outcome`，不输出OAuth state/code、tenant/task、endpoint、token或error。binary tests、
> strict Clippy、Callback deployment、observability及redaction门禁通过；无production scrape/真实fault，Sandbox Egress client与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r320反向扫描全部共享Egress client构造点并在observability checker锁定七个first-release production
> client必须注入observer；新增no-op production构造会fail closed。仅shared client测试、PostgreSQL component fixture及release Docker/Helm明确排除的
> deferred Firecracker/microVM provider可保留no-op。首发WASI/gVisor Sandbox不使用Egress client，因此r319的“Sandbox待注入”不是release缺口。observability、
> Sandbox deployment及redaction门禁通过；production scrape/真实fault与L4～L5仍Pending。

> 2026-08-27 implementation evidence：r321重跑workspace all-target/all-feature门禁时稳定复现rolling-summary fixture在18轮串行SQLite summary
> 压力下越过仅3秒的无关owner lease并被中断；该测试专用lease改为30秒，heartbeat仍为1秒，production owner逻辑和专用lease failure tests不变。修复后
> target与完整workspace tests、strict Clippy、format及doc tests通过；两个外部S3 fixture仍ignored。该批不产生外部S3、production scrape、Kubernetes/
> runsc或L4～L6证据。

> 2026-08-27 implementation evidence：r322为first-release Sandbox WASI/gVisor Executor补齐Core NATS control dependency health。共享
> observer仅接收success/failure，覆盖实际request、subscribe+flush、reply publish、stream closure、unsubscribe及production TLS connect；本地subject/
> envelope拒绝不计数，也不输出worker、tenant/job、payload、server或error。RPC/Executor tests、真实mTLS、strict Clippy及Sandbox deployment/
> observability/redaction门禁通过；可选真实NATS fixture已有观测断言但本轮未配置，故不宣称真实NATS、production scrape或L4～L5完成。

> 2026-08-27 implementation evidence：r323在observability checker固化全部first-release dependency owner inventory，覆盖Security、Artifact三role、
> Model、Capability两role、Context三role、MCP双Host/Cleanup、Sandbox Controller/两Executor、Callback、双Gateway、Orchestration与Egress，以及AWS
> Artifact/Secret、Model/Sandbox NATS adapter边界。移除observer/sampler/client注入会fail closed。相关门禁通过；六类external dependency仓库内L1接线
> 闭合，但production scrape/fault、其他domain backlog/recovery series与L4～L5仍Pending。

> 2026-08-26 implementation evidence：r268在Context owner crate新增closed subscription refresh admission L1合同：冻结tenant、subscription、
> exact Context/MCP Deployment、Discovery identity/digest、authorization/session/event generation、root resource identity、deadline及canonical
> request digest；同时定义bounded shared Context Job payload、caller audit、稳定`request_digest + durable_work_digest + Job + accepted_at`
> acceptance与commit-uncertain错误边界。unit tests覆盖canonical replay输入、字段漂移和oversized root identity fail closed，strict Clippy通过。
> 该批仅建立Rust nominal application port；PostgreSQL Job/Receipt/Event/Outbox owner transaction、production MCP adapter/claim loop及L2/L3仍待实现。

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
