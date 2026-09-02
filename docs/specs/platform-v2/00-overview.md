# Platform v2 规范索引与实施路线

| 属性 | 值 |
|---|---|
| 状态 | Implemented / CR-217 qualification fail-closed hardening; L4～L6 Not run |
| 日期 | 2026-09-02 |
| 目标协议 | `insight.platform/v1` |
| 变更类型 | Clean-cut architecture |
| 当前行为 | OpenSandbox-only；以 [`docs/current`](../../current/README.md) 为准 |

> 2026-09-02 qualification hardening（CR-217）：production profile固定至少24小时持续soak，Evidence manifest的
> 实际起止时间必须覆盖该声明；每个gate至少绑定一个未被其他gate复用的专属artifact digest，artifact闭包禁止别名和未引用项。
> L4 preflight对live BatchSandbox CRD完整规范化contract、Platform workload namespace及NetworkPolicy闭包取证，不能以
> source常量、未标注workload或额外allow-all policy绕过。MCP callback/cleanup仍映射既有`mcp_host`，Context dataset仍映射
> `context_worker`，不新增ComponentRole。产品release先组装并签名candidate，在本地exact cache完成starter资格后重签包含资格
> evidence的final ReleaseBundle，才创建不可变GitHub Release和release tag。上述门禁修复不产生L4～L6 passed evidence，当前状态仍为Not run。

> 2026-09-01 architecture revision（CR-216 revision 1）：首版 Sandbox 物理实现 clean-cut 为 OpenSandbox Kubernetes provider、
> BatchSandbox Controller 与 containerd/runc。create 只产生 inert Armed candidates，PostgreSQL 选择唯一 candidate，fixed runner
> 最多激活一次 Package；不修改 OpenSandbox 源码。shared Job 仍是唯一业务 work authority；只有 Sandbox Dispatcher 可调用内部
> OpenSandbox、激活 runner，并在 terminal transaction 前复核 current Job lease fence。OpenSandbox 不获得 Platform 业务权限。
> workload 调用第三方 API 产生的读写/副作用与目标 API 幂等仍由 Package 及目标服务负责。
> ADR-0007取代ADR-0002，受影响01～04、07、09、10、14、15、17、18及产品开发profile完成全量cross-review；实现与资格尚未开始，
> `docs/current`继续描述现行WASI/gVisor行为。

> 2026-09-02 contract closure（CR-216 revision 2）：Job JSON不复制input/result body；exact RunValue是正文authority，claim/recovery
> 重建bounded request，terminal first-winner原子写output。terminal清除标准Job lease后，Dispatcher只用same-row、database-time、
> generation-fenced cleanup claim写delete/absence evidence，不能改写terminal business state；不新增表、aggregate或第二lease authority。

> 2026-09-02 recovery closure（CR-216 revision 3）：当 external continuation 已持久化 physical evidence 后，expired
> `Running` lease 允许窄化为 `Ready` 并重新 claim 同一 physical attempt；该转换不增加 `attempt_count`，不得重算 provisioning token、
> 创建 candidate/sandbox 或重新激活 Package。没有 durable continuation evidence 的普通 Running Job 不适用此转换。

> 2026-09-02 binding closure（CR-216 revision 4）：Execution Plan/Request semantic closure显式冻结
> `runtime_contract_digest + profile_deployment_digest`；candidate metadata必须逐字段匹配，不能只校验其格式或仅依赖可变ID解析。

> 2026-09-02 orphan closure（CR-216 revision 5）：operator-only candidate metadata增加`tenant_id + job_id + physical_attempt`，
> orphan repository据此point-read唯一shared Job并返回只读closed retain/delete decision；Provisioning、selected、corrupt、ambiguous或
> repository unavailable均retain，不能以全表payload扫描或猜测删除替代。OpenSandbox仍无Platform业务state或credential权限。

> 2026-09-02 continuation closure（CR-216 revision 6）：expired Running reclaim在同一claim transaction内逻辑完成
> `Running -> Ready -> Leased -> Running`且只提交最终Running；lease generation增加，attempt与全部physical identity保持，旧fence零写入。

> 2026-09-02 provisioning closure（CR-216 revision 7）：每次外部candidate create前必须由current shared Job fence在PostgreSQL
> CAS授权exact ordinal，并以database time持久化provisioning start、authorization count与last authorization time。response-loss与
> Dispatcher重启不能重置count/quiescence/total-time预算；OpenSandbox仍无需源码修改，metadata list仍只是发现而非原子幂等authority。

> 2026-09-02 dispatch closure（CR-216 revision 8）：create authorization repository返回`Applied | Replayed`；只有`Applied` caller
> 可调用一次provider，`Replayed`绝不调用。授权后、provider调用前崩溃会burn ordinal，静默窗口后只可申请下一ordinal。

> 2026-09-02 registry consistency repair（CR-216 revision 9）：首版MCP只保留remote Streamable HTTP，删除旧
> managed-stdio physical session 的`SandboxManagedMcpSession` JobKind、注册三元组与dead repository/Host/Egress代码；不增加
> 替代JobKind、兼容映射或fallback。Spec 03与生成registry恢复为同一17-kind/24-triple closed machine合同。

> 2026-09-02 implementation qualification（CR-216 final）：实现、部署、迁移与L1～L3已完成。L1通过20项Sandbox target与
> 118项contract；L2在fresh PostgreSQL 16.14/schema contract 8覆盖claim、lease/fence、terminal、cancel/timeout、quota与orphan；
> L3在真实OpenSandbox Server + BatchSandbox Controller + Kubernetes/containerd-runc覆盖create race/response loss、provider restart、
> Dispatcher kill/reclaim、Direct/Disabled和cleanup/absence。全workspace tests/doc tests/check/strict Clippy及受影响static gates通过。
> L4、L5、L6均Not run，不声明production-ready。

> 2026-08-30 implementation feedback（CR-206）：Context Dataset build的Operation target会公开预留的`dset`，但成功
> `SafeJobResult`只有digest，生成的immutable `dgen`没有任何public discovery路径；fresh客户端因此无法调用既有exact
> generation read route或证明active generation。CR-206把Operation result收敛为closed typed union：普通Job只返回Digest，
> ContextDatasetBuild成功返回`context_dataset_generation` result并携带exact `generation_id + result_digest`；不新增Dataset list/head API。

> 2026-08-31 architecture revision（CR-207）：Agent产品体验需要安全Agent/Run历史页，而现有API只有exact-ID read。CR-207在17增加
> 两个direct-authority bounded list projection，以tenant/principal/filter/purpose-bound opaque cursor执行stable keyset；不新增表、cache、
> Event current projection、route noun或服务role。ADR-0003/0004/0005同步clean-cut为starter feature profile、默认Agent CLI与authoring Console。
> product-experience 00～06完成cross-review后进入Accepted；实现仍须按manifest compiler→API/CLI/Console→distribution/profile顺序取证。

> 2026-08-31 implementation feedback（CR-208）：Spec 01纯本地compiler曾要求输出完整Agent `ResourceDocument`，但该closed
> document必须携带只有Artifact upload成功后才存在的authoring/plan Artifact ID，形成新的身份环。CR-208把compiler输出收敛为
> 无服务端ID的`AgentResourceIntent`与logical lifecycle dependency；publish executor在两项Artifact Ready后用exact返回值纯物化并重验
> 现有`AgentResourceSpec`。不改变public DTO、Artifact/Resource authority、route、table、Job或Receipt语义。

> 2026-08-31 implementation feedback（CR-209）：简化`model_chat`manifest的作者指令原先没有合法的Agent Revision落点，
> 而把它塞入三个platform assembly block会错误提升信任。CR-209在现有`AgentResourceSpec`增加nullable bounded
> `author_instructions`，并在11/16增加独立`AgentInstruction` user/untrusted phase；不增加route、表、Job、role或第二authority。

> 2026-08-31 implementation feedback（CR-210）：产品`deterministic`模板只有`start -> return`，若input/output schema不同，
> 现有05/06 exact port与terminal contract必然拒绝。CR-210将该authoring模板收紧为两个canonical schema digest相同；不改变
> Typed Plan v5 wire、runtime、route、表、Job或兼容边界。

> 2026-08-31 implementation feedback（CR-211）：产品compiler要求CLI/Console逐字节相同，却没有冻结Interface contract与model
> requirement digest preimage。CR-211在05/Product 01定义两个closed v1 canonical文档；算法复用02，不改变public DTO、runtime、
> route、table或server-generated identity。

> 2026-08-31 implementation feedback（CR-212）：Agent/Run产品summary要求authoring name来自Resource authority，但
> `metadata.name`此前只存在于本地manifest/lock，服务端无法安全投影且不能用display name猜测。CR-212把normalized name冻结为
> `AgentResourceSpec.authoring_name`；它在Resource创建后不可更名，随Draft CAS与immutable Revision digest处理，不新增表、route、
> aggregate、ID或tenant-wide uniqueness。

> 2026-08-31 implementation feedback（CR-213）：Agent产品summary要求草稿态也返回`required_features`，但compiler结果此前
> 没有进入Resource authority，服务端只能从不存在的active Deployment或本地manifest猜测。CR-213把closed sorted feature set冻结为
> `AgentResourceSpec.required_features`，随Draft CAS与immutable Revision digest处理；不新增表、route、aggregate、Job或Event projection。

> 2026-09-01 implementation feedback（CR-214）：`agent run`与Console schema-driven Run要求从服务端authority恢复manifest的
> `input.classification`与物化后的`limits.deadlineSeconds`，但两者此前只存在于client compiler intent；lock丢失或`agent adopt`后只能
> 猜测隐藏默认。CR-214把两项冻结为`AgentResourceSpec.input_classification/default_deadline_seconds`，随Draft CAS与immutable Revision
> digest处理；不新增表、route、aggregate、Job、Event projection或Deployment字段。

> 2026-09-01 implementation feedback（CR-215）：Console不能读取CLI私有runtime配置，而现有`/v1`没有提供新Agent编译所需的
> exact Policy revision/Deployment binding；把动态ID或synthetic默认写入browser bundle会形成第二authority。CR-215增加bounded、
> tenant/principal-scoped `GET /v1/agent-authoring-profile`，只从现有Tenant配置和immutable Policy Version/Deployment直接投影；
> 不新增表、cache、aggregate、Event projection、mutation、role或credential字段。

> 2026-08-30 implementation feedback（CR-205）：剩余full-profile产品化场景确认，八类public noun只能发布
> Capability/Context Interface、Model/Sandbox Profile，却没有合法management surface发布它们依赖的Capability/Context
> Implementation、Model Provider与Sandbox Runtime/Package；因此fresh客户端只能伪造数据库前置。CR-205把这五类既有
> ResourceKind加入closed domain noun matrix；definition-only kind只到immutable Version，不获得Deployment route语义。

> 2026-08-30 implementation feedback（CR-204）：public Agent Deployment create曾要求调用方提交完整
> `ContextBindingSnapshot`，其中`adep` owner、`xcb` identity和两层binding digest只能在create transaction内产生，形成第二个
> 无合法public命令可闭合的身份环。CR-204把request收敛为Context binding intent；Gateway预留`adep/xcb`并规范计算digest，
> persisted/response Deployment closure仍保持完整immutable snapshot。调用方提交server-owned字段会因closed DTO被拒绝。

> 2026-08-29 implementation feedback（CR-203）：productization fresh first-Run 探针确认public Agent Draft必须在
> publish前提交typed Plan Artifact，但Plan v4要求嵌入仅在publish时由服务端生成的Agent Interface Revision ID，形成
> 无合法public命令可闭合的身份环。CR-203将current Typed Plan wire提升为v5，以Draft已知且内容寻址的
> `interface_contract_digest`替代预生成Revision ID；publish仍原子生成Interface/Plan Revision，Deployment与Run仍冻结exact ID。
> 本变更不增加route、table、aggregate、Job、role或兼容wire，v1～v4均不进入clean-cut target。

> 2026-08-29 status correction（CR-202）：此前将00～18标作`Verified / CR-201`不够诚实；真实多节点Kubernetes、
> `RuntimeClass=runsc`、production telemetry、容量/混沌/恢复、持续soak与人工GitOps promotion仍未执行，且
> `RegistryValidation` Job 尚未拥有可部署的执行/提交闭环。所有00～18保持`Accepted`或`In Progress`，不得把仓库静态
> evidence、Docker 本地 profile 或 CI candidate 冒充为已完成的 L4～L6 或 production-ready 声明。

> 2026-08-29 historical completion-scope decision（CR-201）：00～18的规范关闭曾以仓库内可复现的合同、实现、fresh PostgreSQL、独立进程、
> 静态部署闭包和候选供应链证据为准。真实多节点Kubernetes、`RuntimeClass=runsc`、production Prometheus、容量/混沌/恢复、持续soak
> 与人工GitOps promotion没有在本项目中执行；它们保留为部署方启用production前的环境资格门禁，不再阻塞spec状态或实现计划关闭。
> `Verified`只表示已验证checked-in实现符合00～18的仓库范围合同，不表示production-ready、已完成clean cut或取得任何实测容量/SLO声明。

> 2026-08-27 implementation feedback（CR-200）：Artifact Data Worker stage接线发现，tenant exact `ArtifactIo` Policy v2未冻结
> `write_storage_binding_digest`与`encryption_domain_id`，进程只能使用全局write binding且MCP caller又不得提交storage authority。CR-200将两者
> 加入`ArtifactIo` closed document v3：owner admission冻结exact tenant选择，Data Worker只验证installed binding支持并生成加密locator/evidence。
> 不新增PolicyKind、表、Resource、Deployment、route、role或Secret路径。

> 2026-08-27 implementation feedback（CR-199）：CR-198 waiting Artifact verification Job接线发现，published `ArtifactIo` Policy只冻结
> media/file rules；`scanner_contract_digest`、verification evidence TTL与retry backoff仍来自Artifact Gateway进程配置。MCP admission无法从
> tenant exact policy closure重建这些事实，也不得读取另一个role的内存配置或使用默认值。CR-199将三者加入`ArtifactIo` Policy document v2，
> 由TenantConfig exact slot冻结；Data Worker startup manifest必须支持exact scanner contract。无新PolicyKind、表、Resource、route、role或Secret路径。

> 2026-08-27 implementation feedback（CR-198）：production MCP discovery接线发现，13允许owner Job在结果Artifact已经
> `Ready`后创建Discovery Snapshot，而15要求Capability/Context/MCP/Sandbox在owner Job开始前预分配Artifact/Blob、验证Job、配额与
> retention closure，并且只有owner terminal transaction可原子执行`Verified -> Ready`、创建typed Link与业务结果。CR-198冻结两段持久化
> 交接：MCP discovery Job只负责远端descriptor attempt；Artifact Data Worker以预分配的shared `ArtifactScan` Job完成stage/verify并最多推进
> `Verified`；随后MCP owner由durable wake恢复，在一个事务中推进Ready、创建Evidence Link与Discovery Snapshot并结算两个Job。无新表、
> aggregate、public route、WorkClass、JobKind或Artifact role；内部Artifact验证Job不投影为public discovery Operation。

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
8. 脚本只能经独立Sandbox Execution Plane中的OpenSandbox运行，API、Scheduler和普通Worker不创建脚本进程；
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
| 00 | `00-overview.md` | Implemented / CR-216 L1～L3 passed | 总体路线、规范模板、依赖和完成定义；L4～L6 Not run |
| 01 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md) | Accepted / CR-216 revision 1 | 系统架构、领域对象和所有权边界 |
| 02 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md) | Accepted / CR-216 revision 1 | ID、Resource、Version、Deployment、Binding |
| 03 | [`03-consistency-events-and-recovery.md`](03-consistency-events-and-recovery.md) | Accepted / CR-216 revision 4 | PostgreSQL、事务、Outbox、Lease、恢复 |
| 04 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) | Accepted / CR-216 revision 1 | 多租户、授权、Secret、Effect、Quota、Approval |
| 05 | [`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md) | Accepted / CR-214 | Agent Interface、Typed Plan、Model Loop |
| 06 | [`06-durable-run-state-machine.md`](06-durable-run-state-machine.md) | Accepted / CR-203（CR-204 reviewed） | Run、NodeExecution、暂停、重试、取消 |
| 07 | [`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md) | Accepted / CR-216 revision 3 | Scheduler、Worker、Lease、背压和隔舱并发 |
| 08 | [`08-subagent.md`](08-subagent.md) | Accepted / CR-203（CR-204 reviewed） | Child Run、父子通信、取消传播和循环限制 |
| 09 | [`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) | Accepted / CR-216 revision 1 | Capability Interface、Implementation、Registry |
| 10 | [`10-capability-invocation.md`](10-capability-invocation.md) | Accepted / CR-216 revision 3 | 调用协议、幂等、同步快路径、异步恢复 |
| 11 | [`11-skill-system.md`](11-skill-system.md) | Accepted / CR-209 | Skill Package、发现、选择、绑定和依赖 |
| 12 | [`12-context-and-retrieval.md`](12-context-and-retrieval.md) | Accepted / CR-206 | ContextSource、检索、引用和数据权限 |
| 13 | [`13-mcp-host.md`](13-mcp-host.md) | Accepted / CR-203（CR-204 reviewed） | MCP Transport、OAuth、投影、Task 和 Subscription |
| 14 | [`14-sandbox-execution-plane.md`](14-sandbox-execution-plane.md) | Accepted / CR-216 revision 8 | OpenSandbox Kubernetes、one-shot create authorization、Armed runner activation、atomic continuation、point-read orphan decision和清理 |
| 15 | [`15-artifacts-and-files.md`](15-artifacts-and-files.md) | Accepted / CR-216 revision 1 | S3、内容寻址、上传、生命周期和内容安全 |
| 16 | [`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) | Accepted / CR-209 | Provider、Model Profile、ModelTurn、流式响应和预算 |
| 17 | [`17-management-and-runtime-api.md`](17-management-and-runtime-api.md) | Accepted / CR-216 revision 1 | 管理 API、Run API、事件流和错误模型 |
| 18 | [`18-deployment-observability-and-qualification.md`](18-deployment-observability-and-qualification.md) | Accepted / CR-216 revision 1 | 部署、指标、Tracing、压测、故障注入和验收 |

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
- **Implemented**：代码和 schema 已交付，但规范范围内的仓库证据尚未完整；
- **Verified**：规范范围内的合同、实现与仓库门禁已有可复现证据；不自动表示任何特定部署环境已通过production资格；
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

一份规范进入 Verified 必须同时满足以下仓库范围条件：

- 每个真实边界的权威机器合同、生成投影、数据库约束和文档语义一致；不要求未跨边界对象重复拥有Rust/protobuf/JSON Schema；
- PostgreSQL real-process integration tests 覆盖正常、重复、乱序、超时、取消和崩溃恢复；
- 未知字段、重复 JSON key、越界集合、非法 ID 和跨租户引用被拒绝；
- 所有Platform拥有的外部adapter写操作具有明确Effect、idempotency和approval语义；Sandbox workload内部网络调用不在
  Platform adapter幂等边界内，其副作用合同由Package及目标服务拥有；
- Secret value 不出现在数据库业务列、API 回读、错误、日志、trace、metric label 或 outbox；
- 所有无界队列、集合、正文、Artifact、并发和等待都有硬限制；
- 进程退出、消息丢失和迟到执行者不能破坏 durable authority；
- 关键指标、告警、runbook、部署preflight与资格执行入口已经提交，并有静态/进程级负向证据；
- 当前未执行的真实集群、容量、故障、soak、restore和promotion门禁已作为明确部署限制记录，不被写成已通过。

`docs/current`更新和活动规范归档发生在实际clean cut之后，不是spec进入Verified的前置条件。CR-201明确将L4～L6中的真实环境执行
从仓库实现完成定义移至production release decision：部署方要声称production-ready、容量/SLO、OpenSandbox production topology/强隔离或完成promotion时，仍必须在
目标环境运行18定义的适用门禁；未运行可以关闭spec，但不得生成通过的release evidence或CapacityProfile。

## 7. 全平台发布门槛

以下端到端证据用于production release，不阻塞CR-201仓库范围的spec关闭。部署方若不执行这些门禁，必须维持“未验证production部署”的声明：

1. 在已资格CapacityProfile的混合并发负载下，Sandbox饱和不降低API、Model Worker和critical-control的准入能力；
2. Runtime、MCP Host、Sandbox Dispatcher或OpenSandbox Server任一进程被终止后，已提交状态可恢复且无越权重放；
3. 丢失或重复全部 wake hint 时，安全扫描最终收敛；
4. Agent、Skill、Capability或Provider active Deployment在Run中途切换，不改变该Run的冻结绑定；
5. 同一个 idempotency key 的并发提交只产生一个逻辑 Invocation；
6. 跨租户 ID、Artifact、Secret、Context 和 callback 均无法读取或关联；
7. Sandbox用户进程可能已开始但结果不确定时不自动重新提交执行命令；其内部外部副作用由Package/目标服务处置；
8. 动态代码只能进入OpenSandbox，不能在控制面、编排面或普通Worker进程执行；
9. MCP Tool、Resource、Prompt 与 Task 分别保持各自语义，不通过通用 JSON 丢失安全元数据；
10. 版本、状态机、事件和公开错误码均通过 machine-readable conformance suite。

## 8. CR-166～CR-171 简化结论与下一步

2026-08-20的CR-166撤销CR-165中超出首版需要的最终形态设计，并已完成受影响规范的全量cross-review：

- 发布、promotion和rollback由Kubernetes/GitOps拥有；Candidate和qualification报告是CI/CD内容寻址产物，不是数据库或公共API状态；
- 数据库不新增`InstallationReleaseState`，目标仍为23张总表/22张业务表；clean-cut ID/owner约束完成后schema contract从当前v6升级为v7；
- root Run在tenant事务中解析并冻结exact ResourceVersion/Deployment binding；后续部署变化不修改既有Run；
- CR-216 revision 2 将首版 Sandbox physical provider clean-cut 为 OpenSandbox Kubernetes/BatchSandbox、containerd/runc 与
  per-attempt ephemeral sandbox；Docker provider、restricted WASI、自建 gVisor、microVM、Firecracker 与 KVM 均不在目标 composition；
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

本节r88～r383均为CR-216之前实现历史。凡涉及WASI、gVisor、runsc、Sandbox Controller/Executor/attestor或旧candidate closure的
记录都不能证明OpenSandbox目标已实现；其他domain证据仍按其原始范围有效。

当前checked-in persistence baseline是23张总表/22张业务表、schema contract v8和单一`0001_platform_baseline.sql`。仓库有
CR-171之前候选架构的多类functional fixture；只有已按CR-171重新对照且通过适用门禁的批次可计为实现证据，尚不能据此宣称全部phase完成。

截至2026-08-25，历史Plan v4的ChildAgent、HumanTask、TimerWait与SignalWait已接入PostgreSQL owner transaction和durable Plan store；
CR-203要求这些语义迁移到current Plan v5后重新通过同级证据，不能把v4通过结果当作v5完成。
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

r329把该machine contract贯通clean baseline与PostgreSQL repository：`jobs.job_kind`成为NOT NULL typed hot column，全部production/test Job INSERT与
row decode按25项合法三元组fail closed；Artifact Data/Maintenance和Context Native/Remote领取改用typed kind，managed MCP Sandbox session收敛为
共享Job的`owner_kind=job`，Sandbox指标进一步固定到`SandboxCapabilityExecution`而不混入MCP session。schema contract升至v8，独立checker锁定
Job INSERT完整性、禁止JSON kind热谓词与未注册`sandbox_job` SQL owner。PostgreSQL all-target 35/35入口、strict Clippy和baseline checker通过；
本轮没有fresh PG16 authority或production scrape，故不把这些本地门禁解释为L2/L4～L6新证据。

r330让Artifact Data Worker按`ArtifactScan|ArtifactRescan`、Maintenance按`ArtifactDelete|ArtifactBlobCleanup`分别观察typed durable queue；两者在同一
受监督process generation内导出固定due/expired count与lag，查询失败保留上一有效snapshot并触发现有durable observation告警。新增Artifact role-set
due/expired告警、runbook和静态部署inventory；目标8/8、baseline 2/2入口、strict Clippy及Artifact/observability/redaction门禁通过。无fresh PG、
production S3/KMS scrape或L4～L5，本批只关闭Artifact两条queue的仓库内L1接线。

r331把Context Native、Remote与Subscription production Worker分别固定到`ContextQueryNative`、`ContextQueryRemote`与
`ContextSubscriptionRefresh` durable queue；同WorkClass下Dataset build不会混入三条role series。sampler与permit/PostgreSQL health共同受process
监督，新增Context role-set due/expired告警、runbook和静态inventory。目标13/13、strict Clippy、Context部署、observability/redaction门禁通过；
无fresh PG、production scrape或remote fault，本批只关闭三条Context Worker queue的仓库内L1接线。

r332把MCP-owned shared Job的专用仓储路径也收紧到exact合法三元组：discovery、logical subscription与Context subscription refresh的scan、
loader、recovery和CAS均显式要求各自`job_kind`，不再仅凭`work_class + owner_kind`接纳候选。独立schema checker会拒绝MCP repository中遗漏
exact kind的相邻owner谓词；PostgreSQL lib 14/14、MCP subscription 2/2、OAuth 8/8与strict Clippy通过。本轮没有fresh PostgreSQL或production
worker运行，因此不新增L2/L3/L4～L6证据；discovery production claim/execute/recovery composition仍是下一实现批次。

r333补齐discovery production driver的typed authority前置：专用claim在shared Job lease事务内固定`McpDiscovery` kind，避免同为`Mcp +
mcp_operation`的logical subscription被错误暂领；expired observation携带并验证`physical_attempt/attempt_limit`，恢复方可按leased、可重试running与
已耗尽running构造唯一合法命令。MCP Host 57/57、PostgreSQL 14/14及strict Clippy通过；本轮未配置fresh PostgreSQL，也尚未组合production
driver、Egress discovery protocol与Artifact输出，因此不新增L2/L3/L4～L6证据。

r334把MCP discovery执行改为prepare/refresh-fence/commit三段式：远端I/O与PostgreSQL terminal commit之间允许heartbeat更新exact Job version，
prepared结果只能接受同worker、同lease generation、同token且严格递增的fence；错token与旧version在commit前fail closed。既有`execute`仍作为无
heartbeat封装。MCP Host 57/57与strict Clippy通过；该L1改造尚未接入production driver，也不替代fresh PostgreSQL竞争或真实协议L3证据。

r335新增MCP discovery durable driver库：独立permit在claim前预留，exact Job从leased启动为running，prepare期间持续heartbeat并以最新fence提交；
physical attempt digest排除可变expected version。恢复循环依据typed attempt计数选择unstarted requeue、running retry或exhausted terminal，deadline
不足一个backoff时等待数据库时间推进后再timeout；进程取消使用bounded drain。driver tests 2/2与service all-target strict Clippy通过；尚未接入
production binary和真实Egress discovery/Artifact端口，因此不新增fresh PostgreSQL L2或process/protocol L3证据。

r354在r347 discovery pool之后继续补齐logical subscription production pool。独立Worker只领取`McpSubscription`，并把global expired
lease/session recovery、periodic reconcile、真实Egress stream、notification digest ingress及Context durable invalidation组合到同一受监督process；
普通MCP Host不再获准调用subscription stream。独立SPIFFE/数据库凭据/NetworkPolicy、permit/queue/dependency metrics、ServiceMonitor与两条
backlog/recovery告警均已接线。当前15个ComponentRole映射为21个隔离workload pool，21个pool具备动态capacity；r289的19-pool和后续
13/19、15/19数字是当时历史证据。仓库内L1门禁通过，但无fresh PostgreSQL、真实外部SSE或logical subscription进程kill-window，L2/L3及
L4～L6仍Pending。

r355在唯一baseline的fresh PostgreSQL 16上补齐logical subscription L2证据：exact Job claim/lease/fence、notification coalescing、tenant
isolation与Context invalidation owner均通过真实事务。subscription与Capability remote discovery fixtures同步采用CR-198的Artifact verification
中间态schema v2，并为subscription fixture安装TenantConfig exact Retention/ArtifactIo Deployment和staging quota；Capability Input Task的
PostgreSQL INSERT列映射错误也在同一真实事务测试中修复。phase3 Invocation、phase4 Subscription、workspace all-target、strict Clippy与
format/diff通过；测试输出明确标记未配置的Resource Refresh、Native/Remote Capability process fixture为跳过，因此本批不声明logical
subscription L3、真实外部SSE或L4～L6完成。

r356在fresh PostgreSQL 16上补齐logical subscription production Worker的process L3切片：真实
`platform-mcp-subscription-worker`以独立SPIFFE client certificate通过mTLS Egress subscription stream调用测试进程内typed connector；第一
进程在远端establish开始、Ready提交前被强杀，旧running Job租约过期后由第二进程经global recovery接管，重建disconnected session并在下一
physical attempt唯一恢复到`active/ready`，Ready Event没有重复。该门禁发现production discovery/subscription Driver没有把durable Job trace
安装到RPC task-local scope，导致必需`traceparent`在client interceptor处缺失；两个Driver现均以exact Job trace包围执行future，传输失败日志只
附带安全错误码。fixture明确把原有四次L2与两次process recovery attempt冻结为六次预算，attempt exhausted继续fail closed。MCP service
all-target、MCP Host 62/62与fresh PostgreSQL目标测试通过。Egress后的connector仍是类型化测试实现，不是真实外部Streamable HTTP/SSE server，
所以仅关闭Worker process/RPC/crash-recovery L3切片，外部SSE及L4～L6仍Pending。

r357把上述typed connector替换为独立Egress OS fixture process中的production `ReqwestMcpStreamableHttpSubscriptionConnector`，并接入独立
TLS MCP fake server。fresh PostgreSQL 16门禁在首个外部`initialize`到达、Ready未提交的窗口同时强杀Egress与production subscription Worker，
只推进exact running Job租约过期，再启动第二组进程；恢复attempt依次完成`initialize`、`notifications/initialized`、
`resources/subscribe`与带session header的SSE GET，最终保持唯一`active/ready`与唯一新增Ready Event。方法日志精确证明两次initialize和其余
协议动作各一次；test-only loopback开关不进入production默认构建，SSRF destination guard保持fail closed。logical subscription真实外部
Streamable HTTP/SSE protocol/crash component L3由此闭合；真实第三方服务、容量饱和、production telemetry scrape与L4～L6仍Pending。

r358在唯一baseline的fresh PostgreSQL 16上补齐MCP discovery exact-kind claim/recovery L2竞争：两个不同Worker generation并发竞争同一
ready discovery Job时只有一个leased winner，typed `McpDiscovery + Mcp + mcp_operation`谓词与行锁同事务生效；winner进入running/physical
attempt 1并合法过期后，exact operation/Job version与lease generation observation只允许一次恢复为`retry_scheduled/pending`，旧observation
重放被fence拒绝。该批关闭discovery claim/running recovery L2，不扩张为Artifact stage/scan/finalize kill-window、production discovery协议
L3、真实S3或L4～L6证据。

r359在唯一baseline的fresh PostgreSQL 16上闭合MCP discovery内部Artifact owner事务L2：恢复后的exact running Job完成stage preflight与
物理证据提交，同generation重放复用原staged identity；park原子推进operation `pending -> running`、owner Job waiting与verification Job ready；
Artifact scan提交Verified evidence并通过producer correlation持久唤醒owner，下一attempt再完成Artifact Ready、immutable Snapshot、active
Evidence Link、双方Job终态及park/finalize幂等重放。该门禁同时修复stage authority预先要求operation running造成的不可达路径，以及
`maximum_bytes`预留却按实际descriptor大小结算导致的quota冲突；终态证明完整预留settle且reserved归零。该证据关闭stage/scan/wake/finalize
与提交后owner接管L2，不扩张为真实S3、production discovery多进程协议L3、容量饱和或L4～L6证据。

r360在唯一baseline的fresh PostgreSQL 16上闭合production MCP discovery进程/协议L3。独立Egress OS fixture、Artifact unary mTLS OS fixture与
真实`platform-mcp-discovery-worker`共同运行：attempt 1在外部TLS MCP endpoint收到`initialize`但尚未响应时强制终止Egress与Worker，exact lease
过期后重启；attempt 2重新执行`initialize`、`notifications/initialized`与`resources/list`，经Artifact RPC提交stage evidence并durable park；真实
`ArtifactWorkerService` scan提交Verified evidence及durable wake后，同一production Worker以attempt 3 claim并完成owner finalize。终态断言固定
discovery Invocation/Artifact/verification Job均成功、owner `attempt_no=3`，且immutable Snapshot和active Evidence Link各唯一；协议日志固定为两次
`initialize`及其余方法各一次。fixture使用S3-shaped测试证据且scan仍在测试进程内，因此该证据不扩张为真实S3/KMS、Artifact Data Worker进程L3、
第三方endpoint、容量饱和、production scrape或L4～L6。

r361在同一fresh PostgreSQL 16 production discovery进程夹具中补齐动态容量surface的饱和点L3观测。唯一`maximum_concurrency=1` permit被一个已
发送到外部TLS endpoint、尚未获得响应的真实discovery attempt持有时，Worker真实HTTP `/readyz`保持200，`/metrics`精确导出固定低基数
`discovery_jobs available=0/used=1`；随后的Egress/Worker强杀、lease recovery与owner finalize仍通过，证明观测读取不改变业务fence。该证据只
关闭单进程真实permit authority到HTTP surface的动态接线，不代表第二lane隔离、approved CapacityProfile、production Prometheus scrape、SLO/soak
或L4～L6。

r362以固定LocalStack Community 4.14.0 HTTPS S3/KMS-compatible endpoint执行既有真实provider round-trip：安装exact immutable S3/KMS binding、
通过HeadBucket/GetPublicKey readiness，KMS envelope加密object locator，向启用versioning的隔离bucket写入bytes并取得非伪造generation；随后验证
wrong-tenant解封拒绝、exact generation head/read、wrong generation NotFound、exact generation delete及删除后NotFound，测试结束bucket无残留version。
该证据关闭AWS-compatible Artifact provider adapter的真实HTTPS S3/KMS L3切片；它不代表production Artifact Data Worker进程、AWS云服务/workload
identity、KMS rotation、restore、真实生产scrape或L4～L6。

r363修复production Artifact Data Worker把进程wall clock混入数据库裁决的问题。`ArtifactWorkerService`现在把调用方提供的authority time作为
`ArtifactScanRequest.observed_at`传给scanner；Data Worker在stage provider I/O前后、scan Receipt构造及scan execution前分别读取PostgreSQL
`clock_timestamp()`，用数据库时间验证deadline、生成staged/scan evidence并执行fenced commit。未来evidence校验没有放宽，host/DB时钟偏差不再
伪造StaleFence或使合法provider结果不可提交。Artifact domain/service目标测试与strict Clippy通过；本批无fresh PostgreSQL provider进程链，故仅
关闭时间authority L1，不扩张r362的L3边界。

r364在fresh PostgreSQL 16唯一baseline与LocalStack Community 4.14.0 HTTPS S3/KMS上运行production
`platform-mcp-discovery-worker`和production `platform-artifact-data-worker`。进程链故意在第一次remote TLS MCP `initialize`后杀死Discovery
Worker，lease recovery后完成descriptor stage；Data Worker以数据库authority time自动领取scan Job，经KMS Encrypt、versioned S3 Put、KMS
Decrypt、exact-version Head/Get验证174-byte discovery descriptor，随后唤醒owner并由attempt 3 finalize。终态为Invocation/owner/verification
`succeeded`、Artifact `ready`、Blob `verified`，保留vendor `application/vnd.insight.mcp-discovery+json`，数据库密文引用356 bytes，DB与S3
exact generation一致，且active Snapshot/Artifact Link各唯一1行。该链发现并修复scan-read authority把持久Artifact owner误判为Job owner，以及
strict JSON scanner错误拒绝合法`application/*+json` structured suffix；fresh PostgreSQL phase3 authority回归、production phase4进程测试、
目标单测与strict Clippy通过。证据关闭两个production Worker+PostgreSQL+AWS-compatible provider的L3切片；remote MCP/Egress仍是独立TLS协议
fixture，LocalStack不代表AWS云服务/workload identity、KMS rotation/restore、production scrape或L4～L6。

r365把既有Model→Capability→Model→Return production进程测试从内存Typed Plan broker升级为production
`platform-artifact-data-worker`与真实HTTPS S3/KMS。夹具先以production AWS provider写入1182-byte exact Typed Plan，再延后source Job，待
Artifact Data Worker与Orchestration Worker均ready后锁定PostgreSQL Artifact authority、释放Job并在其进入`running`时同时强杀两个进程；数据库
过期原lease，重启Data Worker和Orchestration Worker后由attempt 2重新claim、KMS解封并exact-generation读取Typed Plan，最终整条
Model→Capability→Model→Return链及Run均`succeeded`、fence清空。该链同时修复旧Model夹具把EncryptionDomain伪装成Policy ID及使用host clock
构造Artifact时间的问题。证据关闭Typed Plan production Scheduler RPC双进程kill/restart L3；Artifact-backed RunValue的同类故障窗口、AWS云
workload identity及L4～L6仍未由此证明。

r366在同一production进程链中新增独立`Start -> Return(RunInput)` Agent/Run，输入不是Inline，而是由production AWS provider写入
versioned S3并以KMS envelope保护locator的35-byte Artifact-backed RunValue。测试先通过正式Run admission冻结exact Agent/Plan/
ExecutionProfile与principal授权，再锁定PostgreSQL `run_values` authority，使Orchestration Worker发出的Scheduler RunValue mTLS RPC在
Artifact Data Worker内确定处于进行中；随后同时强杀两个进程、释放锁并过期attempt 1 lease。按readiness顺序重启后，attempt 2重新claim，
Data Worker重验Job/Run/value/artifact/fence authority、KMS解封并按exact S3 generation读取正文，最终Run与source Job均`succeeded`、
`active_work_count=0`、fence清空，且`output_value_id`精确等于Artifact-backed `input_value_id`。证据关闭Artifact-backed RunValue production
Scheduler RPC双进程kill/restart L3，并与r365共同闭合Phase 2 terminal materialization的仓库内production进程窗口；LocalStack仍不代表AWS云
workload identity、网络滚动故障或L4～L6。

r367复核Phase 4 telemetry closure时发现MCP Discovery已有exact Job authority的due/expired-lease count/lag及通用dashboard，却是唯一没有
对应symptom-first durable queue alert的已接线role。本批增加固定`mcp-discovery-worker`的due与expired-lease lag告警、逐项runbook及28条exact
alert inventory门禁，并把完成审计中14-panel/13-alert的过期累计修正为当前15-panel/28-alert。该证据关闭仓库内dashboard/alert覆盖缺口；
production Prometheus scrape、alert delivery、L5 SLO/error budget与L4～L6仍未由此证明。

r368在`8cb12ce9`首次推送后的真实GitHub CI发现crate-boundary checker未同步后续Platform v2进程拆分。修复新增第55个
`insight-platform-rpc-trace` closed role，精确列出shared observability HTTP、typed RPC trace、MCP Discovery/Resource、Sandbox Controller与
production worker composition边，并只允许PostgreSQL跨进程资格fixture使用dev-only Artifact/Egress边、trace collector及唯一
`protocol-fixtures` test feature。门禁继续拒绝未知crate、未列边、production subscriber、额外feature及SQL/HTTP/cloud SDK越界；本地扫描通过
55个workspace package、578个resolved package。该修复只恢复CI架构门禁的当前性，不新增L4～L6证据。

r369修复r368 GitHub CI继续暴露的Sandbox部署门禁可移植性：runner只安装Helm且没有`rg`，旧脚本在任何Helm渲染前即
command-not-found。静态正负搜索改用GNU/macOS共同支持的`grep -E/-R`，禁止microVM/Firecracker/KVM/managed-stdio、deferred crate排除、
三进程composition及Helm负向合同保持不变。本地正常PATH与隐藏`rg`的runner-equivalent PATH均通过；该修复不构成真实runsc或L4～L6证据。

r370修复r369 GitHub CI继续暴露的Security/Egress精确RPC清单漂移。CR-198已评审并实现credential-free、object-locator-free的
`DiscoverMcpStreamableHttp`，但部署checker仍停留在CR-192时的13项集合。当前closed inventory精确为14项；checker登记该既有discovery
method并继续以总数相等拒绝任意第15项。该批不改变proto、authority、credential/locator边界或部署拓扑，也不新增L4～L6证据。

r371修复GitHub CI实时RustSec数据库发现的依赖漏洞，不采用advisory ignore。`h2`升级到`0.4.16`；restricted WASI唯一runtime从
`wasmtime 42.0.0`升级到`46.0.2`，覆盖该run报告的全部13项Wasmtime公告，并同步exact runtime evidence和依赖feature baseline。WASI
10/10、workspace all-target/all-feature tests、format、strict Clippy、RustSec audit、cargo-deny及55-package/583-dependency boundary本地门禁
通过；该批不新增backend、权限或L4～L6证据。

r372～r381继续修复完整GitHub CI首次串行执行暴露的资格夹具与持久化边界。r372让隔离Model conformance数据库的`createdb`显式使用
`PGPASSWORD`、`--no-password`和`postgres` maintenance DB，消除非交互runner等待密码；r373把实时Job queue age断言改为数量精确且age
单调，避免两次`clock_timestamp()`之间的合法增长被误判。r374把Phase 2 Task恢复夹具绑定到其创建的exact root orchestration Job，所有claim
均断言同一Job ID，不再从同tenant共享队列任取工作。r375在Child Run admission前把deadline规范化为PostgreSQL微秒精度，并以显式纳秒输入
证明JSON closure、typed column与回读projection保持一致。r376把首发`SqlCatalog` Context backend路由到隔离的
`ContextQueryNative` durable Worker lane；fresh PostgreSQL 16端到端覆盖query claim/fence、Observation提交及Text2SQL只读Capability准入。
r377又把terminal-only Artifact staging catch-up夹具的101条数据冻结到未来`available_at`，只由显式更晚authority time的drain领取，消除
production后台pump合法抢先处理部分行的竞态并保持跨两个bounded batch的证明。上述修复强化L2/L3证据的确定性与真实数据库可移植性，
r378把Capability `InputRequired` deadline也在outcome Receipt/decision之前规范化为PostgreSQL微秒精度，显式纳秒fixture证明Job/Invocation JSON与
Task typed column回读保持exact binding。r379同样在MCP OAuth start事务的Receipt与Task写入前规范化external-authorization deadline，避免首次提交
后的replay因`timestamptz`微秒截断误判`not_found`。r380规范化MCP Discovery operation deadline；r381对MCP Resource Subscription先验证
包含原始deadline的客户端`request_digest`，再在Receipt claim前规范化已验证deadline，使JSON binding与typed列回读一致且不改写外部幂等意图。
fresh PostgreSQL 16完整OAuth文件8/8、Subscription文件3/3通过；GitHub CI run `33102457010`的Test、文档、Lint/format/strict Clippy、依赖策略
和TypeScript/Go MCP SDK互操作四个Job全部成功。其后的文档证据提交run `33104191217`因`chacha20 0.10.1`在两次run间被crates.io撤回而被
cargo-deny拒绝；r382没有增加ignore，而是更新锁文件及exact third-party feature baseline到兼容未撤回的`0.10.2`。GitHub CI run
`33105053408`再次四个Job全部成功。仓库资格门禁由此闭合；上述修复不产生production-equivalent L4～L6证据。

r288新增独立production-candidate CI workflow：所有action固定commit SHA，且必须先以40位commit SHA只读checkout GitOps environment closure；
以两个Docker target构建exact-digest runtime与gVisor guest，生成并
签名SPDX SBOM、BuildKit/GitHub provenance、CandidateManifest和传递闭合的release-bundle index；Candidate冻结15个ComponentRole、7个实际
WorkerManifest、唯一baseline migration、contract/config/limit/policy/qualification摘要和可复现commit timestamp。CI静态门禁、生成器负向测试及
Rust `validate-production-candidate`通过。该批实现可执行的signed candidate producer，但尚无实际外部registry/GitOps运行产物、人工审批
或production-equivalent L4～L6运行证据，因此`signed_supply_chain`仍不得标记passed。

r383把GitHub environment repository作为真实外部发布authority接入：稳定`production/closure`与资格后的`releases`分离，candidate workflow
只允许从`main`通过repository-scoped只读deploy key读取精确commit，并在构建前验证environment对application commit、QualificationProfile、
multi-node/runsc/admission、受保护node selector及无credential Git策略的closed绑定。该接线没有运行registry candidate、目标cluster或人工promotion，
因此不改变L4～L6与spec00～18状态。

r289最终静态复核确认subscription Context Worker与MCP Resource Host加入后，15个ComponentRole当前映射为19个隔离workload pool，
其中9个LocalWorkerPools具备动态permit指标；早期r248～r267记录的17-pool是当时历史证据，不再代表当前拓扑。Security/Egress全局门禁同步
纳入CR-192 `RefreshMcpResources` closed RPC，当时要求Egress只暴露13个reviewed remote-only method；后续CR-198增加仅用于discovery的
第14项，当前exact inventory以r370为准。全局render、observability、redaction和strict workspace Clippy通过；该更正不新增role、RPC或
authority，也不替代live L4。

CR-170进一步确认public Artifact调用方只提交业务意图或opaque completion proof，Blob/Grant/Job/Task/Receipt/Event/Outbox、policy、quota、
storage与audit closure全部由服务端拥有；upload target是唯一显式Secret-bearing响应例外。Public Gateway不取得storage authority，Artifact Gateway
不信任自由principal header，两者以exact audience mTLS连接并由Artifact Gateway从PostgreSQL重绑定current principal。

CR-171把public Artifact使用的Retention与ArtifactIo default revision加入tenant current config exact slot；多条active Policy不再通过排序或
隐式安装默认选择，绑定更新沿用Tenant CAS/Receipt/Event/Outbox且保留其他slot。

CR-216 目标要求继续删除或退出首版 composition 中的 restricted WASI、自建 gVisor、Docker provider、microVM/Firecracker、
Managed stdio session 和 Model Artifact Producer；首版闭包变为 OpenSandbox Kubernetes/BatchSandbox/containerd-runc、三角色
Artifact 和最小 `/v1`。OpenSandbox/Kubernetes provider state 不得成为
第二业务持久状态权威，host process仍禁止。

[implementation-plan.md](implementation-plan.md)保留CR-216之前的历史记录，并以CR-216 final evidence关闭OpenSandbox仓库批次。
受影响01～04、07、09、10、14、15、17、18仍是Accepted目标合同；实现、L1～L3和仓库clean-cut已通过，current behavior已同步到
[docs/current](../../current/README.md)。该状态不声明production多节点拓扑、容量/SLO、强隔离、restore或promotion通过；L4～L6均Not run。
