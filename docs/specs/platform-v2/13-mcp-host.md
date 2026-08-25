# Platform v2 MCP Host 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-193 |
| 日期 | 2026-08-26 |
| 依赖 | 02、03、04、07、09、10、12 |
| 直接下游 | 15、17、18 |

> CR-181 impact：MCP Tool作为Capability backend只消费10已冻结Invocation snapshot；Host不得读取Plan slot、重新选择Deployment、
> 改写node output port或直接创建resume Job。MCP Resource作为Context backend同样只消费12的exact query snapshot。

> CR-188 impact：MCP Tool Capability的Platform↔MCP参数/结果mapping由09 exact installed codec拥有；MCP Host仍独立拥有
> Streamable HTTP、authorization、Task/Elicitation与subscription语义。Capability Worker不得仅凭mapping digest构造自由codec。

## 1. 决策摘要

MCP Host是独立协议边界，不是Action、Capability或Sandbox的别名。它保留Tool、Resource、Prompt、Task、
transport、authorization、subscription和interaction语义，再显式投影到平台合同。

首版只支持remote MCP Server的Streamable HTTP transport。Managed stdio、在平台内启动MCP进程、persistent
Sandbox session及其parent/child Job模型全部推迟。

## 2. 协议基线与投影

MCP protocol version、feature negotiation与transport profile必须是published ResourceVersion的closed字段，不在运行时
猜测或自动升级。首版的投影如下：

| MCP 对象 | 平台投影 |
|---|---|
| Tool | Capability Implementation backend |
| Resource | ContextSource Implementation |
| Prompt | 不受信任候选Artifact；显式发布后才是Agent/Skill asset |
| Task | CapabilityInvocation durable continuation |
| Elicitation | InputRequired 或 ApprovalRequired Task |
| Subscription | Context invalidation/event source |

投影不改变原MCP object identity、server identity、discovery generation、authorization scope或schema digest。

## 3. MCP Server 与Protocol Profile

MCP Server ResourceVersion必须冻结：

- remote endpoint catalog identity，不是自由URL；
- Streamable HTTP profile与exact MCP protocol version；
- TLS、redirect、DNS、proxy、response byte/time hard limit；
- OAuth/Auth requirement和SecretBinding requirement；
- 允许的Tool/Resource/Prompt/Task/Subscription feature集；
- discovery、session、request、notification、retry与rate-limit policy；
- data classification、tenant、region与audit policy。

Deployment绑定exact Server Revision、Protocol Profile、Credential reference和Discovery Snapshot。Run/Invocation再冻结exact
Deployment，active head变化不影响已存工作。

## 4. Streamable HTTP transport

Host通过Egress Broker发起credential-free closed request，Egress Broker在最后一跳解析OAuth/Secret并实施
SSRF、DNS pinning、TLS、redirect和egress policy。Host不获取raw token、client secret、proxy credential或object-store credential。

单次request包含exact tenant、Deployment、Discovery generation、protocol、method、request ID、deadline、body schema digest
和auth binding digest。response在Host trust boundary再验证content type、protocol envelope、schema、bytes和deadline。

MCP session token为opaque密文，仅作为受限continuation使用，不暴露给API/Model/Agent。session loss不解释为业务成功；
它触发bounded reinitialize、reconcile或stable failure。

## 5. Discovery Snapshot

discovery是durable Job，结果是immutable Discovery Snapshot，包含：

- exact Server/Deployment/Protocol/Auth binding；
- Tool、Resource、Prompt和feature descriptor的canonical排序；
- 每个descriptor的schema、annotation、effect与capability digest；
- source generation、observed time、expiry与overall digest；
- rejected/unsupported entry的bounded Event evidence。

发布Capability/Context投影时必须引用exact snapshot与entry digest。discovery变化只产生新snapshot，不改写旧Deployment。

Public discovery command必须显式携带当前principal可用的`authorization_binding_id`与bounded deadline。Host在创建Job的同一事务
重验binding tenant、principal kind/generation、exact MCP Deployment、audience、scope、credential generation与expiry；Gateway不得
通过“第一条可用binding”、active head或自由header猜测授权。Receipt replay返回第一次冻结的binding与Job结果。

## 6. Tool、Resource 与Prompt

Tool调用始终经过09/10的Capability Interface和Invocation。MCP Host只是backend adapter，不跳过policy、approval、
quota、Receipt、Job或output schema validation。

Resource读取通过12的Context Query语义，保留URI/template、pagination、MIME、citation、cursor、subscription和
authorization evidence。有副作用的Resource操作必须是Capability。

Prompt内容一律不受信任，需要通过Artifact scan、review与immutable ResourceVersion发布后才能进入Agent/Skill。

## 7. Remote Task 与Elicitation

MCP Task使用10的WakeContract。Host提交`Deferred`时保存加密remote task/session evidence、poll/callback mode、
next poll、deadline和schema digest，然后释放Worker permit。poll、callback、cancel和timeout竞用同一Receipt/current fence。

elicitation必须投影为shared Task：

- 普通结构化补充为`InputRequired`；
- 权限、高风险副作用或Secret-related决策为`ApprovalRequired`；
- 响应不得超过冻结schema或返回Secret value；
- Task terminal后由新的bounded Job恢复Invocation。

## 8. Subscription 与通知

首版remote subscription复用shared Job/Receipt/Event/Outbox，不建MCP session专用表。每个subscription冻结exact
Deployment、Resource identity、auth binding、protocol profile、cursor、rate limit和consumer target。

通知先通过Receipt去重、大小/schema/tenant验证和rate limit，再写Event/Outbox。活跃连接不是durable authority；
断线后从已提交cursor恢复，无法恢复时做full reconcile。订阅饱和不得占用Capability、Model或Sandbox pool。

notification commit后，MCP subscription Worker从exact pending invalidation构造12定义的closed Context admission request，并调用Context owner
application port。只有owner transaction已提交shared Context Job + Receipt/Event/Outbox且返回的request digest精确匹配时，Worker才可提交
`complete_subscription_refresh/reconcile`并park自身MCP Job；它不生成durable work digest、不直接创建Context结果，也不以内存future等待Context
完成。commit-window不确定必须按同一Receipt key查询/replay，Host restart从PostgreSQL subscription/Job恢复；旧session/worker fence不得再次接线。
Context owner创建的物理刷新Job以closed `Context -> McpOperation` pair绑定当前subscription identity；Host自有connection/recovery Job仍为
`Mcp -> McpOperation`。owner pair相同不代表WorkClass或claim authority相同，任一worker扫描错class/payload必须零claim。

Context Worker执行该Job时只调用Host的typed internal `RefreshResources` RPC。请求携带tenant、subscription、Context Job ID、worker
generation/fence、exact Context/MCP Deployment、Discovery/Auth/session/event/root evidence、cause、deadline和request digest；不得携带raw
session/token/Secret、自由endpoint/header或任意MCP method。Host以独立workload audience重载当前subscription、Job state/fence及published
MCP execution closure，任何漂移均在Egress调用前fail closed。Host随后按冻结protocol profile执行bounded `resources/read`；full reconcile必须
先执行登记为closed ReadOnly method的`resources/list`，再对冻结root约束下的返回集合执行有界`resources/read`。list/read分别使用published
method limits；缺少任一method、capability或limit均在Egress调用前fail closed。Host只返回request/response/resource-set digest、counts、
remote revision/cursor、observed time或closed safe failure。

该RPC是ReadOnly protocol adapter，不是Job owner：Host不claim/heartbeat/terminalize Context Job，不创建Observation/cache，也不因notification
自行调用它。响应后Host崩溃或RPC completion uncertain允许Context owner用新attempt安全重读；Host仍须执行session/Egress permit、rate/body/time
limits，Context Worker在整个调用期间只持有自己的Context permit。Secret由Egress最后一跳解析，Host返回值与默认日志不含remote body或凭据。

## 9. OAuth 与authorization

OAuth authorization code flow使用PKCE、state、nonce、exact redirect URI与short-lived callback Receipt。数据库只保存
AEAD ciphertext、digest、reference identity和expiry，不保存raw code、verifier或access/refresh token。

callback first-winner原子更新AuthorizationBinding、Receipt、Event和Outbox。重放、state/tenant/provider mismatch、过期、
redirect漂移或scope扩大全部fail closed。token refresh/revoke由Egress Broker执行，Host只看sanitized evidence。

## 10. 所有权与持久化

| 事实 | Authority |
|---|---|
| MCP definition/profile/deployment | shared Resource lifecycle |
| Discovery/build/reconcile attempt | shared Job |
| Tool business call | CapabilityInvocation |
| Remote Task/Input/Approval | shared Task + Invocation WakeContract |
| Callback/idempotency | shared Receipt |
| Notification/audit/history | Event + Outbox |
| Large Resource/Prompt body | Artifact |

不增加MCP current-state专用表。session、discovery、task和subscription detail是bounded typed snapshot，每个都有
`schema_version`、closed validation、size limit、canonical serialization和digest。

## 11. 安全、并发与可观测性

- Host、Egress Broker、Artifact Data Worker使用不同workload identity、DB pool与permit；
- 每个请求复核tenant、Deployment、Discovery、Auth、session和Invocation/Job fence；
- 所有stream/body/list/page/log/progress都有item、byte和time hard limit；
- Secret、token、authorization code、query/body与remote content不进log、metric label或Event；
- metric至少包含discovery、request latency/outcome、session loss、task wait、notification drop和rate-limit；
- Host饱和只影响MCP lane，不使API、Model、Sandbox或native Capability readiness失败。

## 12. 验收标准

- protocol/version/feature不匹配在发送业务request前fail closed；
- discovery snapshot排序与digest可重复，旧Run不受新discovery影响；
- Tool/Resource/Prompt分别保留Capability/Context/untrusted-asset语义；
- remote Task不持有常驿future，poll/callback只有一个winner；
- OAuth replay、scope escalation、tenant mismatch、expired state与redirect drift被拒绝；
- subscription在丢失连接或NATS消息后可从durable cursor/reconcile恢复；
- subscription invalidation在Host kill/restart和Context admission commit不确定窗口中只创建一个Context Job，且MCP/Context permit相互隔离；
- 首版部署不包含stdio runner、Sandbox session child、microVM或动态运行时installer。

## 13. 分层证据

protocol parser/property tests、fake-server adapter tests、PostgreSQL Receipt/Job/Task tests、OAuth security tests和
production-equivalent mTLS/NetworkPolicy/saturation tests分层运行。开发fixture不代替发布资格。

## 14. 明确推迟

- Managed stdio、本地MCP process、persistent Sandbox session与其Provider recovery；
- WebSocket或其他transport；
- MCP sampling/roots的完整双向实现；
- 跨region session migration和exactly-once notification delivery。

## 15. 未决问题

CR-181不增加MCP current-state authority；wrong Invocation/Context owner、Plan/binding digest或output schema必须fail closed。

2026-08-25 implementation evidence：独立`platform-mcp-host` production binary已在真实进程fixture中验证两段分离mTLS身份。ReadOnly
ToolsCall到达Egress Broker后强杀Host，Capability Worker侧只得到`CompletionUnknown`；重启同一binary后按安全重放规则提交同一冻结
contract/request并成功，Egress总调用数为2。该证据只关闭Host自身的process restart与completion-unknown边界；PostgreSQL Remote
Worker→Host→Egress三进程的durable claim/lease/reconciliation由fresh PostgreSQL 16 r221补齐：exact protocol/auth/discovery/Capability
binding在claim与I/O前重验，错codec零调用，正确非幂等ToolsCall返回后Worker强杀且恢复不重放。MCP ToolsCall process L3至此闭合；
OAuth、Task和subscription真实协议矩阵仍是后续资格待办。

r268已在Context owner边界交付CR-190 closed subscription refresh request、bounded shared Context Job payload和exact acceptance的L1 Rust合同，
但MCP Host尚未拥有调用该port的production adapter/claim loop，PostgreSQL也尚未实现Job/Receipt/Event/Outbox原子接纳与commit-uncertain replay。
因此subscription多进程L3及本规范第12节对应验收保持未完成。

r270已补齐PostgreSQL owner transaction与notification Receipt replay/唯一Context Job L2 fixture，并让既有MCP completion消费owner返回的
durable work digest；Host production adapter、full reconcile L2、独立claim/refresh进程及commit-window L3仍未完成。

r271新增Host侧typed Context invalidation target，覆盖notification与full reconcile映射、exact root/deadline和commit-uncertain传播；adapter API
没有Job/work-digest输入。fresh PostgreSQL full reconcile acceptance/replay也已通过。该target尚未组合进production binary，Context Job handler与
Host/Context独立进程L3仍待完成。

r272关闭refresh Job的PostgreSQL claim/terminal/retry/recovery L2，并验证MCP Worker先以durable acceptance结算自身Job、清除pending marker后，
Context Worker仍只凭exact admission Receipt与当前session/auth/closure安全claim；旧pending字段不再成为第二执行权威。MCP Host
`RefreshResources` RPC、真实Streamable HTTP read/list及三进程kill-window仍待实现。

CR-193明确Host evidence不得摘要包含可变`expected_version`的整个attempt。Host在I/O前仍重验收到的完整Job fence，但返回的
`execution_identity_digest`只绑定不可变物理attempt closure；Context Worker heartbeat后的最新version仅用于PostgreSQL terminal commit。
Host不能据此缓存、延长或改写Job lease。

r273已在Context侧组合durable driver与typed `ContextSubscriptionRefreshBackend` port，包括独立permit、heartbeat/latest fence commit、
ReadOnly retry分类和expired-lease recovery；CR-193 identity与fresh PostgreSQL heartbeat fixture通过。Host侧仍未提供该port的RPC server/
Streamable HTTP adapter，故Context Worker→Host→Egress三进程L3保持未完成。

r274交付独立closed protobuf `McpResourceRefreshService`及Rust client/server adapter：Context请求不进入Capability Execute RPC，server只接受exact
Context Worker URI SAN，且在envelope decode前授权；request/outcome使用bounded canonical JCS、digest和closed错误码。真实mTLS fixture已通过，
但production Host尚未组合resolver与Resource transport，因此该证据不关闭process L3。

r275实现Host refresh application service与PostgreSQL resolver：RPC进入协议port前重验running Context Job/latest fence、原始payload、成功
admission、active subscription/session及exact MCP execution closure。fresh PostgreSQL heartbeat fixture通过；Egress Resource adapter和production
binary接线仍未完成。

r276交付独立Host→Egress Resource Refresh RPC与Streamable HTTP protocol adapter。Egress从process-installed exact catalog解析endpoint和Secret，
按冻结limits执行initialize、full-reconcile list和exact-root read，正文只产生digest/count evidence；服务器返回的其他URI不会成为后续read target。
全套unit/真实mTLS/strict Clippy已通过。production Host binary、Context Worker binary与三进程kill-window尚未组合，故L3仍未关闭。

r277新增独立`platform-mcp-resource-host` production entrypoint，组合PostgreSQL resolver、closed Host service及MCP Host→Egress mTLS client；
Egress Broker production composition也安装同一Resource Refresh connector。独立subscription Context Worker entrypoint只以Context Worker audience
调用该Host。all-target/既有process L3/strict Clippy通过；尚未render Helm和执行subscription kill-window，故L3仍未关闭。

r279在fresh PostgreSQL 16上以production Resource Host和subscription Context Worker进程覆盖两个崩溃窗口：首次Egress dispatch后终止Host及
Worker并以expired lease恢复；第二次response后暂停Job terminal commit并终止Worker；第三个Worker完成同一ReadOnly Job，三次远端尝试只产生
一个completed Event。Egress Broker在该fixture中通过真实mTLS运行于测试进程内，尚不是独立OS进程，且未连接真实Streamable HTTP fake server；
因此只关闭Host/Context process recovery切片，完整subscription protocol L3仍须补齐独立Egress、真实list/read wire及pre-dispatch零I/O矩阵。

首版remote Streamable HTTP合同无未决设计问题。
