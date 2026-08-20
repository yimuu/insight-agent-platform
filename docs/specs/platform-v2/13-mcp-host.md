# Platform v2 MCP Host 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-20 |
| 依赖 | 02、03、04、07、09、10、12 |
| 直接下游 | 15、17、18 |

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

首版remote Streamable HTTP合同无未决设计问题。
