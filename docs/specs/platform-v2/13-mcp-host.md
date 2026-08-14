# Platform v2 MCP Host 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-07 |
| 依赖 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md)、[`10-capability-invocation.md`](10-capability-invocation.md)、[`12-context-and-retrieval.md`](12-context-and-retrieval.md)、[`16-model-provider-and-invocation.md`](16-model-provider-and-invocation.md) |
| 直接下游 | 14、17、18 |

> Persistence ruling：MCP registry 使用共享 Resource；operation/task/subscription 的 durable work 使用 Invocation/Job/Task/
> Receipt/Event。协议细节保存为 bounded typed payload，不建立 MCP 专用状态表族。

## 1. 决策摘要

MCP 是独立协议适配边界，不是平台内部的通用 Action 模型。MCP Host 拥有版本协商、transport、OAuth、
session、discovery、JSON-RPC、Tasks、Elicitation、Sampling、Subscriptions 和协议限流；平台分别把 MCP Tool、
Resource、Prompt 与 Task 投影为Capability Implementation、Context Implementation、候选Prompt Artifact和
CapabilityInvocation continuation。

生产基线固定 MCP `2025-11-25` protocol profile；Tasks 在该版本仍属于 experimental，默认关闭并使用独立
feature profile。平台不接受 `latest` 或运行时 draft 跟随。Streamable HTTP 是远程默认 transport；stdio
进程只能由隔离的 Managed MCP Runner 创建，MCP Host 自身不得 fork/exec 或运行 server code。

## 2. 目标与非目标

### 2.1 目标

- 让 MCP 协议演进与平台 Capability/Context/Run 状态机解耦；
- 固定 Server Revision、Protocol Profile、Discovery Snapshot、Auth 和 Policy 后再发布投影；
- 为同步 Tool、异步 Task、Resource、Prompt、Elicitation 和 Subscription 保留各自语义；
- 支持 per-user OAuth 与 service identity，同时防止 token passthrough 和 confused deputy；
- 让 MCP I/O 使用独立连接池、并发预算、队列、circuit 和 autoscaling；
- 对重连、重复响应、迟到通知、session 丢失和 server schema 漂移提供 durable 恢复；
- 默认关闭 server-initiated 高风险能力，并通过显式 profile/policy 开启。

### 2.2 非目标

- 不让 MCP 成为平台 Agent、Skill、Capability 或 Context 的内部存储格式；
- 不自动信任远端 annotations、description、schema、icon、URI、error 或 Prompt；
- 不在 discovery 后自动 publish、activate、授权或暴露给模型；
- 不支持 deprecated HTTP+SSE transport 或任意 custom transport；
- 不允许 MCP Host 启动本地二进制、脚本、容器或访问宿主文件系统；
- 不把平台 access token、用户 session cookie 或其他 server token 透传给 MCP server；
- 不保证远端 MCP server、Task 或 Resource 具有平台级 exactly-once/snapshot 语义；
- 不默认支持 server-initiated Sampling、Roots 或 URL navigation。

## 3. 外部协议基线

本规范的外部 wire basis 是 MCP `2025-11-25`。该版本定义 stdio 与 Streamable HTTP、初始化协商、OAuth
resource/audience 规则、Tools、Resources、Prompts、Elicitation、Sampling 和 experimental Tasks。平台 profile
只会收紧外部协议，不能弱化其 MUST。

非规范性官方参考：

- [MCP 2025-11-25 transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [MCP 2025-11-25 authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)
- [MCP 2025-11-25 schema](https://modelcontextprotocol.io/specification/2025-11-25/schema)
- [MCP experimental Tasks](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)
- [MCP Elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation)
- [MCP Sampling](https://modelcontextprotocol.io/specification/2025-11-25/client/sampling)

新增协议版本必须发布新的`PolicyRevision(kind=Protocol)`、wire conformance和migration evidence。Draft页面、SDK
默认版本或 server 宣称的最大版本不会自动改变生产 profile。

## 4. MCP Server Revision

```rust
struct McpServerRevision {
    server_revision_id: RevisionId,
    mcp_server_id: McpServerId,
    transport_kind: McpTransportKind,
    protocol_profile_revision_id: RevisionId,
    deployment_credential_requirements: Vec<SecretPurpose>,
    authorization_credential_purpose: Option<SecretPurpose>,
    limits: McpServerLimits,
    semantic_digest: Digest,
}

struct McpDeployment {
    mcp_deployment_id: DeploymentId,
    server_revision_id: RevisionId,
    canonical_server_identity: CanonicalServerIdentity,
    transport: McpTransportDescriptor,
    auth_profile_revision_id: Option<RevisionId>,
    network_policy_revision_id: RevisionId,
    trust_profile_revision_id: RevisionId,
    deployment_secret_bindings: Vec<ExactSecretBindingRef>,
    conformance_evidence_id: EvidenceId,
    deployment_digest: Digest,
}

enum McpTransportDescriptor {
    StreamableHttp(StreamableHttpDescriptor),
    ManagedStdio(ManagedStdioDescriptor),
}
```

Server Revision固定transport family、protocol profile、部署级credential purpose、独立authorization credential purpose和
message/body/session limits；Deployment固定canonical URI或signed runner package、TLS/redirect/auth/network/trust policy、
部署级SecretBinding与绑定exact环境的
conformance evidence。二者都不包含
access token、session ID、discovery objects或实时health。运行时只使用exact `mcdep` Deployment，不直接执行Revision。
Deployment创建前先固定candidate spec并异步完成connectivity/auth/protocol conformance；evidence与candidate digest匹配后
才能在单个command中创建immutable Deployment。deploy不等于activate，active head变化只影响后续discovery/绑定。

Deployment Policy closure按transport闭合：Streamable HTTP固定`protocol/trust/network/tls`，Managed stdio固定
`protocol/trust/isolation/resource/artifact_io`；`auth_profile`是唯一可选附加role，但创建AuthorizationBinding前必须
存在。一个Policy Revision不能填充多个role，`protocol`必须与Server Revision冻结的Protocol Policy完全一致。

Deployment closure 的 `deployment_secret_bindings` 只满足 `deployment_credential_requirements`，例如 OAuth client secret、
mTLS client identity 或 Managed Runner bootstrap secret。用户或service identity完成授权后产生的access/refresh token由
AuthorizationBinding自己的exact SecretBinding持有；它绑定tenant/principal/audience/scope/generation，不属于Deployment closure，
也不能因新增用户而重建Deployment。两类purpose不得重复，Host/egress必须按角色分别解析，禁止把client credential当作resource
token或反向复用。

MCP transport machine wire固定为`streamable_http | managed_stdio`；authorization principal binding固定为
`per_user | service_identity`。前者必须绑定tenant内具体Principal且不能跨Principal复用，后者必须绑定
`PrincipalKind=service_identity`。Session状态继续消费06唯一state machine registry，不在MCP adapter复制字符串状态。

Phase 1只交付Server Entity/Draft/Validation/Revision、Conformance/Deployment、Head/Suspension与
AuthorizationBinding registry authority；Session、Operation、RemoteTask、Subscription和Notification属于Phase 4运行面，
必须等待共享Attempt、Invocation/ContextQuery与Artifact owner，不能在registry migration中预建孤立运行态表。

## 5. Protocol Profile

```rust
struct McpProtocolPolicyDocument {
    offered_versions: Vec<ExactProtocolVersion>,
    transport_features: McpTransportFeatures,
    client_capabilities: ClosedClientCapabilities,
    allowed_server_capabilities: ClosedServerCapabilities,
    experimental_features: BTreeSet<ExperimentalFeature>,
    method_limits: BTreeMap<McpMethod, MethodLimits>,
    metadata_policy: McpMetadataPolicy,
    profile_digest: Digest,
}
```

该document只能作为04 `PolicyRevision(kind=Protocol)`的closed body发布，外部引用使用其`prev` ID。MCP Host不拥有
第二套profile revision lifecycle/table。

- offered version 是 exact allowlist，不含 range/`latest`；
- server 选择未支持版本时初始化失败；
- 未知 capability/experimental namespace 被记录但不可调用，除非新 profile 显式允许；
- Tasks 必须同时满足 profile feature flag、server negotiation、Capability Implementation 声明和 tenant policy；
- client capabilities 使用最小披露，不能为了 discovery 宣告运行时不会兑现的功能；
- profile 固定 method、message、metadata、progress、pagination、notification 和 nesting 上限。

## 6. Transport

### 6.1 Streamable HTTP

- endpoint 必须是 canonical HTTPS URI，开发例外由独立 policy 明确；
- HTTP POST/GET、Accept、MCP protocol version、session header 和 SSE 按固定 profile 实现；
- redirect 默认禁止；允许时只能同源、HTTPS、固定 hop count 并重新执行 SSRF/TLS/auth 检查；
- DNS 每次新连接通过 egress resolver，防止 rebinding、link-local、metadata 和私网越界；
- response/request bytes、header count、SSE event、idle time 和连接寿命有硬限制；
- session ID 视为 credential-like opaque value，加密保存且不进入日志/事件；
- 连接断开不是 Invocation 结果，必须回 durable state 判断 retry/reconcile。

### 6.2 Managed stdio

Managed stdio 不是 Host 内的 `Command::spawn`：

```text
Capability / MCP subscription durable owner
  -> Sandbox Gateway (same physical Job for an operation)
Sandbox Executor
  -> trusted Managed MCP protocol adapter
  -> authenticated brokered byte stream
  -> signed immutable server package in a fresh microVM
```

- package/image/entrypoint/dependency lock 必须是 exact digest；
- Runner 使用 14 的进程、文件、网络、资源和生命周期隔离；
- stdout 只允许 MCP frame，stderr 是 bounded private diagnostic；
- Host 只看协议 byte stream，不获得 Runner namespace、PID、socket 或文件路径；
- 任意 tenant-uploaded executable 必须使用 MicroVm isolation，不得降级到 Host Pod；
- Runner kill、resource exhaustion 或 protocol contamination 映射为 session/backend failure。

`managed_stdio`不能沿用`CapabilityRemote`的直接transport调用：operation admission必须从exact MCP Deployment识别该transport，
直接建立唯一`work_class=sandbox`物理Job并释放Capability/MCP Worker permit。Sandbox request冻结Capability Interface/Implementation、
MCP Deployment/Discovery/Authorization、operation/continuation以及Package/Runtime/Profile/Policy的完整closed closure；
WorkerProcessGeneration与lease只由Sandbox claim绑定。trusted protocol adapter可以复用本规范的Host state machine，但只能运行在
Sandbox Execution Plane，且只能令microVM provider创建server process；API、Capability Worker、MCP Worker和普通Host Pod均不得
spawn、持有PID或访问VMM/runtime socket。

Resource subscription的逻辑Job与物理session确有两个不同生命周期：`work_class=mcp` Job唯一拥有binding、session generation、
notification/reconcile和等待状态；每个generation最多关联一个`work_class=sandbox` Job，后者唯一拥有microVM lease、process、resource、
cleanup和terminal evidence。prepared→durable Ready→activation必须同时回绑两个Job及generation；旧Sandbox Job未terminal或未取得
process absence/node quarantine证明时不得创建replacement。两者都复用shared Job/Event/Receipt/Outbox，不增加MCP或Sandbox专用表。

不支持 deprecated HTTP+SSE fallback；需要旧服务时先部署受控 protocol gateway 并发布新的 Server Revision。

## 7. Session 状态机

```rust
enum McpSessionState {
    Disconnected,
    Connecting,
    Initializing,
    Ready,
    ReauthRequired,
    Degraded,
    Draining,
    Closed,
    Failed,
}
```

```text
Disconnected -> Connecting
Connecting -> Initializing | ReauthRequired | Failed
Initializing -> Ready | ReauthRequired | Failed
Ready -> Degraded | ReauthRequired | Draining | Failed
Degraded -> Ready | ReauthRequired | Draining | Failed
ReauthRequired -> Connecting | Draining | Failed
Draining -> Closed | Failed
```

Session 是可重建 transport 状态，不是 Run/Invocation 权威。Host restart 后根据 pending durable work 创建新
session；不能假设 remote session continuity。Server 要求 session affinity 而 session 丢失时，Tool Effect 决定
safe retry 或 reconciliation，Resource read 可以新 session 重试。

## 8. Discovery Snapshot

Discovery 顺序固定为 initialize/negotiation 后，在 bounded pagination 下读取允许的 tools/resources/templates/
prompts 和 server metadata。结果写入：

```rust
struct McpDiscoverySnapshot {
    snapshot_id: DiscoverySnapshotId,
    mcp_deployment_id: DeploymentId,
    server_revision_id: RevisionId,
    protocol_profile_revision_id: RevisionId,
    authorization_context_digest: Digest,
    negotiated_version: ExactProtocolVersion,
    negotiated_capabilities: ClosedNegotiatedCapabilities,
    objects_artifact_id: ArtifactId,
    objects_digest: Digest,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}
```

- authorization context digest 包含 identity class/scope，不含 token；
- list changed notification 只触发新 discovery candidate，不改写 snapshot；
- tool/resource/prompt 描述、schema、annotations、icons 和 URI 都是不受信任 candidate；
- Snapshot 只有 validation evidence，不能直接成为模型可见对象；
- Operator 选择 candidate、补齐平台 Effect/schema/policy、通过 conformance 后发布投影 Revision；
- 同一 server 对不同 principal/scope 返回不同目录时，不跨 authorization context 复用 snapshot。

DiscoverySnapshot是exact typed resource，不能只把`mdsc`和digest塞进Implementation Artifact。它的typed source、
Artifact owner、durable discovery operation以及Tool/Resource projection强外键在Phase 4同批交付；在此之前exact registry
识别该kind但拒绝无source projection，MCP-backed Capability/Context Implementation也保持不可绑定。这样不会为了提前
创建Snapshot表而产生无Operation owner的Artifact或弱文本引用。

## 9. Tool 投影

MCP Tool 只实现已发布 Capability Interface：

```rust
struct McpToolBackend {
    mcp_deployment_id: DeploymentId,
    discovery_snapshot_id: DiscoverySnapshotId,
    remote_tool_name: String,
    remote_schema_digest: Digest,
    protocol_profile_revision_id: RevisionId,
    supports_task: bool,
    supports_progress: bool,
}
```

- MCP input schema 必须可安全映射到平台 closed schema；不兼容时不能 publish；
- MCP 没有可信 output schema 时，Operator 必须定义 bounded platform output mapping/schema；
- annotations 只能作为 evidence，不能降低平台 Effect、Approval、Idempotency 或 data classification；
- Model 看到平台审核 tool name/description，不看到 remote endpoint/backend/auth；
- `tools/call` 永远由 durable CapabilityInvocation 发起；
- text、image、audio、resource links、embedded resources 和 structured content 逐类规范化并走 Artifact/Context policy；
- remote error/data 不直接成为 public Failure 或 system instruction。

## 10. Resource 投影

MCP Resource/Resource Template 投影为 Context Implementation：

- exact MCP Deployment、Snapshot、resource identity/template、URI policy和schema digest固定；
- `resources/list` 只用于 discovery，`resources/read` 只由 ContextQuery 执行；
- 模型不能直接提供任意 URI；URI 由 typed template parameters 编译并验证 scheme/host/path/length；
- Resource 内容形成 ExternalObservation 或受控 ingest Dataset Generation；
- Resource subscription 只产生 invalidation hint，不把 notification 当成可信新正文；
- remote MIME/size/annotation 需重新 sniff/validate；
- MCP URI 只作为 opaque citation locator，不授予客户端直接网络访问。

## 11. Prompt 投影

MCP Prompt只能成为“不受信任候选Prompt Artifact”：

- discovery/get 与参数 schema 固定到 Snapshot；
- Prompt content 经过 size、media、role 和 injection boundary validation；
- remote `system` role 不映射为平台 system priority，统一降为外部 untrusted content；
- Prompt 不自动成为 Skill，也不自动注入 Agent；
- Operator可以将其Artifact/digest显式引用进Agent/Skill Draft，再发布新的owner Revision；
- list changed不改变已经发布的Agent/Skill Revision内Prompt Asset；
- Prompt 中的 Tool、URL、Secret 请求和模型选择建议都没有授权效力。

首版没有独立PromptAsset Entity、Revision、Head、ID或API。Canonical类型完全复用05的`PromptAssetRef`；只有Agent/Skill
owner Revision发布事务能创建，MCP Host只能交付候选Artifact与discovery evidence。

## 12. Experimental Tasks

MCP `2025-11-25` Tasks 作为显式 experimental profile 映射到 10 的 Deferred continuation：

```text
CapabilityInvocation InFlight
 -> tools/call returns remote Task
 -> save opaque task handle + auth/session context + poll contract
 -> Invocation Deferred
 -> tasks/get / tasks/result / tasks/cancel
 -> committed terminal outcome
```

- Task ID 只在加密 backend state 中保存，不进入 public event、Model output 或 Artifact name；
- handle绑定exact MCP Deployment、authorization context、Invocation、attempt generation和deadline；
- poll interval/TTL 只能在平台 min/max 内采用，server 建议不能形成 busy loop；
- task list 不用于恢复或跨 Invocation 发现未知任务；平台只访问自己已绑定的 task ID；
- `input_required` 映射到 Invocation AwaitingInput，并保持 related-task association；
- remote terminal status 仍需 output schema、Effect 和 Artifact validation；
- Tasks profile 变化必须新 Implementation Revision；
- experimental feature 可被全局 kill switch 关闭，未决写 Effect 进入 reconciliation。

## 13. Elicitation

Elicitation 是 server-to-client interaction，不是远端授权决定：

- 每个请求必须关联当前 MCP request/Task/Invocation，禁止 standalone server-initiated interaction；
- Form mode schema 映射为平台 BackendInputRequest，平台决定 principal、公开文案、classification 和 deadline；
- Form mode 禁止密码、API key、access token、支付凭据和 SecretPurpose value；
- 用户必须能 accept、decline 或 cancel，response 经过 closed schema 和 generation fence；
- URL mode 必须显示完整 HTTPS URL/突出 origin，并在用户明确同意后由安全外部浏览器打开；
- URL 不预取、不携带平台 token、不允许预认证 bearer link、不在嵌入式页面读取用户输入；
- completion 必须绑定启动 interaction 的同一 tenant/principal/session；
- server 请求任意 approver、隐藏域或权限扩张时 fail closed。

平台 Approval 与 MCP Elicitation 是两个概念：Approval 在调用前授权 Effect；Elicitation 在已授权调用中补充
输入，不能替代 Approval。

## 14. Sampling、Roots、Logging 与 Completion

### 14.1 Sampling

Server-initiated Sampling 默认不宣告。启用时：

- 创建真实ModelTurn，受Agent/tenant Model policy、budget、deadline、classification和audit控制；MCP operation只保存
  association，不定义第二种模型调用对象或状态机；
- server 提供的 prompt/messages 都是不受信任输入；
- model preference 只是 hint，平台选择固定 Provider binding；
- `includeContext` 一律为 none，Context 必须显式通过已授权内容提供；
- Sampling tools 默认禁止；若单独启用，只能投影 Invocation 固定 allowlist，限制递归深度/总调用/Effect；
- 高风险 sampling 需要 human approval，server 不能看到 Provider Secret；
- 结果输出经过 data-flow policy 后才回传 server。

### 14.2 Roots

Roots 默认不宣告。若某个已发布实现需要 root，只暴露本次 Invocation ArtifactGrant 对应的虚拟只读 URI；绝不
暴露宿主目录、workspace、tenant bucket、Secret mount 或其他 Run 文件。

### 14.3 Logging 与 Completion

MCP logging 是 bounded private diagnostic，按 level/rate/bytes 过滤，不进入 public Run event。Completion 只用于
authoring UI 的不受信任建议，不参与 runtime 路由或授权。Icon 必须经 Artifact 安全下载、同源检查、media sniff
和主动内容清理；Host 不携带 credential 抓取 icon。

## 15. OAuth 与 Credential

```rust
struct McpAuthorizationBinding {
    authorization_binding_id: AuthorizationBindingId,
    tenant_id: TenantId,
    mcp_deployment_id: DeploymentId,
    principal_binding: AuthorizationPrincipalBinding,
    audience: CanonicalServerIdentity,
    granted_scopes: BTreeSet<Scope>,
    token_secret_binding: ExactSecretBindingRef,
    expires_at: DateTime<Utc>,
    generation: u64,
}
```

- Streamable HTTP OAuth 使用 Authorization Code + PKCE、state/nonce、固定平台 redirect URI；
- Protected Resource Metadata、Authorization Server Metadata/OIDC discovery 经过 SSRF/TLS/issuer allow policy；
- authorization/token request 使用 canonical MCP resource indicator，token 必须 audience-bound；
- access/refresh token只在Secret Manager，数据库保存AuthorizationBinding自己的exact SecretBinding与非敏感metadata；该引用
  不属于也不要求出现在MCP Deployment的部署级Secret集合；
- token 不出现在 query string、session ID、日志、Artifact、outbox、callback 或模型；
- 平台 MCP token 不能传给下游其他 API；MCP server 的 upstream credential 与 inbound token 必须分离；
- per-user binding 不能被其他 principal/session 复用；service identity 必须显式声明并限制数据语义；
- incremental scope challenge 创建新的 consent task，不能自动接受扩大 scope；
- DCR/Client ID Metadata 只有在 auth profile 允许且 redirect/issuer 验证后使用，不接受任意客户端注册脚本；
- revoke/expiry 提升 generation，连接和未开始调用立即失效。
- OAuth token SecretBinding只允许`Pinned` resolution。refresh/reauthorize在同一持久化command中替换为同一binding的更高
  Secret generation（或由明确的新grant command建立新binding）并提升AuthorizationBinding generation；不得用
  `FollowProviderRotation`让principal token在authorization generation不变时静默漂移。
- exact Auth Profile分别冻结`pkce_secret_provider_id`与`token_secret_provider_id`。它们必须是CandidateManifest已安装的
  exact Provider identity，并进入对应preparation digest；运行时不得按purpose或“默认Provider”重新选择。
- 当前生产OAuth profile只接受signed JWT access token并要求`openid`与ID token。CandidateManifest必须按exact Auth Policy
  revision安装完整Auth Profile、canonical public JWKS digest和严格递增的`kid`集合；算法allowlist只允许`EdDSA`、`ES256`
  或`RS256`，不得接受共享密钥、`none`、远端算法降级或同时声明冲突的JWK `use`/`key_ops`。该catalog必须在readiness时
  完整验证，不能在收到one-time authorization code后临时发现或下载信任根。
- Egress在兑换后本地验证access token的`typ/alg/kid/iss/aud/sub/iat/exp/scope`及ID token的
  `typ/alg/kid/iss/aud/sub/iat/exp/nonce`；两个token的subject必须相同，nonce的domain-separated digest必须与Task冻结值
  完全一致。验证证据绑定exact Auth Policy、JWKS digest、两个token的敏感domain digest、算法、kid与subject digest，
  但不得包含token正文。

OAuth授权开始时创建共享`ExternalAuthorization` Task。Task冻结exact MCP Deployment、AuthorizationBinding ID、principal binding
generation、audience、请求scope、fixed callback binding、state/nonce digest以及保存PKCE verifier的Pinned exact SecretBinding；raw
state、nonce、verifier、authorization code和token不得进入Task、Receipt或Event。callback ingress只能从已认证的固定redirect解封得到
tenant/Task identity，并把authorization code交给credential broker在事务外兑换；broker先把token写入Secret Manager，只把Pinned
`ExactSecretBindingRef`、scope/audience/issuer/subject verification digest和expiry交给repository。

授权开始application service必须先从PostgreSQL authority解析exact Deployment/Auth Profile并验证permission、principal binding
generation、callback、resource和scope；随后由独立Egress bulkhead生成至少256-bit PKCE verifier与nonce，并使用平台AEAD issuer生成state。
Egress以绑定tenant/Task/AuthorizationBinding/Deployment/请求digest/callback/deadline的stable preparation digest调用Secret Manager
`prepare-or-load`：首次创建短TTL entry，重试返回byte-identical state、nonce、verifier与同一Pinned exact SecretBinding。普通Secret
resolver通过该binding只能取得verifier；state/nonce只是prepare-or-load的transient metadata。Host只接收state、nonce、S256 challenge和
exact binding，按固定顺序构造包含`response_type=code`、client ID、fixed redirect、scope、state、nonce、`code_challenge_method=S256`
与canonical resource indicator的authorization URL，再提交Task。提交事务重新验证全部authority；loser或提交不确定留下的entry必须由
deadline TTL/GC清理，不能把raw值补存到PostgreSQL实现重试。

callback repository command以`(Task generation, Task version, state digest, callback binding digest)`做first-winner，使用`Callback`
Receipt去重；成功时在一个PostgreSQL事务内终结Task、创建或CAS更新AuthorizationBinding、写Event/Outbox并终结Receipt。返回scope必须
是请求scope的子集，audience和token purpose必须与Task/Server closure精确一致。不同idempotency key的迟到callback稳定写为
`rejected_stale`且不得替换winner；事务失败或loser留下的prepared Secret Manager entry由短TTL/GC回收，不得通过放宽数据库原子性
解决。

token兑换必须使用stable token preparation identity：至少绑定tenant、Task generation/version、AuthorizationBinding、exact MCP
Deployment、state digest、authorization-code的domain-separated digest、token purpose、exact token Provider、请求scope、audience、issuer
与Task deadline。Egress在任何DNS、PKCE/client Secret解析或token endpoint调用前先向Broker执行`load-prepared`；已有winner时只重放其
credential-free exact binding与验证metadata，不得再次消费one-time authorization code。不存在winner时才兑换并验证token，再调用同一
Provider的`prepare-or-load`。外部Provider写入是prepared winner；Broker随后用KMS/AEAD封装opaque reference，并以preparation digest调用
受信ServiceIdentity authority，把generation 1、Pinned的SecretBinding登记到现有聚合。登记与Receipt/Event/Outbox原子；Event/Receipt不得
包含ciphertext、key ID、opaque reference或token。Provider成功但数据库响应丢失时，重试必须load同一winner并修复登记；metadata/provider/
version/storage evidence任一漂移均fail closed。该协议不新增OAuth token表或第二current authority。

critical-control safety scan必须按tenant和数据库时间有界领取已到期的pending `ExternalAuthorization` Task，并以Task
generation/version first-winner终结；同一事务写内部Event/Outbox，携带且只携带PKCE SecretBinding ID/generation的清理提示，不能携带
Secret ref/value、raw verifier、state、nonce或authorization code。Secret Manager cleanup consumer必须再次验证exact binding/generation；
重复、迟到或已被callback解决的Task不得删除winner使用的credential。

AuthorizationBinding状态机固定为：`Active -> ReauthRequired | Revoked | Expired`，
`ReauthRequired -> Active | Revoked | Expired`，`Revoked/Expired`为终态。每次迁移必须使用generation/ETag CAS并更换
audit request；只有`ReauthRequired -> Active`可以在同一事务替换Principal binding generation、scope closure、
exact token SecretBinding和expiry，其他状态边必须保持这些credential字段不变。恢复Active前重新验证exact Deployment、
Auth Profile、Principal与Secret当前可绑定。

## 16. Connection Pool 与授权隔离

Pool key 至少包含：

```text
tenant
MCP deployment
protocol profile
authorization binding generation
principal/service identity class
scope digest
network/TLS policy
```

per-user session 不跨 principal 复用。只有明确无用户状态、使用同一 service credential 且 server conformance
证明 request isolation 时才允许 multiplex。每个连接有 max in-flight、session age、idle timeout 和 response
byte budget；每 host/tenant/server 的 permit 独立。Draining 连接不接新 request。

## 17. Request/Response 机器合同

MCP Host 接收平台内部 envelope：

```rust
struct McpOperationRequest {
    mcp_operation_id: McpOperationId,
    tenant_id: TenantId,
    mcp_deployment_id: DeploymentId,
    snapshot_id: DiscoverySnapshotId,
    authorization_binding_id: AuthorizationBindingId,
    method: PublishedMcpMethod,
    params: ClosedJsonValue,
    deadline: DateTime<Utc>,
    job_id: JobId,
    lease_generation: u64,
}

enum McpOperationOutcome {
    Completed(BoundedMcpResult),
    RemoteTask(OpaqueRemoteTask),
    InputRequired(BoundedElicitation),
    ReauthorizationRequired(ReauthChallenge),
    RetryableFailure(SafeMcpFailure),
    PermanentFailure(SafeMcpFailure),
    Uncertain(SafeMcpUncertainty),
}
```

内部 envelope 不是原始 JSON-RPC pass-through。Host 从 published projection 生成 wire method/params；未知 method、
metadata、server request 或 result variant fail closed。Host 不能直接更新 Run/Invocation 表，只返回带 attempt/fence
的 outcome。

## 18. Subscription 与通知

- 只对 published Resource binding 和 profile 允许的 URI 建立 subscription；
- subscription identity绑定tenant、principal auth generation、exact MCP Deployment和exact resource；
- `resources/updated`/list changed/tool changed/prompt changed 都只是 invalidation/discovery wake；update URI 可以是订阅根的
  sub-resource，Host 只保留其digest作为evidence，下游仍重新读取exact published binding root；
- 通知正文不直接进入 Context、Registry 或模型；
- Host 对 notification 做 method/size/rate/session 验证后写 bounded inbox receipt；
- consumer 从 durable binding 重新 read/discover，重复/乱序 notification 按 event key/generation 去重；
- session 断开后 subscription 可重建，但不能假设 gap-free；要求完整性的来源必须周期 reconcile；
- notification storm 受独立队列/permit/backpressure，不能耗尽 Tool 请求容量。

## 19. 所有权接口

```rust
trait McpHostClient {
    async fn execute(&self, request: McpOperationRequest) -> McpOperationOutcome;
    async fn cancel(&self, request: McpCancelRequest) -> McpCancelOutcome;
    async fn discover(&self, request: McpDiscoveryRequest) -> McpDiscoveryCandidate;
}

trait McpSessionStore {
    async fn save_session(&self, command: SaveMcpSession) -> SessionReceipt;
    async fn load_pending(&self, server: ServerBindingKey) -> Vec<PendingMcpOperation>;
}
```

`ServerBindingKey`是tenant + exact MCP Deployment + authorization binding + principal scope的复合键，不是
Server Revision或名称；任何一项不同都不能复用session。

MCP wire crate 不依赖 Agent/Plan。Capability/Context adapter 负责把平台 typed request 映射到
PublishedMcpMethod；Host 负责 transport/protocol；repository 负责 durable state 和 first-winner。

## 20. Persistence 与 Artifact 映射

MCP Server/Profile/Projection 使用共享 Resource/ResourceVersion/Deployment。Discovery、wire operation、remote task 与 poll
分别映射为 Invocation/Job；OAuth/elicitation 等人机等待使用 Task，callback/notification 去重使用 Receipt，session、cursor、
authorization generation 与 opaque remote handle 保存在 bounded typed payload 中。原始 discovery/result/diagnostic 使用
encrypted short-retention Artifact。Secret value、OAuth token、session header 与响应正文不进入普通列。

## 21. 不变量

- MCP wire 对象必须通过显式投影，不能绕过 Capability/Context/Prompt/Invocation 合同；
- 每次 runtime operation 固定 Server/Profile/Snapshot/Auth/Projection Revision；
- discovery/list changed 永不自动 publish/activate；
- MCP annotation/schema/description 不具有平台授权效力；
- Host 不执行 server code，不访问宿主文件，不保存明文 token；
- Tool Effect 不确定时按 Capability reconciliation，不能因 session 重连自动重放；
- Resource/Prompt content 始终是不受信任输入；
- Task、session、cursor 和 remote handle 不能跨 tenant/principal/auth generation；
- server-initiated Sampling/Elicitation/Roots 只能在显式 profile 和 originating request 内发生；
- transport 快路径、SSE 或 notification 都不拥有 durable authority。

## 22. 幂等、并发与背压

- operation 使用稳定 Invocation/ContextQuery-derived idempotency key，JSON-RPC request ID 仅为 transport identity；
- Tool retry 受 10 的 Effect/idempotency intersection，Host 不自行重试可能产生 Effect 的 request；
- discovery、tool call、resource read、task poll、notification、sampling 使用独立 sub-permit；
- 每 tenant/server/host/auth binding 有 in-flight、connection、queue、task 和 subscription 上限；
- Deferred Task 释放 connection request permit，只保留 durable poll deadline；
- server `Retry-After`/poll interval 受平台上下限和 jitter；
- notification/log/progress 是可丢弃或合并的 bounded observation，不阻塞 control work；
- circuit state 按 exact server/auth/operation class 隔离，不自动移动 Registry head。

## 23. 超时、重试、取消与恢复

- connect/initialize/request/idle/task/total deadline 分离，后者不能被心跳无限延长；
- read-only Resource 在未得到 committed result 时可以新 session 重试；
- Tool 在 dispatch 后断线按 Effect/Idempotency 返回 retryable 或 uncertain；
- JSON-RPC cancel/Task cancel 是 best-effort，remote cancelled 不证明此前 Effect 未发生；
- Host crash 后从 PostgreSQL pending operation/task/subscription 恢复，不依赖内存 session；
- auth expiry 进入 ReauthRequired，用户授权完成后提升 generation 并创建新 session；
- response、elicitation、notification 和 Task result 必须通过 operation/generation/attempt fence；
- NATS 丢失由 safety scan 发现 pending task/poll/reauth/discovery；
- server schema 漂移导致新 response 不兼容时失败并触发 discovery/suspension，不动态接受新 shape。

## 24. 安全与租户

- endpoint、metadata URL、issuer、icon、elicitation URL、resource URI 全部执行 SSRF/DNS/TLS/redirect policy；
- Host egress只能到exact MCP Deployment的endpoint/network/auth policy允许目标，不能访问cloud metadata、Kubernetes API
  或未授权私网；
- tenant/principal/auth generation 写入所有 operation、session、task、subscription 和 cache key；
- per-user OAuth connection 不跨用户 multiplex；
- remote error、Prompt、Resource、Tool output 和 logs 经过 size/media/schema/redaction；
- Elicitation 防 phishing，Sampling 防资源/权限递归，Roots 防文件泄露；
- stdio Runner 使用 14 隔离，无 hostPath、Docker socket、service account token 或共享 writable volume；
- MCP server suspension、credential revoke 和 network kill switch 阻止新 operation；
- authorization failure 使用不可区分错误，防止 server/principal/scope 枚举。

## 25. 可观测性与隐私

```text
mcp_operations_total{method_class,outcome,transport}
mcp_operation_duration_seconds{method_class,outcome}
mcp_sessions_active{transport,state}
mcp_connect_total{transport,outcome}
mcp_remote_tasks_active{state}
mcp_notifications_total{class,outcome}
mcp_reauth_total{outcome}
mcp_protocol_violation_total{class}
```

server/tenant/tool/resource/URI/principal/scope 不进入 metric label。Trace 记录受控 binding hash、protocol version、
method class、bytes、latency、attempt 和 outcome；不记录 params/result/token/session/task ID。审计覆盖 server
publish、snapshot approval、OAuth grant/revoke、sampling/elicitation consent 和 suspension。

## 26. 配置与部署

- MCP Host 是独立 Worker Deployment、连接池、DB pool、NATS consumer 和 autoscaling target；
- Managed MCP Runner 位于 Sandbox node pool，与 Host Pod 物理隔离；
- Host readiness 依赖 PostgreSQL、Secret resolver 和至少一个可用 execution slot，不依赖任一远端 server；
- 单 server/circuit 失败不能使整个 Host unready；
- runtime image固定SDK/wire implementation digest，protocol profile数据来自immutable Policy Revision；
- rolling deploy 先 drain sessions/request，再由新实例恢复 durable work；
- platform hard limits 只能被 server/tenant profile 收紧。

## 27. 测试矩阵与验收标准

- 官方 `2025-11-25` initialize/transport/schema positive/negative fixture 全部通过；
- 未支持版本、capability、experimental feature、method 和 result variant fail closed；
- discovery change 不会自动改变已发布 Tool/Resource/Prompt projection；
- Streamable HTTP redirect、DNS rebinding、metadata IP、oversized SSE 和 invalid session 被拒绝；
- stdio server 只能在 Managed Runner 启动，kill Runner 不影响 Host/API 并可恢复；
- Tool 断线按 Effect safe retry/reconcile，非幂等操作不自动重放；
- experimental Task poll/callback/input/cancel/timeout 竞态只有一个 Invocation outcome；
- Resource subscription storm 被合并且不会耗尽 Tool/control permit；
- Form Elicitation 无法索取 Secret，URL mode 无 consent 不打开且不预取；
- Sampling/Roots 默认不宣告，启用后受 budget/depth/grant 限制；
- OAuth token audience、resource、principal 和 generation 隔离，token passthrough fixture 被拒绝；
- authorization-start并发只有一个Task winner；exact retry返回同一URL材料，state/nonce/verifier canary不进入Task/Receipt/Event/Outbox；
- Secret/session/task/content canary 不进入公共事件、metric、默认日志或错误。

### 27.1 当前实施证据边界（非规范性）

CR-131已经交付exact execution/discovery resolver、Streamable HTTP与Managed stdio broker边界、typed transport failure、
discovery worker、cancel/retry/expired-lease recovery和PostgreSQL safety scan。持久化继续复用共享Invocation/Job/Receipt/Event/
Outbox与Resource/ArtifactLink；MCP Job是closed tagged payload，未增加表或migration。Host transport现传递04的
`ExactSecretBindingRef`与已解析generation，不再把policy缩减为Binding ID。OAuth callback contract/repository首片同时交付shared
ExternalAuthorization Task、Callback Receipt first-winner、AuthorizationBinding原子创建/更新，以及按数据库时间有界领取Task的
expiry safety scan；过期winner写只含PKCE binding ID/generation的内部cleanup hint。固定redirect的strict callback ingress现先认证state，
再解析exact Task/Deployment/Auth Profile并仅在全部issuer/redirect/resource/scope绑定成立后把raw code交给独立Egress。Egress生产HTTPS
broker固定token endpoint，执行DNS全量public-IP校验/连接pinning、HTTPS-only/no-proxy/no-redirect、late PKCE/client Secret resolution、
closed duplicate-safe token response、token verifier/prepared Secret Manager端口与独立in-flight bulkhead，Host/repository只接收
credential-free grant。terminal Event中的cleanup hint通过closed type限制为PKCE SecretBinding ID/generation；cleanup consumer从可信Event
envelope取得tenant/Task/cause，PostgreSQL再次验证terminal Task与exact Pinned binding，Egress exact-version delete adapter拒绝错误purpose、
rotation policy或stale authority。contracts 62项、Egress 21项、Platform API 3项、Task 2项unit及相应targeted gate通过。
callback state现由AES-256-GCM codec密文封装tenant/Task identity，固定callback digest进入AEAD audience；active key与最多4个verification
key支持有界rotation，TTL、clock skew、key ID与token长度均受hard limit，篡改、未知key、错误callback和过期不会进入broker或repository。
authorization-start application service与Egress preparation broker也已交付：PostgreSQL先解析exact Auth Profile，Egress在独立bulkhead
生成256-bit verifier/nonce、调用AEAD state issuer并通过Secret Manager `prepare-or-load`返回稳定材料，Host构造canonical S256 authorization
URL后才提交Task；commit再次验证permission、principal generation、Deployment/Auth Profile与exact PKCE binding。fixture覆盖URL字段/脱敏、
幂等challenge、错误store identity及饱和前拒绝；fresh PostgreSQL first-winner/replay/Secret canary fixture已在全新PostgreSQL 16数据库
实际执行通过。
独立`insight-platform-api`候选transport只在固定`/v1/mcp/oauth/callback`接受GET、空body和最多8192-byte raw query；响应不反射
state/code或内部错误，强制no-store、no-referrer与closed CSP，错误class映射为静态400/503/202。该path现已进入Rust generator拥有的
target OpenAPI和manifest digest，独立checker限制当前只能暴露这一条reviewed path；4项Axum fixture通过。router仍未接入可部署进程，
且OpenAPI保持`implementing-not-current`，不能据此声明public callback已上线。

durable Resource subscription现已复用shared Invocation/Job/Receipt/Event/Outbox交付：exact binding同时冻结tenant/principal/auth generation、
Deployment/Discovery/Profile、transport closure、published Context Deployment与canonical credential-free URI；session opaque state只允许加密保存。
严格notification ingress拒绝duplicate/unknown字段、错误method/session/generation和越界正文，并使用独立permit与keyed rate authority；同一pending
窗口只产生一次durable wake，后续更新合并为最高generation。fenced subscription worker按`Connecting -> Initializing -> Ready`提交session；
成功或终止phase的evidence digest同时进入请求摘要、Receipt和Event。该Worker只允许Streamable HTTP connector建立订阅；Managed stdio
在到达该transport port前必须转交Sandbox admission，Host-local Worker即使收到声称为Managed stdio的transport也会在dispatch前拒绝。
notification/周期reconcile必须先取得下游Context/Discovery
durable acceptance，随后才清除pending并把Job停回Waiting。按tenant和数据库时间的bounded safety scan使用独立critical-control permit唤醒
长期未更新的Waiting Job。显式session-loss报告与expired lease/session safety scan会以version/generation/CAS first-winner清除旧加密opaque state、
把同一Job重排到Ready并设置`full_reconcile_required`；重建的新generation在下游完整reconcile取得durable acceptance前不得回到Waiting。
此前subscription PostgreSQL fixture已扩展
exact resolver、session阶段、并发coalescing、downstream evidence、周期scan/wake/reconcile、session-loss/expired-lease rebuild和Secret
canary，并已在全新PostgreSQL 16数据库实际执行通过。上述实现不增加表或migration。

生产Streamable HTTP subscription connector现已复用独立Egress的exact catalog、DNS公网验证与pinning、no-proxy/no-redirect和late Pinned
token resolution，并按`2025-11-25`完成`initialize`、`notifications/initialized`、`resources/subscribe`与独立GET/SSE。建立阶段只返回
AEAD加密、绑定tenant/Deployment/Auth/binding/session generation的prepared handle；Host先durable commit Ready，再通过不可失败activation发布
后台stream，避免通知早于session authority。GET发送敏感session header，按SSE ID使用`Last-Event-ID`恢复，并约束event/idle/session/reconnect/
event-count；有ID redelivery保持稳定去重，无ID data在断线后触发full reconcile。Host ingress生成UUIDv7 Receipt/Event/Outbox identity并把断线
绑定exact auth/session/Worker generation交给PostgreSQL，后者原子清除opaque state、标记full reconcile并把同一Job重排Ready。MCP允许的
sub-resource update只保留URI digest作为evidence，下游始终重新读取exact published root。Host 56项与Egress MCP 18项定向unit、相关check和
strict Clippy通过；扩展后的PostgreSQL fixture已编译，但本轮Docker daemon无响应，尚未取得该新路径的fresh PostgreSQL实际执行证据。

同步及experimental Task-aware Streamable HTTP operation现在有生产Egress connector：只按process-installed exact Deployment catalog选择HTTPS endpoint，重验
Protocol/Network/TLS/Trust/Auth Policy与Pinned token purpose，全部DNS答案必须为公网地址并固定到当前连接；reqwest强制HTTPS-only、无代理、
无重定向，token只在Egress内late resolve并作为sensitive Bearer header使用。connector按`2025-11-25`执行新的`initialize`、
`notifications/initialized`与目标method POST，冻结版本、client capability和Discovery negotiated capability；初始化结果出现任一capability drift
即fail closed。JSON与SSE响应都执行bounded body/header/event/idle/initialize/request timeout及strict duplicate-safe JSON-RPC验证，session header只在
当前交换内以敏感内存值存在，401/403只向Host返回challenge digest。Task-aware `tools/call`只有在profile、Discovery与tool contract共同允许时才附加
`task.ttl`；返回的task/session使用AES-256-GCM active/verification keyring封装，并绑定tenant、exact Deployment/Auth generation、Invocation/Job、
physical attempt、Discovery/Profile与deadline。持久层只复用Capability Job payload保存密文、key reference digest、plaintext digest和稳定远端identity；
poll恢复原session，只调用已绑定task ID的`tasks/get`/`tasks/result`并强制related-task一致。poll间隔被上下限夹紧，Host消除最小间隔的时钟竞态，
transient failure保留原handle；次数耗尽时read明确失败，write进入reconciliation并保留密文handle。Task取消复用同一AEAD-bound task/session，
按协商的`tasks/cancel`与独立method limits执行；只有同一task ID的`cancelled`结果才确认accepted，任意错误或未知状态继续保守记录取消观察且不构成
no-effect proof。Egress 40项、Host 54项、Capability adapter
13项unit及fresh PostgreSQL 16 Capability fixture实际通过，覆盖篡改/错误binding/未知key、Task completion、密文claim/wake/resume、同一Job/attempt与
Input RunValue恢复；仍为23表/单一`0001`。远端Task进入`input_required`时，`tasks/result`现在在冻结上限内跳过
`notifications/progress`，只接受`_meta["io.modelcontextprotocol/related-task"].taskId`精确匹配的`elicitation/create`。form schema经closed、
bounded、non-secret profile转换为共享Interaction Task并固定eligible principal；`accept | decline | cancel`都以Task generation first-winner恢复
同一Capability Job、physical attempt、MCP session和remote task。发送后结果不确定会保留exact action/response及remote-state digest，禁止丢失用户决定或
开启新attempt。扩展后Egress 43项、Host 54项、Capability adapter 14项、Contracts 64项和Invocation 9项unit及strict Clippy通过；PostgreSQL
repository测试完成编译，但最新扩展尚未重新取得fresh PostgreSQL 16执行证据，不能借用此前fixture声明完成。该证据不等于subscription或
real-process资格。

CR-144现已交付OAuth写入组合合同与内核：exact Auth Profile分别选择PKCE/token Provider；token preparation在兑换前先load prepared winner；
provider write、KMS sealing、现有SecretBinding登记及Receipt/Event/Outbox通过可信ServiceIdentity串接，数据库响应丢失可由同一winner修复，
exact-version delete也复用current authority重验。9项Secret Broker、10项OAuth定向Egress与14项Host OAuth测试通过；新增PostgreSQL
authority fixture已完成编译，但本轮本机Docker daemon无响应，未登记fresh PG实际运行证据。该实现不增表或migration。

AWS KMS/Secrets Manager Provider adapter与生产token verifier现已进入独立Egress进程组合。verifier只使用CandidateManifest安装的
exact Auth Policy/JWKS catalog，启动时校验canonical digest、asymmetric算法、严格`kid`顺序与JWK签名用途；兑换前先确认exact catalog
可用，兑换后本地校验signed access token、ID token、共同subject和Task nonce。MCP Host通过新增的两个closed internal gRPC method请求
authorization-code exchange与exact PKCE delete；raw code只存在于digest-bound payload，返回只含credential-free grant，服务端以exact
MCP Host URI SAN授权。真实mTLS测试覆盖正向PKCE delete与错误角色拒绝；JWT fixture覆盖真实Ed25519签名、policy/key drift、key顺序和
nonce负向。该切片不增加表或migration。

同步及experimental Task-aware operation现通过两个额外closed internal gRPC method进入独立Egress进程：execute与remote-task cancel只接受
MCP Host exact workload URI SAN，并在两端重验canonical envelope、request digest、payload bound和closed outcome/failure wire shape。
Egress启动从Candidate配置安装exact endpoint catalog与limits，远程Task AEAD keyring的原始32-byte key只从专用只读Kubernetes Secret
投影目录读取；配置仅保存key ID、reference digest与投影路径。RPC/transport响应丢失按request idempotency digest保守归类
post-dispatch uncertain，不把未观察到的结果伪造为no-effect。Resource subscription另使用一条closed mTLS双向gRPC流跨该进程边界：
Egress先返回加密session evidence，Host只有在durable Ready提交后才于同一流发送activation，随后接收bounded notification/termination
frame。同一连接保证多副本部署不会把prepare与activate路由到不同Egress进程；流或进程丢失不伪造关闭结果，而是触发既有session-loss与
full reconcile。

生产callback route现已由独立Callback API候选进程组合：进程只持有PostgreSQL callback command authority、AEAD state keyring和
MCP Host身份的Egress mTLS client；raw authorization code只跨digest-bound RPC，状态key从专用只读Secret投影读取并按Candidate冻结的
material digest复核。Helm只发布exact `/v1/mcp/oauth/callback` Ingress，并以default-deny NetworkPolicy禁止该进程访问公网。
生产PKCE cleanup delivery已由独立Worker组合：共享Outbox使用bounded `SKIP LOCKED` claim与owner/epoch/lease fence，PostgreSQL先重验
terminal Task及Pinned binding，Egress再删除exact Secret Manager version；成功推进`cleanup_completed`但保留`published_at=NULL`供后续
committed Event投影，临时/不确定失败退避重试，永久合同错误dead-letter。fresh PostgreSQL 16 fixture覆盖lease reclaim、stale fence拒绝和
first-winner完成，双副本/PDB/default-deny NetworkPolicy部署合同也已通过，不增加表或migration。Managed stdio operation现已直接进入
唯一Sandbox Job，并由production Firecracker Provider经Controller Artifact Broker取得exact Package/input、在private vsock中bounded
materialize后才执行；Provider不再拒绝已安装的Managed MCP runtime。Managed subscription的durable authority现已从原子admission继续到
专用claim与阶段提交：普通Capability Sandbox claim不能看见session workload，两个Managed worker并发claim只有一个lease winner；
`Preparing`绑定exact Executor/Attestor，`Starting`在同一事务把逻辑session推进到`Initializing`，Ready事务再同时提交逻辑
`Active/Ready`与物理`Running`。加密opaque session只保存在逻辑Invocation这一处current-state authority；物理Job只保存无Secret的
sandbox/protocol/ready-evidence digest binding。每一步均使用fenced Receipt、Event和Outbox，admission replay在后续阶段仍稳定。
全新PostgreSQL 16 fixture实际覆盖普通/专用队列隔离、并发claim first-winner、phase replay、stale fence和双状态Ready原子性。
Sandbox domain现又增加closed establishment Worker/Provider port，唯一允许顺序为`commit Preparing -> provider prepare -> commit Starting ->
provider initialize（通知仍关闭）-> commit Ready -> provider activate`；provider evidence逐字段回绑同一request、lease、Worker、Executor与
sandbox identity。两个故障注入unit fixture证明activation严格晚于durable Ready，并证明Ready提交失败会destroy prepared instance且不
activate。cleanup port进一步改为按exact request/fence销毁，允许后续Provider RPC在prepare响应丢失时以缺失prepared evidence收敛，而不把
未登记的VM留在运行面。独立Managed session authority internal gRPC已接入Controller，并以node attestor登记和exact microVM Executor URI
SAN限制claim/phase/Ready方法；Executor library也新增专用claim driver，与普通Sandbox共享同一`LocalWorkerPools`，先保留本地容量再claim，
并在长生命周期command future结束前持续持有permit。mTLS authority、Executor pool及Sandbox domain定向测试分别9、3、33项通过。
Controller的microVM Artifact RPC现保留closed workload tag，不再把Managed session请求降格为普通WASI请求；同一个无状态
Artifact Broker按workload选择PostgreSQL authority并共享一个in-flight bulkhead。Managed runtime bundle只允许物理Job处于
`Starting`、exact Executor lease仍有效且`read_whole` grant仍为active时读取，并在object I/O前后各授权一次。Provider销毁后的grant
回收也按Managed workload、Job/request/attempt/lease/Executor及Ready后的sandbox identity幂等验证。全新PostgreSQL 16 fixture实际覆盖
成功读取、错误Executor/workload拒绝、Ready后两次回收得到同一evidence及active grant归零；该切片不增加表或migration。
Managed runtime Secret现通过独立Egress与Sandbox Controller之间的两阶段交付：Controller使用现有`receipts`在exact Managed Job、
request、attempt、lease、Executor、Provider process generation、sandbox identity和ScopedSecretGrant上保留一次read，Egress才通过既有
Security Authority、KMS和Secret Provider解析明文；解析后Controller再次锁定并复验全部authority，再原子提交Receipt/Event/Outbox。
只有fresh reserve与fresh commit同时成功的一次调用可以向Provider返回bytes；reserve/commit重放、响应丢失或任一fence漂移均fail closed，
已经提交的重放不得再次返回明文。Controller永不接触明文，Egress没有数据库credential，Provider没有数据库、KMS或Secret Manager权限。
`maximum_reads`由现有Receipt计数执行，不增加表或migration，也不修改Job version；`Starting` phase evidence改为提交包含Provider generation
和sandbox identity的完整prepared canonical digest，防止交付时用较弱prepare evidence替换实际运行实例。
真实microVM Managed session Provider、guest Artifact/一次性Secret注入和同实例activation现已进入独立Provider进程；Managed authority又新增
非事件化、exact Job/version/lease/Worker/token fenced heartbeat，PostgreSQL只推进物理Job version与lease，不能延长request deadline或
session expiry，也不创建Receipt/Event/Outbox。domain与gRPC测试已执行，fresh PostgreSQL fixture已编译；本机Docker daemon无响应，故本次不把
该fixture声明为实际运行证据。Sandbox domain establishment Worker现会在每段Provider I/O期间按profile续租，并把每次返回的新Job version
串行带入后续phase；heartbeat失败时不会中途丢弃Provider future，而是等待其收敛并对任何已创建实例执行exact destroy。该循环尚未与
长期liveness观察、terminal supervisor及Executor进程组合，terminal/session-loss recovery以及
真实Linux KVM/jailer/guest-agent、process-kill/recovery与escape/saturation资格也未交付，因此该证据不关闭MCP或Phase 4，也不把本规范标记为
Implemented/Verified。此前workspace
all-target/all-feature check、test、doc-test与strict Clippy及public API/contract/schema/cutover门禁证据不自动覆盖本次变更；本次完整门禁
结果以实施计划的最新记录为准。
两个显式ignored RustFS qualification test仍不计为当前切片资格证据。

## 28. 明确推迟的工作

- deprecated HTTP+SSE compatibility；
- arbitrary custom transport/plugin；
- 自动发布 MCP Registry server；
- experimental Tasks 默认启用；
- server-initiated Sampling tools 的通用递归 Agent runtime；
- 跨地域 session continuation；
- 面向终端用户的 MCP server marketplace；
- Host 直接访问本地 workspace roots。

## 29. 未决问题

没有阻止 Sandbox、Artifact 或 API 设计的未决问题。后续MCP版本通过新增Protocol Policy Revision和conformance
引入，不得原地修改 `2025-11-25` profile，也不得改变平台 Capability/Context/Invocation 的稳定合同。
