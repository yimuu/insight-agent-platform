# Platform v2 Context 与 Retrieval 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-203 |
| 日期 | 2026-08-26 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md)、[`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md)、[`11-skill-system.md`](11-skill-system.md) |
| 直接下游 | 13、15、17、18 |

> CR-197 impact：ContextQuery/Dataset/subscription refresh Job复制其admission trace ID；MCP Host/Egress内部hop传播同一trace ID与新span。
> Remote Context请求不向第三方转发平台trace header，query、URI、tenant/source identity仍不得成为trace attribute。

> CR-181/203 impact：ContextQuery由05 Plan v5冻结Context slot、request/result port、item limit与resume；Scheduler、Context Worker和
> public API不得另行提供dataset head、schema、classification或continuation。

> Persistence ruling：Context registry 使用共享 Resource；查询是共享 Invocation，物理工作是 Job，结果进入 run_values/
> Artifact/Event。Dataset 只是 ResourceVersion 内容，不建立专用 query/observation/continuation 表族。

## 1. 决策摘要

ContextSource 是独立的只读数据来源合同，不是 Capability 的别名。它保留 query、filter、pagination、
provenance、citation、score、数据快照和逐条授权语义。平台索引、SQL schema catalog、受控 Artifact collection、
远程搜索和 MCP Resource 都可以实现 ContextSource Interface，但任何写操作或业务 SQL 执行必须使用
CapabilityInvocation。

Agent Deployment 固定 exact Context Deployment 和一致性策略；Run 或每次 ContextQuery 再固定实际 dataset
generation/observation。外部动态来源无法保证可重读时，平台持久化 bounded observation 与来源证据，而不是伪造
可重现性。

Plan node创建事务从RunBindings exact ContextBinding解析一致性对象，按Scope解析`request`，并以node maximum与Interface/tenant
limit的最小值创建只读query owner/Job。terminal owner验证Observation schema、citation/provenance与classification，写node声明的
`result` RunValue、终结当前ContextQuery Node并按Plan `resume`创建目标NodeExecution及唯一Orchestration Job；不得重新执行同一query Node。
`LatestAtQueryStart` generation只能在该创建事务冻结，Worker不能追随更晚head。

## 2. 目标与非目标

### 2.1 目标

- 给 ContextSource Interface、Implementation、Binding、Query、Observation 和 Citation 机器合同；
- 支持平台索引、实时远程来源和 MCP Resource，同时保持统一安全语义；
- 在结果进入 Model/Plan 前完成逐条授权、分类、大小和 provenance 验证；
- 明确 mutable dataset 与 immutable Run binding 的关系；
- 为 pagination、fan-out、rerank、cache、timeout 和 deferred retrieval 提供 durable 行为；
- 为 Text2SQL 提供受控 schema/catalog 上下文，不让 Retrieval 直接执行任意 SQL；
- 让引用能定位到本次观察的内容，而不是只保存易漂移 URL。

### 2.2 非目标

- 不把有副作用的 API、SQL mutation 或文件写入包装成 ContextSource；
- 不保证所有外部来源支持一致 snapshot、稳定 score 或全文 citation；
- 不让模型任意枚举 tenant 数据源、拼接 endpoint 或构造后端 credential；
- 不将向量数据库、embedding provider 或 reranker SDK 暴露给 Agent；
- 不持久化无限搜索结果、全文 corpus、原始 ACL token 或用户 query 日志；
- 不提供开放互联网爬虫、通用浏览器或跨租户知识共享；
- 不让 ContextSource 直接成为 Model tool 以绕过 query policy。

## 3. 术语与信任边界

| 术语 | 含义 |
|---|---|
| Context Interface | query、result、citation、consistency 与数据策略合同 |
| Context Implementation Revision | 索引、远程协议或资源集合的不可变查询/映射合同 |
| Context Deployment | Implementation Revision 与实际source、Secret、network、parser/ranking/data policy的exact binding |
| Dataset Generation | 平台管理语料的一次不可变可查询快照 |
| Context Binding | Interface、Implementation、consistency、ranking 和 policy 的 exact 绑定 |
| ContextQuery | durable 的一次只读检索叶节点 |
| Context Observation | 本次 query 已提交的 bounded 结果与来源证据 |
| Context Item | 授权后可供 Plan/Model 使用的一条结果 |
| Citation | 连接 item、来源、locator、内容 digest 和观察时间的证据 |

来源正文、metadata、URI、文件名、远程 score、MCP description、embedding input 和检索出的指令都不受信任。
Context Worker 可以读取经过授权的数据，但不拥有 Principal 权限决策；它必须使用平台签发的 bounded
DataAccessGrant，并在 commit 时再次验证 policy generation。

## 4. Context Interface Revision

```rust
struct ContextInterfaceRevision {
    interface_revision_id: ResourceVersionId,
    context_source_id: ContextSourceId,
    qualified_name: ContextSourceName,
    query_schema: ClosedJsonSchema,
    filter_schema: ClosedJsonSchema,
    item_schema: ClosedJsonSchema,
    observation_schema: ClosedJsonSchema,
    citation_contract: CitationContract,
    consistency_modes: BTreeSet<ConsistencyMode>,
    pagination_contract: PaginationContract,
    ranking_contract: RankingContract,
    data_policy: ContextDataPolicy,
    limits: ContextInterfaceLimits,
    semantic_digest: Digest,
}
```

Interface 必须是 ReadOnly。`qualified_name` 只用于 authoring；执行使用 opaque Revision/Binding ID。Query schema
顶层为 closed object，通用字段只包含受限 text、structured filters、requested fields、page size 和 locale；
endpoint、tenant、principal、ACL、index name 和 credential 不属于模型可写 query。

`query_schema`、`filter_schema`、`item_schema`和`observation_schema`全部使用05唯一权威的
`insight.closed-json-schema/1`；Context实现不得添加索引/SQL引擎私有keyword或返回开放object。
`item_schema`描述单个normalized item payload；`observation_schema`描述写入RunValue/Artifact的完整平台envelope，二者
不可互换或复用digest。

## 5. Implementation Revision

```rust
struct ContextImplementationRevision {
    implementation_revision_id: ResourceVersionId,
    implementation_id: ContextImplementationId,
    interface_revision_id: ResourceVersionId,
    backend_contract: ContextBackendContract,
    credential_requirements: Vec<SecretPurpose>,
    backend_limits: ContextBackendLimits,
    implementation_digest: Digest,
}

enum ContextBackendContract {
    ManagedIndex(ManagedIndexContract),
    RemoteSearch(RemoteSearchContract),
    McpResources(McpResourceContract),
    SqlCatalog(SqlCatalogContract),
    ArtifactCollection(ArtifactCollectionContract),
    NativeCatalog(NativeCatalogContract),
}
```

- `ManagedIndex` 固定 lexical/vector/hybrid query与result contract；
- `RemoteSearch` 使用固定 HTTP/gRPC contract、mapping和credential requirements；
- `McpResources`固定resource identity/template、schema mapping、required protocol和URI policy，不固定MCP Deployment；
- `SqlCatalog` 只暴露 schema、table、column、relationship、统计摘要和已批准样例；
- `ArtifactCollection` 只声明读取Ready Artifact collection的contract；
- `NativeCatalog` 是平台内置的只读 metadata adapter。

增加 backend 必须同时增加 policy、conformance、metrics 和 recovery 实现。实现不能改变 Interface 的分类、
结果上限或 citation 要求。

Context backend machine wire固定为`managed_index | remote_search | mcp_resources | sql_catalog |
artifact_collection | native_catalog`；consistency固定为`pinned_generation | pin_at_run_admission |
latest_at_query_start | external_observation`；citation strength固定为`exact | observation_only`；backend outcome
固定为`completed | deferred | retryable_failure | permanent_failure`。SQL、typed repository、worker adapter与事件
projection必须消费同一registry，不得保留大小写variant或自由字符串别名。

## 6. Binding 与一致性

```rust
struct ContextDeployment {
    context_deployment_id: DeploymentId,
    interface_revision_id: ResourceVersionId,
    implementation_revision_id: ResourceVersionId,
    required_worker_manifest_digest: Digest,
    backend_binding: ContextBackendBinding,
    secret_bindings: Vec<ExactSecretBindingRef>,
    network_policy_revision_id: Option<ResourceVersionId>,
    tls_policy_revision_id: Option<ResourceVersionId>,
    trust_policy_revision_id: Option<ResourceVersionId>,
    parser_profile_revision_id: ResourceVersionId,
    ranking_profile_revision_id: ResourceVersionId,
    data_policy_revision_ids: Vec<ResourceVersionId>,
    conformance_evidence_id: EvidenceId,
    deployment_digest: Digest,
}

enum ContextBackendBinding {
    ManagedIndex(ExactManagedIndexBinding),
    RemoteSearch(ExactRemoteSearchBinding),
    McpResources(ExactMcpResourceBinding),
    SqlCatalog(ExactSqlCatalogBinding),
    ArtifactCollection(ExactArtifactCollectionBinding),
    NativeCatalog(InstalledNativeCatalogBinding),
}

struct ContextBinding {
    context_binding_id: ContextBindingId,
    tenant_id: TenantId,
    owner_agent_deployment_id: DeploymentId,
    context_deployment_id: DeploymentId,
    consistency: ConsistencyPolicy,
    allowed_projection: FieldProjection,
    binding_digest: Digest,
}

enum ConsistencyPolicy {
    PinnedGeneration { generation_id: DatasetGenerationId },
    PinAtRunAdmission,
    LatestAtQueryStart,
    ExternalObservation,
}

struct ExactRemoteSearchBinding {
    endpoint: CanonicalHttpEndpoint,
    endpoint_identity_digest: Digest,
    region: CanonicalRegion,
}
```

`PinnedGeneration` 与 `PinAtRunAdmission` 提供平台可重读 snapshot。`LatestAtQueryStart` 在每次 ContextQuery 创建
事务中固定当前 generation。`ExternalObservation` 用于无法获得平台 snapshot 的远程来源；必须保存 response
digest、remote revision/watermark（若有）、观察时间和 bounded normalized result。

`PinnedGeneration`只能保存exact `dataset_generation` ID+digest并通过typed source projection证明。Dataset使用共享
`resources`中的`ContextDataset` root；每个Dataset Generation是该root下不可变的共享`resource_versions` row，版本ID使用
`dgen`，active data head使用该Dataset root唯一的`active_version_id`。它不与Context Interface/Implementation争用active head，
也不增加dataset专用表。Generation必须与真实build Job、Ready manifest Artifact和validation Event同事务创建；
任何没有typed Dataset root、exact generation digest和source projection的Binding都拒绝，不允许暂存裸`dgen`文本。

RunBindings 固定 ContextBinding，不表示底层数据永远不变。一致性模式与本次 observed generation/token 必须
同时出现在 Run diagnostic 和 citation 中，客户端不能把 `ExternalObservation` 宣称为 reproducible snapshot。

Context Deployment是02定义的环境绑定：它固定Implementation、required Worker manifest、实际source identity/backend deployment、
SecretBinding、Network、parser/chunker/embedding/ranking/data policy和绑定exact环境的conformance evidence。backend binding variant必须
与Implementation contract一致；例如McpResources绑定exact MCP Deployment与Discovery Snapshot，RemoteSearch绑定
canonical endpoint、endpoint digest、region及exact Network/TLS/Trust Policy，ManagedIndex绑定exact index service/region。RunBindings中的ContextBinding只引用exact Deployment
并增加本Run的consistency/dataset/projection选择；runtime不得从Implementation Revision重新解析source、credential、
network或active head。

所有Context contract、Deployment和observation中的region字段只使用02 `CanonicalRegion`及其common schema；clean-cut目标删除
字符集不同的`DataRegion`。同一exact binding内的region逐字段比较，不做provider alias或大小写归一化。

Deployment validation除backend variant一致外，还必须执行contract/binding字段级兼容校验；例如`SqlCatalog`的dialect必须
与Implementation contract完全一致；RemoteSearch的endpoint canonical digest必须等于binding identity，Policy必须是正确kind且当前gate
允许该Deployment，required Worker manifest必须属于已资格静态adapter registry。只比较`backend_kind`不能证明exact binding，任何不匹配
均在publish/admission时fail closed。

ContextBinding由Agent Deployment resolution事务创建且不可变；它是`AgentDeploymentClosure`中带`xcb` identity和canonical
digest的closed snapshot，不是独立current row、Resource或生命周期。`owner_agent_deployment_id`必须是预留的`adep`并与
Context Deployment同tenant；Run admission把完整snapshot复制到RunBindings并重新验证digest。Binding不能跨Agent Deployment复用
或在Run admission后修改。Agent Deployment validation必须在同一事务验证exact Context Deployment、Dataset/Policy closure与
binding snapshot；不得只保存裸`xcb`、另建Binding表、延迟补写或从active head重建Binding。

`PinAtRunAdmission`不能只复制策略文本。Run admission必须在同一事务读取ContextDataset root的exact active `dgen`，并把
`(context_binding_id, binding_digest, dataset_id, generation_id, generation_digest)`写入RunBindings的规范排序
`context_dataset_views`。`PinnedGeneration`可写入同一列表作为显式重申；`LatestAtQueryStart`与`ExternalObservation`不得预填。
ContextQuery admission对`PinAtRunAdmission`只读取该Run快照，绝不再次读取active head；缺项、重复项、digest不一致或多余项均
fail closed。该列表属于现有RunBindings JSON/canonical digest，不建立新表或第二active-head authority。

ContextSource Interface与Context Implementation分别拥有AdministrativeGate；source gate阻止该source的全部新
binding/query，implementation gate只隔离对应backend实现。active head只属于ContextSource Interface并指向exact
Context Deployment，不为Implementation创建第二个head。

## 7. Dataset 与 Index 生命周期

```text
Context Deployment
  -> Dataset Build Job
  -> Fetching
  -> Scanning
  -> Parsing
  -> Chunking
  -> Embedding/Indexing
  -> Validating
  -> create immutable Dataset Generation
  -> Active Data Head
```

```rust
struct DatasetGeneration {
    dataset_generation_id: DatasetGenerationId,
    tenant_id: TenantId,
    context_deployment_id: DeploymentId,
    source_manifest_digest: Digest,
    parser_profile_revision_id: ResourceVersionId,
    chunker_profile_revision_id: ResourceVersionId,
    embedding_model_deployment_id: Option<DeploymentId>,
    ranking_profile_revision_id: ResourceVersionId,
    index_manifest_artifact_id: ArtifactId,
    validation_evidence_id: EvidenceId,
    created_by_job_id: JobId,
    generation_digest: Digest,
    created_at: DateTime<Utc>,
}
```

Context Deployment必须冻结`parser_profile_revision_id`、`chunker_profile_revision_id`、可选exact
`embedding_model_deployment_id`与`ranking_profile_revision_id`；build command不得从active head或请求正文临时选择这些依赖。
不需要embedding的backend必须将该slot显式冻结为`None`，需要embedding的backend缺失slot则Deployment validation fail closed。

Dataset root identity由build admission确定。首次build的public request不携带`dataset_id`时，服务端预留一个新的`dset`并把它冻结为
shared Job的typed target；预留ID本身不是Resource current state，只有成功提交时才在同一事务物化ContextDataset root和首个`dgen`。
失败、取消或超时不得留下空root或半成品generation。重建可携带既有`dataset_id`；admission必须验证该root的active generation
绑定同一exact Context Deployment，否则拒绝。相同tenant、principal、Deployment与Idempotency-Key重放返回第一次预留的ID，不能再分配新ID；
同一Dataset同时最多一个非terminal build Job，串行成功用expected active generation/version CAS防止lost update。

构建阶段是shared Job的bounded progress，不是第二个Dataset状态机；构建detail只进入Job typed
payload或Artifact。Job成功事务在ContextDataset root下创建完整DatasetGeneration
ResourceVersion并CAS active data head，失败/取消/超时不创建半成品Generation。

- 每个 Dataset Generation 不可变，包含 source manifest digest、parser/chunker/embedding/ranking profiles；
- source item 删除/更新会创建新 generation，不原地覆盖旧 chunk；
- active data head 使用 CAS，只影响未来 pin/query；
- generation 构建不持有长数据库事务，阶段结果通过 Artifact 和 durable receipts 连接；
- Ready 前必须完成 Artifact 安全、ACL materialization、count/size、citation coverage 和 index integrity 验证；
- 旧 generation 在 Run、citation、retention 或 legal hold 引用期间不能 GC；
- embedding model 更新必须新建 generation，不能后台静默重嵌入 active index。

外部实时来源不伪装成 Dataset Generation；其 discovery metadata 可以是 immutable Revision，但读取结果仍为
ExternalObservation。

## 8. Query 模型

```rust
struct ContextQuery {
    context_query_id: ContextQueryId,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    context_binding_id: ContextBindingId,
    state: ContextQueryState,
    query_ref: ValueRef,
    query_digest: Digest,
    principal: PrincipalSnapshot,
    dataset_view: DatasetView,
    observation_id: Option<ContextObservationId>,
    deadline: DateTime<Utc>,
    projection_version: u64,
}
```

模型或 Skill 只能提出符合 query/filter schema 的值和 RunBindings 中的 context alias。创建事务负责解析 binding、
固定 dataset view、验证 current principal/policy、计算 deadline/cache scope，并写 Ready work/outbox。

## 9. Query 状态机

```rust
enum ContextQueryState {
    Created,
    AwaitingAuthorization,
    Ready,
    InFlight,
    Deferred,
    RetryScheduled,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

```text
Created -> AwaitingAuthorization | Ready | Failed
AwaitingAuthorization -> Ready | Failed | Cancelled | TimedOut
Ready -> InFlight | Cancelled | TimedOut
InFlight -> Succeeded | Deferred | RetryScheduled | Failed | Cancelled | TimedOut
Deferred -> InFlight | Succeeded | RetryScheduled | Failed | Cancelled | TimedOut
RetryScheduled -> Ready | Cancelled | TimedOut
```

终态不可离开。Deferred 用于远程异步检索或大索引短轮询，必须释放 Context permit。授权任务只允许获得已声明
scope，不接受来源通过正文索取 token/密码。

Context Query的每次物理执行复用06的 Job、`(JobId, lease_generation)`、lease/fence 与 commit
disposition；`ContextQueryState`只表示逻辑 Query，不是另一套物理执行 current state。

Context Worker提交物理outcome时使用专用`ContextWorkerAudit`：tenant、WorkerProcessGeneration必须与Job
lease fence完全一致，幂等结果使用`JobCommit` Receipt并以Job为scope、WorkerProcessGeneration为dedupe owner。
callback/poll/timer等外部信号使用不携带Principal的`ContextSignalAudit`和`Callback` Receipt；它们只证明已认证的
信号摄取，不授予用户权限。accepted signal在同一事务更新Job、Event和Outbox；stale/late signal只把Receipt稳定终结为
`rejected_stale`，不得改写ContextQuery/Job、创建公共事件或冒充Principal command。

## 10. 查询执行管线

规范顺序：

```text
validate query
 -> resolve fixed binding/dataset view
 -> issue scoped DataAccessGrant
 -> authorization pre-filter/pushdown
 -> retrieve bounded candidates
 -> verify per-item tenant/ACL/classification
 -> normalize and deduplicate within binding policy
 -> rerank authorized candidates only
 -> assemble citation and bounded content
 -> commit Observation + complete leaf Node + activate exact Plan resume
```

禁止把未授权 candidate 发送到共享 reranker、embedding API 或模型后再过滤。远程 backend 无法证明 server-side
ACL 时，必须使用 principal-scoped credential 并进行 result-side entitlement validation，否则该 source 不能发布。

## 11. Observation、Item 与 Citation

```rust
struct ContextObservation {
    observation_id: ContextObservationId,
    context_query_id: ContextQueryId,
    dataset_view: DatasetView,
    normalized_query_digest: Digest,
    items: Vec<ContextItem>,
    next_cursor: Option<ContextCursor>,
    retrieval_evidence: RetrievalEvidence,
    observed_at: DateTime<Utc>,
}

struct ContextItem {
    item_id: ContextItemId,
    source_item_identity: OpaqueSourceItemId,
    content: BoundedContextContent,
    structured_fields: ClosedJsonValue,
    score: Option<NormalizedScore>,
    classification: DataClassification,
    citation: Citation,
}
```

`BoundedContextContent`只能是受`item_schema`与`maximum_item_bytes`约束的inline closed value，不允许在item中嵌套
ArtifactRef。若完整Observation超过RunValue inline上限，则把完整、canonical的Observation保存为一个Ready Artifact，并由输出
RunValue创建一个exact ArtifactLink；不得为逃避大小、授权或retention约束在Observation JSON内部埋入未链接ArtifactRef。

```rust
struct Citation {
    context_deployment_id: DeploymentId,
    interface_revision_id: ResourceVersionId,
    dataset_view: DatasetView,
    locator: CitationLocator,
    content_digest: Digest,
    observed_at: DateTime<Utc>,
    display_label: SafeDisplayLabel,
}
```

CitationLocator 是 closed union，例如 Artifact page/byte span、document section、catalog object、MCP resource URI
digest 和 remote opaque locator。它不能包含 credential、raw object key、主机路径或未授权 query parameter。
如果来源不能提供足够证据，item 必须标记 `citation_strength = ObservationOnly`，不能生成虚假精确页码。

## 12. Pagination、Fan-out 与 Ranking

- Cursor 是平台签名/加密的 opaque token，绑定 tenant、principal snapshot、binding、dataset view、query digest、
  page size、expiry 和 generation；
- 客户端不能修改 remote cursor、offset、ACL 或 source identity；
- next page 是新的 ContextQuery 或 continuation attempt，结果独立提交并保留 parent observation link；
- 多 source fan-out 只能来自 Deployment 固定的 ContextGroup，数量与每源并发有硬上限；
- 每个来源先完成授权再进入 aggregator；部分成功是否允许由固定 settlement policy 决定；
- score 只在声明的 ranking profile 内可比较；不同 score domain 必须先校准或使用 rank fusion；
- dedupe 使用 tenant-scoped content/source identity，不跨安全边界推断内容相同；
- reranker 输入、版本、截断策略和结果 digest 进入 observation evidence。

## 13. Cache

Cache key 至少包含：

```text
tenant
binding revision/digest
dataset view
normalized query/filter/projection digest
principal entitlement digest
data policy generation
ranking profile
locale
```

ExternalObservation 默认不跨 Run cache；只有来源提供稳定 validator/watermark 且 policy 允许时才可短期复用。
权限 revoke 提升 policy generation 并使旧 cache 不可命中。缓存正文采用与源数据相同或更强的加密、分类和
retention；cache miss 不能退回未授权公共结果。

## 14. Text2SQL 边界

Text2SQL Agent 使用两类明确对象：

1. `SqlCatalog` ContextSource：提供已授权 schema、table、column、relationship、dialect、统计摘要和安全样例；
2. `database.query.readonly` Capability：接收验证后的 SQL/typed query plan，在只读事务、statement timeout、
   row/byte limit、allowlist schema 和专用数据库身份下执行。

ContextSource 不执行模型生成 SQL，不返回数据库 credential，也不把任意 DDL/样例中的指令提升为系统指令。
SQL validation、EXPLAIN/cost gate、执行、审计和结果 Artifact 属于 Capability/Sandbox 或专用数据库 adapter，
不能藏在 Retrieval backend。

## 15. 所有权接口与机器合同

```rust
trait ContextBackend {
    async fn query(&self, request: ContextBackendRequest) -> ContextBackendOutcome;
    async fn continue_query(&self, request: ContextContinuationRequest) -> ContextBackendOutcome;
    async fn cancel(&self, request: ContextCancelRequest) -> ContextCancelOutcome;
}

enum ContextBackendOutcome {
    Completed(RawContextResult),
    Deferred(OpaqueContextHandle),
    RetryableFailure(SafeContextFailure),
    PermanentFailure(SafeContextFailure),
}
```

Backend request 只接收 bounded query、scoped Artifact/Data grants、exact implementation/dataset view、attempt/fence、
deadline 和 safe trace context。Secret 在 adapter 内 late resolve。Backend 不能提交 Observation 或 Run 状态。
只有持有exact WorkerProcessGeneration lease fence的Context Worker outcome command可以通过`JobCommit` Receipt请求提交；
backend response、callback body或消息消费者本身都不是current-state authority。

管理 API 覆盖 Draft、Validation、Publish、Dataset Build、Activate Data Head、Suspend；运行 API 只通过 Plan
创建 ContextQuery，不提供绕过 Agent binding 的任意 source endpoint。

公共Run事件闭集为`context.started/completed/failed/cancelled/timed_out`；正文、query、ACL、citation locator和
backend handle不进入事件，成功内容只能通过授权的ValueRef/ArtifactRef projection读取。

## 16. Persistence 与 Artifact 映射

Context Interface、Implementation与ContextDataset使用共享Resource；Interface/Implementation revision与DatasetGeneration使用
共享ResourceVersion，环境绑定使用共享Deployment。Query 是
`InvocationKind::Context`，物理 build/query/poll 是 Job，等待合同嵌入 Job；bounded Observation/Item/Citation 写入 Invocation
result 或 run_values并使用Interface的`observation_schema` digest；完整Observation超出inline上限时保存为一个Ready Artifact并创建
一个exact ArtifactLink，index manifest、parser output 与 diagnostic也保存为Artifact。业务聚合只能引用 Ready、
同 tenant Artifact。历史与安全 projection 进入 Event，不建立 Context 专用 query/observation/continuation 表族。

## 17. 不变量

- ContextSource 永远只读；任何写操作都是 Capability；
- Query 只能使用 RunBindings 中的 exact Context Binding；
- 每个 item 在 rerank、cache、Prompt assembly 前已经通过逐条授权；
- Citation 必须对应本次 committed content digest 和 dataset/observation；
- item content必须是bounded inline closed value；Artifact-backed结果只允许位于完整Observation输出边界并拥有exact ArtifactLink；
- active source/data head 变化不改写已有 Query/Observation；
- ExternalObservation 明确标记非 snapshot，不伪装可重复读取；
- Secret、remote handle、raw object key 和 ACL token 不进入 ContextItem；
- 失败、部分成功、截断和 citation weakness 不被静默隐藏；
- Context output 进入 Model 时保持不受信任来源边界。

## 18. 幂等、并发与背压

### 18.1 MCP subscription invalidation admission

MCP Resource subscription的notification refresh与full reconcile由Context application owner接收，不由MCP Host直接执行Context backend。
输入冻结tenant、subscription、exact Context/MCP Deployment、Discovery identity/digest、authorization/session/event generation、root resource
identity/digest、reason与canonical request digest。owner transaction必须重载published Context/MCP closure和当前subscription evidence，按
`(tenant, subscription, session generation, event generation, request digest)` claim `Command` Receipt，并以shared `Context` Job承载刷新工作；
同一事务写Job、Receipt、Event和Outbox后返回`request_digest + durable_work_digest + accepted_at`。
该Job使用03 closed `Context -> McpOperation` pair，`owner_id`是已锁定的同tenant `mcp_subscription` identity；这只是Job的typed business owner，
不把Context执行权交给MCP Host，也不新建ContextQuery/ContextDataset或第二份subscription current state。

replay返回第一次接受的相同Job/evidence；字段漂移、stale generation、撤权、wrong Deployment/Discovery、Receipt key复用不同digest均fail closed且
不创建Job。MCP Host只能在验证acceptance绑定exact request后结算自身MCP subscription Job；RPC可传输命令但不能替代owner transaction，
in-memory callback、caller生成work digest和直接写Context结果均禁止。refresh Job使用Context pool/tenant quota，MCP connection/Job不持有
Context permit；Context Job失败或重试由其自身lease/recovery推进，不回滚已提交notification history。

### 18.2 Subscription refresh execution 与outcome

Context Worker按`Context -> McpOperation`精确扫描/claim，不能把payload投影为ContextQuery。每个attempt冻结tenant、subscription、Job、
worker generation/fence、exact Context/MCP Deployment与Discovery/Auth/session/event/root evidence、cause、deadline和admission request digest，
然后调用13的typed internal MCP Resource Refresh RPC。RPC请求不含raw session/token/Secret、自由URL/header或调用方选择的method；MCP Host必须在
外部I/O前从PostgreSQL重载当前subscription、exact MCP execution closure及Job fence。

首版refresh执行对root resource做有界`resources/read`，full reconcile按冻结root/profile及各自method limits执行有界
`resources/list` + `resources/read`集合；具体wire method由13的published protocol profile决定，不由Context Worker自由选择。成功响应只返回closed evidence：request/response/resource-set digest、
resource/item/byte count、remote revision或cursor（若有）和observed time。正文、URI locator、session、token与未归一化remote error不得返回或
持久化。Context owner重验evidence digest与限制后，以`JobCommit` Receipt在单一事务terminalize Job、结算quota并写Event/Outbox。

该成功事实仅表示exact subscription source已完成一次协议刷新/重验；首版没有materialized subscription cache，因而不创建Context Observation、
RunValue、Dataset Generation或Context-specific row，也不得宣称后续ContextQuery命中缓存。dispatch前永久拒绝直接失败；dependency/capacity和
ReadOnly post-dispatch uncertain按attempt/deadline做bounded retry。取消、超时、lease recovery或旧worker返回均由Job fence first-winner，
不得回滚notification history或让MCP Host提交terminal。

- ContextQuery 以稳定 node/query ordinal 和 query digest 幂等；
- MCP subscription refresh/reconcile以stable subscription/session/event/request digest幂等并创建至多一个Context Job；
- refresh成功只产生bounded terminal evidence，不产生Observation/cache/dataset；
- Attempt 使用 lease/epoch/fence，迟到结果不能覆盖 first-winner；
- worker outcome按`(tenant, Job, WorkerProcessGeneration, operation, idempotency digest)`使用`JobCommit` Receipt；
- callback/poll/timer按`(tenant, Job, operation, idempotency digest)`使用`Callback` Receipt，重复信号返回同一稳定disposition；
- Context work class、每 tenant、每 binding、每 remote host 和 reranker 使用独立 permit；
- fan-out 在领取子 permit 前不持有 aggregator execution permit；
- page size、candidate count、item bytes、total bytes、filter complexity、query length、fan-out 和 deadline 有硬上限；
- remote `429/503` 进入 bounded backoff，不占连接或 Worker；
- Dataset build 与 online query 使用不同队列、连接池和资源配额；
- cache stampede 以 bounded single-flight receipt 合并，但等待者仍受 deadline。

## 19. 超时、重试、取消与恢复

- Pure/ReadOnly query 在没有收到结果时可按 binding policy 重试；
- ExternalObservation 的重复读取可能得到不同内容，每次 retry 必须记录 attempt 和 final observation source；
- cancel 提升 generation，尝试 backend cancel，并拒绝迟到 commit；
- remote continuation/poll 与 callback 由wake generation first-winner；loser只终结为`rejected_stale` Receipt，不产生状态或事件；
- Dataset build 崩溃从阶段 receipt/Artifact 恢复，不从头重复已验证阶段；
- index object 丢失、digest 错误或 ACL generation 不匹配时 fail closed 并 suspend generation；
- Run terminal 后结果只能成为安全审计，不能写入 Plan Value；
- NATS 丢失由 PostgreSQL safety scan 恢复 Ready/Deferred/expired lease。

## 20. 安全、租户与 Secret

- tenant key 出现在所有 row、index namespace、cache、Artifact grant 和 unique/foreign key；
- DataAccessGrant 绑定 tenant、principal snapshot、source、allowed fields/classification、deadline 和 query ID；
- OAuth/service credential 按 source + tenant + principal purpose 隔离，不跨用户连接复用；
- remote URI/endpoint 经 SSRF、DNS、TLS、redirect 和 network policy 验证；
- 用户可控 filter 不直接拼 SQL、Lucene、GraphQL 或模板，必须编译为 backend typed AST；
- Prompt injection、恶意文档和公式只作为不受信任 content，不授予 Tool/Secret 权限；
- deletion、legal hold、consent revoke 和 source suspension 立即阻止新 query/cache hit；
- diagnostic Artifact 和 query 内容遵守数据分类、最小 retention 和访问审计。

## 21. 可观测性与隐私

```text
context_queries_total{backend_class,outcome,consistency}
context_query_duration_seconds{backend_class,outcome}
context_candidates_total{stage}
context_items_rejected_total{reason_class}
context_cache_total{outcome}
context_deferred_active{backend_class}
dataset_build_total{stage,outcome}
context_citation_strength_total{strength}
```

tenant/source/query/item/URI 不进入 metric label。默认 trace 记录 count、bytes、latency、Revision、dataset
generation 的受控 hash 和 rejection class，不记录正文、filter value 或 citation locator。授权失败采用不可区分错误，
防止 source/item 枚举。

## 22. 配置与部署

- Context Worker 与 Dataset Builder 使用独立 Deployment、队列、连接池和 autoscaling signal；
- managed index 数据面与 PostgreSQL authority 分离，generation manifest/digest 由 PostgreSQL 持有；
- parser、chunker、embedding、reranker 和 tokenizer 都是版本化、固定 digest 的实现；
- remote adapter 只能使用 Registry 已发布配置，不能从 query 接收 endpoint；
- 平台硬 limit 只能由 tenant/binding 收紧；
- production readiness 需要 source health、policy resolver、Artifact store 和 PostgreSQL，NATS 仅用于加速。

## 23. 测试矩阵与验收标准

- ManagedIndex、RemoteSearch、MCP Resource、SqlCatalog 和 ArtifactCollection 通过同一 Interface fixture；
- PinnedGeneration、PinAtRunAdmission、LatestAtQueryStart、ExternalObservation 语义可区分且可审计；
- active data head 中途切换不改变已创建 Query 的 dataset view；
- 跨租户/撤权 item 在 retrieval、rerank、cache、citation 和 Prompt 中均不可见；
- cursor 篡改、过期、换 principal、换 dataset 或换 query 被拒绝；
- callback/poll/retry/timeout 并发只有一个 Observation winner；
- worker identity与lease fence不一致时整个outcome事务回滚；重复worker commit精确replay，stale signal稳定返回`rejected_stale`；
- partial fan-out 按 settlement policy 显式成功或失败；
- citation digest 与本次 content 一致，漂移 URL 不会改写历史 Observation；
- archive bomb、恶意 parser input、oversized item 和 Prompt injection fixture fail closed；
- Dataset build kill/restart 后从 durable stage 恢复且不会发布半成品；
- Text2SQL fixture 证明 Context 只读 catalog，SQL 执行只能经 ReadOnly Capability；
- Context contract/Deployment/observation的每个region字段都引用02 CanonicalRegion common schema并逐字段exact compare；覆盖1/63-byte、排序/
  唯一约束及空/64/大写/下划线/Unicode/provider alias/旧DataRegion负向，不允许大小写归一化或adapter alias；
- Secret、ACL、query 和 content canary 不进入 public event、metric 或默认日志。

### 23.1 当前实施证据边界（非规范性）

Context query/item/citation domain与caller-owned PostgreSQL repository已交付。fresh PostgreSQL 16 fixture覆盖exact Run binding、
Deferred/wake同attempt恢复、worker fence、stale signal、quota、citation digest/foreign deployment拒绝及Event/Receipt/Outbox原子性。
后续r234增加bounded/sharded expired-lease owner recovery，覆盖Deferred恢复后的Worker丢失、旧reservation原子结算、同一Job进入新
attempt、`context.lease_recovered` Event/Outbox及最终唯一Observation；read-only query按真实物理调用计数，result bytes只在terminal结算。
后续批次新增独立`platform-context-worker`和NativeCatalog静态adapter：只读候选扫描在任何lease/quota mutation前精确比较冻结的
`adapter_contract_digest`与`installed_adapter_digest`，命中后才使用exact-slot claim；进程以独立Context本地permit、heartbeat、owner
commit和bounded expired-lease scanner组合执行。其Helm角色只允许DNS与PostgreSQL出站，不挂载Egress、Secret、NATS或Sandbox凭据。
Text2SQL `ReadOnlySqlPlan`同时冻结catalog Query/Observation/projection、database identity/dialect及exact Capability
Interface/Deployment/Effect；generic Invocation admission在同一事务锁定这些事实，只接受规范名精确为`database.query.readonly`且Effect为
ReadOnly的已绑定Capability。成功/replay、错误名称/Effect、foreign Run/citation与Observation drift fixture均通过，拒绝路径不留下
Invocation或Receipt。该证据只是Context/Text2SQL的L1～L2候选实施证据，不替代生产Context backend、SQL adapter、
public `/v1`或18的L4～L6资格。fresh PostgreSQL 16 r240已用真实`platform-context-worker`双进程覆盖错installed adapter digest
零claim/Job保持ready、terminal commit数据库窗口强杀、expired-lease owner恢复、同一Job物理attempt 2、lease/quota清除和唯一
Observation，NativeCatalog process L3至此闭合；remote backend protocol仍未闭合。

r268实现CR-190的Context subscription refresh admission L1 nominal contract：closed request冻结exact Deployment/Discovery、授权与session/event
generation、root identity、deadline及canonical digest，shared Context Job payload有独立schema/size validation，acceptance精确绑定request、durable work、
Job和commit time。unit negative fixtures覆盖字段漂移与oversized identity。该证据尚不包含PostgreSQL owner transaction、实际refresh backend、
MCP Host生产接线或commit-window L2/L3，因此不满足18.1的durable acceptance验收。

r270新增PostgreSQL Context owner transaction并在fresh PostgreSQL 16上证明notification admission、exact Receipt replay、唯一
`Context -> McpOperation` Job/Event/Outbox及stale session generation零写入；事务还重载published MCP/Context closure与当前授权。该fixture尚未覆盖
full reconcile正向、Context worker执行/恢复、Host adapter或accept commit-window进程强杀，故18.1只完成部分L2。

r271补齐MCP→Context typed adapter与full reconcile L2：adapter不能提供Job identity/work digest，Context authority返回值才成为MCP completion
evidence；fresh PostgreSQL在前一refresh Job已终态的分层fixture中证明reconcile acceptance/replay与下一唯一Job。测试中的前序Job终态是fixture
安排，不代表Context worker已交付；真实handler、lease/recovery与进程kill-window仍未闭合。

r272实现CR-192 PostgreSQL execution owner：只读scan按exact Context Worker manifest过滤，claim按subscription→Job锁序重验当前
session/auth/Deployment/root及成功admission Receipt，预留独立Context concurrent quota并生成fenced attempt；JobCommit Receipt原子提交
bounded success/retry evidence、quota settlement、Job/Event/Outbox。fresh PostgreSQL 16 fixture覆盖MCP先清除pending marker后Context claim、
wrong manifest零claim、success replay、retryable outcome、expired running lease恢复到attempt 2、attempt 3唯一成功及零Context
Invocation/Observation。该L2尚不包含真实MCP Resource RPC或独立Context/MCP/Egress进程L3。

CR-193修正成功evidence绑定：`execution_identity_digest`由tenant/subscription/Job、worker generation、lease generation/token、physical
attempt与exact request计算，排除heartbeat可变的Job `expected_version`/lease expiry。Context Worker把dispatch时的identity交给Host，续租只推进
最终owner commit fence；因此一次或多次heartbeat不会使合法Host evidence失效。新lease或新physical attempt必须产生不同identity。

r273新增subscription专用Context Worker durable driver：exact manifest scan后绑定`WorkClass::Context` permit，再claim、heartbeat并把
credential-free attempt交给typed Host backend；返回后只通过PostgreSQL Context owner提交outcome，另执行bounded expired-lease recovery。
domain/unit fixture覆盖CR-193 identity与closed failure mapping；fresh PostgreSQL 16覆盖heartbeat后的stale fence零写入和latest fence成功。
Host RPC composition和真实进程kill-window仍未完成。

## 24. 明确推迟的工作

- 跨地域 index replication 与主动容灾；
- 开放互联网 crawling；
- 跨租户 semantic cache/dedupe；
- 在线学习 ranking 和自动 embedding migration；
- 通用 federated query language；
- row-level lineage 的行业专用可视化；
- 对所有外部来源提供强 snapshot guarantee。

## 25. 未决问题

r331将Native query、Remote query与MCP subscription refresh按三个exact JobKind接入各自production Worker metrics surface；ContextDataset build仍由
独立owner/lane承担，不计入这些Worker backlog。固定role due/expired告警与runbook要求关联permit、PostgreSQL及对应adapter/Host依赖，禁止跨role
搬移或手工重放remote I/O。目标13/13、strict Clippy和静态部署/observability/redaction门禁通过；production scrape、fault与L4/L5仍按18取证。

CR-203 cross-review已确认Plan v5 publication identity不改变Context dispatch/result binding；r234已补齐expired-lease L2 owner recovery，独立production
Context Worker及NativeCatalog受限部署已经接线，r240关闭其真实多进程kill-window；remote backend protocol L3 evidence仍待完成。

CR-166已将CanonicalRegion和Context binding exact-match统一到02/12，Dataset build直接使用shared Job。本规范已
Accepted；Context backend、SQL adapter、Artifact与public API的分层fixture仍待实现。具体索引引擎、embedding provider
与reranker可以替换，但不得改变逐条授权、dataset view、observation、citation和只读边界。
