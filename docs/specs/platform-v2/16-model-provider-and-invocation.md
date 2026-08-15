# Platform v2 Model Provider 与 Invocation 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-15 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md)、[`06-durable-run-state-machine.md`](06-durable-run-state-machine.md)、[`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md)、[`10-capability-invocation.md`](10-capability-invocation.md)、[`15-artifacts-and-files.md`](15-artifacts-and-files.md) |
| 直接下游 | 13、17、18 |

> Persistence ruling：Provider/Profile/Deployment 使用共享 Resource；ModelTurn 是共享 Invocation，物理调用是 Job，usage/
> backend evidence 是 bounded snapshot 或 Event，不建立 Model 专用 lifecycle/turn/receipt 表族。

## 1. 决策摘要

Model 是独立执行隔舱，不是 Capability backend。Model Provider Revision 固定 adapter、protocol 与 credential
requirements；Model Provider Deployment 固定 endpoint、Secret、network、TLS 与 region policy；Model Profile Revision
固定模型身份、modalities、context、tool/structured-output 能力和数据策略；Model Deployment 固定 exact Provider
Deployment、Profile、预算和 policy。RunBindings 只能引用 exact Model Deployment 候选。

ModelLoop 的每一次推理调用是 durable ModelTurn，拥有 Attempt、lease、deadline、token/cost reservation、stream
assembly、output validation 和 first-winner。流式 delta 只是可丢失观察，只有完整、通过本地 schema 与 policy 的
terminal response 才能进入 Plan。Provider 内置 web/search/code/tool execution 默认禁止；需要的外部操作必须成为
平台 CapabilityInvocation。

外部模型通常无法保证权重/服务完全不可变。平台固定可见 provider model identity，并记录响应返回的 version/
fingerprint/evidence；若 Provider 发生不可验证漂移，明确标记为 external observation，不伪造 bitwise reproducibility。

## 2. 目标与非目标

### 2.1 目标

- 给 Provider、Model Profile、Deployment、Binding、Selection、ModelTurn 和 Attempt 完整机器合同；
- 统一 text/image/audio/document input、message、tool intent、structured output、usage 与 finish reason；
- 让模型流式响应不阻塞 durable authority，不因客户端断开丢失最终结果；
- 对 token、cost、request、并发、context window、tool round 和 Artifact transfer 实施硬预算；
- 在 dispatch 前固定数据区域、retention、training、Secret 和 Provider policy；
- 对 rate limit、timeout、retry、cancel、Provider drift 和 Worker crash 给出可恢复语义；
- 保证模型不能选择未绑定能力、伪造 tool result、输出 Secret 或修改平台 policy；
- 通过 adapter conformance 支持多个 Provider，而不把 Provider SDK 类型泄露给 Domain/Agent。

### 2.2 非目标

- 不把模型响应、tool intent、safety label 或 usage 当成可信授权事实；
- 不保证相同 prompt/retry 得到相同 token 或业务结果；
- 不保存或公开 Provider hidden reasoning、chain-of-thought、内部 logprobs 或安全系统正文；
- 不允许 Agent/Model input 覆盖 endpoint、credential、model ID、region、retention 或任意 Provider 参数；
- 不允许 Provider 直接执行未建模的 web、retrieval、code interpreter、computer use 或第三方 Tool；
- 不在 Runtime API 提供任意 Provider pass-through endpoint；
- 不承诺所有 Provider 支持完全相同的 streaming、cancel、seed、tool 或 schema 能力；
- 不定义模型训练、微调、评测数据生产、模型托管或 GPU inference cluster。

## 3. 术语与信任边界

| 术语 | 含义 |
|---|---|
| Provider Entity | 可管理的外部/内部模型服务身份 |
| Provider Revision | immutable adapter、credential requirements、request limits 和 wire protocol |
| Provider Deployment | Provider Revision 与 endpoint、Secret、network、TLS、region policy 的 exact binding |
| Model Profile Revision | 某 Provider 下一个 exact model identity 与平台能力声明 |
| Model Deployment | Provider Deployment + Profile + data/budget/policy 的 exact binding |
| Model Requirement | Agent/Skill 对 modality、context、tools、schema 和 policy 的需求 |
| Model Binding Set | Deployment 允许的 exact Model Deployment 候选及选择策略 |
| ModelTurn | ModelLoop 某一 round 的 durable 逻辑推理调用 |
| ModelAttempt | ModelTurn 的一次 Worker dispatch/Provider request |
| Model Observation | Provider 返回的 version、fingerprint、usage、safety 和 latency evidence |

Provider metadata、catalog、model description、response、tool intent、usage、finish reason、safety annotation、header 和
raw error 都是不受信任外部输入。Model Worker 可以解析 Provider wire 和 late-resolve Provider Secret，但不能推进
Run/Node、授权 Capability、发布 Revision 或改变绑定。

## 4. Model Requirement

```rust
struct ModelRequirement {
    required_modalities: BTreeSet<Modality>,
    minimum_context_tokens: u32,
    minimum_output_tokens: u32,
    tool_use: ToolUseRequirement,
    structured_output: StructuredOutputRequirement,
    streaming: StreamingRequirement,
    data_classification_ceiling: DataClassification,
    allowed_regions: BTreeSet<DataRegion>,
    provider_retention_ceiling: Duration,
    provider_training: ProviderTrainingPolicy,
    determinism: DeterminismRequirement,
}
```

Model requirement 是 Agent/Skill 的 interface slot，不含 provider 名、endpoint、credential 或具体 model string。
Deployment verifier 从已发布 Model Deployment 中选择满足全部 requirement/policy 的候选；运行时不能以 feature
fallback 静默弱化 modality、data policy、tool schema 或 context limit。

## 5. Provider Revision

```rust
struct ModelProviderRevision {
    provider_revision_id: RevisionId,
    provider_id: ModelProviderId,
    adapter: InstalledModelAdapter,
    protocol_profile_revision_id: RevisionId,
    credential_requirements: Vec<SecretPurpose>,
    request_limits: ProviderRequestLimits,
    semantic_digest: Digest,
}

struct ModelProviderDeployment {
    provider_deployment_id: DeploymentId,
    provider_revision_id: RevisionId,
    canonical_endpoint: CanonicalEndpoint,
    secret_bindings: Vec<ExactSecretBindingRef>,
    network_policy_revision_id: RevisionId,
    tls_policy_revision_id: RevisionId,
    provider_region_policy: ProviderRegionPolicy,
    conformance_evidence_id: EvidenceId,
    deployment_digest: Digest,
}
```

- Adapter 是平台安装、签名并报告 module digest 的静态实现，Registry 不接受用户动态库；
- Adapter不是新的tenant ResourceKind：Provider Revision固定qualified adapter name、signed WorkerManifest digest与
  adapter contract digest，validation/conformance必须证明候选worker manifest精确包含它；运行时worker manifest不匹配时
  fail closed，不能按adapter名称选择“当前版本”；
- Revision 只固定 adapter、protocol、credential requirements 与 request limits；
- endpoint canonicalization、TLS、redirect、proxy、DNS、auth、region 与 SecretBinding 由 Provider Deployment 固定；
- HTTP redirect 默认禁止，允许时逐 hop 重做 endpoint/network policy；
- Provider 原生 header/parameter allowlist 在 protocol profile 固定；
- Secret value 不进入 Revision/Deployment digest；04的`ExactSecretBindingRef`进入Deployment digest，Worker只通过
  受信Secret broker按exact generation/policy late resolve；
- Provider health、rate-limit window、circuit、credential revoke 和 suspension 是独立动态状态；
- adapter/protocol/credential requirement 改变必须新 Revision；endpoint/Secret/network/TLS/region 改变必须新
  Provider Deployment；已发布 row 都不能修改。

首版closed machine contract把`InstalledModelAdapter`冻结为qualified name、signed WorkerManifest digest与adapter contract
digest；`ProviderRequestLimits`逐项冻结request/response/message/part/tool/delta上限以及connect/first-byte/idle/total timeout。
credential requirements是按wire排序、无重复的`SecretPurpose`集合。任一字段缺失、为零、越过platform hard max，或局部timeout
不严格小于total timeout都fail closed。

## 6. Model Profile Revision

```rust
struct ModelProfileRevision {
    model_profile_revision_id: RevisionId,
    model_profile_id: ModelProfileId,
    provider_revision_id: RevisionId,
    provider_model_identity: ProviderModelIdentity,
    modalities: ModelModalities,
    context_contract: ContextWindowContract,
    tool_contract: ModelToolContract,
    structured_output_contract: StructuredOutputContract,
    generation_parameter_schema: ClosedJsonSchema,
    artifact_delivery_contract: ModelArtifactDeliveryContract,
    usage_contract: ModelUsageContract,
    data_handling: ProviderDataHandlingContract,
    model_limits: ModelLimits,
    catalog_evidence_id: EvidenceId,
    semantic_digest: Digest,
}
```

Provider model identity 是 exact published string/version/profile，不使用 `latest`、alias 或运行时 catalog lookup。
若 Provider 只提供可能漂移的别名，Profile 必须标记 `ExternallyMutable`，保存 discovery evidence/observed_at，
并在 response 中记录实际 version/fingerprint（若可用）。无法检测漂移时不得宣称 deterministic/reproducible。

Identity stability machine wire固定为`pinned | externally_mutable`；input/output modality统一消费
`text | image | audio | document`闭集，数组按wire value排序、无重复且input至少包含text。Tool intent、parallel tool、
native structured output和streaming使用显式boolean capability，不以未知Provider feature字符串扩展闭集。

`generation_parameter_schema` 只允许平台认可的 temperature、top-p、max output、stop、seed 等 bounded 参数；未知
Provider extension 不可由 Agent JSON 透传。

该schema以及Model structured output/tool arguments统一使用05的`insight.closed-json-schema/1`。Provider原生
schema dialect只由adapter从此profile做能力映射，永远不成为第二权威。

首版closed Profile payload必须逐字段保存`ProviderModelIdentity`、input/output `ModelModalities`、`ContextWindowContract`、
`ModelToolContract`、`StructuredOutputContract`、generation parameter schema digest、`ModelArtifactDeliveryContract`、
`ModelUsageContract`、`ProviderDataHandlingContract`、`ModelLimits`与bounded `ModelCatalogEvidence`。这些不是可选extension bag；
input modalities必须包含text，所有集合按wire排序且无重复，limit之间必须交叉验证。

## 7. Model Deployment 与 Binding

```rust
struct ModelDeployment {
    model_deployment_id: DeploymentId,
    model_profile_revision_id: RevisionId,
    provider_deployment_id: DeploymentId,
    data_policy_revision_ids: Vec<RevisionId>,
    budget_policy_revision_id: RevisionId,
    generation_defaults: ClosedJsonValue,
    public_projection_policy_revision_id: RevisionId,
    deployment_digest: Digest,
}

struct ModelBindingSet {
    candidates: Vec<ExactDeploymentRef>,
    selection_policy: ExactRevisionRef,
    model_slot_mappings: Vec<ModelSlotMapping>,
    binding_digest: Digest,
}
```

`ModelBindingSet`是authoring/deployment verifier视图，持久化时必须逐字段编码为02的
`FrozenSlotTarget::Model`；不能形成第二种Run binding schema。

- Agent Deployment 固定候选 Model Deployment 和 slot mapping；
- Model Deployment 的 Provider Deployment 必须引用与 Model Profile 相同的 Provider Revision，并通过 compatibility
  与 conformance 检查；
- Provider Deployment Policy closure固定`protocol/network/tls/trust/data`，其中protocol必须exact等于Provider Revision；
  Model Deployment固定`data/budget/public_projection`，一个Policy Revision不能填多个role；
- Run admission 复制 exact candidate IDs/digest，之后 active head/catalog变化不影响 Run；
- runtime selection 只在候选内，输入是 requirement、policy、remaining budget 和健康门；
- health/circuit 可以使候选不可用，但不会自动选择未绑定 Provider；
- 多候选选择必须使用已冻结 policy并保存 ModelSelectionReceipt；
- 自动 failover 只有全部候选、顺序/规则、data policy 和预算已冻结才允许；
- 已发送 request 后不能以 failover 重放可能仍在执行的 Attempt，必须先应用 retry/uncertainty规则。

Provider Deployment closure按角色分别冻结`protocol/network/tls/trust/data`五个exact Policy Revision、region与conformance
Artifact；同一Policy Revision不能兼任多个role。Model Deployment closure分别冻结`data/budget/public_projection`三个exact
Policy Revision与`ClosedJsonValue` generation defaults。`ClosedJsonValue`携带schema digest、canonical digest并执行统一bytes/depth/
object/array/string hard limit；Deployment不能只保存opaque digest后在dispatch时读取mutable defaults。

## 8. Catalog、Discovery 与发布

```text
Provider Draft
 -> author/protocol validation
 -> Provider Revision
 -> Provider Deployment Candidate/Resolution
 -> connectivity/auth/conformance validation
 -> Provider Deployment
 -> catalog discovery candidate
 -> Model Profile Draft
 -> capability/data/limit conformance
 -> Model Profile Revision
 -> Model Deployment
 -> Active Head / Suspension
```

- catalog discovery 只针对 exact Provider Deployment，是 bounded async management Operation，不持有 DB transaction；
- candidate 保存 Provider Deployment/Revision、source、digest、observed_at、expires_at、adapter/profile version 和 raw
  Artifact；
- description、pricing、context、modality 和 feature flags 必须验证，不能直接授权；
- discovery 不自动 publish/deploy/activate；
- catalog list changed 只产生新 candidate，不改写 Profile；
- conformance evidence 绑定 exact Provider Deployment，至少测试 message、stream、tool、schema、usage、timeout、cancel、
  oversize、safety 和 error mapping；
- evidence 过期可阻止新绑定或触发 suspension，不改写历史 RunBindings。

## 9. ModelTurn 模型

```rust
struct ModelTurn {
    model_turn_id: ModelTurnId,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    scope_instance_id: ScopeInstanceId,
    round_ordinal: u16,
    model_deployment_id: DeploymentId,
    state: ModelTurnState,
    request_ref: ValueRef,
    request_digest: Digest,
    output_ref: Option<ValueRef>,
    failure: Option<Failure>,
    usage_reservation_id: UsageReservationId,
    deadline: DateTime<Utc>,
    projection_version: u64,
}
```

`model_turn_id` 从稳定 NodeExecution/round identity 创建；同一 round 只允许一个逻辑 ModelTurn。Retry 保持
Job ID、递增 `attempt_count`/`lease_generation`，不会创建另一个 round。Tool result 回填后继续推理时创建下一
round，而不是修改已完成 ModelTurn request。Model 物理执行复用06的统一 Job generation 与 fence，不定义
Provider 专用物理尝试 current state。

## 10. 状态机

```rust
enum ModelTurnState {
    Created,
    AwaitingBudget,
    Ready,
    InFlight,
    RetryScheduled,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}
```

```text
Created -> AwaitingBudget | Ready | Failed
AwaitingBudget -> Ready | Failed | Cancelled | TimedOut
Ready -> InFlight | Cancelling | TimedOut
InFlight -> Succeeded | RetryScheduled | Failed | Cancelling | TimedOut
RetryScheduled -> Ready | Cancelling | TimedOut
Cancelling -> Cancelled | Failed | TimedOut
```

终态不可离开。`InFlight` 可以有 live delta，但没有 partial durable output。Provider request 不是业务外部 Effect，
但可能产生费用和数据传输；timeout/cancel 后迟到成功由 turn/attempt fence 拒绝，usage reconciliation 可以继续，
不能修改 Run 结果。

## 11. Request Assembly

```rust
struct CanonicalModelRequest {
    model_turn_id: ModelTurnId,
    messages: Vec<CanonicalMessage>,
    tools: Vec<ModelToolProjection>,
    response_contract: ModelResponseContract,
    artifact_inputs: Vec<ModelArtifactInput>,
    generation_parameters: ClosedJsonValue,
    max_output_tokens: u32,
    deadline: DateTime<Utc>,
    trace_context: SafeTraceContext,
}
```

Assembler 使用 05/11/12/15 的 fixed source map、trust tags、classification 和 token estimator。提交前必须：

1. 验证当前 Run/Node/round 和 exact Model binding；
2. 验证 Provider region/retention/training 与每个 message/Artifact classification；
3. 固定 tool name/schema/call limits；
4. 对 provider tokenizer/profile 计算 bounded input estimate；
5. 应用版本化 truncation/summarization policy；
6. reserve request/token/cost budget；
7. 保存 canonical request digest、source map 与安全 projection；
8. 写 Ready work/outbox。

无法在 context window 内安全装配时明确失败；不能静默删除 platform/Agent contract 或把 untrusted content 提升。

## 12. Message 与 Multimodal Contract

```rust
enum CanonicalMessagePart {
    Text(BoundedText),
    Image(ArtifactRef),
    Audio(ArtifactRef),
    Document(ArtifactRef),
    ToolResult(ModelToolResult),
}
```

- role 是平台 closed enum，Provider role 映射由 adapter 固定；
- untrusted Skill/Context/MCP/user content 保留来源边界，不伪装 platform role；
- Artifact 必须 Ready、tenant-matched、media/size/count/classification 合法；
- adapter 根据固定 contract 选择 inline bytes、Provider file upload 或 derived conversion；
- Provider file ID 加密保存为 backend handle，不进入 Model/用户/ArtifactRef；
- remote file retention、region 和 delete semantics 进入 deployment policy；
- unsupported modality 不做无声明降级；例如 image OCR 必须是显式 derived Artifact/Capability；
- message、part、text、image dimension/audio duration/document page 和 total bytes 有硬上限。

## 13. Tool Projection 与 Intent

Model request 只能包含 09 生成的固定 ModelToolProjection。Provider 返回：

```rust
struct ModelToolIntent {
    call_id: ModelCallId,
    projected_tool_name: ModelToolName,
    arguments: ClosedJsonValue,
}
```

- call ID 在 ModelTurn 内唯一且长度受限；缺失时 adapter生成稳定 transport-local ID并记录 evidence；
- tool name 必须精确命中 RunBindings/ModelLoop mapping；
- arguments 在创建 Invocation 前通过 Interface schema、size/depth/number validation；
- tool choice、parallel count、round/call budget 与 Effect/Approval 由平台决定；
- intent 只是 proposal，不能改变 Run/Capability state；
- Provider 内置 web/search/code/file/computer tool 默认关闭；需要时必须发布平台 Capability，并由平台执行；
- tool result 只来自 committed CapabilityInvocation，模型文本/Provider continuation不能伪造；
- Provider-specific server tool identifier、credential 和 backend handle 不进入 Plan Value。

## 14. Structured Output

- Agent/ModelLoop 的 ClosedJsonSchema 是最终权威；Provider native response format 只是约束加速；
- adapter 将 schema 映射到 Provider subset，不支持时可以使用平台批准的 bounded textual JSON mode，不能绕过本地校验；
- response 必须拒绝 duplicate key、unknown field、NaN/Infinity、深度/大小/集合越界和 trailing non-whitespace；
- repair 不是隐式字符串修改：需要修复时创建新的 ModelTurn round/Attempt policy并计入预算；
- Provider 宣称 schema success 但本地 validation 失败时 ModelTurn 失败或按固定 retry policy处理；
- structured output 与 tool intent 的互斥/组合语义由 Model Profile contract 固定；
- hidden reasoning 字段不进入 output schema，不向客户端映射。

## 15. Streaming

Streaming 由 Model Worker 异步消费，不占 Tokio thread。规范行为：

- 每个 delta 绑定 ModelTurn、Attempt、epoch 和 monotonic transport sequence；
- delta 通过 bytes/rate/part type/public policy 后才能发 `run.live`；
- live delta 可丢失、合并或截断，不进入 durable transition ledger；
- tool arguments 可以增量接收，但完整解析/校验前不创建 Invocation；
- Worker 在内存使用 bounded assembler，超限立即 cancel/fail；
- 只有 Provider terminal frame/EOF contract、finish reason、完整 message 和 usage 通过校验后才提交；
- 客户端 SSE 断开不取消 ModelTurn；显式 Run cancel 才传播；
- Worker crash 丢失 partial delta，Attempt lease/retry 决定后续，不尝试从客户端重构；
- public stream 最终以 durable `model.completed/failed` 或 Run snapshot 校准。

## 16. Response Normalization 与 Commit

```rust
struct CanonicalModelResponse {
    message: Option<CanonicalAssistantMessage>,
    tool_intents: Vec<ModelToolIntent>,
    finish_reason: CanonicalFinishReason,
    usage: ModelUsage,
    observation: ModelObservation,
}
```

Commit 前验证：current attempt/epoch、response bytes/parts、role、tool calls、schema、finish reason、usage bounds、
Artifact handles、safety/data policy 和 model fingerprint。成功事务同时写 ModelTurn output/usage、Node/ModelLoop wake、
budget settlement 和 outbox。重复 terminal frame返回已有 receipt。

Finish reason 是 closed enum：`Completed`、`ToolUse`、`Length`、`ContentFiltered`、`CancelledByProvider`、
`ProviderError`。未知值映射 stable protocol failure；`Length` 不能被伪装为合法完整 JSON。

## 17. Usage、Budget 与 Cost

```rust
struct ModelUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    provider_reported_cost: Option<DecimalMoney>,
    accounting_quality: AccountingQuality,
}
```

- dispatch 前 reserve request、input estimate、max output、cost ceiling 和 concurrency unit；
- `usage_reservation_id`引用04唯一共享Quota reservation bundle，不创建Model专用reservation/settlement authority；一个
  ModelTurn的envelope可按Attempt追加`Consume` settlement，terminal commit以`Close`消费最终actual并释放余额；
- Provider usage 仅为 observation，平台验证非负、上限、currency/profile 和明显异常；
- Provider 未报告 usage 时使用固定 estimator并标记 Estimated；
- reasoning token 只记录数量，不记录 hidden content；
- actual settlement 不能超过 policy ceiling而继续同 Run；超额形成 quota incident/stop；
- retry/failover 每次 Attempt 单独计费并消耗同一 ModelTurn/Run budget；
- cache discount 不提升 token/context limit；
- price catalog 是版本化 accounting input，不参与 Provider授权；
- billing export 与业务 Run terminal 分离，失败不能改写模型输出。

## 18. Retry、幂等与不确定性

模型推理逻辑上是 Pure，但非确定且可重复计费。Retry 交集：

```text
Node retry policy
∩ Provider/Profile conformance
∩ remaining turn/run deadline
∩ remaining request/token/cost budget
∩ data/safety policy
```

- 同一 ModelTurn retry 保持 canonical request digest、binding 和 Job ID，递增 `attempt_count`/`lease_generation`；
- Provider idempotency key 若支持则从 ModelTurn派生，但平台不依赖其保证；
- connect 前明确失败可安全 retry；dispatch 后断线可 retry推理，但必须记录可能重复费用/数据传输；
- partial output永不作为 retry input，除非明确成为下一 round的 committed message；
- retry结果可能不同，first committed terminal winner 成为权威；
- Provider late response 只用于 usage/audit，不覆盖 terminal；
- failover 到另一已绑定模型视为新 Attempt并记录模型变化，只有 selection policy显式允许；
- safety/content-filter failure 是否 retry 由固定 policy决定，不能无限改写规避安全门。

## 19. Timeout、取消与恢复

- queue、connect、first-byte、idle、total turn和Run deadline分离；总 deadline不能被delta无限延长；
- cancel intent提升 ModelTurn generation，Ready/Retry可直接取消，InFlight调用adapter cancel/abort；
- provider cancel是best-effort，平台以attempt fence决定terminal；
- Model Worker shutdown先drain stream，在grace后停止heartbeat让lease接管；
- crash后如果没有committed output，按retry/budget policy创建新Attempt；
- response已收完但DB commit前crash，重复provider调用可能产生不同输出，仍由first committed attempt决定；
- usage reconciliation可在Run terminal后继续，但不能改变Run/ModelTurn output；
- active head/catalog/price变化不改变existing turn binding/reservation；
- NATS 丢失由 Ready/Retry/deadline safety scan 恢复。

## 20. Safety 与内容策略

- Provider safety signal是untrusted evidence，平台policy可以收紧，不能被Provider metadata放宽；
- request/response classification、DLP、allowed modality、age/tenant policy在发送前后执行；
- Prompt injection不是Provider授权，模型输出中的指令/URL/tool name仍需平台验证；
- ContentFiltered映射稳定safe failure或Agent可处理结果，由NodePolicy固定；
- 原始moderation正文、Provider policy文本和用户敏感内容不进入public error；
- model output通过Secret canary、Artifact/data-flow和public projection policy；
- hidden reasoning、internal safety chain、logprobs默认丢弃，不写DB/Artifact/trace；
- break-glass diagnostic需要独立permission、短retention encrypted Artifact和audit。

## 21. Tenant、数据与 Secret

- Provider/Model/Deployment/Turn/Attempt/usage/cache/handle都tenant-scoped；
- Provider Secret通过exact SecretBinding按声明SecretPurpose late resolve，不进入request ValueRef、DB、event、trace或error；
- per-tenant/per-principal credential不会跨安全域连接复用；
- 数据发送前验证Provider、region、retention、training、subprocessor和classification；
- Provider不能得到平台tenant ID、内部object key、SecretRef或未必要的Run metadata；
- Artifact upload/file handle绑定Provider、tenant、Turn、digest、deadline，结束后按retention删除/revoke；
- credential revoke/suspension阻止新Attempt，已InFlight按kill policy cancel/drain；
- endpoint执行SSRF/DNS/TLS/redirect/proxy限制；
- data residency失败或未知provider region时fail closed。

## 22. Provider Prompt Cache

- cache只作为Provider性能特性，不是平台durable状态；
- cache policy固定允许的message前缀、classification、tenant隔离、TTL和Provider；
- cache key/handle加密保存，不向Agent/用户/其他tenant暴露；
- 不跨tenant共享含用户/Skill/Context/Tool result的cache；
- active binding/profile/source digest变化产生新cache identity；
- cache hit是Provider observation，不能跳过request assembly/data policy/token budget；
- credential/policy revoke使新request不再使用旧cache；
- cache miss不会改变语义或回退到未绑定Model。

## 23. 并发与背压

Model permit层级：

```text
global Model work
 -> Provider Deployment
   -> Model Deployment/Profile
     -> tenant
       -> Run/ModelLoop
```

- request、stream connection、token-throughput和cost reservation分别计量；
- 每Provider Deployment/endpoint有connection pool、rate-limit state、circuit和bounded queue；
- `429/Retry-After`保存durable retry_at并释放Worker/connection；
- 等待 budget/rate-limit/deadline 不持有 execution permit；
- large Artifact upload与Model request使用不同IO permit；
- tenant公平调度，单tenant/Provider backlog不阻塞其他Provider；
- Model饱和不占用API/Scheduler/Sandbox/MCP/Context permit；
- critical cancel/usage reconciliation使用保留capacity；
- autoscaling依据ready age、active streams、token throughput、connection utilization，而非只看CPU。

## 24. 所有权接口

```rust
trait ModelProviderAdapter {
    async fn invoke(&self, request: ProviderModelRequest) -> ProviderModelStream;
    async fn cancel(&self, request: ProviderCancelRequest) -> ProviderCancelOutcome;
    async fn discover(&self, request: ProviderDiscoveryRequest) -> ProviderCatalogCandidate;
}

trait ModelTurnRepository {
    async fn create(&self, command: CreateModelTurn) -> ModelTurnReceipt;
    async fn claim(&self, claim: ModelWorkClaim) -> Vec<LeasedModelTurn>;
    async fn commit(&self, command: CommitModelOutcome) -> CommitReceipt;
}
```

Adapter返回闭合normalized frame/failure，不接触Run repository。Domain crate不依赖Provider SDK/HTTP。Worker
负责wire/stream；repository负责state/fence/budget/outbox；orchestrator负责ModelLoop纯决策。

## 25. Persistence、Artifact 与事件

Provider、Profile 与 Deployment 使用共享 Resource/ResourceVersion/Deployment。ModelTurn 是
`InvocationKind::Model`；selection、canonical request digest、usage observation、provider handle 与安全 projection 保存在
Invocation/Job 的 bounded typed payload，物理调用、stream 与 recovery 使用 Job。超限 request/response 写入 Artifact，
usage settlement 使用共享 quota ledger，历史进入 Event。不得建立 Model 专用 lifecycle、turn、usage 或 handle 表族。

Public event最小集合：

```text
model.started
model.delta
model.tool_intent
model.completed
model.failed
model.cancelled
model.timed_out
```

`model.delta` 是 live-only hint；其他来自 committed outbox。默认事件不含 prompt、delta、response、tool arguments、
Provider/model name、usage cost或raw error。

## 26. 可观测性与隐私

```text
model_turns_total{provider_class,outcome}
model_turn_duration_seconds{provider_class,outcome}
model_time_to_first_token_seconds{provider_class,outcome}
model_tokens_total{direction,accounting_quality}
model_attempts_total{provider_class,outcome}
model_rate_limit_total{provider_class}
model_output_rejected_total{reason_class}
model_budget_wait_seconds{budget_class}
model_circuit_state{provider_class,state}
```

tenant/Provider/model/Run/Turn/endpoint/prompt不进入metric label。Trace只记录受控binding hash、attempt、latency、
byte/token count、finish/failure class，不记录message/delta/response/Secret。审计覆盖Provider/Profile publish、
Deployment/activate/suspend、credential grant、high-risk data transfer和break-glass。

## 27. 配置与部署

- Model Worker是独立Deployment、service account、DB/HTTP pool、queue和HPA；
- Provider adapter随signed Worker image安装，startup报告manifest/digest；
- 不同data region/high-sensitivity Provider可使用独立Worker pool；
- Provider endpoint/auth不来自环境自由字符串，只来自immutable Provider Deployment；model identity只来自immutable
  Model Profile Revision；
- readiness依赖PostgreSQL/Secret resolver和至少一个符合manifest的worker，不依赖所有Provider健康；
- 单Provider circuit open不使Model Worker或Runtime API全局unready；
- rolling deploy按adapter digest drain，旧binding需要的adapter在历史work清空前保留；
- hard request/token/bytes/deadline/queue上限只能由tenant/deployment收紧。

## 28. 测试矩阵与验收标准

- 至少两个Provider adapter通过同一message/stream/tool/schema/usage/error conformance fixture；
- catalog discovery不会自动publish/deploy/activate，model alias漂移可检测/标记；
- active head/catalog切换不改变existing Run/Turn binding；
- unknownProvider field、finish reason、tool/schema、usage和oversized delta fail closed；
- stream Worker kill、duplicate/late frame、timeout/cancel/retry竞态只有一个terminal output；
- 客户端SSE断开不取消Turn，live delta丢失后durableterminal可校准；
- Provider native schema success但本地invalid不能进入Plan；
- 未绑定tool、伪造tool result、Provider built-in tool无法执行；
- retry保持request digest/binding并单独结算usage，可能不同输出只有first-winner；
- token/cost/request/concurrency budget在并发和crash下不超卖；
- high-classification Artifact无法发送到不合规Provider/region；
- Secret、prompt、response、hidden reasoning、file/cache handle不进入public event/metric/default log；
- Model饱和或单Provider `429`不影响API/Scheduler/Sandbox/MCP/Context准入；
- credential revoke/provider suspension在限定窗口阻止新Attempt并有审计。

### 28.1 当前实施证据边界（非规范性）

CR-124对应的Resource foundation已经交付：`insight-platform-contracts`为Provider/Profile/两级Deployment提供上述closed Rust
payload、canonical generation defaults、sorted-set与cross-field hard-limit验证。CR-125进一步交付`insight-platform-models`的
canonical request/response、stream fence、tool/schema validation、retry/control/cancellation与attempt accounting，以及caller-owned
PostgreSQL adapter；shared Invocation/Job/RunValue/ArtifactLink/Receipt/Event/Outbox和四维Quota bundle均未增加专用表。fresh PostgreSQL 16
fixture证明invalid local schema rollback、retry新reservation、tool-intent、stale fence和cancel/completion first-winner；strict Clippy、
schema contract及23表/单一`0001`保持通过。该证据关闭Phase 3的ModelTurn domain/repository交付项，不替代Phase 4 Provider adapter、
Phase 5 public API或Phase 6 qualification，因此不能把文档16整体标记为Implemented或Verified。

CR-132进一步交付Phase 4的首个adapter-host slice：独立`insight-platform-model-adapters`按完整signed adapter descriptor做
exact process-local resolution，消费closed normalized stream并强制Provider级delta/first-byte/idle/total timeout、sequence、terminal、
response local validation、cancel与panic containment。worker materializer和PostgreSQL authority之间只有fenced
`CommitModelOutcome`；claim显式返回fence、usage reservation、quota ledger identity与exact request input，后者逐字段回绑冻结
RunValue；Inline正文复核canonical digest，Artifact-backed只暴露已验证的ArtifactLink identity。terminal command还在Provider I/O前
拒绝复用reservation ledger identity。独立`insight-platform-model-worker`现在先预留本地Model permit再claim，逐项回绑WorkerManifest、
Job/lease/request/quota identity，并在materialize与Provider stream期间按统一HardLimitProfile heartbeat；heartbeat只推进Job version，
已经规范化的响应刷新到新fence后再提交，不因续租重放Provider调用。未知dispatch按冻结token/cost ceiling保守结算。
OpenAI Responses与Anthropic Messages现各有独立production wire adapter：它们从同一canonical request映射固定endpoint/protocol body，
通过credential-free `ModelProviderWireConnector`消费bounded SSE event，隐藏reasoning，且把text、tool arguments、structured output、usage与
terminal归一为同一closed stream。共同fixture覆盖message/stream/tool/schema/usage、请求digest与未知Provider field fail-closed；原有host/
worker之外又增加incremental SSE与brokered connector：raw body按总字节上限分帧，SSE field/event type歧义、重复JSON key、非法content-type、
未知HTTP status和`[DONE]`后字节均fail closed；429/选定5xx映射为dispatch后可重试。Inline output materializer还会根据冻结的Provider
最大响应字节和closed envelope开销，在Provider dispatch前拒绝必须使用Artifact的请求；fixture证明未调用Provider、未生成usage。
20项fixture与strict Clippy通过。connector只接收exact
`ExactSecretBindingRef`、endpoint identity digest和冻结network/TLS/trust/data policy，不接收Secret value、任意URL或调用方header；role-scoped
Egress broker负责late Secret resolution、DNS/network/TLS/redirect policy并只返回sanitized status/content-type/raw bounded stream。生产broker
首片已经由独立`insight-platform-egress` crate实现process-installed exact endpoint catalog、全量DNS answer验证与per-request连接pinning、
public-IP SSRF deny、HTTPS-only、no-proxy/no-redirect、Pinned/Follow Secret evidence校验、固定OpenAI/Anthropic auth header、请求/响应字节
限制和exact in-flight cancellation。CR-143进一步交付共享Secret resolution组合内核，串联current revoke/generation门、KMS/AEAD
reference解封与digest、process-installed Provider catalog、独立permit/总超时和actual version evidence，但具体KMS/Secret Manager Provider仍未交付。
独立`platform-model-worker`候选进程现把exact双adapter manifest、独立bounded PostgreSQL pool、schema verify、Model driver与Model Worker
身份的mTLS Egress RPC组合起来；候选镜像和独立namespace/ServiceAccount/Deployment/PDB/HPA/default-deny NetworkPolicy已通过静态正负向门禁，
Pod没有Service/Ingress、云Provider credential、Kubernetes API token或直接Provider客户端。该进程当前明确只安装Inline request/output
materializer。真实Secret Manager provider、catalog provisioning、Artifact-backed request/output IO、live-delta/cancel控制组合、
real-process Provider conformance、跨work-class饱和隔舱和Phase 6 fault fixture仍未交付，因此CR-132/CR-136和本规范状态保持进行中。

Provider wire request还必须冻结`JobId`、`attempt_no`、`lease_generation`和`WorkerProcessGenerationId`，并与
ModelTurn、tenant、Provider Deployment和request digest共同形成一次物理请求identity。Egress broker只允许exact identity注册一个
in-flight request；cancel必须携带相同完整identity并且只能终止该generation，旧lease、旧Worker或相邻tenant的cancel均fail closed。
这一进程内索引只负责bounded连接取消，不是Job/Attempt current-state authority，进程丢失后仍由PostgreSQL lease/recovery收敛。

## 29. 明确推迟的工作

- 模型训练、微调、评测平台和dataset生产；
- 自托管GPU inference scheduler；
- Provider内置web/search/code/computer tools直接启用；
- speculative decoding/multi-model racing/ensemble；
- cross-Provider semantic response cache；
- hidden reasoning存储或展示；
- 自动price arbitrage和未绑定Model failover；
- bitwise deterministic外部模型保证。

## 30. 未决问题

没有阻止API或Qualification设计的未决问题。Provider adapter和模型能力可以通过新Revision扩展，但不能改变
durable ModelTurn、本地schema/tool验证、exact binding、数据策略和独立Model隔舱。
