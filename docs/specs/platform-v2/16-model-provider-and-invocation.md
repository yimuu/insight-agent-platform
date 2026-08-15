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

完整 canonical response 超过冻结 Inline threshold 时必须保存为 Artifact-backed `RunValue`，不能拆分正文、截断后伪装成功，
也不能把 `ArtifactRef` metadata 当作逻辑 response 进行 schema validation。Model claim 在 Provider dispatch 前冻结 exact
output schema/classification、Retention/ArtifactIo Policy、HardLimitProfile、最坏 Artifact bytes、一次物理 Attempt 的 Artifact/
candidate Blob/RunValue/Link/Receipt identity 与Artifact-owned count/logical、candidate-Blob-owned upload/staging/physical两个quota bundle。
实际 response 可 Inline 时原子释放两个未使用bundle；
需要 Artifact 时只经独立 Model Artifact Producer 写入 staging。Producer 与只读 Model Artifact Broker 是两个不同进程和权限角色，
只能把 exact-attempt-bound bytes 推进到 `Verified`，不能推进 ModelTurn、Run/Node、Artifact `Ready` 或创建 output `RunValue`。

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
| Model Artifact Broker | 只读物化 Artifact-backed Model request 的独立受信服务 |
| Model Artifact Producer | 只为 exact Model Attempt 接收 canonical output stream，并把预留 Artifact 推进到 Verified 的独立受信服务 |

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

`ModelArtifactDeliveryContract`只描述 Model request 中显式 image/audio/document Artifact 如何交给 Provider；它不决定完整
canonical response 因 Inline threshold 而采用的存储形状，也不授权 Provider 或 Model Worker 创建 output Artifact。完整逻辑
response 的 Inline/Artifact选择只由本规范的 output materialization closure、15的Artifact状态机与18的HardLimitProfile决定。

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
    model_output_retention_policy_revision_id: RevisionId,
    model_output_artifact_io_policy_revision_id: RevisionId,
    model_output_ready_retention_seconds: u64,
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
  Model Deployment固定`data/budget/public_projection/model_output_retention/model_output_artifact_io`，一个Policy Revision不能填
  多个role；后两者必须分别引用04的exact `Retention`与`ArtifactIo` Policy Revision；
- Run admission 复制 exact candidate IDs/digest，之后 active head/catalog变化不影响 Run；
- runtime selection 只在候选内，输入是 requirement、policy、remaining budget 和健康门；
- health/circuit 可以使候选不可用，但不会自动选择未绑定 Provider；
- 多候选选择必须使用已冻结 policy并保存 ModelSelectionReceipt；
- 自动 failover 只有全部候选、顺序/规则、data policy 和预算已冻结才允许；
- 已发送 request 后不能以 failover 重放可能仍在执行的 Attempt，必须先应用 retry/uncertainty规则。

Provider Deployment closure按角色分别冻结`protocol/network/tls/trust/data`五个exact Policy Revision、region与conformance
Artifact；同一Policy Revision不能兼任多个role。Model Deployment closure分别冻结
`data/budget/public_projection/model_output_retention/model_output_artifact_io`五个exact Policy Revision与`ClosedJsonValue`
generation defaults。Retention closure必须给出15要求的Ready `RunOutput` minimum retention、tombstone与hold规则；Deployment的
`model_output_ready_retention_seconds`必须为正、不短于该minimum且不长于18 effective `artifact.ready_retention_seconds`。ArtifactIo
closure必须是04 exact `ModelOutputArtifactIoPolicyDocument`，固定staging grace、唯一verified media、classification ceiling、maximum
materialized bytes、storage/encryption binding与content-validation contract。`ClosedJsonValue`携带schema digest、canonical digest并执行统一
bytes/depth/object/array/string hard limit；Deployment不能只
保存opaque digest后在dispatch时读取mutable defaults，运行时也不能重新追随任一Policy active head。

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

### 16.1 Artifact-backed output admission

Model start transaction先计算冻结合法response上限与effective Inline threshold。前者不大于后者时冻结`InlineOnly`；只有
response合同可能越过Inline threshold时，才创建完整Artifact reservation。不能为了实现方便让全部小响应强制写对象存储，也不能等
付费请求返回后再判断本次输出是否有Artifact身份、存储容量或retention权限：

```rust
enum ModelOutputStorageReservation {
    InlineOnly,
    ArtifactCapable(ModelOutputArtifactReservation),
}
```

```rust
struct ModelOutputArtifactReservation {
    schema_version: u32,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    model_turn_id: ModelTurnId,
    expected_model_turn_version: u64,
    job_id: JobId,
    claim_job_version: u64,
    attempt_no: u32,
    lease_generation: u64,
    lease_token_digest: Digest,
    worker_process_generation_id: WorkerProcessGenerationId,
    admission_digest: Digest,
    request_digest: Digest,
    model_deployment_digest: Digest,
    hard_limit_profile_digest: Digest,
    output_schema_digest: Digest,
    output_classification: DataClassification,
    artifact_id: ArtifactId,
    candidate_blob_id: InternalBlobId,
    duplicate_blob_cleanup_job_id: JobId,
    blob_security_domain_digest: Digest,
    output_value_id: RunValueId,
    output_link_id: ArtifactLinkId,
    upload_grant_id: ArtifactGrantId,
    stage_receipt_id: ReceiptId,
    artifact_quota: ModelOutputArtifactQuotaIdentities,
    maximum_materialized_bytes: u64,
    maximum_chunk_bytes: u32,
    verified_media_type: MediaType,
    retention_policy_revision_id: RevisionId,
    artifact_io_policy_revision_id: RevisionId,
    storage_binding_digest: Digest,
    maximum_put_completion_uncertainty_milliseconds: u64,
    staging_retain_until: DateTime<Utc>,
    ready_retention_seconds: u64,
    deadline: DateTime<Utc>,
    reservation_digest: Digest,
}

struct ModelOutputArtifactQuotaIdentities {
    artifact_bundle: ModelOutputArtifactBundleQuotaIdentities,
    candidate_blob_bundle: ModelOutputCandidateBlobBundleQuotaIdentities,
}

struct ModelOutputArtifactBundleQuotaIdentities {
    reservation_id: UsageReservationId,
    artifact_count_line_id: QuotaLedgerEntryId,
    logical_bytes_line_id: QuotaLedgerEntryId,
    ready_consume_settlement_id: QuotaLedgerEntryId,
    nonready_close_settlement_id: QuotaLedgerEntryId,
    artifact_delete_refund_settlement_id: QuotaLedgerEntryId,
}

struct ModelOutputCandidateBlobBundleQuotaIdentities {
    reservation_id: UsageReservationId,
    uploads_line_id: QuotaLedgerEntryId,
    staging_bytes_line_id: QuotaLedgerEntryId,
    physical_bytes_line_id: QuotaLedgerEntryId,
    owner_terminal_settlement_id: QuotaLedgerEntryId,
    no_object_close_settlement_id: QuotaLedgerEntryId,
    candidate_cleanup_settlement_id: QuotaLedgerEntryId,
    blob_delete_refund_settlement_id: QuotaLedgerEntryId,
}
```

两个bundle及其中line按04 fixed owner/dimension顺序canonical编码；Artifact bundle只服务count/logical与Artifact deletion，candidate Blob
bundle只服务upload/staging/physical、candidate cleanup与最后alias物理删除。所有ID必须互异并与各自owner、Attempt及reservation digest
绑定。自由`Vec`、运行时临时生成settlement ID、dedupe后把candidate bundle转给resolved Blob或把一个ID跨owner generation复用均非法；
未用到的预留ID保留为空洞，不得改作其他ledger操作。

Producer数据库读取只能物化以下closed、row-scoped projection；调用方必须提供全部exact key，repository不得暴露generic SQL/filter或
prompt/request/output正文：

```rust
struct ModelOutputStageAuthorizationProjection {
    schema_version: u32,
    tenant_id: TenantId,
    model_turn_id: ModelTurnId,
    model_turn_state: ModelTurnState,
    model_turn_version: u64,
    job_fence: JobFence,
    request_digest: Digest,
    binding_digest: Digest,
    reservation_digest: Digest,
    artifact_id: ArtifactId,
    artifact_state: ArtifactState,
    artifact_version: u64,
    bound_blob_id: Option<InternalBlobId>,
    blob_state_version_generation: Option<BlobStageProjection>,
    exact_reusable_verified_blob: Option<ReusableBlobStageProjection>,
    grant_id_state_version_digest: ArtifactGrantStageProjection,
    retention_policy_revision_id: RevisionId,
    retention_policy_digest: Digest,
    artifact_io_policy_revision_id: RevisionId,
    artifact_io_policy_digest: Digest,
    artifact_quota_state_generation_limit_digest: QuotaStageProjection,
    candidate_blob_quota_state_generation_limit_digest: QuotaStageProjection,
    stage_receipt_id: ReceiptId,
    stage_receipt_state_claim_digest: Option<ReceiptStageProjection>,
    deadline: DateTime<Utc>,
}
```

这些嵌套projection同样是closed nominal type，只含完成授权/CAS所需的state、version、generation、digest、limit和opaque locator
ciphertext；不含Principal资料、RunValue正文、Provider request/response、Policy外的Secret或可枚举object key。Producer role只能通过
固定repository query/受限column projection按exact tenant/Turn/Job/Artifact/Receipt加载它，不能拥有任意Run/Invocation/Quota/Event查询API。

`ArtifactCapable`事务在一个savepoint内重验当前Run/Node/Turn、exact Model binding、Job/lease/Worker generation、remaining deadline、
Retention/ArtifactIo closure与HardLimitProfile，并只使用本事务PostgreSQL `db_now`按04公式冻结`staging_retain_until`与
Model Deployment的`ready_retention_seconds`；intent当前`retain_until`必须等于前者，Ready absolute time此时尚不存在。
事务同时创建audience为exact Model Artifact Producer的
`WriteStaging | CommitStaging` grant、预留的candidate Blob/duplicate-cleanup Job/RunValue/Output Link/Receipt identity和04 exact
Artifact-owned count/logical、candidate-Blob-owned upload/staging/physical两个quota bundle；Model Worker只有调用
Producer的mTLS权限，不取得write bearer。此时Artifact允许`blob_id=NULL`，物理
Blob只能由Producer在首次stage时按预留`candidate_blob_id`建立，或在完整stream验证后绑定同安全域existing Verified Blob。
`blob_security_domain_digest`必须由tenant、classification、Retention、storage与encryption closure canonical派生，不能由Worker覆盖。
`maximum_materialized_bytes`是冻结Provider
response上限、canonical response envelope上限、Model response hard limit、Artifact single/staging limit与Run剩余预算的checked
intersection；所有加法/转换溢出都fail closed。两个output quota bundle按该最坏值在同一savepoint预留，不能仅按期望平均输出或
Inline threshold预留。

同一物理Attempt重放必须返回同一reservation；不同request/binding/fence/digest返回idempotency conflict。Retry/failover的新
Attempt创建新的Artifact、Link、Receipt、grant与quota identity，不能继承上一Attempt的可写能力。若无法创建完整reservation，
ModelTurn保持未dispatch，Provider不得被调用，也不得生成Provider usage或“可能已发送”的evidence。

这项预留只决定平台是否能够保存冻结合同允许的任意合法response。实际完整canonical response不超过effective Inline threshold时，
terminal事务只有在证明预留Artifact未绑定Blob/locator且candidate/object不可能存在时，才创建Inline RunValue、以零actual关闭并释放两个
output quota bundle、撤销grant，
并把未使用的Staging intent推进`Deleting`交给15的
GC；不得为了已预留Artifact而强制小值走对象存储。超过Inline threshold但不超过reservation时必须走Artifact，不能返回
`model_output_artifact_required`。response超过`maximum_materialized_bytes`或ArtifactIo/hard limit时是稳定content rejection，不能
截断、拆成多个未建模值或提高本次上限。

### 16.2 Model Artifact Producer wire 与权限

Model Artifact Producer只暴露versioned client-streaming `StageModelOutput`。wire是`Header -> Data+ -> Terminal`三variant闭集；
不定义metadata map、keepalive或`FenceRefresh` frame。首帧必须是唯一header，随后只能出现严格递增且单chunk/总bytes有界的data，
最后是携带客户端最后观察到的fence lower bound的唯一terminal；header、data或terminal之后的额外frame、sequence gap、fence generation/token变化、version倒退、
重复字段、长度/digest漂移和未知variant/enum全部fail closed：

```rust
enum StageModelOutputFrame {
    Header(StageModelOutputHeader),
    Data(StageModelOutputData),
    Terminal(StageModelOutputTerminal),
}

const MODEL_OUTPUT_PROTOBUF_ENVELOPE_OVERHEAD_BYTES: u64 = 4096;

struct StageModelOutputHeader {
    schema_version: u32,
    reservation: ModelOutputArtifactReservation,
    initial_fence: JobFence,
    content_digest: Digest,
    byte_length: u64,
    media_type: MediaType,
    classification: DataClassification,
    output_schema_digest: Digest,
    validation_evidence_digest: Digest,
    stage_request_digest: Digest,
}

struct StageModelOutputData {
    schema_version: u32,
    sequence: u32,
    bytes: BoundedBytes,
    chunk_digest: Digest,
}

struct StageModelOutputTerminal {
    schema_version: u32,
    final_fence: JobFence,
    chunk_count: u32,
    byte_length: u64,
    content_digest: Digest,
    stage_request_digest: Digest,
}

struct StageModelOutputReceipt {
    schema_version: u32,
    receipt_id: ReceiptId,
    artifact_id: ArtifactId,
    artifact_version: u64,
    candidate_blob_id: InternalBlobId,
    resolved_blob_id: InternalBlobId,
    blob_disposition: StageModelOutputBlobDisposition,
    object_generation_digest: Digest,
    new_physical_bytes: u64,
    candidate_cleanup_bytes: u64,
    candidate_cleanup_object_generation_digest: Option<Digest>,
    candidate_cleanup_job_id: Option<JobId>,
    content_digest: Digest,
    byte_length: u64,
    media_type: MediaType,
    classification: DataClassification,
    validation_evidence_digest: Digest,
    stage_request_digest: Digest,
    receipt_digest: Digest,
}

enum StageModelOutputBlobDisposition {
    PreexistingHit,
    CandidateWinner,
    RacingCandidateLoser,
}

enum StageModelOutputFailureReason {
    InProgress,
    ArtifactTooLarge,
    ArtifactInvalid,
    StaleFence,
    IdempotencyConflict,
    DependencyUnavailable,
    IntegrityFailure,
    DeadlineExceeded,
}

enum StageModelOutputFailureDisposition {
    RetrySameAttempt,
    RejectResponse,
    RejectStale,
    Conflict,
    IntegrityIncident,
}

enum StageModelOutputFailure {
    Terminal(StageModelOutputTerminalFailure),
    Transient(StageModelOutputTransientFailure),
}

struct StageModelOutputTerminalFailure {
    schema_version: u32,
    stage_receipt_id: ReceiptId,
    reason: StageModelOutputFailureReason,
    disposition: StageModelOutputFailureDisposition,
    safe_evidence_digest: Option<Digest>,
    receipt_digest: Digest,
}

struct StageModelOutputTransientFailure {
    schema_version: u32,
    stage_receipt_id: ReceiptId,
    reason: StageModelOutputFailureReason,
    disposition: StageModelOutputFailureDisposition,
    retry_after_milliseconds: Option<u32>,
}
```

Blob disposition cross-field规则是closed：`CandidateWinner`要求candidate=resolved、`new_physical_bytes=byte_length`且所有cleanup字段为
zero/None；`PreexistingHit`要求candidate!=resolved、candidate未创建、`new_physical_bytes=0`且cleanup字段为zero/None；
`RacingCandidateLoser`要求candidate!=resolved、`new_physical_bytes=0`、cleanup bytes等于byte length且generation digest/预留cleanup Job ID
均为Some。resolved Blob必须是同tenant/security-domain Verified winner；不能用disposition字符串绕过Blob唯一键或quota settlement。

每个data frame必须非空且不超过reservation的`maximum_chunk_bytes`；除最后一个data外，每片必须恰好等于该值，一旦出现短片，
下一帧只能是terminal。`sequence`从零开始逐一递增，`chunk_digest`逐片复算，terminal的`chunk_count`必须等于实际片数。总bytes同时受
声明`byte_length`、`maximum_materialized_bytes`、服务端累计payload counter与absolute deadline约束；gRPC receive limit只约束每条
encoded Header/Data/Terminal message，配置时必须为protobuf envelope预留开销，不能误当累计stream上限。因此调用方不能用碎片化、
空片或底层HTTP/2 flow-control buffer绕过容量门禁。`maximum_chunk_bytes`来自18冻结的HardLimitProfile，必须为正且不大于扣除
envelope开销后的wire message hard maximum。
该4096-byte envelope overhead是proto/schema生成与server receive-limit fixture共同验证的machine constant并进入contract digest，不是部署
自由值；每条encoded Header/Data/Terminal超过相应payload bound加该常量都在解码前拒绝。

容量准入分两阶段。exact TLS/service-role authorization后、读取bounded header前先取得18 ComponentCapacityManifest冻结的global stream与
唯一per-stream wire-buffer weighted permit，weight exact为`effective model_output_chunk_bytes + 4096`且三个frame variant复用；不足时尚无
valid stage identity，只返回固定body-free unavailable status。解析valid header、完成terminal Receipt replay/current pre-authorization并得到
trusted tenant/declared length后，在读取首个data frame前再原子取得declared bytes与per-tenant
stream permits；不足时不得读取正文或排入application queue，若已claim Processing则缩短lease并返回transient
`DependencyUnavailable + RetrySameAttempt`。全部permit持有到唯一terminal response、stream drop或absolute deadline，所有DB/S3/KMS
pool waiter都已受global stream permit封顶，不能形成第二个无界等待层。

连接进入bounded accepted backlog即由transport front记录monotonic start；TLS/service-role、backlog/第一阶段permit等待与完整Header decode
共同受effective `transport_accept_timeout_milliseconds`封顶。silent或fragmented pre-header流到期必须body-free终止并释放已取得permit，
且不得创建Receipt。valid Header完成current授权后，所有Data/Terminal和外部I/O切换到reservation冻结的Attempt absolute deadline；任何
重试、flow-control活动或重新计时都不能延长期限。

完成有效header identity解析后，tagged failure DTO是唯一wire业务失败合同，不返回自由字符串、raw backend error、object locator、grant、
content digest或正文。无法认证或无法解析出valid closed header/stage Receipt identity时不能伪造DTO，必须使用固定body-free gRPC status。
`Terminal`只允许已有terminal Receipt的safe result并必须携带receipt digest；`Transient`不携带receipt digest。
`retry_after_milliseconds`只允许transient `InProgress | DependencyUnavailable` + `RetrySameAttempt`，必须为正、分别不长于current Receipt lease
剩余时间或受HardLimitProfile backoff约束，并小于remaining deadline；其他transient组合必须为None。TooLarge/Invalid只能映射
RejectResponse，Stale/Deadline只能映射RejectStale，Conflict只能映射Conflict，Integrity只能映射IntegrityIncident；InProgress/
Dependency只能映射RetrySameAttempt。unknown reason/disposition、terminal Dependency/InProgress/Conflict或非法组合fail closed。15的stable
reason class与该enum一一映射。

Receipt/Artifact persistence矩阵同15且优先级固定：terminal replay不变；different digest Conflict不改任何事实；fresh Stale/Deadline不得由
Producer写Receipt/Artifact；Dependency/InProgress保持Processing并只缩短/观察lease；current TooLarge/Invalid原子写Receipt Rejected与
Artifact Rejected；current Integrity原子写Receipt Failed，并将candidate Artifact从current Staging、Uploaded或Verifying推进Quarantined
（candidate generation证据充分时才可把candidate Blob标Corrupt）；两类terminal failure都撤销write grant。Success原子写resolved
Blob/Artifact Verified与Receipt Succeeded。final guard竞争失败时Stale优先于content/integrity结果。
Model owner cancel/timeout/cleanup可把遗留Processing按same key/digest终结为Rejected，Producer不能把transient Dependency持久化为Failed。

`stage_request_digest`是reservation digest、正文digest/length/media/classification、output schema与validation evidence的canonical
digest；它排除自身字段、chunk framing以及允许heartbeat旋转的initial/final Job expected version，因此同Attempt同正文在上传期间的
合法heartbeat不会改变幂等identity。任何其他immutable字段变化都必须产生不同digest并被同一预留Receipt判定为conflict。

Worker只有在完整Provider terminal response已通过§14～16的schema/tool/finish/usage/safety/data-flow校验后，才把整个
`CanonicalModelResponse`编码为strict canonical JCS bytes并调用Producer。header的digest/length/media/classification/schema/evidence
必须由Worker从真实正文计算，不能来自Provider声明。显式image/audio/document `ModelArtifactOutput`仍是response正文中的nominal
ArtifactRef及独立Output Link，不得与承载整个canonical response的Artifact/Link复用identity或quota。

Producer在任何KMS或object I/O前使用自己的restricted PostgreSQL pool，在同一repeatable-read snapshot中重验header全部字段、current
`InFlight` ModelTurn、Running Job、expected-version lower bound、attempt/lease token digest、Worker generation、request/binding/profile、未过期
deadline、open quota reservation、Staging intent、active exact grant及两个Policy revision。它只接受exact
`spiffe://insight.platform/workload/model-worker.artifact-output` mTLS URI SAN，并拒绝Model read使用的`.../model-worker`身份；
tenant、owner、port、purpose、Artifact ID、classification、retention、storage binding或deadline均不能由stream body覆盖。

上述current授权只适用于新建或Processing lease过期接管。完成exact mTLS、tenant及closed key/digest解析后，Producer必须先查
stage Receipt：terminal同key/digest直接重放safe result，同key不同digest返回transient Conflict，active Processing返回transient
`InProgress + RetrySameAttempt`且retry-after不长于current Receipt lease；
这三条路径都不重验已经改变的current Job、不创建locator且不做KMS/S3 I/O。只有没有Receipt或接管expired Processing时才执行pre-I/O
授权并递增`claim_generation`。

授权与quota reservation通过后，Producer才可用完整
`tenant/backend/storage_binding/encryption_domain/security_domain/content_digest`查询同安全域existing Verified Blob；查询必须是constant-shape
exact lookup且命中/未命中不能改变对调用方的stream validation。预命中时不创建candidate/object，但仍读取并验证本次完整stream，最终事务
把Artifact依次应用`Staging -> Uploaded -> Verifying -> Verified`并绑定resolved Blob。

未预命中时，Producer为预留candidate Blob生成唯一opaque locator，以tenant/Blob/storage/encryption/key的canonical context执行KMS seal，
并在再次重验current fence的短事务中锁定current Receipt `claim_generation`、两个exact quota bundle header/line、ModelTurn/Job共享serialization guard与Artifact/Blob，
以create-if-absent插入Staging candidate并把它绑定到预留Staging Artifact；事务响应丢失时按相同reservation加载同一Blob，不能另造locator。
随后以独立S3/KMS workload identity对该exact staging object做conditional create、绝不overwrite，在内存不聚合完整正文；成功HEAD后用current
guard提交`Staging -> Uploaded`checkpoint，再提交`Uploaded -> Verifying`checkpoint，bounded verifier与任何GET仍在数据库事务外执行。
每个conditional PUT都必须携带不晚于Attempt deadline的write deadline并使用reservation冻结的storage-binding quiescence合同；本地timeout
只表示结果未知，不能当作backend absence或释放Blob bundle的证据。
验证覆盖exact generation、长度/SHA-256、`application/json` media、strict canonical JCS、固定Model-response nominal decoder、ArtifactIo
content policy和KMS encryption context。
Agent/ModelLoop的exact output schema仍由Worker与terminal repository各自重验；Producer不读取Artifact-backed request正文，也不能凭一个
schema digest伪造语义验证成功。object I/O完成后、提交Verified前必须使用
terminal frame的`final_fence`和同一closed header再次授权。initial/final supplied `expected_version`是current Job version的单调lower
bound而不是Producer CAS值：final不得小于initial，pre/post snapshot的current version必须分别大于等于对应lower bound；frame捕获后
并发成功的合法heartbeat可以使数据库current version更大而不使stage失效。Producer仍必须要求current Running/InFlight state、tenant/
Turn/Job/attempt、lease generation/token、Worker generation、request、reservation、grant与policy closure全部exact；generation/token、
cancel/terminal state或任一immutable业务字段漂移都拒绝当前提交。最终post-I/O短事务必须持有同样的Job共享serialization guard，并按
CR-119完整security-domain key取得transaction advisory fence。candidate是唯一winner时将candidate Blob与Artifact推进Verified；若此时已有
racing Verified winner，则Artifact改绑resolved winner、candidate推进Deleting并记录exact cleanup bytes/generation/预留Job ID；预命中路径
直接绑定existing winner。三条路径都把Artifact Verified evidence和stage Receipt `Processing -> Succeeded`在同一commit中CAS current
`claim_generation`；不存在terminal Receipt但Artifact未Verified或Verified却无可重放terminal Receipt的可提交窗口。Producer不创建cleanup
Job/Event；Model terminal或bounded Artifact cleanup reconciler必须从stage Receipt用预留same ID幂等创建exact InternalBlob cleanup Job。

Processing claim、Blob bind、Uploaded checkpoint、Verifying checkpoint与final Verified每一个短事务都必须遵守03/04的同一锁序：stage
Receipt与current `claim_generation`，两个bundle按ID及BudgetKey排序的`FOR SHARE`，current ModelTurn/Job serialization guard，最后
Artifact/Blob。每次都在锁后重验冻结的`UsageReservationId/generation`、Open state与line closure；Quota owner的Close/Expiry/settlement使用
冲突锁并递增generation。不能以repeatable-read snapshot或一次pre-I/O检查代替这些事务内guard，任何S3/KMS/validator I/O均在锁外。

`StageModelOutput`使用03的`JobCommit` Receipt，operation固定为`model_output.stage`；dedupe key是tenant、Job、lease generation与
commit request ID（预留`stage_receipt_id`），`stage_request_digest`单独保存为Receipt `request_digest`且不进入key。attempt必须匹配该
generation的durable reservation。相同key/digest重放返回相同Verified receipt；同key不同digest返回idempotency conflict。Receipt只证明
该exact Artifact/resolved Blob bytes已Verified，并闭合candidate disposition、新增physical bytes与待cleanup staging bytes；它不是Model
output、continuation或terminal receipt。Producer不得把Artifact推进Ready、创建或修改
ModelTurn/Run/Node/RunValue/Output Link、settle Model usage、关闭Artifact quota，亦不得发布`model.completed`。

Producer是独立于Model Worker、只读Model Artifact Broker、Sandbox Artifact Broker和Artifact Gateway的进程、Deployment、ServiceAccount、
restricted DB write credential/pool、S3/KMS workload identity、mTLS identity、two-phase admission permit与transport backlog hard cap。其数据库role只允许读取上述exact authority并
执行Staging Artifact/Blob/grant与stage Receipt所需的closed mutation；必须被数据库权限拒绝对Run、RunNode、Invocation/ModelTurn、Job、
RunValue、Quota余额、Event和Outbox的任意更新。`artifact.ready`与全部业务事件只由§16.3 terminal事务产生。只读Broker不能注册
`StageModelOutput`，Producer不能注册`ReadModelRequest`或Sandbox RPC，二者不得
共享Pod、ServiceAccount、DB pool、storage identity或process-local semaphore。

### 16.3 Artifact terminal first-winner transaction

Worker取得Verified stage receipt后，以最新Job fence提交同一个`CommitModelOutcome`。Artifact-backed成功路径由caller-owned
PostgreSQL transaction按03全局锁序完成，至少必须原子执行：

1. 按ID排序锁定stage与terminal JobCommit Receipt，claim/replay terminal Receipt，并从已锁stage Receipt确定candidate disposition与可选
   cleanup Job ID；按04 canonical顺序锁定Model quota、Artifact-owned count/logical bundle与candidate-Blob-owned upload/staging/physical
   bundle，再锁current Run/Node/ModelTurn parent aggregate；
2. 取得任何Job-rank锁之前，把current Model Job与可选`RacingCandidateLoser` cleanup Job组成canonical sorted-unique集合，在同一个Job-rank
   阶段依ID顺序lock existing或create-or-lock。cleanup Job必须使用预留ID、exact `InternalBlob` owner并逐字段匹配candidate bytes/generation；
   随后重验current Job、Attempt、lease token、Worker generation、request、binding、output reservation及全部identity。reconciler先创建或
   已terminal的same Job/Receipt可以复用，different payload是invariant failure；禁止先锁current Job再补锁排序更小的cleanup Job；
3. 锁定同tenant预留Artifact、resolved Verified Blob、可选Deleting/Deleted candidate、active grant和Retention/ArtifactIo revision，逐项比较
   Artifact/Blob与canonical response的
   digest、length、media、classification、schema与validation evidence；
4. 将exact Artifact `Verified -> Ready`，以本事务唯一PostgreSQL `db_now` checked-add冻结的`ready_retention_seconds`，把
   `retain_until`从`staging_retain_until`切换为新absolute `ready_retain_until`并写入terminal Receipt；撤销写grant并创建唯一
   `owner=ModelTurn, reference_kind=Output, purpose=RunOutput, port=model_response`的预留Output Link；
5. 用预留RunValue ID写immutable `model_response` RunValue，其`ValueRef::Artifact`、schema/content digest与classification必须和
   Ready Artifact及已验证`CanonicalModelResponse`逐字段一致；
6. 提交ModelTurn/Job first-winner terminal、Node/ModelLoop wake、Provider usage observation、每Attempt Model settlement与ModelTurn close；
   Artifact bundle消费Count=1/LogicalBytes=canonical length。candidate Blob bundle按disposition结算：PreexistingHit Close(0)；
   CandidateWinner释放Uploads/Staging并消费PhysicalBytes=`new_physical_bytes`；RacingCandidateLoser只释放Uploads，保留Staging与未Consume
   Physical到cleanup；cleanup Receipt已先提交则只复验Closed事实。Artifact bundle随Artifact，new-winner Blob bundle随Blob最后alias，
   二者不互相Refund；
7. 追加Artifact Ready与Model terminal Event/Outbox并回绑stage/candidate-cleanup evidence；public projection只携带safe ValueRef/状态，
   不携带response正文、object locator、grant、Provider handle、usage cost或raw error。

任一CAS、policy、Artifact、quota或Receipt检查失败必须回滚全部七步；不存在Ready但无Output Link/RunValue，或Model succeeded但Artifact
仍Verified的可提交状态。S3/KMS I/O不得发生在该事务内。重复terminal frame/commit只返回已有terminal Receipt，不重复Ready、Link、
RunValue、cleanup Job、quota settlement、Event或Outbox；重放返回首次保存的absolute `ready_retain_until`。

### 16.4 崩溃、取消与 orphan

- staging PUT成功而Producer未提交Uploaded/Verified时，由15的staging inventory和exact object-generation GC回收；不能从bucket listing
  推断Model成功；
- Producer提交Verified而Model terminal事务未提交时，该Artifact仍不可读、没有Output Link，也不是partial durable Model output；
- Worker crash、lease丢失、stale second authorization、timeout、cancel或另一个terminal first-winner使旧Attempt reservation/grant失效，
  Staging/Uploaded/Verifying/Verified对象进入15的bounded orphan/Deleting/GC流程；旧fence不得把它提升Ready；
- 同Attempt的stage response丢失可以凭相同digest重放取得相同receipt；lease generation被接管后的新Attempt必须使用新reservation，不能把旧
  receipt当作continuation。若仍无committed Model output，继续适用§18～19既有Provider retry/可能重复计费语义，不能因存在Verified orphan
  推断Provider response已成为业务结果；
- Inline或bind前failed terminal事务证明无Blob/locator后关闭两个output bundle、撤销未使用grant并标记intent可GC；bind/PUT后的
  cancel/timeout/failed/loser可Close未Consume Artifact bundle，但candidate Blob bundle保持Open；`cleanup_required`只是Artifact/Blob
  lifecycle classification。candidate cleanup不得在冻结`staging_retain_until`前执行/采纳DELETE或absence；到点后必须丢弃早期HEAD结果，
  重新对exact locator/generation取得write-quiescence后的stable deletion/absence evidence，才关闭其staging/未Consume physical line。race-loser candidate即使
  resolved Artifact已Ready也保留Blob bundle到预留cleanup Job完成；已dispatch Attempt的usage仍按实际或保守ceiling结算，Artifact cleanup
  失败不能改写Model first-winner；
- Model terminal后的durable Artifact/audit evidence只能由既有cleanup或incident authority追加并回绑stage Receipt；Producer或迟到Worker
  只能收到stable rejection并发出redacted非durable telemetry，不得再写Receipt/Event/Outbox、Artifact/Blob或output。

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
- 同一start transaction另从04共享Quota authority原子预留Artifact-owned `Count/LogicalBytes` bundle与candidate-Blob-owned
  `Uploads/StagingBytes/PhysicalBytes` bundle；Model usage与这两个output bundle使用不同dimension、reservation及ledger identity，任一
  reserve ID都不能作为另一方settlement ID。Inline/no-object失败Close两者；bind/PUT后失败只Close未Consume Artifact bundle，Blob bundle
  保持到exact cleanup；Ready消费Artifact count/logical，new Blob winner消费physical，Artifact删除与最后Blob alias删除分别Refund；
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
- response已由Producer推进Verified但Model terminal事务前crash，仍视为“没有committed output”；同Attempt exact stage可重放，但lease
  被接管后的新Attempt遵循既有Provider retry语义并使用新output reservation，旧Artifact只进入orphan GC，不能跨fence收养；
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

### 20.1 Artifact-backed output 错误合同

| 观察窗口 | Stable Failure / disposition | Retry 与结算 |
|---|---|---|
| dispatch前无法创建exact identity、Policy closure或最坏bytes reservation | `content_rejected`、`budget_exhausted`、`deadline_exceeded`或`artifact_unavailable` | Provider未调用、无Provider usage；临时Producer容量等待只defer，不伪造terminal failure |
| Provider response schema/canonical JCS/media/classification/content policy非法或超过冻结上限 | `content_rejected`，source为Model或Artifact | 已dispatch usage照实结算；是否产生新Model Attempt只由冻结retry/safety/budget policy决定 |
| S3/KMS/Producer在dispatch后暂时不可用且没有Verified receipt | `artifact_unavailable`，class为dependency/resource | 原Attempt仍current且exact bytes/digest可证明时只重试stage；Attempt失效后才由冻结Model retry/budget policy决定新Attempt并记录可能重复Provider费用，不把部分bytes作为输入或在同Attempt重放Provider |
| digest/length/object generation/KMS context不一致、Ready/Blob事实损坏 | `artifact_unavailable`或`platform_invariant_failed`，并触发Artifact incident | fail closed；不得以retry覆盖integrity事实或合成Ready |
| old lease/Worker、cancel/timeout或相邻terminal先赢 | `RejectedStaleFence` commit disposition | 不改变Model current state；关闭/回收旧reservation与orphan，不把stale当Provider retry hint |
| 相同stage key不同request/content digest | `idempotency_conflict` internal disposition | 不写第二Blob/Receipt，不重试为同一Attempt |

`model_output_artifact_required`只描述Artifact-backed output尚未交付时的当前开发期pre-dispatch防护，不属于目标成功路径或公开
Platform FailureCode；实现本节后，合法的超Inline response必须晋升Artifact。错误、Event、Receipt、trace和metric只保存safe code、
reason class、bytes bucket及opaque evidence digest，不回显response、object key、KMS context、grant、tenant-sensitive storage状态或raw
provider/backend error。

## 21. Tenant、数据与 Secret

- Provider/Model/Deployment/Turn/Attempt/usage/cache/handle都tenant-scoped；
- Provider Secret通过exact SecretBinding按声明SecretPurpose late resolve，不进入request ValueRef、DB、event、trace或error；
- per-tenant/per-principal credential不会跨安全域连接复用；
- 数据发送前验证Provider、region、retention、training、subprocessor和classification；
- Provider不能得到平台tenant ID、内部object key、SecretRef或未必要的Run metadata；
- Artifact upload/file handle绑定Provider、tenant、Turn、digest、deadline，结束后按retention删除/revoke；
- 平台自身的Artifact-backed response写入只接受`model-worker.artifact-output` exact workload identity与§16.1 reservation；Provider、Model文本、
  Run input或调用方均不能选择Artifact/Blob/Link/Receipt ID、retention、classification、object key、storage binding、grant或quota scope；
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
- Provider request/stream、Model request Artifact read与Model output Artifact stage使用三组不同IO permit；Model Artifact Broker和
  Model Artifact Producer分别使用自己的进程、DB pool、storage client与permit，二者都不能借用Sandbox Artifact Broker容量；
- Producer没有application queue；client-stream count/declared+buffer bytes、durable staging对象、DB/S3/KMS连接与per-tenant in-flight
  均受18 machine manifest/HardLimitProfile约束。permit必须在读取首个data frame前取得并持有到Verified receipt或stream
  drop/deadline，transport accept backlog有server hard cap/timeout，不能靠gRPC/SDK内部buffer形成无界旁路；
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

trait ModelArtifactProducer {
    async fn stage_model_output(
        &self,
        frames: ClientStream<StageModelOutputFrame>,
    ) -> Result<StageModelOutputReceipt, StageModelOutputFailure>;
}
```

Adapter返回闭合normalized frame/failure，不接触Run repository。Domain crate不依赖Provider SDK/HTTP。Worker
负责wire/stream与本地response validation；Producer只拥有Artifact stage port；repository负责Model/Artifact terminal
first-winner、state/fence/budget/outbox；orchestrator负责ModelLoop纯决策。

## 25. Persistence、Artifact 与事件

Provider、Profile 与 Deployment 使用共享 Resource/ResourceVersion/Deployment。ModelTurn 是
`InvocationKind::Model`；selection、canonical request digest、usage observation、provider handle 与安全 projection 保存在
Invocation/Job 的 bounded typed payload，物理调用、stream 与 recovery 使用 Job。超限 request/response 写入 Artifact，
usage settlement 使用共享 quota ledger，历史进入 Event。不得建立 Model 专用 lifecycle、turn、usage 或 handle 表族。

output reservation保存在该Attempt的Job/Invocation bounded typed payload及共享Quota/Artifact/Link/Receipt行中，不建立
`model_outputs`、producer session或stage proof专用表。Producer只提交Artifact/Blob current state和共享JobCommit Receipt；Verified
stage不是第二份Model current state。Artifact-backed terminal transaction复用同一RunValue、ArtifactLink、Quota、Receipt、Event与Outbox
聚合，将Verified提升Ready并写唯一Model output。Inline terminal必须同时释放预留Artifact事实，不能让reservation或Staging intent成为
另一份输出状态。

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
model_output_artifact_stage_total{outcome,reason_class}
model_output_artifact_bytes_total{outcome,size_bucket}
model_output_artifact_orphan_total{state,reason_class}
model_budget_wait_seconds{budget_class}
model_circuit_state{provider_class,state}
```

tenant/Provider/model/Run/Turn/endpoint/prompt不进入metric label。Trace只记录受控binding hash、attempt、latency、
byte/token count、finish/failure class，不记录message/delta/response/Secret。审计覆盖Provider/Profile publish、
Deployment/activate/suspend、credential grant、high-risk data transfer和break-glass。

## 27. 配置与部署

- Model Worker是独立Deployment、service account、DB/HTTP pool、queue和HPA；
- Model Artifact Broker是只暴露Model read RPC的独立Deployment、ServiceAccount、restricted DB pool和permit；它不得注册WASI/
  microVM RPC，也不得与Sandbox Artifact Broker共享Pod、连接池或process-local bulkhead；
- Model Artifact Producer是另一个只暴露client-stream `StageModelOutput`的独立Deployment、ServiceAccount、restricted DB write
  credential/pool、S3/KMS workload identity、mTLS identity、two-phase admission permit、transport backlog hard cap、PDB与HPA；它不得与Model Worker、Model Artifact Broker、
  Sandbox Artifact Broker或Artifact Gateway共享Pod、ServiceAccount、数据库credential/pool、storage identity或process-local bulkhead；
- NetworkPolicy只允许持有exact `model-worker.artifact-output`身份的Model Worker调用Producer，Producer只可到restricted PostgreSQL、private exact S3/KMS endpoint与必要
  DNS；无public Ingress、Provider/Secret Manager endpoint、Kubernetes API token或任意egress。只读Broker与Producer的Service、URI SAN、
  TLS Secret和数据库role不能互换；
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
- Inline threshold前一字节/恰好阈值/后一字节、Model response Q1/hard boundary分别证明Inline、Artifact晋升和稳定拒绝；每条路径
  验证最坏Artifact+candidate Blob双bundle reservation、实际结算/余量释放及预留identity不复用；
- `StageModelOutput` contract覆盖unknown/duplicate frame field、sequence gap、empty/short-nonterminal/chunk/total overflow、final fence
  version倒退、错误URI SAN、
  tenant/Turn/Job/Attempt/lease/Worker/request/policy/grant/quota swap、digest/length/media/classification/JCS/object generation/KMS context漂移；
- fresh PostgreSQL 16与真实S3-compatible/KMS test provider覆盖preauthorize→PUT/HEAD、crash后同reservation exact staging GET复验、
  Uploaded/Verifying/Verified→reauthorize→
  Ready/Output Link/RunValue/Model terminal全过程，并在每个DB/object I/O/response-loss窗口kill进程；任何窗口都无双Ready、双Link、双usage/
  Artifact settlement或可读半成品；
- Producer response loss的同Attempt重放返回同一receipt；stale/cancel/timeout/new Attempt不能收养旧Verified Artifact，orphan最终GC且
  existing Provider retry/可能重复费用 evidence保持；
- pre-header silent/逐字节Header和bounded accept backlog等待都在同一monotonic transport timeout释放global stream/wire-buffer且不创建
  Receipt；valid Header后只使用冻结Attempt deadline；
- deadline前发出的conditional PUT在client timeout、cancel与lease takeover后迟到成功时，任何barrier前HEAD absence均不能触发DELETE/
  quota Close；`staging_retain_until`后重新观察exact generation并收敛且不留下无quota object；
- preexisting hit、candidate new winner与racing loser三种dedupe disposition逐字段验证resolved/candidate Blob、新增physical bytes、cleanup
  generation/Job；Artifact先删除但shared Blob仍有alias时physical不Refund，最后alias删除才由original Blob bundle退款；
- InProgress/Dependency transient、fresh Stale/Deadline、TooLarge/Invalid Rejected、Integrity Failed+Quarantined及different-digest Conflict
  覆盖Receipt persistence、claim-generation takeover与response-loss replay；Integrity isolation分别覆盖candidate current Staging、Uploaded、
  Verifying三种来源状态；
- 数据库权限和mTLS负向fixture证明Producer不能更新Run/Node/Invocation/Job/RunValue/Quota/Event/Outbox，Broker不能stage，Producer不能read
  Model request或调用Sandbox RPC，Model Worker没有locator/S3/KMS credential；
- Producer、Model Broker与Sandbox Broker分别100% permit/DB-pool/S3 lane饱和或rolling restart时，其他两个audience/lane及API/Scheduler
  admission仍满足18的隔舱；Producer saturation不得形成无界gRPC buffer、staging、连接或quota reservation；
- 客户端SSE断开不取消Turn，live delta丢失后durableterminal可校准；
- Provider native schema success但本地invalid不能进入Plan；
- 未绑定tool、伪造tool result、Provider built-in tool无法执行；
- retry保持request digest/binding并单独结算usage，可能不同输出只有first-winner；
- token/cost/request/concurrency budget在并发和crash下不超卖；
- high-classification Artifact无法发送到不合规Provider/region；
- Secret、prompt、response、hidden reasoning、file/cache handle不进入public event/metric/default log；
- Model饱和或单Provider `429`不影响API/Scheduler/Sandbox/MCP/Context准入；
- Sandbox Artifact Broker队列、DB pool或permit饱和时，Model Artifact Broker仍能接受已授权request materialization；
- Model Artifact Producer与只读Model Artifact Broker任一transport backlog、DB pool、permit或storage client饱和时，另一方仍保持独立准入；
- credential revoke/provider suspension在限定窗口阻止新Attempt并有审计。

### 28.1 当前实施证据边界（非规范性）

CR-124对应的Resource foundation已经交付：`insight-platform-contracts`为Provider/Profile/两级Deployment提供本次架构修订前的closed Rust
payload、canonical generation defaults、sorted-set与cross-field hard-limit验证。它尚不包含§7新增的Model output Retention/ArtifactIo
exact binding/Ready duration，也不包含§16.1/18要求的HardLimitProfile v5、WorkerManifest v2、ComponentCapacityManifest或Candidate
closure。CR-125进一步交付`insight-platform-models`的
canonical request/response、stream fence、tool/schema validation、retry/control/cancellation与attempt accounting，以及caller-owned
PostgreSQL adapter；shared Invocation/Job/RunValue/ArtifactLink/Receipt/Event/Outbox和四维Quota bundle均未增加专用表。fresh PostgreSQL 16
fixture证明invalid local schema rollback、retry新reservation、tool-intent、stale fence和cancel/completion first-winner；strict Clippy、
schema contract及23表/单一`0001`保持通过。该证据关闭Phase 3的ModelTurn domain/repository交付项，不替代Phase 4 Provider adapter、
Phase 5 public API或Phase 6 qualification，因此不能把文档16整体标记为Implemented或Verified。

CR-132进一步交付Phase 4的首个adapter-host slice：独立`insight-platform-model-adapters`按完整signed adapter descriptor做
exact process-local resolution，消费closed normalized stream并强制Provider级delta/first-byte/idle/total timeout、sequence、terminal、
response local validation、cancel与panic containment。worker materializer和PostgreSQL authority之间只有fenced
`CommitModelOutcome`；claim显式返回fence、usage reservation、quota ledger identity与exact request input，后者逐字段回绑冻结
RunValue；Inline正文复核canonical digest。Artifact-backed request现由closed `ModelArtifactReadRequest`绑定tenant、ModelTurn、
当前Job version/lease/fence/Worker generation、request digest、deadline、exact RunValue与active ModelTurn ArtifactLink；PostgreSQL
authority在Broker I/O前后重验同一请求，Worker再复核canonical JSON与逻辑content digest。terminal command还在Provider I/O前
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
Pod没有Service/Ingress、云Provider credential、Kubernetes API token或直接Provider客户端。Artifact-backed request的domain、PostgreSQL
authority、Model Broker pipeline和Worker materializer内核已交付；生产进程现在安装`ArtifactModelBrokerGrpcClient`与
`BrokeredModelRequestMaterializer`，经Model Artifact Broker的exact Model Worker mTLS端点读取并逐片复验Artifact-backed request。Model
Broker的目标边界只注册Model RPC，并使用独立于Sandbox audience的进程、Deployment、ServiceAccount、restricted PostgreSQL pool和permit；
既有只读数据库与错误workload role fixture仍证明authority最小权限，但旧单Broker拓扑证据不能证明新的跨audience
隔舱。双Broker部署、身份互换和独立饱和门禁完成前不把该拓扑登记为Phase 4/6完成。output materializer当前仍只支持Inline。Model取消路径现以reserved critical-control permit运行bounded PostgreSQL safety scan，只接受当前generation仍持有lease的
Cancelling Turn/Job，调用Egress exact cancel后用旋转fence提交保守usage ceiling；重试失败不提交terminal，late completion由first-winner拒绝。
通过完整Turn/Job/attempt/lease/Worker/request fence的text delta现在会进入credential-free canonical内部envelope，并由同时限制message数和
bytes、把容量permit保留到有界批次flush结束的non-blocking队列投影到TLS/mTLS NATS tenant/run scoped subject；tool argument与Provider metadata不发布，NATS不可用、背压或单帧
超限只丢live observation，不影响durable执行。真实Secret Manager provider、生产storage/KMS catalog provisioning、Artifact-backed output IO、
公开SSE消费与live-gap/backpressure资格、real-process Provider conformance、跨work-class饱和隔舱和Phase 6 fault fixture仍未交付，因此
CR-132/CR-136和本规范状态保持进行中。

04的closed Model-output ArtifactIo Policy Rust/JSON合同与pure checked staging/Ready时间计算已经交付，但尚未接入Model Deployment或
admission。`ModelOutputArtifactReservation`、最坏Artifact quota预留、`StageModelOutput` client-stream机器合同、独立Model Artifact
Producer进程/ServiceAccount/restricted写role/S3-KMS identity、双重授权、Verified→Ready terminal transaction、Inline reservation释放、
orphan GC与对应real-process/fault/capacity fixture仍尚未实现。既有Inline output materializer、只读Model Artifact Broker、能够引用预先
Ready Artifact的repository shape或普通Artifact prepare/finalize测试都不能单独证明本合同。上述全部代码、schema、protobuf、数据库权限、
Helm与资格证据完成前，`model_output_artifact_required`防护仍只是当前缺功能边界，Artifact-backed Model output、Phase 4/6与Gate均保持Open。

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
