# Platform v2 Model Provider 与 Invocation 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-186 |
| 日期 | 2026-08-25 |
| 依赖 | 02、03、04、06、07、09、10、15 |
| 直接下游 | 17、18 |

> CR-181 impact：ModelLoop由05 Plan v4冻结model/skill/capability slots、input/output、route和全部budget；Model Worker不能从
> prompt/tool intent扩大bindings、budget或选择另一个output port。

## 1. 决策摘要

Model Provider、Model Profile和Model Deployment复用shared Resource lifecycle。每个ModelTurn冻结exact provider/model/profile、
prompt/tool/schema/safety/budget合同，shared Job拥有物理attempt、lease和retry。

首版Model request与canonical response都是有界Inline RunValue。不建设Model Artifact Producer、Model Artifact Broker、
output reservation、weighted materialization permit或Artifact-backed Model state machine。需要文件或大输出时，Agent调用
Capability/Sandbox产生Artifact，Model只接收其有界结构化摘要或reference metadata。

## 2. Resource 与Deployment

Provider ResourceVersion冻结：

- adapter kind/version/protocol、endpoint catalog identity和provider model catalog digest；
- credential requirement、TLS/egress/region/data-retention policy；
- request/response/stream/error normalization合同；
- supported modalities、tool/structured-output features、token/context limits；
- rate limit、retry、idempotency、timeout、cancel和usage semantics。

Model Profile ResourceVersion冻结：

- model requirement与允许的provider/model selector；
- system/prompt assembly policy、sampling parameters和stop policy；
- tool exposure/round limit、structured output schema policy；
- input/output token、byte、cost、latency、attempt和deadline budget；
- safety、classification、logging、cache和fallback policy。

Model Deployment绑定exact Provider Revision、provider model ID、Profile Revision、Credential reference、region、adapter/runtime
和compatibility evidence。Run admission只从tenant active binding解析exact Deployment，不从Installation Release或Candidate读取
业务state。GitOps发布不改写已存Run/Turn。

## 3. ModelTurn 模型

```rust
struct ModelTurn {
    model_turn_id: ModelTurnId,
    tenant_id: TenantId,
    run_id: RunId,
    node_execution_id: NodeExecutionId,
    deployment_id: DeploymentId,
    state: ModelTurnState,
    request_value_id: RunValueId,
    response_value_id: Option<RunValueId>,
    current_job_id: Option<JobId>,
    tool_round: u32,
    usage: UsageAccumulator,
    deadline_at: Timestamp,
    projection_version: u64,
}

enum ModelTurnState {
    Ready,
    Running,
    WaitingForTool,
    RetryScheduled,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    UnknownOutcome,
}
```

ModelTurn拥有业务request/response、tool round、usage/budget和terminal result。Job拥有lease generation、attempt count、
retry time和Worker fence。两者不复制current state。

## 4. Admission 与request assembly

创建ModelTurn的事务必须冻结：

- Run/Node/model slot、exact Deployment和expected versions；
- message history、system/Skill/Context指令的canonical排序与digest；
- exposed Capability Interface、tool schema/name mapping和approval/effect evidence；
- output schema、sampling、safety、cache、retry和budget policy；
- input/output canonical byte与token hard limit、deadline和trace identity；
- current policy/suspension/tenant/region/admission evidence。

request assembly是pure deterministic function。相同冻结输入必须得到相同canonical request digest。所有list、message、
content part、tool、schema、string和bytes有平台hard limit。prompt injection不能改变exact tool/policy/Secret权限。

CR-186 canonical `ModelContentSource`固定包含`source_kind/source_id/source_digest/content_digest/assembly_phase/ordinal/
byte_budget/token_budget/trusted_instruction`。初始request必须完整包含11的四个必选phase（platform safety、Agent contract、Plan node
instruction、current user input），Skill与Context phase按exact activation/query事实可选；同phase ordinal唯一。canonical message顺序
就是phase/ordinal顺序，source-map digest来自同序closed entries。任何overflow整批返回稳定admission failure，不允许Provider adapter、
Model Worker或恢复路径自行截断或重排。

ModelTurn在Provider dispatch前必须已经存在有效Inline request RunValue、Job、quota reservation和全部binding。

## 5. Message、tool 与structured output

canonical message是closed typed enum，支持system/user/assistant/tool role和有界content parts。首版可支持有界Inline text、
JSON及provider capability明确允许的小型Inline media；不允许object URL、未授权remote URL或Artifact定位。

tool name是稳定、可逆、有冲突检查的InterfaceVersion投影。Provider返回tool intent后：

1. adapter正规化tool-call ID、name和arguments；
2. 验证exact exposed tool与input schema；
3. ModelTurn原子进入`WaitingForTool`并创建10的CapabilityInvocation；
4. tool terminal后以typed RunValue继续同一Turn的下一个bounded round。

Model Worker不直接调用backend，不跳过approval、quota、policy或Receipt。重复tool-call ID、round超限、unknown tool、
invalid arguments与schema drift使Turn进入stable failure。

structured output必须绑定exact JSON Schema digest。adapter只提供provider映射，canonical validator在平台信任边界
重新验证closed schema、number/string normalization、maximum depth/items/bytes和digest。

## 6. Streaming

provider stream是非权威observation，不是ModelTurn current state。Model Worker使用有界parser处理text delta、tool-call
delta、usage和terminal frame，并通过sequence、byte/token和time hard limit防止无界内存。

SSE/live observation背压时可丢弃中间delta，不得丢失最终canonical response。client disconnect不取消ModelTurn，除非
另行提交durable cancel command。最终response只能从完整provider terminal frame形成。

## 7. Response normalization 与commit

adapter将provider response归一为closed result：

```rust
enum ModelOutcome {
    Completed { response: CanonicalModelResponse, usage: Usage },
    ToolCalls { calls: Vec<ToolIntent>, usage: Usage },
    RetryableFailure { failure: ModelFailure },
    TerminalFailure { failure: ModelFailure },
    Unknown { evidence: UnknownOutcomeEvidence },
}
```

`CanonicalModelResponse`必须通过schema、safety、byte和token hard limit，并序列化为Inline RunValue。超过上限返回
`model_output_too_large`，不截断、不自动stage Artifact、不让Provider或Worker伪造成功。

terminal winner事务必须验证ModelTurn/Job当前fence、response digest/schema/safety、usage/budget和Receipt，然后
创建RunValue、推进ModelTurn、关闭Job、settle quota、追加Event/Outbox并唤醒Node。不涉及Artifact Ready或Link。

当ModelTurn属于Plan v4 ModelLoop时，创建事务重验model route的04 selection evidence，并只装配node列出的exact Skill/Capability
slots。最终structured output只能写node声明的`output` RunValue，终结当前ModelLoop Node并按Plan `resume`创建目标NodeExecution及唯一
Orchestration Job；不得把同一ModelLoop Node置回Ready。tool intent仍经10创建Invocation。
Model Worker不能扩大tool集合、修改budget/output port或把ModelTurn terminal直接当Run terminal。

CR-182 model slot可以用ModelLoop `model_route`执行`route_hash`；Skill/Capability tool slots只能按各自`only_candidate | ordered_first`
选择并在ModelTurn snapshot中冻结，provider error不触发隐式candidate切换。

## 8. Usage、budget 与cost

usage以provider-reported与platform-observed两个有界来源保存，并标记confidence。budget至少覆盖input/output/total
tokens、request/round/attempt count、wall deadline和estimated cost。每次dispatch前预留，terminal后settle，重试不清零
已消耗budget。

provider价格、tokenizer和capability catalog是published versioned input，不在历史Turn上动态重算。provider缺失usage时
保存bounded estimate和diagnostic，不伪造exact usage。

## 9. Retry、timeout、cancel 与recovery

- 只有冻结policy允许且failure class安全时才retry；
- provider `Retry-After`只是bounded hint，不超过deadline或budget；
- retry通过Job `retry_at`持久化，不在Worker内sleep；
- cancel先写durable intent，provider cancel是最佳努力；
- 连接中断且无法证明provider outcome时进入`UnknownOutcome`或按published idempotency合同恢复；
- Worker崩溃后只有新lease generation能commit，旧stream结果被拒绝。

自动fallback只能在Model Profile已冻结有序compatible Deployment、data/region/safety相容，且当前Effect安全时发生；
首版可以选择不实现fallback，不得自由切换provider。

## 10. Safety、tenant 与Secret

- prompt/tool/context/provider content都是不受信任输入，不能更改平台authorization；
- pre-dispatch和post-response safety policy冻结exact version/digest；
- blocked response保存stable reason/evidence digest，不必保存敏感正文；
- provider endpoint由catalog决定，Secret只在Egress/adapter最后一跳解析；
- raw credential、prompt/response、tool arguments和Inline media不进log、metric label、Event或error body；
- cache key包含tenant、exact Deployment、request/safety/tool/schema digest，cache不跨tenant。

## 11. 并发与隔舱

Model Worker使用独立queue、Deployment、ServiceAccount、DB pool、provider client、permit和rate-limit budget。它不与Sandbox、
MCP、Artifact Data Worker或critical-control共享容量。claim前预留local Model slot和durable tenant/provider quota，失败或
少领时RAII释放。不存在Model output materialization第二组permit。

## 12. Persistence 与owner interface

ModelTurn是shared invocation/turn aggregate中的typed business owner，Job复用shared Job，RunValue复用shared value，历史/审计
复用Event，幂等复用Receipt，不建provider-attempt、stream-delta、usage-evidence、output-reservation或Artifact producer专用表。

owner Rust type是request/response/snapshot的语义authority。OpenAPI/protobuf/JSON Schema投影从owner type/registry生成或对照验证，
不手写三份平行合同。JSONB有`schema_version`、closed validation、size limit、canonical serialization和digest。

## 13. 可观测性

metric至少包含admission/queue/provider/first-token/total latency、outcome、retry、timeout、unknown、input/output tokens、
estimated cost、tool rounds、safety block、permit/rate-limit utilization和stream drop。tenant、Run、ModelTurn、provider request ID和
model name不作高基数label。受限trace只记录stable ID/digest和sanitized metadata。

## 14. 验收标准

- Run/Turn始终使用创建时exact Model Deployment，active head/GitOps变化不影响已存工作；
- request assembly和tool-name projection在相同输入下逐字节稳定；
- invalid tool、arguments、structured output、usage、stream和schema drift全部fail closed；
- Provider dispatch前已有Inline request、Job、quota和exact binding；
- canonical response只以Inline RunValue提交，超限不创建Artifact或截断伪造success；
- tool intent必须通过CapabilityInvocation，Model Worker不直接调backend；
- duplicate/old lease generation不能双commit或重复消耗budget；
- provider、Sandbox、MCP和Artifact各自饱和时Model隔舱与critical-control仍符合容量合同。

## 15. 分层证据

domain normalization/property tests、provider fake-server contract tests、PostgreSQL lease/budget/terminal tests、真实adapter canary和
production-equivalent saturation/fault qualification分层运行。开发fixture不自动成为发布资格证据。

## 16. 明确推迟

- Model Artifact-backed request/response、Model Artifact Producer/Broker和专用materialization capacity；
- provider batch API、fine-tuning、文件上传API和长期assistant/thread资源；
- 未发布的自动provider/model failover和cross-region cache；
- 无法在Inline hard limit内表示的直接多模态正文。

## 17. 未决问题

CR-181 cross-review已确认Plan v4 ModelLoop dispatch/tool/result并恢复Accepted；实现与L2/L3 evidence仍待完成。

首版Inline-only Model合同无未决设计问题。具体provider adapter与model catalog作为发布Resource增量增加。
