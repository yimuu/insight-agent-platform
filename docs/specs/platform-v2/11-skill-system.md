# Platform v2 Skill System 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-185 |
| 日期 | 2026-08-07 |
| 依赖 | [`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`05-agent-and-typed-plan.md`](05-agent-and-typed-plan.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md) |
| 直接下游 | 12、17、18 |

> CR-181 impact：ModelLoop的`skill_slot_ids`来自Plan v4并按04 exact selector解析；Skill仍只是immutable method package，
> selection结果不能把Skill变成Invocation、script runner或execution owner。

> CR-182：ModelLoop没有Skill route port，故Skill slot首版只接受`only_candidate | ordered_first` Selection Policy；`route_hash`在
> Agent Deployment publication拒绝。

> CR-185：canonical Skill package 的物理字节合同已冻结为无压缩、长度前缀的
> `insight.skill-package/1` frame；运行时不得猜测ZIP/TAR、按文件系统展开package或接受实现私有archive格式。

> Persistence ruling：Skill 使用 02 的共享 Resource/ResourceVersion；activation/selection 是 Run 或 Invocation 的 typed
> snapshot/event，不建立 Skill 专用 lifecycle、activation 或 receipt 表族。

## 1. 决策摘要

Skill 是不可变、可选择的方法包，不是 Agent、Tool、进程或脚本运行器。Skill Revision 只包含结构化说明、
Prompt 片段、参考资料、示例、静态资产引用，以及对 Capability、Context、Model feature 和其他 Skill 的
显式需求。脚本必须发布为 Sandbox Capability Implementation，Skill 只能依赖它，不能直接执行文件。

Agent Deployment 解析并冻结完整 Skill 闭包和候选集合。Run 可以在这些候选中动态选择 Skill，但不能运行时
安装、追随 active head、解析版本范围或扩大权限。每次选择和激活都形成 durable、可审计的
SkillActivation；Skill 本身不拥有执行状态。

## 2. 目标与非目标

### 2.1 目标

- 给 Skill package、Revision、Requirement、Binding、Selection 和 Activation 明确机器合同；
- 同时支持作者显式选择、Plan 固定选择和受约束的模型动态选择；
- 让 Skill 可复用而不隐式继承 Capability、Secret、Context 或网络权限；
- 在 Deployment 阶段完成依赖闭包、冲突、循环、预算和安全验证；
- 让 Prompt assembly 有确定顺序、大小上限、来源标记和注入隔离；
- 让 Skill 更新只影响未来 Deployment/Run；
- 为本地目录、管理 API 和未来 Marketplace 使用同一种 canonical package。

### 2.2 非目标

- 不让 Skill 创建线程、Run、Worker、连接、数据库事务或后台任务；
- 不让 Skill 声明系统级权限、绕过 Approval 或改变 Capability Effect；
- 不在 Skill 包中运行 Python、Node、Shell、WASM、模板表达式或安装脚本；
- 不在运行时下载依赖、求解 semver、自动升级或访问公共 Marketplace；
- 不允许模型从 Registry 全量搜索并安装未绑定 Skill；
- 不把历史对话、用户 memory 或执行结果写回 Skill Revision；
- 不定义 Skill 商业分发、计费、评分或跨租户共享市场。

## 3. 术语与信任边界

| 术语 | 含义 |
|---|---|
| Skill Entity | 可变名称、标签和生命周期容器 |
| Skill Draft | 可编辑作者内容，不可用于 Run |
| Skill Revision | 已验证、不可变的方法包与依赖声明 |
| Skill Closure | Deployment冻结的Revision及全部传递Skill dependency有向无环闭包 |
| Skill Candidate Set | Agent Deployment 允许在运行时选择的exact Skill Deployment集合 |
| Skill Binding | requirement alias 到 exact Skill/Capability/Context/Model binding 的映射 |
| Skill Selection | 在固定候选集合内决定使用哪些 Skill 的纯决策 |
| Skill Activation | 某 Run scope 实际启用 Skill 的 durable 事实 |

Skill 作者、包内文本、引用内容、示例、外部导入 metadata 和模型选择结果均为不受信任输入。Registry validation
可以证明结构、digest 和 policy 合规，不能证明说明文本真实或没有 Prompt injection。平台 Policy 和 Agent
system contract 的优先级始终高于 Skill 内容。

## 4. 领域模型

```rust
struct SkillRevision {
    skill_revision_id: ResourceVersionId,
    skill_id: SkillId,
    interface: SkillInterface,
    manifest: SkillManifest,
    instruction_sections: Vec<InstructionSection>,
    references: Vec<SkillReference>,
    examples: Vec<SkillExample>,
    assets: Vec<SkillAsset>,
    skill_dependencies: Vec<ExactSkillDependency>,
    capability_requirements: Vec<CapabilityRequirement>,
    context_requirements: Vec<ContextRequirement>,
    model_requirements: Vec<ModelFeatureRequirement>,
    package_artifact_id: ArtifactId,
    semantic_digest: Digest,
}

struct SkillDeploymentClosure {
    skill_revision: ExactSkillRevisionRef,
    requirements: Vec<FrozenSlotBinding>,
    selection_policy: ExactPolicyDeploymentRef,
    qualification_evidence: ArtifactRef,
}
```

```rust
struct SkillInterface {
    qualified_name: SkillName,
    purpose: String,
    task_input_schema: ClosedJsonSchema,
    produced_guidance_schema: ClosedJsonSchema,
    compatible_agent_interfaces: Vec<ResourceVersionId>,
}
```

`produced_guidance_schema` 描述 Skill 可向 ModelLoop/Plan 提供的结构化 guidance，不是 Agent 的最终业务输出。
Skill 不能声明可执行 entrypoint。

## 5. Canonical Package

Skill package 是按 canonical path 排序的不可变 archive，逻辑布局为：

```text
skill.json
instructions/*.md
references/*
examples/*.json
assets/*
```

首版物理编码固定为 `insight.skill-package/1`。整数全部为 unsigned big-endian，字节序列无padding：

```text
24 bytes  magic = ASCII "INSIGHT-SKILL-PACKAGE/1\n"
u32       entry_count
repeat entry_count times, in manifest canonical path order:
  u16     path_byte_length
  u64     content_byte_length
  bytes   UTF-8 path
  bytes   file content
EOF       不允许footer或trailing bytes
```

Artifact verified media type固定为`application/vnd.insight.skill-package`。frame不压缩，不保存owner、mode、mtime、link、
设备号或扩展header；所有entry metadata只来自Revision内的closed manifest。`entry_count`、ordered path、content length、
每个file digest、展开总长度、manifest digest与整个Artifact digest必须同时匹配，任一不一致fail closed。因为首版无压缩，
压缩比恒为1；未来压缩或第二archive编码必须经过新的协议revision，不得按media sniffing自动接受。

规范要求：

- `skill.json` 是 closed JSON schema，未知字段 fail closed；
- path 必须是规范化 UTF-8 relative path，禁止绝对路径、`..`、symlink、hardlink 和设备文件；
- archive 展开后的文件数、单文件大小、总大小、路径深度和压缩比都有硬限制；
- Markdown 只作为文本解析，不允许 HTML active content、远程 include 或运行时代码插值；
- reference/asset 使用 media allowlist；可执行位被清除，二进制必须经过 Artifact content policy；
- package digest 由 canonical manifest 与每个 path/media/length/file digest 计算；
- package 本体由 15 的 Artifact 保存，运行热路径使用已验证的 materialized metadata。

代码片段可以作为参考文本存在，但不能被解释为可执行文件。需要执行的代码必须成为独立 Capability
Implementation Revision，并通过 Capability Requirement 引用。

## 6. Instruction Section

```rust
struct InstructionSection {
    section_id: SectionId,
    phase: InstructionPhase,
    audience: InstructionAudience,
    body_ref: ArtifactSliceRef,
    max_tokens: u32,
    data_classification: DataClassification,
}

enum InstructionPhase {
    TaskUnderstanding,
    Planning,
    ToolUse,
    Validation,
    OutputComposition,
}
```

Instruction phase machine wire固定为`task_understanding | planning | tool_use | validation | output_composition`；
audience固定为`planner | tool_user | validator | composer`。audience是目标模型角色而不是消息角色，不能扩展为
`system`、`developer`、`administrator`或自由字符串。package entry kind固定为
`manifest | instruction | reference | example | asset`。

每个 Instruction slice 必须引用同一 Revision 内的 exact `instruction` package entry，path、content digest 与
`data_classification`必须一致，且 byte range 不得越过该 entry。Assembler 不允许为局部 slice 降低来源文件的
classification。

Skill 只能在平台定义的 phase 中提供内容，不能声明 `system`/`developer`/`administrator` 等消息角色。Assembler
将 Skill 内容放入带来源边界的不受信任 instruction block；Skill 内出现的角色标记只是正文。

## 7. Requirement 与依赖闭包

```rust
struct CapabilityRequirement {
    alias: RequirementAlias,
    interface_revision_id: ResourceVersionId,
    required_effect_ceiling: Effect,
    required_features: BTreeSet<CapabilityFeature>,
    optional: bool,
}

struct ContextRequirement {
    alias: RequirementAlias,
    interface_revision_id: ResourceVersionId,
    required_classification_ceiling: DataClassification,
    optional: bool,
}
```

Requirement kind machine wire固定为`capability | context | model_feature`；Skill-to-Skill exact dependency使用独立
dependency表，不伪装成Requirement。Selection mode wire固定为
`required | plan_selected | policy_selected | model_proposed`，Registry、Run binding与Activation必须消费同一集合。

- Skill Revision 的 Skill dependency 必须是 exact Revision，不保存运行时版本范围；
- Capability/Context requirement 绑定 Interface Revision，不绑定 endpoint、Implementation 或 Secret；
- Agent Deployment 为每个非 optional requirement 解析 exact Deployment；
- optional 只表示可在没有该能力时不激活相关 instruction，不允许 silent fallback 到同名资源；
- alias 在整个闭包中经 namespace 展开后唯一；
- Skill dependency graph 必须无环，深度、节点数、总 package bytes 和总 instruction tokens 有上限；
- 父 Skill 不会继承子 Skill 未显式 re-export 的 requirement；
- 依赖闭包与所有 resolution receipt 进入 Agent Deployment digest。

Draft authoring 可以使用人类可读 constraint 协助选择 Revision，但 publish 前必须解析为 exact ID；运行时
不携带 semver 或范围求解器。

## 8. Agent Binding

```rust
struct SkillBindingSet {
    required: Vec<BoundSkill>,
    candidates: Vec<BoundSkill>,
    requirement_bindings: Vec<SkillRequirementBinding>,
    selection_policy_revision_id: ResourceVersionId,
    closure_digest: Digest,
}
```

Agent Deployment validation 必须验证：

1. Skill interface 与 Agent/ModelLoop 输入兼容；
2. 闭包、alias、instruction phase 和 token budget 合法；
3. 所有 Capability/Context requirement 都有固定候选并满足 policy；
4. Skill 数据分类不超过 Agent、Model Provider 和输出策略；
5. Skill suspension、来源信任和 conformance evidence 允许新绑定；
6. 候选集合不会使 Model tool name、Context alias 或 child binding 冲突。

Run admission复制exact Skill Deployment、Revision IDs和closure digest到RunBindings。之后active binding、package更新、
Registry 删除请求或 discovery 变化均不影响该 Run。

## 9. Selection

选择模式是闭合枚举：

```rust
enum SkillSelectionMode {
    Required,
    PlanSelected,
    PolicySelected,
    ModelProposed,
}
```

- `Required` 在 scope 开始时激活；
- `PlanSelected` 由 Typed Plan 的固定 alias 和条件选择；
- `PolicySelected` 使用版本化、确定性的 metadata/rule selector；
- `ModelProposed` 只允许模型从平台提供的 bounded candidate projection 返回 ID 列表。

Model proposal 不是授权结论。平台重新验证候选 membership、数量、token budget、phase、data policy 和当前
suspension 后才提交 Activation。模型不能提供 package URL、版本、权限或 dependency override。

选择输入包含 task 的受控摘要和非敏感 metadata；默认不向 selector 暴露完整 Secret、Artifact 正文或跨租户
目录。相同 Run scope 的相同 selection key 幂等返回同一 committed decision。

## 10. Activation 与 Prompt Assembly

```rust
struct SkillActivation {
    activation_id: SkillActivationId,
    tenant_id: TenantId,
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    skill_revision_id: ResourceVersionId,
    selection_mode: SkillSelectionMode,
    selection_evidence_ref: ValueRef,
    state: SkillActivationState,
    activated_at: DateTime<Utc>,
}
```

Assembler 顺序固定为：平台安全合同、Agent contract、Plan node instructions、required Skills、已选择 Skills、
Context observations、当前用户输入。每一块带 source ID、classification 和 byte/token budget；同一 phase 内按
Deployment 固定的 ordinal 排序，不按包名、更新时间或模型偏好排序。

Skill 激活不复制可变 memory。Skill reference 只有在需要时通过 Artifact/Context grant 读取，并经过当前
principal 和 data-flow policy。Assembler 输出保存 digest 和 source map；敏感正文是否持久化由 retention
policy 决定。

## 11. 所有权接口

```rust
trait SkillRegistryStore {
    async fn publish(&self, command: PublishSkillRevision) -> PublishReceipt;
    async fn resolve_closure(&self, request: ResolveSkillClosure) -> ClosureResolution;
}

trait SkillSelector {
    async fn propose(&self, request: SkillSelectionRequest) -> SkillSelectionProposal;
}

trait SkillActivationRepository {
    async fn commit_activation(&self, command: CommitSkillActivation) -> ActivationReceipt;
    async fn load_active(&self, scope: ScopeKey) -> Vec<SkillActivation>;
}
```

Registry/selector 不读取 Secret value，不调用 Capability，不推进 Run。Orchestrator 根据 proposal 产生纯 command，
repository 负责 current-state CAS 和持久化。

## 12. 管理 API 与事件合同

管理命令至少包含：

```text
CreateSkillDraft
UpdateSkillDraft
ValidateSkillDraft
PublishSkillRevision
ActivateSkillHead
ClearSkillHead
SuspendSkill
ResumeSkill
```

所有mutation使用02/03的tenant/principal/command scope/idempotency key与request digest。上传package先得到ArtifactRef，再由
validation 异步解析。发布事件只携带 ID、digest、状态和安全分类，不携带 instruction/reference 正文。

Skill Deployment不是进程或执行状态；它冻结exact Skill Revision、已解析的Capability/Context/Model/Skill requirement closure、
适用Policy与qualification evidence。Agent Deployment只绑定允许的exact Skill Deployments并再次冻结其closure digest。
`BindSkillDeploymentToAgentDeployment`是Agent Deployment resolution的一部分，不是Skill mutation或运行时lookup。

运行事件的公开最小集合为 `skill.selected`、`skill.activated`、`skill.rejected`。默认只公开 display label 和
状态；选择证据、包内容、Capability map 与安全失败仅在授权的 diagnostic projection 中可见。

## 13. 状态机

Skill管理生命周期严格沿用02，Revision永不改变状态：

```text
Draft(version N) -> Validation Operation -> ValidEvidence | InvalidEvidence
ValidEvidence + unchanged Draft -> immutable Skill Revision
Skill ActiveBinding(generation N) -> Deployment | Cleared  # CAS
Skill Suspension(generation N) -> Enabled | Suspended      # 独立安全门
```

`Deployable`只在Skill Deployment validation时计算，不是Skill Revision字段。Retired属于Entity lifecycle，Suspended属于独立
generation gate；两者都不能改写Revision/Deployment或用active binding为空推断。

Activation 状态机：

```rust
enum SkillActivationState {
    Proposed,
    Active,
    Rejected,
    Superseded,
}
```

```text
Proposed -> Active | Rejected
Active -> Superseded
```

`Superseded` 只允许同一可重入 ModelLoop scope 根据固定 policy 创建新的 activation generation；历史 activation
不可改写。普通 Plan scope 默认不允许撤销已激活 Skill。Run pause 不改变 Activation。

## 14. Persistence 与 Artifact 映射

Skill Draft/Revision/Deployment/active binding/suspension 使用共享 Resource/ResourceVersion/Deployment。依赖、requirements、package entry index 与
prompt asset refs 是 closed、bounded ResourceVersion payload；正文、大 reference 和 package archive 只存 Artifact。
Selection/Activation 保存为 Run 或 Invocation 的 typed snapshot，历史进入 Event；selection key 由 owner aggregate CAS 保证。

## 15. 不变量

- Skill Revision与Deployment发布后不可变，运行时只能使用RunBindings中的exact Deployment/Revision；
- Skill 不能直接产生外部 Effect，所有 Effect 必须经过真实 CapabilityInvocation；
- Skill 的 instruction 不能提高消息优先级或扩大 Principal 权限；
- Skill package 不含可执行 entrypoint、Secret value、动态 endpoint 或 mutable dependency；
- 每个 Activation 属于一个 tenant、Run 和 scope，不能跨 Run 共享；
- 选择结果只能来自固定候选集合；
- requirement 缺失时显式失败或不激活 optional section，不按名称猜测；
- Prompt assembly 的来源、顺序、digest 和截断决策可审计。
- Instruction slice 的path、content digest、classification和byte range必须由同一package entry证明，禁止分类降级；

## 16. 幂等、并发与背压

- publish以command scope、idempotency key与package digest幂等；
- 同一 scope/selection key 的并发 proposal 由 generation first-winner；
- selector 有独立并发预算，队列满时 durable defer，不占 Model/Capability permit；
- package validation、malware scan 和 closure resolution 使用有界 Worker；
- candidate 数、description bytes、embedding inputs、activation 数和 assembly tokens 均有硬上限；
- Registry cache key必须包含tenant、Deployment/Revision ID、policy generation和package digest；
- cache miss不允许退回active binding或网络自动发现。

## 17. 超时、重试、取消与恢复

- validation 可以安全重试，但每次 evidence 固定 package digest 和 validator version；
- model selection timeout 按 NodePolicy 明确失败或使用已固定 required Skills，不能扩大候选；
- Run cancel 停止未提交 selection work；已提交 Activation 作为历史事实保留；
- Worker 崩溃后从 Proposed receipt 和 current generation 恢复；
- package Artifact 缺失/损坏使新 activation fail closed，并触发 suspension/incident；
- selector 结果迟到时必须通过 Run/scope/generation fence；
- Skill suspension 阻止尚未开始的新 Activation，不改写已完成 Prompt assembly。

## 18. 安全、租户与 Secret

- 所有 Skill row、Artifact、cache 和 selector 请求 tenant-scoped；
- package 导入必须验证 digest、media、archive safety、来源和 supply-chain policy；
- Skill 无权读取 Secret；Capability adapter 根据独立 SecretPurpose late resolve；
- reference 的数据分类参与 Provider、Context、Artifact 和 public-output policy intersection；
- Prompt injection scanner 可以产生 evidence/警告/拒绝，但不能替代消息优先级隔离；
- 远程链接不会在 runtime 自动抓取；必须先 ingest 为 Artifact/Context revision；
- 展示名称、作者和说明不得用于授权或跨租户 dedupe；
- suspension、kill switch 和 credential revoke 由 04 执行，Skill 文本不能覆盖。

## 19. 可观测性与隐私

```text
skill_validation_total{outcome,reason_class}
skill_selection_total{mode,outcome}
skill_activation_total{mode,outcome}
skill_assembly_tokens{phase}
skill_candidate_count{mode}
skill_package_rejected_total{reason_class}
```

metric label 不包含 tenant、Skill ID/name、package path、用户 task 或正文。Trace 记录 Revision ID 的受控 hash、
activation generation 和 token counts；默认不记录 instruction/reference 内容。审计覆盖 publish、bind、activate、
suspend 和 selection override。

## 20. 配置与部署

- Skill Registry 与 validator 属于 Control Plane，可与 registry-domain 模块化部署；
- selector work 使用独立 WorkClass/permit，不在 API 请求内执行模型选择；
- package reader 只通过 Artifact service，不能访问宿主目录；
- validator/compiler 版本是发布 evidence 的一部分；
- token estimator/model profile 必须版本化，Deployment 时固定；
- 所有 size/depth/token/candidate limit 有平台硬上限，tenant policy 只能收紧。

## 21. 测试矩阵与验收标准

- canonical package 在 Rust/TypeScript fixture 中产生相同 digest；
- path traversal、symlink、archive bomb、未知字段和 executable entry 被拒绝；
- Skill dependency cycle、alias conflict、missing requirement 和过大闭包被拒绝；
- active Skill Deployment在Run中途变化不改变candidate/closure digest；
- 模型返回未绑定 Skill、重复 ID 或权限 override 时 proposal 被拒绝；
- Skill code file 无法在 API、Worker 或 Sandbox 自动执行；
- Capability Effect/Approval 不会被 Skill instruction 弱化；
- 并发 selection 只有一个 generation winner；
- Prompt assembly 顺序、截断和 source map 在故障恢复后一致；
- Secret/prompt canary 不进入事件、metric、默认日志或 mutation receipt；
- package Artifact 丢失、suspension 与 selector crash 均 fail closed 并可恢复；
- 端到端 fixture 证明 required、PlanSelected、PolicySelected、ModelProposed 四种模式。

满足以上测试、公开 schema、数据库约束、runbook 和容量证据后，本规范才可进入 Verified。

## 22. 明确推迟的工作

- 公共 Marketplace、签名信任联盟和收费分发；
- 运行时 semver solver 与自动升级；
- 跨租户 package/blob dedupe；
- Skill 自动学习、对话写回和在线权重更新；
- 任意第三方模板引擎；
- Skill 直接触发后台定时任务；
- 基于用户行为的个性化推荐。

## 23. 未决问题

CR-181只增加Plan-driven exact Skill selection与evidence复核，不改变Skill无执行状态、脚本必须发布为Sandbox Capability的authority。
CR-181 cross-review已确认相关边界并恢复Accepted。

没有阻止 Context、MCP、Sandbox 或 API 设计的未决问题。Skill authoring UI 可以提供目录、预览和 constraint
辅助，但发布产物必须收敛为本规范的 exact closure、canonical package 和固定 candidate binding。
