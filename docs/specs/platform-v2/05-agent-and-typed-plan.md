# Platform v2 Agent 与 Typed Plan 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-213 |
| 日期 | 2026-08-30 |
| 依赖 | [`01-architecture-and-domain-boundaries.md`](01-architecture-and-domain-boundaries.md)、[`02-identity-revision-and-deployment.md`](02-identity-revision-and-deployment.md)、[`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md) |
| 直接下游 | 06、08、09、11、12、16、17、18 |

> CR-204 impact：Agent Deployment create body提交resolved slot intent，但不提交本次command才生成的`adep`、`xcb`或
> binding digest。resolution transaction预留这些identity并物化完整`AgentDeploymentClosure`后再验证和持久化。

> CR-209 impact：简化`model_chat`manifest的作者指令必须由immutable Agent Revision拥有，不能被提升为platform policy或在Run时
> 从active head补取。`AgentResourceSpec.author_instructions`冻结其bounded UTF-8正文；Model assembly只把它投影为独立的
> `AgentInstruction` user/untrusted block。

> CR-211 impact：产品compiler生成的Agent Interface contract digest使用closed v1 preimage，只包含input/output/error
> `ClosedJsonSchema.canonical_digest`；`primary_model` requirement digest使用kind、normalized manifest ref与schema_version。两者均按02
> canonical JSON/SHA-256计算，不包含Artifact或server ID。

> CR-212 impact：简化manifest的normalized `metadata.name`冻结到`AgentResourceSpec.authoring_name`，成为Agent Resource的
> bounded authoring identity；创建后不可更名。Agent/Run产品summary只从该Resource authority投影，不从本地lock、Artifact文件名或
> display name猜测。

> CR-213 impact：简化compiler的closed `required_features`同时冻结到`AgentResourceSpec.required_features`。该bounded sorted set
> 随Draft CAS与immutable Revision digest处理，是草稿态和发布态`AgentSummaryV1`的唯一来源；API不得从active Deployment、Plan Artifact、
> client lock或Event反推。

> Persistence ruling：Agent、Revision 与 Deployment 复用 02 的共享 Resource 模型；运行事实复用 03 的共享聚合。
> 本规范不再定义 Agent 专用 lifecycle/evidence/head/suspension 表。

> 2026-08-29 implementation feedback（CR-203）：public Agent Draft在publish前无法知道服务端将生成的Interface Revision ID。
> current Typed Plan wire提升为v5，使用Draft内已验证的`interface_contract_digest`绑定Interface语义；publish原子生成的
> Interface/Plan Revision ID只进入immutable Version、Deployment与Run binding。materialization必须同时重验同一Agent publish
> batch、exact Version owner和contract digest，不能把digest当作可变head或compatibility selector。

> 2026-08-24 implementation feedback（CR-180）：Run terminal接线时确认无字段的`Return`/`Raise`无法从冻结Plan确定
> final value或safe Failure，而06要求Succeeded/Failed各自拥有typed terminal authority。CR-180将两者收紧为exact data-port
> consumer并把未发布Plan wire提升为version 3；调用方和Worker不得提交自由terminal正文或schema。

> 2026-08-24 implementation feedback（CR-181）：外部叶节点接线时确认Plan v3只保存`resume`，不能确定exact dependency
> slot、输入输出port、预算、deadline或等待合同。CR-181补齐全部external leaf payload，并把未发布Plan wire提升为version 4；
> Scheduler只可从exact Plan、RunBindings和已提交RunValue构造下游命令。

> 2026-08-25 implementation feedback（CR-184）：external leaf terminal不重新把同一leaf Node置为Ready。唯一terminal owner
> 在写入声明output/result port的同一事务将当前leaf Node终结，并按Plan `resume`创建目标NodeExecution及其唯一Orchestration Job；
> 否则`None` observation会再次dispatch同一leaf。retry/deferred仍由leaf owner/current Job处理，不激活Plan resume。

## 1. 决策摘要

Agent publish从同一Draft原子产生类型化Interface Revision与编译后的不可变Plan Revision。YAML与Graph只是两种
authoring surface；运行时只执行Plan。Agent Deployment再把全部dependency slot解析成exact typed binding。Plan使用
闭合节点代数、显式控制/数据端口和结构化循环，不允许任意跳转或隐式共享状态。v2只有一种durable执行语义，不再
提供terminal-only双运行时。

## 2. 目标与非目标

### 2.1 目标

- 让 Agent 具有稳定、机器可验证的输入输出 Interface；
- 把 authoring syntax 编译为同一个规范化、类型正确、可恢复的 Plan；
- 在 publication 时完成类型、作用域、可达性、依赖和策略验证；
- 将模型产生的工具调用视为调用意图，并转成真实 CapabilityInvocation；
- 支持 branch、parallel、map、loop、try/catch、wait、human task 和 child Run；
- 保证 Plan 中所有外部叶节点通过 Deployment slot 绑定精确后端；
- 让错误、取消、预算、Artifact 和公开观测具有显式语义。

### 2.2 非目标

- 不兼容 `insight.agent/v1` YAML 或 Graph wire format；
- 不允许用户代码作为 Plan 节点实现；代码只能成为 Sandboxed Capability；
- 不允许任意图环、动态 `goto`、隐式全局 state 或运行时 AST 注入；
- 不允许模型创建新节点、扩大 Capability 白名单或更改 Policy；
- 不把 Skill 作为可执行节点；Skill 只参与 prompt/method assembly 和已绑定 Capability 选择；
- 不提供 volatile/terminal-only 执行模式；
- 不用 Provider 原生结构化输出声明替代本地 Schema 校验。

## 3. Agent Interface

```rust
struct AgentInterfaceRevision {
    interface_revision_id: ResourceVersionId,
    agent_id: AgentId,
    input_schema: ClosedJsonSchema,
    output_schema: ClosedJsonSchema,
    error_schema: ErrorSchema,
    artifact_ports: Vec<ArtifactPort>,
    data_policy: DataFlowPolicy,
    semantic_digest: Digest,
}
```

- 输入输出使用 JSON Schema 2020-12 的平台闭合子集；
- object 默认 `additionalProperties: false`；
- optional 与 nullable 分离；
- number 必须有限，integer 必须声明可接受范围；
- array、string、object depth 和 union branch 都有硬上限；
- 二进制和大正文使用 `ArtifactRef`，不以内联 base64 逃避大小限制；
- `Message`、`ArtifactRef`、`Citation` 和 `Failure` 是平台内建 nominal types；
- `any` 只允许显式 `BoundedJson`，且必须声明 depth、properties、items 和 bytes 上限。

规范Schema Profile ID是 `insight.closed-json-schema/1`。它只允许JSON Schema 2020-12中的
`$schema/$id/$defs/$ref/type/title/description/properties/required/additionalProperties/items/minItems/maxItems/
uniqueItems/minLength/maxLength/minimum/maximum/exclusiveMinimum/exclusiveMaximum/multipleOf/enum/const/oneOf`，以及平台
扩展`x-platform-max-bytes`和`x-platform-classification`。`x-platform-classification`的值只能是04的closed
`public | internal | confidential | restricted`；它是字段data-flow annotation，不改变JSON Schema validation结果。
额外规则：

- Interface input/output/error根节点必须是object，且每个object显式`additionalProperties: false`；
- array/string/number/integer必须显式声明适用的上下界；UTF-8 byte上限使用规范扩展`x-platform-max-bytes`；
- `oneOf`只表达有唯一string discriminator且每个branch使用不同`const`的TaggedUnion；nullable使用显式null branch，
  optional只由父object的`required`决定；
- `$ref`只允许同一document的acyclic `$defs`或固定digest的platform nominal schema；禁止递归、remote ref和运行时fetch；
- `default`、coercion、`pattern`、开放`format`、`anyOf/allOf/not`、conditionals、unevaluated和unknown keyword全部拒绝；
- `ArtifactRef`、`Message`、`Citation`、`Failure`、UTC timestamp、UUIDv7 ID和DecimalMoney只能引用platform nominal schema；
- parser在schema校验前拒绝duplicate key、invalid UTF-8、NaN/Infinity、越界number和超过HardLimitProfile硬限制的
  depth/property/item/byte count；
- schema按02 canonicalization计算digest；05、09、12、16、17不得定义第二套“近似closed”profile。

所有schema document使用closed `ClosedJsonSchema { schema_version=1, profile, schema, canonical_digest }`保存完整
validation snapshot；`schema`的canonical JSON上限为262144 bytes。精确Revision保存完整document，下游durable admission可在
已经解析并验证exact Revision后只冻结digest。Capability Interface额外要求每个object property都具有有界非空
`description`和`x-platform-classification`；该要求属于09的Capability publication profile，不把annotation变成第二套schema
keyword registry。

Agent Interface Revision 与 Agent Plan Revision 分离；一个 Agent Revision 明确实现一个 Interface Revision。
二者由同一个Agent Draft validation/publish command原子产生：Interface使用`aif_<uuidv7>`，Plan Revision使用
`arev_<uuidv7>`。Interface没有独立Draft/Head/API；任何interface语义变化都随新Agent Revision发布，但多个
Plan Revision可以在schema/digest完全相同时引用同一immutable Interface Revision。

`AgentResourceSpec`包含`authoring_name`、closed `required_features`与nullable `author_instructions`。`authoring_name`必须匹配
`[a-z][a-z0-9-]{0,62}`，由简化manifest的normalized `metadata.name`逐字节物化；Agent Resource创建后，Draft update必须保持同一值，
不能用display name、Artifact文件名、project lock或当前Deployment重命名。name只需在project-local lock内唯一，不引入tenant-wide
唯一约束。`required_features`是最多16项、按wire value严格排序且不重复的closed `AgentRequiredFeature`集合；简化compiler当前只允许
`model`，`deterministic`物化空集合，`model_chat`物化`[model]`。高级authoring也必须显式提交该字段，不能在读取时从Plan或Deployment猜测。
`author_instructions`非空时为1～16384 UTF-8 bytes并拒绝NUL；这些字段随完整Agent document参与canonical digest与Draft CAS，
publish后不可变。作者正文不是platform/system policy，不得改变Capability、Secret、Effect、approval、budget或exact binding authority。
没有模型作者指令的Agent固定保存`null`，不能使用空字符串表示另一种状态。

简化产品compiler的`contract_digest` preimage固定为closed
`{schema_version,input_schema_digest,output_schema_digest,error_schema_digest}`；三个digest来自对应完整ClosedJsonSchema snapshot。
该规则只规范简化authoring source，不改变高级作者显式提交已验证Agent document的能力。Typed Plan与Agent document必须携带同一结果。

## 4. 作者文档

每个 Draft 只能选择一种 authoring mode：

```text
StructuredAuthorDocument | GraphAuthorDocument
```

对应machine wire registry固定为`structured | graph`；数据库、API和SDK不得从Rust type名自行推导另一套值。

作者文档必须声明：

- Agent Interface；
- named type definitions；
- dependency slots；
- workflow body；
- node policy；
- prompt/asset references；
- source metadata。

坐标、颜色、viewport、折叠、注释和协作 presence 位于独立 ViewDocument，不进入 semantic digest。

Structured authoring 使用受限 YAML/JSON schema 和显式稳定 ID。Graph authoring 使用 typed ports/edges。
两者编译到同一个 Plan；格式转换是显式操作，无法无损表达时返回稳定 `not_reducible`，不得静默丢语义。

## 5. 类型系统与值

```rust
enum PlanType {
    Null,
    Boolean,
    Integer(IntegerBounds),
    Number(NumberBounds),
    String(StringBounds),
    Array { item: Box<PlanType>, bounds: ArrayBounds },
    Object(ClosedObjectType),
    TaggedUnion(TaggedUnionType),
    Message,
    ArtifactRef,
    Citation,
    Failure,
}
```

- Plan Value 不可变；
- 控制边与数据边分离；
- 节点只能读取声明的输入端口；
- branch/local scope 值必须通过 typed yield/merge 离开作用域；
- 大值在节点提交前提升为 ArtifactRef；
- SecretBindingId和Secret value都不是普通Plan Value；Binding只存在于Deployment，只有受信任backend adapter能按04解析；
- 类型兼容使用精确结构规则，不做 string-to-number 等隐式转换。

## 6. 表达式

作者表面允许受限表达式，Compiler 将其 lower 为纯、类型化 Expression IR：

```rust
enum Expr {
    Literal(TypedValue),
    ReadPort(PortRef),
    ReadField { source: Box<Expr>, field: FieldId },
    Index { source: Box<Expr>, index: Box<Expr> },
    Unary { op: UnaryOp, value: Box<Expr> },
    Binary { op: BinaryOp, left: Box<Expr>, right: Box<Expr> },
    CallPure { function: PureFunction, args: Vec<Expr> },
    ConstructObject(Vec<(FieldId, Expr)>),
    ConstructArray(Vec<Expr>),
}
```

表达式不得执行 I/O、读时钟、生成随机数、访问环境变量或遍历无界集合。`now`、UUID 和随机数如果需要，
必须成为显式 Capability。运行时只执行已编译 IR，不重新解析作者字符串。

## 7. Dependency Slot

Plan 不引用具体 implementation：

```rust
enum DependencySlot {
    Model(ModelRequirement),
    Capability(CapabilityRequirement),
    Context(ContextRequirement),
    ChildAgent(AgentRequirement),
    Skill(SkillRequirement),
}
```

每个 slot 有稳定 `slot_id`、精确 Interface Revision 或受限 compatibility requirement、用途和数据策略。
Agent Deployment将每个slot解析为exact resource Deployment并同时冻结其owner Revision；RunBindings固定该结果。
运行时只能在已经绑定的candidate集合中选择，不能discovery、绕过Deployment或追随active binding。

简化`model_chat`的slot ID固定为`primary_model`，requirement digest preimage固定为closed
`{schema_version:1,kind:"model",manifest_ref}`。exact Model Deployment与selection Policy属于Deployment binding，不得混入requirement。

## 8. Canonical Typed Plan

```rust
struct TypedPlan {
    plan_version: PlanVersion,
    interface_contract_digest: Digest,
    entry_node_id: PlanNodeId,
    nodes: BTreeMap<PlanNodeId, PlanNode>,
    control_edges: Vec<ControlEdge>,
    data_edges: Vec<DataEdge>,
    dependency_slots: BTreeMap<SlotId, DependencySlot>,
    source_map: SourceMap,
    semantic_digest: Digest,
}
```

`interface_contract_digest`必须等于同一Agent Draft的`AgentResourceSpec.contract_digest`。它只消除publication前的身份
依赖，不替代exact Version identity：publish仍为Interface和Plan生成不同UUIDv7；Deployment必须绑定同一Agent、同一publish
batch的exact Interface/Plan Revision；Run admission继续冻结这两个ID与semantic digest。materialization若发现owner、batch、
digest或schema任一不一致，必须在创建Node/Job前fail closed。

### 8.1 可执行表达式闭包

Typed Plan不能只保存Branch目标、Map上限或Loop边；否则Scheduler无法从已提交RunValue确定下一步，只能由fixture或
调用方注入自由`ControllerObservation`。Plan Revision必须为所有纯数据计算冻结可执行、类型验证后的表达式程序：

```rust
struct TypedExpressionProgram {
    expression_version: u32,              // 首版固定1
    input_ports: Vec<ExactDataPortRef>,
    instructions: Vec<TypedInstruction>,
    output_schema_digest: Digest,
    maximum_stack_depth: u16,
    semantic_digest: Digest,
}

enum TypedInstruction {
    LoadPort { port: ExactDataPortRef },
    Literal { value: BoundedJson, schema_digest: Digest },
    GetField { field: BoundedName },
    GetIndex,
    ArrayLength,
    MakeArray { item_count: u16 },
    MakeObject { ordered_fields: Vec<BoundedName> },
    Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
    BooleanAnd, BooleanOr, BooleanNot,
    IntegerAdd, IntegerSubtract, DecimalAdd, DecimalSubtract,
    StringConcat,
    Coalesce,
    Select,
}
```

该registry是closed machine wire；不得保存源码字符串、动态函数名、脚本、JSONPath/JMESPath或provider表达式。每条
instruction在publication时完成stack effect、input/output schema、nullable/optional、integer/decimal范围和最大输出bytes验证。
程序的绝对上限是4096条instruction、64个input port、stack depth 256；每个deployment还必须读取18的exact
`HardLimitProfile v5+`中`expression_instructions`、`expression_input_ports`和`expression_stack_depth`，三项有效值必须非零且
分别不大于绝对上限。缺字段、旧profile version、unit错误或profile值放大绝对上限均fail closed。执行只读exact
RunValue，不访问时钟、随机数、网络、Secret、文件或active Resource；同一输入bytes与program digest必须产生相同canonical
JSON或相同typed failure。增加opcode必须提升`expression_version`并同步schema、verifier、Scheduler exhaustive match与fixture。

以下node字段属于Plan semantic digest，不得在运行时补猜：

```rust
struct ComputeNode { assignments: Vec<PortAssignment>, next: PlanNodeId }
struct BranchNode { ordered_arms: Vec<{ when: TypedExpressionProgram, target: PlanNodeId }>, otherwise: PlanNodeId }
struct MapNode { items: TypedExpressionProgram, item_port: ExactDataPortRef, body: PlanNodeId, next: PlanNodeId, max_items: u32, failure_policy: MapFailurePolicy }
struct LoopNode { condition: TypedExpressionProgram, carried_ports: Vec<LoopCarriedPort>, body: PlanNodeId, exit: PlanNodeId, max_iterations: u32 }
struct ReturnNode { value: ExactDataPortRef }
struct RaiseNode { failure: ExactDataPortRef }
```

产生值的external leaf使用同一条exact data contract：`input/request`引用当前词法Scope中已经提交的RunValue，`output/result/
response/payload`必须是producer等于当前node的`NodeOutput`。其schema在publication时与slot所绑定Interface或Task/Signal response
schema完全一致；下游
terminal result只授权owner transaction写该port，Worker、provider callback和API均不能另选port、schema或classification。

```rust
struct ModelLoopNode {
    model_slot_id: SlotId,
    skill_slot_ids: Vec<SlotId>,
    capability_slot_ids: Vec<SlotId>,
    input: ExactDataPortRef,
    model_route: Option<ExactDataPortRef>,
    output: ExactDataPortRef,
    max_rounds: u16,
    max_capability_calls: u32,
    max_parallel_calls_per_round: u16,
    token_budget: u64,
    resume: PlanNodeId,
}

struct CapabilityCallNode {
    capability_slot_id: SlotId,
    input: ExactDataPortRef,
    candidate_route: Option<ExactDataPortRef>,
    output: ExactDataPortRef,
    attempt_limit: u16,
    retry_backoff_milliseconds: u64,
    resume: PlanNodeId,
}

struct ContextQueryNode {
    context_slot_id: SlotId,
    request: ExactDataPortRef,
    result: ExactDataPortRef,
    maximum_items: u32,
    resume: PlanNodeId,
}

struct ChildAgentCallNode {
    child_agent_slot_id: SlotId,
    input: ExactDataPortRef,
    candidate_route: Option<ExactDataPortRef>,
    output: ExactDataPortRef,
    budget: ChildBudgetLimit,
    cancellation_policy: CascadeAndWait | CascadeWithDeadline,
    attempt_limit: u16,
    retry_backoff_milliseconds: u64,
    resume: PlanNodeId,
}

struct ChildBudgetLimit {
    maximum_duration_milliseconds: u64,
    maximum_model_tokens: u64,
    maximum_capability_calls: u32,
    maximum_artifact_bytes: u64,
    maximum_descendant_runs: u32,
}

struct HumanTaskNode {
    definition: HumanTaskDefinition,
    response: ExactDataPortRef,
    timeout_milliseconds: u64,
    resume: PlanNodeId,
}

enum HumanTaskDefinition {
    Interaction {
        interaction_kind: Form | UrlConsent | BusinessInput,
        eligible_principal_rule_digest: Digest,
        safe_prompt_key: BoundedName,
    },
    HumanWork {
        eligible_principal_rule_digest: Digest,
        safe_prompt_key: BoundedName,
    },
}

struct TimerWaitNode {
    delay_milliseconds: u64,
    resume: PlanNodeId,
}

struct SignalWaitNode {
    signal_key: BoundedName,
    payload: Option<ExactDataPortRef>,
    timeout_milliseconds: u64,
    resume: PlanNodeId,
}
```

`HumanTaskDefinition`是06 shared `TaskDefinition::Interaction | HumanWork`的Plan-safe closed子集；TaskKind由variant/interaction kind
唯一推导，response schema digest来自`response` port，assignment/claim由exact eligible-principal Policy rule冻结。不得把Approval、
CapabilityInput、MCP OAuth、自由prompt正文、principal discovery或opaque assignment JSON塞进HumanTask node。`ChildBudgetLimit`
冻结duration、model token、Capability call、Artifact bytes与descendant Run上限；实际child deadline取parent remaining deadline与node首次
Running数据库时间加`maximum_duration_milliseconds`的较小值，随后形成08的absolute `ChildBudget.deadline`；
Timer/Task/Signal timeout从node首次Running的数据库时间计算且不得越过Run deadline。Signal无payload时不声明output；
有payload时该port必须由当前Signal node产生并由signal owner transaction按schema写入。

所有slot ID必须存在于同一Plan的`dependency_slots`且variant匹配；ModelLoop的skill/capability slot规范排序且不重复。可选route
port是04 exact candidate selector的唯一动态输入，也必须在当前Scope可达并与Policy Revision声明的route schema一致。所有数值预算
必须非零、不超过18的effective HardLimitProfile；`output/result/response/payload` producer必须是当前node；所有input schema必须与
exact bound Interface input schema一致。publication同时验证输入port在node路径可达、output port只声明一次、resume可达且不会越过
结构化region。运行时selection只能在RunBindings对应slot已冻结的candidate集合中执行其exact Selection Policy，并在owner transaction
重验选择；没有candidate、policy不确定或binding漂移均稳定失败，不能追随active head。

CR-182 mode约束：`route_hash`必须且只能对应一个route port；`only_candidate/ordered_first`禁止route port。ModelLoop的Skill与
Capability tool slots没有逐slot route字段，因此这些slot首版只允许`only_candidate | ordered_first`；Model/CapabilityCall/ChildAgentCall
的route分别只作用于其主slot，不能复用一次hash为其他slot选candidate。

`Compute.assignments`必须按拓扑排序且每个output port只写一次；Branch按声明顺序执行并始终有`otherwise`；Map的`items`
输出必须是有界array；Loop的`condition`输出必须是non-null boolean。表达式所需input port是Node readiness条件的一部分。
Map `item_port`必须是producer等于当前Map node的`NodeOutput` ref并冻结element schema digest；Compiler/publication必须从items
array schema验证其element schema完全一致，不能在runtime从array digest猜测、复用array schema或接受caller声明。该wire变更将未发布
Typed Plan `plan_version`提升为5，version 1/2/3/4不进入clean-cut target。v5保留v4全部external leaf与terminal
port语义，并以`interface_contract_digest`消除publish前身份环。`Return.value`必须在其词法Scope可达，schema digest必须等于
exact Agent Interface Revision的`output_schema.canonical_digest`；`Raise.failure`同样必须可达并等于该Interface的
`error_schema` canonical digest，且物化正文必须能解码为平台safe `Failure` nominal type。Compiler/publication拒绝RunInput/NodeOutput
owner不合法、schema不等、跨结构化region或无法在该terminal路径到达的port。

表达式classification不是caller输入，也不增加可降级的output annotation。同一expression controller先对全部external input
RunValue classification做`Public < Internal < Confidential < Restricted` lattice join；Compute的全部派生output继承该join结果。
Compute assignment在同一node内引用先前assignment时不重复改变classification，因为这些值已经包含在同一external closure中；完全没有
external input的常量Compute输出固定为`Internal`。Branch/Map/Loop不产生表达式output RunValue，因而不增加observation classification字段；
其evidence保留exact input RunValue identity即可。任何提交方提供的更低classification都必须在写RunValue前拒绝。

`ExactDataPortRef`是closed source enum：`RunInput { schema_digest }`或
`NodeOutput { producer_node_id, port_id, schema_digest }`；RunInput不是伪造的Plan node，也不存在自由source kind。它标识Plan级
输入或producer node/port/schema，不直接等同某个动态NodeExecution。运行时从当前Scope开始按父Scope词法链解析
`ExactDataPortRef`，得到一个`ExactRunValueRef { value_id, schema_digest, content_digest }`；最近Scope胜出。只有Map item port、Loop carried
port及显式结构化scope entry可以shadow，普通Compute重复绑定同一port必须冲突。解析链不超过现有
`registry_plan.plan_nodes` effective limit，每Scope binding数和全Run引用数均不超过`run_scheduler.value_refs_per_run` effective limit；
不新增隐式profile字段，也不扫描整个Run。

## 9. Agent Deployment

```rust
struct AgentPlanRevision {
    plan_revision_id: ResourceVersionId,
    agent_id: AgentId,
    interface_revision_id: ResourceVersionId,
    author_document_artifact_id: ArtifactId,
    typed_plan_artifact_id: ArtifactId,
    compiler_manifest_digest: Digest,
    validation_id: ValidationId,
    semantic_digest: Digest,
}

struct AgentDeployment {
    agent_deployment_id: DeploymentId,
    agent_id: AgentId,
    interface_revision_id: ResourceVersionId,
    plan_revision_id: ResourceVersionId,
    entry_node_id: PlanNodeId,
    entry_node_kind: PlanNodeKind,
    resolved_slots: Vec<FrozenSlotBinding>,
    policy_deployments: Vec<ExactPolicyDeploymentRef>,
    execution_profile_deployment: ExactPolicyDeploymentRef,
    public_projection_policy_deployment: ExactPolicyDeploymentRef,
    validation_evidence_id: EvidenceId,
    deployment_digest: Digest,
}

```

每个candidate ID必须通过slot variant对应的resource kind/prefix验证：Model只接受`mdep`，Capability只接受`cdep`，
ChildAgent只接受`adep`，Skill只接受`skdep`；Context slot必须内嵌带`xcb` identity/canonical digest且引用exact `xdep`的
ContextBindingSnapshot。`entry_node_id`/`entry_node_kind`必须与exact Plan Revision内已验证Typed Plan的入口一致，并进入
Deployment closure digest；Run admission不得重新读取Artifact或接受调用方提供的内部入口。candidate集合规范排序、非空（除明确
optional slot）且有硬上限。Deployment validation计算完整dependency closure、Interface兼容、Effect/Policy、Secret
purpose coverage、data region/classification、循环、预算、worker/runtime availability与conformance evidence。
每个slot外层`binding_digest`按`{slot_id, requirement_digest, target}`的canonical JSON计算；Context target中的
`ContextBindingSnapshot.binding_digest`按12的closed snapshot计算。两者均为服务端派生事实，不属于create request。

生命周期为：

```text
Agent Entity -> Agent Draft -> Validation
 -> atomic Interface Revision + Plan Revision
 -> bounded Deployment resolution/validation command
 -> immutable Agent Deployment -> Resource active binding / administrative gate
```

resolution/validation是创建immutable Deployment前的命令内工作或shared Job，不建立mutable Deployment state/evidence aggregate。
Active Head与Suspension由02的Resource active binding/gate唯一拥有。

publish、deploy与activate是三个独立command；active head只指向Agent Deployment。Revision/Deployment均不可变，Run
admission只复制exact Deployment与binding closure，不重新编译Plan或解析slot。

节点代数是闭合集合：

```rust
enum PlanNodeKind {
    Start,
    Compute(ComputeNode),
    Branch(BranchNode),
    Fork(ForkNode),
    Join(JoinNode),
    Map(MapNode),
    Loop(LoopNode),
    ErrorBoundary(ErrorBoundaryNode),
    ModelLoop(ModelLoopNode),
    CapabilityCall(CapabilityCallNode),
    ContextQuery(ContextQueryNode),
    ChildAgentCall(ChildAgentCallNode),
    HumanTask(HumanTaskNode),
    TimerWait(TimerWaitNode),
    SignalWait(SignalWaitNode),
    Return(ReturnNode),
    Raise(RaiseNode),
}
```

Machine wire固定为`start | compute | branch | fork | join | map | loop | error_boundary | model_loop |
capability_call | context_query | child_agent_call | human_task | timer_wait | signal_wait | return | raise`；Compiler、Plan
artifact、Scheduler和fixture只消费该registry，不能按Rust variant debug名称或自由字符串编码。

增加节点类型需要提升 `plan_version`、更新 verifier、storage schema、scheduler exhaustive match、public
schema 和 conformance fixtures。未知节点必须 fail closed。

## 10. 控制流语义

### 9.1 Branch

条件按声明顺序执行，首个 true 分支胜出；条件错误产生 typed expression failure，不按 false 处理。
未选择分支不创建 NodeExecution，也不生成伪 `Skipped` 记录。产生值的 branch 每条可继续路径必须 yield
兼容类型。

Scheduler产生的选择不是外部输入。它必须携带由exact expression program和immutable RunValue导出的
`ControllerObservationEvidence { source_node_version, expression_digest, ordered_input_value_refs, result_digest }`；提交事务重验
Job fence、Node version、每个RunValue的tenant/run/schema/content digest及所选target。Evidence只属于command/Receipt/Event低频
详情，不建立current observation表，也不复制RunValue正文。

### 9.2 Fork/Join

Fork leg 使用稳定 ID。Join policy 只有：

```text
AllSuccess | AllSettled | Quorum(n)
```

Quorum 必须声明剩余 leg 的 cancel/drain 策略。父 scope 在已接纳 child work settle 或安全 drain 前不能闭合。

### 9.3 Map

Map 输入必须是有上限数组，运行时另受 fan-out 与并发 quota。每个 item 创建稳定 ScopeInstance；结果按
输入 index 排序，不按完成顺序。失败策略只有 fail-fast、all-settled 或 bounded-error-count。

fan-out 分批准入时，`fail-fast` 与 `bounded-error-count` 必须在批次之间设置 durable admission barrier：只有当前已准入
item 全部结算且失败计数仍未触发停止条件，下一批才可进入 Ready。任一已提交 item 结果触发停止条件时，同一事务必须
关闭 admission barrier、取消仍活动的已准入同级 item，并唤醒 Map settlement；未准入 item 不创建 ScopeInstance、
NodeExecution 或 Job。`all-settled` 不会因 item 失败停止准入，因此允许有界地流水化后续批次。失败计数、批次 cursor 与
settlement 都必须从同一组 durable Scope/Node facts 推导，不能依赖进程内计数或 wake hint。
首轮item count必须由`items`程序对exact输入求值；continuation只能使用首次提交时冻结在Node payload中的input value ref、
item count和cursor，不能重新读取调用方输入或active binding。

### 9.4 Loop

任意环只能由 Loop node 表达，必须声明 `max_iterations`、deadline/budget 和 loop-carried typed values。
`break`、`continue` lower 为 Loop 的结构化 control port，不形成任意 edge。
每次iteration的condition从该iteration冻结的loop-carried RunValue求值；iteration number来自durable Scope/Node事实，不能由
进程内计数器或调用方提供。condition evidence与iteration advancement在同一first-winner事务中验证/提交。
首轮condition在词法父Scope求值；若为true则创建iteration 0 Scope。此后每轮body settlement先把每个`body_output_port`的
immutable值复制为新的RunValue，以对应`next_iteration_port`绑定预建的下一iteration open Scope，再唤醒该Scope内的Loop
continuation。continuation为true时body复用当前iteration Scope，不再创建第二个Scope；为false时关闭当前Scope并从其词法父Scope
激活exit。所有iteration Scope直接归属首次Loop controller owner，禁止把前一iteration Scope作为下一轮词法父Scope。

每个carried pair必须是两个`NodeOutput` ref、schema digest完全相同，`next_iteration_port.producer_node_id`为当前Loop node；
`body_output_port`必须由Loop body区域内声明的producer产生。Compiler/publication拒绝RunInput、跨region producer、schema不等或重复pair。

### 9.5 Error Boundary

Catch 只匹配稳定 Failure code/class，不匹配原始错误字符串。Finally work 在 success/failure/cancel 下的运行
策略必须显式声明；它不是通用外部副作用补偿。补偿工作流推迟到独立规范。

## 11. 外部叶节点

### 10.1 CapabilityCall

节点引用 Capability slot，参数在调用前通过 Interface input schema；结果在提交前通过 output schema。
节点不包含 endpoint、credential、MCP server、脚本或原生函数名。

### 10.2 ContextQuery

节点引用 Context slot，输出保留 item provenance、citation、score、cursor 和 policy evidence。ContextQuery
默认只读，不允许用普通 JSON Capability 结果替代。

### 10.3 ChildAgentCall

节点引用 ChildAgent slot，创建独立 child Run；详细语义见 08。

### 10.4 Wait/HumanTask

Timer、Signal 和 HumanTask 都是 durable wait。等待态不占 execution permit。HumanTask 与 Signal 不同：
它拥有 assignment、claim、response schema、deadline 和 approval policy。

## 12. ModelLoop

ModelLoop 是 durable controller，不是一个隐藏的同步函数：

ModelLoop的唯一machine wire是10.3的Plan v5 `ModelLoopNode`；本节不另定义含自由`messages`或运行时模板的旧node shape。
Agent contract、作者指令、exact Plan node语义和Prompt Asset来源均由已发布Agent Revision、Typed Plan与Deployment closure冻结，运行时
只能将这些exact材料投影成11/16的canonical assembly block，不能从active head、自由模板或caller正文补猜。

运行过程：

1. 组装固定Agent/Skill Deployment closure内exact Revision的Prompt Asset、Skill内容、Context与安全Message；
2. 创建 durable ModelTurn；
3. 校验模型响应；
4. 如果是 tool intent，校验名称映射、参数、轮次、次数、Effect 和 policy；
5. 为每个合法 intent 创建 CapabilityInvocation；
6. 等待 committed result，按 call ID 续接模型；
7. 最终输出必须通过本地 output schema；
8. 超过任一预算产生稳定 failure。

模型 intent 永远不等于真实执行事件。模型无法选择未绑定 Capability、修改参数 schema、跳过审批或
把文本伪装为平台 tool result。

## 13. Prompt、Skill 与 Message Assembly

```rust
struct PromptAssetRef {
    owner: PromptAssetOwner,
    artifact_id: ArtifactId,
    content_digest: Digest,
    media_type: MediaType,
    schema_digest: Option<Digest>,
    data_classification: DataClassification,
    trust_tag: PromptTrustTag,
    assembly_phase: PromptAssemblyPhase,
}

enum PromptAssetOwner {
    AgentPlanRevision(ResourceVersionId),
    SkillRevision(ResourceVersionId),
}

enum PromptTrustTag { AuthorReviewed, TenantProvided, ExternalUntrusted }
enum PromptAssemblyPhase { AgentInstruction, SkillInstruction, Reference, Example }
```

owner variant严格验证`arev`或`srev`；`PromptAssetRef`是owner-scoped immutable引用，不是全局ID/Entity。Artifact必须
Ready且media/schema/size符合phase profile。MCP/导入内容初始只能是`ExternalUntrusted`，只有显式author review与新owner
Revision publish才能标记为`AuthorReviewed`；任一trust tag都不能获得platform policy优先级。

- Prompt Asset由其Agent/Skill owner Revision + Artifact/digest固定，不存在独立active head或运行时lookup；
- Agent Revision的inline `author_instructions`不是Prompt Asset；它只投影为11/16定义的`AgentInstruction` canonical block，
  source identity/digest来自exact Agent Revision，role固定为`user`且`trusted_instruction=false`；
- 模板只能读取声明端口和安全 metadata；
- instruction、retrieved content、tool output 和 user content 使用不同 trust tag；
- 模型输入构造必须保留消息顺序和来源，不把不受信任内容提升为 platform policy；
- 文件通过 ArtifactRef 和冻结 delivery policy 解析，模型不接收内部 object key/credential；
- context window 超限使用确定性、有版本的 truncation/summarization policy。

## 14. Node Policy

每个外部节点可引用不可变 policy：

```rust
struct NodePolicy {
    timeout: Duration,
    retry: RetryPolicy,
    failure_route: FailureRoute,
    public_projection: PublicProjectionPolicy,
    budget: NodeBudget,
}
```

Retry policy 不能比 Capability Effect 允许范围更宽。编译器和 Deployment verifier 共同拒绝非法组合，
例如 Irreversible Capability 自动重试。

## 15. 编译与发布管线

```text
parse
 -> schema validation
 -> name/type resolution
 -> expression lowering
 -> structured-control lowering
 -> scope/data-flow analysis
 -> dependency slot collection
 -> policy/effect validation
 -> canonicalization
 -> Plan verifier
 -> semantic digest
 -> immutable Agent Revision
```

Verifier 至少检查：

- stable ID 唯一；
- 所有可达路径 Return 或 Raise；
- control/data port 类型和 cardinality；
- 非结构化环不存在；
- Map/Loop/ModelLoop/parallel 有硬预算；
- dependency slot 全部声明且无通配符；
- Secret 不作为 Plan Value；
- public projection 不超过 data classification；
- source map 覆盖所有 authored semantic node。

发布保存作者文档、Typed Plan、source map、schema、diagnostics 和 digest。运行时绝不使用新 compiler 重新
编译历史 Revision。

Validation evidence必须能表达编译早期失败：失败结果可以整体缺少Interface/TypedPlan编译输出，不能创建占位Artifact；
一旦携带编译输出则整组Artifact ID/content digest、Interface/Plan semantic digest、compiler manifest和slot count必须完整。
只有`Succeeded`且mandatory checks全部通过、整组编译输出完整、仍绑定当前Draft generation/author digest并在有效期内的
evidence可以被publish复合外键引用。

## 16. Persistence 与 Artifact 映射

Agent 的 Draft、Interface、TypedPlan、validation result、active target 与 suspension 使用 02 的共享 Resource/
ResourceVersion；环境绑定和完整 FrozenSlotBindings 使用共享 Deployment。作者文档、TypedPlan、source map 与大型
diagnostic 保存为 immutable Artifact，ResourceVersion 只保存 bounded typed metadata、ArtifactRef 与 digest。

Run admission 把完整 binding closure 复制成 02 的 `RunBindingsSnapshot`。数据库只需保证 tenant/FK/版本不可变与 CAS；
对Context的`PinAtRunAdmission`，RunBindings还必须保存按`context_binding_id`规范排序的exact dataset-view；该view由Run admission
在同一事务从ContextDataset active version解析并进入RunBindings canonical digest，后续ContextQuery不得重新追随active head。
root Run在tenant事务中冻结当前active Agent及所有slot的exact ResourceVersion/Deployment；同一snapshot内所有Model slot候选都必须为
Ready且与其Model Profile/Provider binding兼容，runtime Selection Policy只能在已经完整验证的集合内选择。slot连续性、kind/schema/digest匹配、
validation completeness、全Run候选上限和bindability由Rust closed types在同一事务校验。

## 17. 错误合同

```rust
struct Failure {
    code: FailureCode,
    class: FailureClass,
    retryability: Retryability,
    safe_message: Option<SafeMessage>,
    details_ref: Option<ArtifactRef>,
    source: FailureSource,
}

enum FailureCode {
    Platform(PlatformFailureCode),
    Declared {
        interface_revision_id: ResourceVersionId,
        code: DeclaredFailureCode,
    },
}

enum Retryability {
    Never,
    SafeWithinPolicy,
    ReconcileBeforeRetry,
}

enum FailureSource {
    Agent,
    Plan,
    Capability,
    Context,
    Model,
    ChildAgent,
    Artifact,
    Interaction,
    Dependency,
    Platform,
}
```

`FailureClass`闭集为：

```text
Validation | Authorization | Policy | Quota | Deadline | Dependency |
External | Resource | Cancelled | UncertainEffect | Platform
```

`FailureCode`只有上述两个variant：平台全局code，或绑定某个Agent/Capability Interface error schema Revision的
declared code。`PlatformFailureCode`首批闭集为：

```text
agent_input_invalid
agent_output_invalid
plan_invariant_failed
budget_exhausted
deadline_exceeded
capability_failed
context_query_failed
model_turn_failed
child_agent_failed
artifact_unavailable
interaction_failed
dependency_unavailable
content_rejected
uncertain_effect
platform_invariant_failed
```

declared code必须满足`^[a-z][a-z0-9_]{0,63}$`并存在于fixed error schema，不能覆盖平台code。`not_reducible`
等compiler/validation issue属于ValidationIssueCode，不是runtime Failure。原始Provider/MCP/script error默认不进入
`safe_message`。Catch、Retry和public terminal projection只依赖稳定code/class；17的`ApiProblem.code`是HTTP command
错误的独立闭集，不能用它替代Run Failure。

## 18. 可观测性与隐私

内部事件按 Plan node ID、NodeExecution ID、ModelTurn ID、call ID 和 Invocation ID 关联。public stream
只能投影 publish policy 允许的状态；token delta、tool arguments、progress 和 result 分别授权，互不推导。
live observation 丢失不改变结果。

## 19. 验收标准

- Structured 与 Graph fixture 编译为相同 semantic digest；
- 任意不可归约图不会被静默转换；
- 未选择 branch 不创建 NodeExecution；
- Map 输出在乱序完成时仍按输入 index 排序；
- Loop、ModelLoop 和 parallel 超过预算时稳定失败；
- 模型产生未知 tool、非法参数或越权 intent 时不创建 Invocation；
- Provider 原生 structured output 成功但本地 schema 失败时节点失败；
- 历史 Agent Revision 在 compiler 升级后仍执行保存的 Plan；
- v2 code path 不包含 terminal-only/volatile wait 分支；
- fuzz/property tests 覆盖 schema、expression、scope、graph cycle 和 verifier；
- PlanNodeKind exhaustive match 由编译器与 architecture test 强制。
- expression opcode exhaustive match、unknown opcode/unknown field/noncanonical program负向fixture通过；
- production Scheduler从exact Plan与committed RunValue自行导出Branch/Map/Loop/Compute observation，public/internal调用方均不能
  直接提交selected target、item count、loop condition或Compute结果；
- observation提交事务拒绝错误expression digest、RunValue schema/content digest、Node version、Job fence与跨tenant/run引用；
- root Run admission fixture在Receipt→Tenant→Resource锁序中冻结完整02 binding；active target并发切换只影响未来Run，任一候选不兼容时
  整个admission回滚且没有Run/Receipt成功结果；

## 20. 明确推迟的工作

- authoring YAML 的最终字段名称；
- Graph UI wire format；
- arbitrary graph cycle；
- compensation/saga DSL；
- runtime-generated Plan；
- Skill 选择算法和 prompt packaging 的 authoring UI 细节。

## 21. 未决问题

CR-166已确认root Run只冻结tenant active exact Deployment closure，Model为Inline-only，无installation/candidate binding。
本规范已Accepted；相关API、持久化和分层证据仍待实现。05冻结Plan语义，
Model Provider/ModelTurn由16冻结；具体authoring表面只能在不改变Plan节点代数、类型和恢复语义的前提下单独演进。
