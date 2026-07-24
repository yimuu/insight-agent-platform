# DSL v1 持久化图执行架构规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 变更类型 | Breaking / Runtime Rewrite |
| 日期 | 2026-07-18 |
| 目标版本 | `insight.agent/v1` |

## 1. 范围与规范效力

本文定义 DSL v1 与持久化执行内核的架构和可验证合同，覆盖：

- 结构化 YAML 与画布 Graph 两种作者表面；
- Canonical Typed Plan Graph；
- Branch、Merge、Fork、Join、Map、Loop、Wait 与终止语义；
- Activation、Attempt、Lease 与持久化状态机；
- PostgreSQL execution ledger、projection、outbox、inbox 与 timer；
- 定义版本固定、恢复、redrive、fork 与 migrate；
- 从 Region/SSA 内存运行时到当前持久化内核的九阶段 clean-break 切换结果。

本文是当前 parser、compiler、Plan、durable runtime 与 Graph API 的规范性合同。
[DSL 作者语法精简规范](../../archive/specs/2026-07-17-dsl-authoring-syntax-simplification.md)及更早文档只作为
历史决策记录；本文没有明确保留的旧语法或旧运行时行为均不属于当前生产合同。

### 1.1 与现有规范的关系

| 现有规范 | 当前决策 |
|---|---|
| [DSL 作者语法精简规范](../../archive/specs/2026-07-17-dsl-authoring-syntax-simplification.md) | 保留类型、`$`、`{{ }}`、Message、自然 YAML 和 LLM response 合同 |
| [DSL 作者层重设计规范](../../archive/specs/2026-07-17-dsl-authoring-surface-redesign.md) | 保留 LLM、Action、Prompt、安全和错误合同；替代 structured `switch` 与 Region lowering |
| [DSL vNext Region/SSA Design](../../archive/specs/2026-07-16-dsl-vnext-region-ssa-design.md) | 保留词法作用域、类型验证、取消/drain 和唯一终态；替代 executable Region、递归 scope runtime、路径派生 ID 与进程内中间值 |
| [PostgreSQL Exclusive Store Ownership](../../archive/specs/2026-07-15-postgresql-exclusive-store-ownership-design.md) | 保留 fencing 原则；以多 runtime 的 lease、epoch 与 CAS 替代 singleton store owner |
| [Durable Recovery Finalization](../../archive/specs/2026-07-11-durable-recovery-finalization-design.md) | 保留权威终态收敛；以 Activation checkpoint resume 替代启动时统一标记 interrupted |
| [Live-only SSE](../../archive/specs/2026-07-11-live-only-sse-design.md) | 保留 Attached live-only、terminal 后 EOF 与无 replay；内部 execution ledger 不成为公共 replay API |
| [Response 实时流与 LLM 发布控制规范](./2026-07-19-response-streaming-and-llm-publication-design.md) | Implemented / Verified 窄增量：增加 OpenAI-aligned 实时回答、LLM `stream`/`publish`、工具/RAG 安全投影与 durable final snapshot |
| [Production Lifecycle V1](../../archive/specs/2026-07-15-production-lifecycle-v1-design.md) | 保留健康检查、admission 和 drain；替代 singleton ownership readiness |
| [Authoritative Stop Semantics](../../archive/specs/2026-07-11-authoritative-stop-semantics-design.md) | 保留 runtime first-winner stop authority；改由 durable termination intent 承载 |
| [Terminal CAS](../../archive/specs/2026-07-15-independent-connection-terminal-cas-race-design.md) 与 [Public Agent Contract](../../archive/specs/2026-07-15-public-agent-contract-design.md) | 保留唯一终态 CAS 和公共输入 Schema；内部状态通过明确映射投影到现有公共状态/事件 |
| 历史 `core.*` 控制流规范 | 继续为历史文档；Fork/Join 是私有 Plan 原语，不重新开放 public `core.*` 节点 |

当前实现仅保留 clean-break 路径；发布级完成以本文状态与证据节所列门禁通过为准。
冲突规范按历史记录保留，当前权威关系由
[设计文档权威关系](../README.md)定义；旧 parser、Region/SSA scheduler 与 singleton store
ownership 不得重新进入生产入口或 checked-in positive surface。

### 1.2 规范性术语

- **必须**：实现和定义都必须满足；
- **不得**：明确禁止；
- **应**：除非有单独记录的充分理由，否则必须满足；
- **可以**：可选能力。

本次迁移是 clean break，不提供旧控制流兼容层、双运行时长期共存或运行时自动翻译。

## 2. 目标、非目标与核心不变量

### 2.1 目标

1. 人类作者可以使用自然的结构化控制流，不必手写运行时边和内部节点。
2. 画布上的 LLM、HTTP、Tool 和 Action 始终是一等节点，不被巨型 Switch 或容器节点隐藏。
3. 任一作者格式都编译成同一个不可变、可验证、有类型的 Plan Graph。
4. 进程在任意节点边界崩溃后，可以从已提交 Activation 继续，而不是重跑整个 Run。
5. 多个 scheduler 与 worker 可以共享一个 PostgreSQL store，陈旧 worker 永远不能推进状态。
6. 分支、并行、循环、等待、取消、恢复和版本升级具有明确、可测试的合同。
7. 公共事件继续隐私安全；内部执行事实足以恢复和审计。

### 2.2 非目标

- 不复制 Temporal 的用户代码确定性 replay 模型；
- 不实现 Restate/Temporal 级别的共识、分片或自建存储；
- 不承诺外部 HTTP、LLM、Tool 的 exactly-once 副作用；
- 第一阶段不允许任意图环，只允许一等 `loop`；
- 不保证任意画布图都能还原成漂亮的结构化 YAML；
- SQLite 不提供生产级多进程、lease 或恢复语义；
- 本文不重新定义已稳定的 Message、Prompt、LLM 和基础类型语法。

### 2.3 术语

- **Definition Revision**：已发布且不可变的工作流定义版本。
- **Deployment Revision**：把一个 Definition Revision 绑定到具体模型和可执行组件的不可变部署版本。
- **Author Document**：Structured YAML 或 Graph 中唯一一个可编辑源文档。
- **Plan**：作者文档编译出的规范化执行图，是发布和执行的语义真相。
- **Node**：Plan 中的静态节点定义。
- **Activation**：某个 Node 在一次 Run 中的一次逻辑执行。
- **Attempt**：Activation 的一次实际执行尝试；重试只增加 Attempt。
- **Control Token**：使下游节点获得执行资格的控制凭证。
- **Value**：通过有类型数据端口传递的不可变值。
- **Scope Instance**：Map item、Loop iteration 或 Subflow invocation 的动态作用域实例。
- **Artifact**：文件、图片、大文本或大对象的外部存储引用。
- **Internal Execution Event**：权威执行事实。
- **Public Event**：由内部事实派生的脱敏观测记录；它不是恢复真相，但 terminal delivery 具有单独的 durable outbox 合同。

### 2.4 全局不变量

1. 控制边与数据边必须分离。
2. Published Plan 在 Run 生命周期内不得改变。
3. 一个 Activation 只能提交一次 succeeded、failed、cancelled 或 timed_out 终态。
4. 一个 Run 只能提交一个终态。
5. 父作用域不得在已接纳子 Activation settle 并完成必要 drain 前结束。
6. Retry 不得改变 Activation 身份。
7. 重复或过期 worker completion 不得产生第二次状态推进。
8. 非选中分支不创建 Activation，不伪造 `skipped` 执行记录。
9. 每个外部副作用都必须关联稳定 effect key 和可审计 Attempt。
10. Secret 值不得进入 Plan、event、public event 或默认日志。

## 3. 参考框架与总架构

### 3.1 借鉴与拒绝

| 框架 | 借鉴 | 不复制 |
|---|---|---|
| [Dify](https://docs.dify.ai/en/cloud/use-dify/nodes/ifelse) / [Graphon](https://github.com/langgenius/graphon) | 平面业务节点、Branch 命名端口、画布追踪 | UI 坐标与语义同文档、隐式 skip propagation、巨型容器节点 |
| [Windmill](https://www.windmill.dev/docs/core_concepts/workflows_as_code) | Workflows as Code 与 Flow Editor 双入口、步骤 checkpoint | 递归 `modules[]` 作为人类 canonical DSL、通用 value wrapper |
| [Dagster](https://docs.dagster.io/guides/build/ops/graphs) | 类型化输入输出端口、受控动态 mapping | 隐式 missing output 跳过语义 |
| [LangGraph](https://docs.langchain.com/oss/python/langgraph/persistence) | checkpoint、interrupt、subgraph、pending writes | 隐式全局可变 state、普通节点任意 `goto`、恢复时从节点开头重跑副作用 |
| [Temporal](https://docs.temporal.io/workflow-execution) | append-only history、版本固定、signal/timer、redrive 思路 | 用户代码确定性 replay 和完整服务拓扑 |
| [Restate](https://docs.restate.dev/references/architecture) | journal、durable promise、lease epoch/fencing | 自建共识与分区存储 |
| [DBOS](https://docs.dbos.dev/architecture) | PostgreSQL checkpoint 与事务恢复 | 依赖步骤调用顺序生成身份 |
| [Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-statemachines.html) | 显式状态类型、Retry/Catch/Wait、原版本 redrive | 冗长 ASL 和 provider 绑定 |

### 3.2 分层

```text
StructuredAuthorDocument ----+
                             +--> Compiler --> Canonical Typed Plan
GraphAuthorDocument ----------+                       |
                                                      v
                                           Durable Scheduler
                                              |          |
                                              v          v
                                      PostgreSQL       Task Outbox
                                  Ledger + Projection       |
                                                         Workers

ViewDocument -- stable IDs --> author/plan nodes
Public Event Projection <----- internal execution facts
```

分层职责：

1. 作者层负责可读语法，不暴露 scheduler 指令。
2. Compiler 负责类型、作用域、可达性、稳定身份和 lowering。
3. Plan 是发布与执行的唯一语义真相。
4. Scheduler 只推进持久化状态机，不执行 LLM/HTTP 等叶子副作用。
5. Worker 只执行已租赁的 Task，不决定控制流。
6. ViewDocument 和 Public Event 都不是执行真相。

## 4. 双作者表面

### 4.1 StructuredAuthorDocument

结构化 YAML 面向代码评审、版本控制和人工维护。作者可以使用：

```text
llm | action | http | tool
if / elif / else
parallel
map
loop
match
try / catch / finally
wait
call
yield
break / continue
return / raise
```

示例：

```yaml
api_version: insight.agent/v1
kind: agent

inputs:
  messages:
    type: Message[]
    default: []
  question: string
  report_text: string
  image_url:
    type: string
    optional: true

output: string

workflow:
  steps:
    - id: context
      if: size(messages) > 0
      then:
        - id: follow_up
          type: llm
          model: general_chat
          messages:
            - $messages
            - role: user
              content:
                - text: $question
          response: string
        - yield: $follow_up

      elif:
        - id: has_image
          when: image_url != null
          then:
            - id: image_analysis
              type: llm
              model: general_chat
              messages:
                - role: user
                  content:
                    - text: $question
                    - image_url: $image_url
              response: string
            - yield: $image_analysis

      else:
        - yield: $report_text

    - id: answer
      type: llm
      model: general_chat
      messages:
        - role: user
          content:
            - text: $context
      response: string

    - return: $answer
```

合同：

1. `if` 与 `elif.when` 是 CEL 表达式位置，使用词法名称，不使用 `$` 或 `{{ }}`。
2. 条件必须静态为 non-null boolean，并且无副作用、可确定重放。
3. `if/elif` 按声明顺序求值，首个 true 胜出。
4. 产生值的 `if` 必须有稳定 `id`，每条未提前终止的成功路径必须 `yield` 兼容类型。
5. `yield` 是结构化块终止符，不是可注册节点，也不结束 Run。
6. `return` 结束当前 workflow/subflow；`raise` 产生 typed failure。
7. 不产生值的 `if` 可以省略 `else`，false path 直接到达共同后续。
8. DSL 中产生值的 `if` 必须有 `else`；不依赖 Compiler 对任意 CEL 条件做穷尽证明。
9. 条件表达式异常、null 或类型错误不得按 false 处理；静态可见错误编译失败，其余产生稳定 expression failure。
10. `then` 与 `else` 的稳定端口名分别是 `then`、`else`；每个 `elif` 必须声明稳定 `id` 作为端口名。

`match` 仅是纯值表达式：

```yaml
mode:
  match: $route
  cases:
    follow_up: conversation
    image: vision
  default: report
```

`match` 不得包含步骤、创建 Activation 或改变控制流。当前 DSL 删除 `choose` 和 authored
`switch/case`；等值流程路由使用 `if/elif/else`，纯值选择使用 `match`。未来若增加便捷语法，
也只能 lower 到既有 Branch，不得形成另一套 IR 或 runtime 原语。

并行分支使用稳定 leg ID，并以 `yield` 发布分支值。以下是结构片段，不是可单独编译的
完整 Agent 文档：

```yaml
- id: analyses
  settle: all_settled
  parallel:
    technical:
      - id: technical_analysis
        type: llm
        response: Perspective
      - yield: $technical_analysis

    risk:
      - id: risk_analysis
        type: llm
        response: Perspective
      - yield: $risk_analysis
```

`technical`、`risk` 是稳定 leg ID。`parallel` lower 为 Fork、分支 Activation、Join 和
typed Collect。并行 child scope 中不得使用 `return` 抢占整个 Run；正常结果使用 `yield`，
失败使用 `raise`。`break/continue` 只能出现在 Loop body，并 lower 为 Loop controller 的
结构化端口，不是任意 `goto`。

### 4.2 GraphAuthorDocument

GraphAuthorDocument 使用显式节点、端口和边。以下片段只说明控制端口形态，不是完整可执行的
GraphAuthorDocument；字段级 Schema 由当前 GraphAuthorDocument wire contract 定义：

```yaml
authoring_mode: graph

nodes:
  - id: route
    type: branch
  - id: follow_up
    type: llm
  - id: image_analysis
    type: llm
  - id: merge_context
    type: merge

edges:
  - kind: control
    from: route.follow_up
    to: follow_up.control
  - kind: control
    from: route.image
    to: image_analysis.control
  - kind: control
    from: follow_up.done
    to: merge_context.follow_up
  - kind: control
    from: image_analysis.done
    to: merge_context.image
```

GraphAuthorDocument 必须使用与 Plan 对齐的稳定 ID 和有类型端口，但不得包含坐标、颜色、
折叠状态或 viewport。

### 4.3 单一作者真相与 ViewDocument

1. 每个 Draft 只能选择一个 `authoring_mode: structured | graph`。
2. StructuredAuthorDocument、GraphAuthorDocument 和 Plan 不得同时成为三个可编辑真相源。
3. 发布时保存作者源和 Canonical Plan，但只执行 Canonical Plan；作者源只能用于审计，或创建新的 Draft/Revision，不得用新 Compiler 替换已发布 Plan。
4. 显式格式转换会创建新的作者文档，不进行无提示双向实时改写。
5. 可归约的 Branch、Parallel、Map、Loop 图可以显式转换为 structured；转换必须保留稳定语义 ID。
6. 交叉跳转、不可归约图或未来任意环保持 graph，并给出无法结构化的诊断。
7. 带 verifier 可执行矩阵内 `Policy[]` 的 GraphAuthorDocument 是合法且可发布的 graph-only 作者文档。
   当前 structured 语法不能无损表达 Policy 时，显式 graph -> structured 转换必须返回稳定的
   “不可归约”诊断并保留原 Graph；这不是 Graph 校验错误、Plan 错误或发布错误，也不得通过丢弃
   Policy 强行转换。矩阵外 Policy 则必须作为 Plan/发布错误 fail closed，不能借 graph-only 绕过。
8. `ViewDocument` 单独保存坐标、viewport、颜色、注释和折叠状态。
9. ViewDocument 通过稳定 ID 关联作者节点，不进入 semantic hash。
10. 移动画布节点不得改变 Plan、Node ID 或正在运行的 Run。
11. Compiler 必须保留 Source Map，使错误可定位到 YAML 行列或 Canvas Node/Port。

## 5. Canonical Typed Plan Graph

### 5.1 Plan 结构

Plan 至少包含：

```text
DefinitionRevision
PlanMetadata
Node[]
ControlPort[]
DataPort[]
ControlEdge[]
DataBinding[]
PhiBinding[]
ScopeMetadata[]
Policy[]
SourceMap
SemanticHash
```

Plan 节点分为：

- 叶子任务：`LlmTask`、`ActionTask`、`HttpTask`、`ToolTask`、`RetrievalTask`；
- 控制节点：`Branch`、`Merge`、`Fork`、`Join`、`Map`、`Collect`、`Loop`、`ErrorBoundary`；
- 生命周期节点：`SubflowCall`、`WaitSignal`、`Timer`、`Return`、`Raise`。

Const、Project、Object、Array、Template 与 Match 默认 lower 为 typed pure expression，
不创建 worker Task。只有需要独立追踪或预算时才物化为确定性 Plan Node。

Policy 采用封闭的可执行矩阵，Plan verifier 必须在发布前拒绝矩阵外组合，scheduler 不得接受后
静默忽略：

| Policy | 当前允许的 Node | 执行合同 |
|---|---|---|
| `Retry` | LLM、Action、HTTP、Tool 叶子任务 | 固定到 durable task envelope，恢复后沿用同一策略 |
| `Timeout` | 上述叶子任务、`WaitSignal`、`HumanTask` | 叶子由 attempt deadline/fence 执行；等待由 durable timer first-winner 执行 |
| `Budget` | 无 | 在 token/cost authority、扣减与恢复合同实现前，稳定拒绝发布 |
| 结构节点 `Timeout` | 无 | Map、Loop、Subflow、Timer 等在专用 durable deadline 合同实现前，稳定拒绝发布 |

GraphAuthorDocument 可以合法携带并发布矩阵内 Policy；“Policy 是 graph-only”不代表任意尚无执行
语义的 Policy 都可以发布。Policy 类型可以为后续能力保留 wire 变体，但可发布 Plan 中的每一项
必须具有真实 runtime consumer。

### 5.2 ID 与版本

1. 作者 Node ID 在一个 workflow revision 内全局唯一，必须稳定且不得由 YAML 数组位置、AST 路径或画布坐标派生。
2. 编译器生成节点使用“稳定父 ID + 固定语义角色 + arm/leg ID”生成 ID，并检测碰撞。
3. 重排不具有顺序语义的 YAML 字段不得改变 semantic hash。
4. Branch case 顺序、retry policy、表达式版本和节点 descriptor 具有执行语义，必须进入 hash。
5. Published Revision 必须保存规范化 Plan、semantic hash 与 compiler version。
6. Semantic hash 只覆盖规范化语义投影；SourceMap、作者格式和 ViewDocument 明确排除。
7. Structured/Graph 显式转换必须保留作者 ID，并用同一 canonical naming 生成隐藏节点；独立创作但恰好行为相似的文档不保证 hash 相同。

### 5.3 Region 的新职责

Region 只作为：

- 词法作用域；
- 生命周期和取消所有权；
- capture、dominance 和类型验证边界；
- 画布 group/subflow 的层级元数据。

Region 不得拥有嵌套 scheduler，不得递归执行私有 operation list，不得把真实 LLM、HTTP、
Tool 或 Action 隐藏成一个运行节点。

### 5.4 Branch 与 Merge

1. Branch 只保存有序条件和命名输出端口，不拥有分支业务节点。
2. 每个 Branch Activation 最多选择一个端口。
3. Control Token 必须携带 Branch Activation、selected port 与 scope instance 的相关身份。
4. 选择结果、Control Token 和下游 Activation 必须原子提交。
5. Merge 只消费同一 Branch correlation 下的 Token，重复 Token 幂等。
6. 未选路径不创建 Activation；观测层可以派生 `not_reached`，但这不是执行状态。
7. Merge 只汇合排他控制路径，只等待实际到达的一个 Token，不看静态入度。
8. 数据通过 typed Phi 或 authored `yield` 合并；Merge 本身不猜测值。
9. 所有可到达路径必须产生兼容类型；缺值必须显式使用 optional、default 或 Result。
10. `core.branch_end` 不存在。路径自然到达 Merge，或者通过 Return/Raise 终止。

`PhiBinding` 是附着于 Merge 的确定性数据绑定，不是 Node，也不创建 Activation。它具有由
`if` owner ID 和固定 `result` role 生成的稳定 ID、每个 arm 的 source ValueRef、一个 typed
output port 和统一结果类型。Merge 接收哪个 arm 的 Token，Phi 就发布该 arm 的 source；
提前 Return/Raise 的路径不到达 Merge，因此不要求产生 Phi 输入。示例中的 `$context` 精确
引用该 Phi output port。

### 5.5 Fork、Join 与 Collect

1. 每个 Fork Activation 创建唯一 `fork_group_id`。
2. 静态 parallel 必须在同一事务固化声明顺序和成员集合。
3. 每个 leg Token 携带 `fork_group_id`、稳定 `leg_id` 和 `scope_instance_id`。
4. Join 等待指定 fork group 的实际成员集合，不使用静态入度推断。
5. 当前实现只支持 `all_success` 与 `all_settled`。
6. `all_success` 遇到失败后发出兄弟取消意图，并在 drain 后失败。
7. `all_settled` 等待全部成员，按声明顺序返回带 leg ID 的 typed `Result<T, SafeError>`。
8. 基础设施失败、Run cancel 和 root deadline 不得降级为业务 `SafeError`。
9. Join 是纯状态推进，不派发 worker Attempt，也不执行外部副作用。
10. Collect 只聚合数据；Join 只解决 barrier。两者不得合并成一个模糊节点。
11. 零分支静态 Fork 编译失败；空 Map 是合法执行并立即 Collect 为空有类型数组。
12. race、any、quorum、first_success 必须作为独立后续合同设计，不得成为模糊 boolean option。

静态 Parallel 的 Collect 输出是以 leg ID 为 key 的 closed record；`all_success` 的每个字段是
分支值，`all_settled` 的每个字段是 tagged Result。Map Collect 输出是按持久化输入顺序排列的
typed array，不按完成时间排序。

### 5.6 Map、Loop 与动态实例

1. 静态 Plan 不在运行时增加任意边；动态性只通过 Map、Loop、Subflow 和 AgentLoop 产生实例。
2. Map item 身份使用稳定 `item_key`；无用户 key 时使用规范化 ordinal。
3. Map 必须在派发前持久化输入快照、item key 和声明顺序，恢复时不得重新枚举可变外部来源。
4. 用户 key 在一个 Map Activation 内必须唯一；重复 key 运行失败。
5. Loop 每轮创建新的 `scope_instance_id` 和 Activation，不重新打开旧 Activation。
6. Loop 必须声明退出条件以及 max iterations、deadline 至少之一。
7. 子工作流每次调用具有独立 invocation identity。
8. 普通叶子节点不得创建边或执行任意 `goto`。
9. Agent tool-call 循环使用一等 AgentLoop/child scope，不改变全局图拓扑。

### 5.7 数据、作用域和表达式

1. Value 是不可变 typed value；不存在隐式全局 mutable dictionary。
2. 并行分支默认 capture 启动时的不可变快照。
3. 跨 Map/Loop 读取多实例输出必须显式 Collect。
4. null、字段缺失和 optional 是不同语义，不得互相替代。
5. Template、CEL、Match 和其他纯表达式必须无副作用，并固定引擎版本。
6. 若未来提供共享对话状态，必须定义 StateChannel schema、reducer 和冲突策略。
7. `all_settled`、Catch 和失败输出必须使用 closed tagged Result，不得输出无结构错误 JSON。

公共输入存在性由一个冻结的 `PlanInputContract { accepted_type, defaults }` 表达，而不是把
`optional` 编码成 nullable union。`accepted_type` 描述 API 接受的原始对象；`run_type` 是默认值
补齐后供 Run、端口绑定和恢复使用的类型。顶层字段合同如下：

| 声明/输入情形 | `accepted_type` 中 required | 接受缺失 | 规范化后的 Run 输入 | `run_type` 中 required |
|---|---:|---:|---|---:|
| `name: T`（Required） | 是 | 否 | 保留调用者提供的 `T` | 是 |
| `{ type: T, optional: true }`（Optional） | 否 | 是 | 缺失仍为缺失，不制造 `null` | 否 |
| `{ type: T, default: v }`（Default） | 否 | 是 | 仅字段缺失时补入已验证的 `v` | 是 |
| 调用者显式提交 `name: null` | 取决于该字段声明 | 不适用；这是“存在” | 仅当 `T` 包含 `null` 时原样保留；不得触发 default | 与声明相同 |

补充合同：

1. `default` 与 `optional: true` 互斥；default 必须在编译时满足完整约束类型。
2. required nullable 字段仍要求键存在；optional non-null 字段可以缺失，但显式 `null` 仍类型失败。
3. `defaults` 是 Plan 语义的一部分，必须进入 wire、Graph JSON round-trip 和 semantic hash；改变
   default 必须改变 hash，布局变化不得改变 hash。
4. Run admission 只执行一次冻结合同的规范化；重试、恢复、redrive 不得重新读取当前作者文档的
   default。规范化结果必须同时满足 `run_type`。
5. Subflow child Run 使用其固定 Definition/Deployment Revision 中的同一个 `PlanInputContract`：
   parent 省略 child optional 时保持缺失，省略 child default 时在创建 child Run 输入 payload 前补值，
   显式 `null` 仍按 child 字段类型判断；不得由 parent scheduler 另造一套存在性规则。
6. `StartSubflow` action wire 不是 child admission authority。repository 必须从 parent Run 的完整固定
   Plan/binding identity、该调用 occurrence 的 durable facts、parent `SubflowCall` descriptor 以及 child
   的完整固定 Plan 重新推导 normalized input 与 output contract，并逐项复验 execution revision、
   interface version、timeout、invocation scope、outputs 和 run input；类型正确但来源不同的 default
   替换、optional 注入或 caller 重算 intent hash 均必须失败。
7. 上述复验可以在提交事务前生成确定性快照，但最终提交必须继续以同一 projection version CAS；
   并发推进只能得到 exact replay 或 StateConflict，不能使过期快照成为新 child Run 的授权。

## 6. 持久化执行内核

### 6.1 身份层次

```text
definition_revision_id
deployment_revision_id
run_id
node_id
scope_instance_id
activation_id
effect_id
attempt_no
lease_epoch
```

合同：

- `scope_kind` 是闭集：`root | parallel_leg | map_item | loop_iteration | subflow_invocation | agent_loop_turn`；
- Parallel leg scope 使用稳定 `leg_id`，不得使用声明数组位置或完成顺序派生身份；
- `activation_id` 在节点首次获得执行资格时创建，重试和恢复保持不变；
- `attempt_no` 每次实际执行递增；
- `lease_epoch` 每次重新租赁递增，用于拒绝 zombie worker；
- Attempt 与 lease epoch 是不同概念；
- 初始 `effect_id` 基于 `run_id + activation_id`，Retry 保持不变；Redrive 按第 7.2 节继承映射来源；
- 外部 idempotency key 使用 `effect_id`，不得使用 Attempt ID；
- 同一静态 Node 在不同 Map、Loop 或 Subflow 实例中拥有不同 Activation。

### 6.2 状态机

Run 的业务生命周期和运维 admission 状态必须分离：

```text
lifecycle: created -> active <-> waiting
           active|waiting -> completing -> succeeded
           created|active|waiting|completing -> terminating(reason)
           terminating(reason) -> failed|cancelled|interrupted|timed_out

admission: open <-> paused
           open|paused -> draining -> closed
```

`waiting` 表示业务等待；`paused` 只表示停止新 admission。Pause 不得覆盖或丢失 Run 原本的
active/waiting 生命周期。只有不存在 ready、leased、running 或 retry_wait Activation，并且至少
存在一个 durable wait 时，Run 才投影为 waiting。Run terminal 提交后 admission 必须为 closed。

Cancel、timeout、interruption 或 fatal failure 必须先原子写入唯一 termination intent 并关闭
admission，再 fencing 当前 lease、传播取消并 drain，最后提交与 intent reason 对应的 terminal。
如果 success terminal 先提交，后来的 termination intent 无效；如果 termination intent 先提交，
普通 success 不得覆盖它。Cancel request 本身不是 terminal。

Activation 状态：

```text
created -> ready
ready -> leased -> running -> succeeded
leased|running -> retry_wait -> ready
ready -> waiting -> succeeded
ready -> succeeded
created|ready|leased|running|retry_wait|waiting -> terminating -> failed|cancelled|timed_out
```

Attempt 状态：

```text
created -> leased -> running -> succeeded|failed|timed_out
leased|running -> abandoned|cancelled
```

Lease 到期事务必须先把旧 Attempt 标为 abandoned、递增 lease epoch，再把非终态 Activation
送入 retry_wait/ready；旧 worker 的迟到 completion 因 epoch 不匹配失败。

失败 Attempt 可以按 policy 使尚未终态的 Activation 进入 `retry_wait`，之后创建同一 Activation
的新 Attempt；不得重新打开已经提交终态的 Activation。Retry exhaustion 才使 Activation
进入 terminating，并在必要 drain 后 failed。Activation timeout 也必须先 fencing 当前 lease、
取消并 drain，不能从 running 直接提交 timed_out。
终止意图命中 `retry_wait` 时必须在同一事务取消其 retry Timer，再提交 Activation terminal；
不得留下可在终态后重新 admission 的定时器。父 Scope 只能接受由 Activation/Attempt 聚合在
验证所有 Attempt、lease、retry 和 cancellation drain 均已清空后签发的 terminal proof，不能
信任 caller 提供的 `attempts_drained: true` 一类布尔声明。
Branch、Merge、Join、Return 等 scheduler-native 节点可以从 ready 在单个调度事务中直接推进，
不创建 lease 或 worker Attempt。

### 6.3 PostgreSQL 数据模型

至少包含：

```text
workflow_definitions
workflow_definition_revisions
deployment_revisions
workflow_runs
scope_instances
node_activations
node_attempts
control_tokens
execution_events
task_outbox
public_event_outbox
signals_inbox
timers
fork_groups
fork_legs
join_arrivals
run_reuse_candidates
payloads
artifacts
```

`execution_events` 是权威执行事实；Run、Activation、Attempt 表是同事务维护、可重建的当前
projection。正常恢复读取 projection，不重放用户代码；event 用于审计、校验、重建和未来
time-travel/fork。

“可重建 projection”只指由 `projection_ledger_batch` 明确登记的闭合集合。Public Event Outbox、
Inbox、transition receipt、Artifact retention release 等分别承载对外交付、输入去重、exact replay
或生命周期权威，不得伪装成可删除后重建的 materialized projection。尤其已发布 Public Event 的
交付 receipt 若从 Internal Event 重建，可能造成重复外部广播；因此它使用独立 retention 合同，
terminal 行不可清理，非 terminal 行只可在发布成功且数据库保留期限届满后由有界 GC 删除。

`scope_instances` 是动态结构化并发的权威 projection：每个 Run 必须原子创建唯一 root scope；
child scope 以 `(run_id, parent_scope_id, static_scope_id, stable_dynamic_key)` 唯一。创建 child、
增加父 scope admitted 计数、首次 settle child 和增加父 scope settled 计数都必须分别在同一事务
完成。父 scope 只有 admission 已关闭、全部 required child 已 settle，且不存在 leased/running
Attempt 时才能 settle。

`control_tokens` 是控制流 provenance 的权威 projection，至少保存 token ID、Run、当前端口、
scope instance、来源 Activation、Branch decision 或 Fork group/leg 关联及消费状态。Branch、
Merge、Redrive 和恢复不得只根据 event payload 或节点输出猜测控制来源。

Internal Event envelope 至少包含 `event_id`、`run_id`、Run 内 `seq`、`schema_version`、
`kind`、`node_id`、`scope_instance_id`、`activation_id`、可选 `attempt_no`、causation ID 和
安全 payload。未知 event schema version 必须 fail closed，不能按最新结构猜测。

Fork member set 与 Join arrival 必须是结构化 projection，不得只藏在 event payload：

- `fork_groups` 唯一键为 `(run_id, fork_group_id)`；
- `fork_legs` 唯一键为 `(run_id, fork_group_id, leg_id)`，保存 scope instance 和声明顺序；
- `join_arrivals` 唯一键为 `(run_id, join_activation_id, fork_group_id, leg_id)`；
- 创建 group/legs、发出 Token 和创建首批 branch Activation 必须同事务；
- 重复 arrival 由唯一键和预期状态 CAS 幂等吸收。

### 6.4 原子状态推进

每次推进必须在一个 PostgreSQL 事务中：

1. 先取得该 Run 的 event-writer `FOR UPDATE` 行锁，再按确定顺序锁定并检查 Scope、Activation、Attempt、Token 等 projection；任何会分配 event sequence 的路径都不得先持有 projection 写锁再升级 Run 锁。一个事务涉及多个 Run 时，必须按 Run ID 排序、去重后取得全部 Run 锁；
2. 校验 definition/deployment revision、attempt number、lease epoch 和 fencing token；
3. 为该 Run 分配严格单调 event sequence；
4. 追加 Internal Execution Event；
5. 更新当前 projection；
6. 保存 ValueRef 或 ArtifactRef；
7. 创建下游 Activation、Timer、Signal wait、Task Outbox 或 terminal Public Event Outbox；
8. 提交事务；
9. 提交后才向任务来源 ack。

Event、projection、outbox 与下游 activation 不得异步双写。系统不承诺不同 Run 之间的全局
event 顺序。

### 6.5 Lease、fencing 与多 runtime

1. PostgreSQL 生产环境允许多个 scheduler 和 worker 实例。
2. 可以使用短期 run lease 降低竞争，但正确性不得只依赖单一进程所有权。
3. Task claim 必须使用数据库锁/CAS，带到期时间、heartbeat 和单调 lease epoch。
4. Lease 到期后 Task 可以重新投递；旧 worker completion 必须因 epoch 不匹配被拒绝。
5. Scheduler 重复推进必须被唯一键、预期状态 CAS 和 event idempotency 阻止。
6. singleton store advisory lock 已在 clean-break cutover 时删除，不作为分布式调度基础。
7. Scheduler facts 必须来自同一个数据库 snapshot；该 snapshot 可以在返回时已经落后于合法并发
   提交，但不得因此被报告为数据损坏。Planner 只生成带 snapshot projection version 的 intent，
   最终 repository CAS 负责拒绝陈旧计划并触发重新读取。
8. Runtime 的单调 operation budget 必须在提交 `task_started` 之前开始，且 worker、Artifact
   materialization 与最终 handoff 复用同一预算；数据库仍以自己的 `started_at` 和时钟作最终
   deadline authority。本地预算先到时必须停止或禁止外部调用，但只有数据库确认到期并授予
   timeout-finalization lease 后，runtime 才能提交私有 timeout intent。

### 6.6 外部副作用

生产合同只承诺至少一次 worker 执行：

- worker 在外部调用后、结果提交前崩溃时，调用可能重复；
- Node descriptor 必须声明 effect class、idempotency 支持、retry、timeout 和 cancellation 能力；
- 支持幂等键的 Provider/Action 必须接收稳定 effect key；
- 不支持幂等的副作用必须产生编译风险诊断并采用保守 retry；未来 UI 可以展示该诊断；
- Attempt 必须记录 `not_started | started | committed | unknown` effect evidence；`unknown` 且不支持幂等时禁止自动 retry/redrive；
- 只有业务写入与 workflow checkpoint 共享一个数据库事务时，才可以声明事务一致性；
- 不得把 lease、CAS 或 event ledger 宣传为外部 exactly-once。

### 6.7 Payload、Artifact 与流式事件

1. 小型结构值以 ValueRef 持久化。
2. 文件、图片、大文本和大对象使用 content-addressed ArtifactRef。
3. Artifact 必须先写入 durable content-addressed object 并校验 hash，数据库事务随后提交引用。
4. 已写入但未被事务引用的对象由 GC 清理。
5. Internal Event 只保存必要值、引用、hash 和安全元数据。
6. Token stream 是有界、进程内、best-effort 的观测数据，不是执行事实；订阅者关闭或落后只允许丢失观察 chunk，不能使 Worker 失败。最终验证后的节点输出才是 durable result，token 文本不得进入 ledger、Public Event、trace 或默认日志。
7. Public SSE/trace 由独立 projection/outbox 产生，不得反向充当恢复日志。
8. Run terminal 事务必须同时写 internal terminal fact 和 durable public-terminal outbox；Attached SSE 发出对应 terminal event 后立即 EOF。
9. Nonterminal public event 可以按保留策略清理，但已排队 terminal event 不得因进程崩溃丢失。
10. Internal Event 不自动进入公共 API；公共 replay 需要独立 API 与隐私规范。
11. 多 runtime 下，dispatcher 必须在标记 public outbox 已发布的同一 PostgreSQL 事务中执行
    `pg_notify(public_event_id)`；通知仅是唤醒信号，各 runtime 必须按 ID 回读已脱敏的 durable
    envelope，再广播给本地订阅者。
12. EventHub 只承担进程内 subscriber registry，不分配权威序号，也不写执行 journal。
13. Attached 建连采用“先建立本地订阅，再查询 durable terminal，最后按 public event ID 去重”的
    握手；保持 live-only，但不得在建连竞态中漏掉 terminal 和 EOF。
14. 生产模式必须使用所有可能接管同一 durable repository 的 runtime/worker 均可访问的 shared
    Artifact store；进程本地目录只允许单进程开发/测试，不能声明生产恢复能力。
15. Artifact 内容由 `ArtifactRef` 中的 `artifact_id`、`content_hash`、`size_bytes` 等闭合字段校验；
    store 的部署身份是 `(backend, namespace, store_id)`，不是路径字符串或某个进程实例。
16. durable repository 中的 `artifact_store_authority` 是该 repository 的不可变单例 authority。
    runtime 必须在发布 catalog、接纳 Run 或启动后台 GC 前原子绑定/校验完全相同的部署身份；缺失、
    local-only 或冲突身份必须 fail closed，且不得先产生 catalog/Run 写入。
17. object store 保存字节但不自行决定可达性或删除资格；Artifact/reference/retention、数据库时钟、
    GC claim/fence 才是引用和删除 authority。相同 content hash 被多个 Run 引用时，任何单个 Run 的
    release 都不得授权删除共享对象。

Attached 用户响应的实时正文、工具/RAG 扩展事件和 LLM `stream`/`publish` 目标合同由
[Response 实时流与 LLM 发布控制规范](./2026-07-19-response-streaming-and-llm-publication-design.md)
定义。该增量已经实现并完成切换：第 7 条现细化为 durable lifecycle、terminal snapshot 与 terminal
delivery 仍由 projection/outbox 产生；transient response delta 由独立、非持久化的 Live Response
Broker 产生。多 runtime 的公开流要求共享 broker，并在 deployment publication 与 admission 阶段
fail closed；默认二进制的 `RetrievalRegistry` 为空，只执行部署方已经注册的 retrieval 实现。

当前实现中，大值字节由 content-addressed object 承载，ArtifactRef 固定其身份与 hash；数据库
中的 Artifact、reference 与 retention 记录是元数据、引用和 GC authority。当前实现还必须在
`scheduler_values`、occurrence projection 和任务完成 checkpoint 中持久化由同一 hash 约束的
完整 RuntimeValue。该数据库副本是当前恢复路径使用的可校验 materialized projection，不是
独立的字节内容或 GC authority。

本版本不承诺“大值从不进入数据库”或数据库 footprint 已卸载。ArtifactRef-only 存储需要另行
定义读时 hydration、hash 校验、下游 worker 输入、Return、replay 与 recovery 合同，不属于
阶段 4 或阶段 8 的退出标准。

公共映射保持：

| internal terminal | public Run status | public event |
|---|---|---|
| `succeeded` | `completed` | `run.completed` |
| `failed` | `failed` | `run.failed` |
| `timed_out` | `failed` + `RUN_TIMEOUT` | `run.failed` |
| `cancelled` | `cancelled` | `run.cancelled` |
| `interrupted` | `interrupted` | `run.interrupted` |

普通进程崩溃不再产生 `interrupted`；只有 durable authoritative interruption intent 才产生该
终态。Attached transport 仍然 live-only，不提供 cursor 或 replay。

### 6.8 错误 taxonomy 与事务结果

错误必须先按执行语义分类，再映射为稳定 code；不得依赖错误字符串决定 retry、Collect 或公共响应：

| 类别 | 语义与处理 |
|---|---|
| `definition_invalid` | Parser、Compiler 或 Plan verifier 拒绝；不创建 Run |
| `intent_conflict` | 同一 transition key 携带不同 canonical intent；调用方逻辑错误，不追加 event |
| `state_conflict` | projection version、预期 lifecycle 或唯一 winner CAS 失败；回读权威状态后决定是否重试 |
| `stale_lease` | Attempt、lease epoch 或 fencing token 过期；拒绝 zombie completion，不改变权威状态 |
| `safe_business_failure` | descriptor 明确声明、可进入 typed `Result<T, SafeError>` 的业务失败 |
| `infrastructure_failure` | provider、网络、存储或 worker 基础设施失败；仅按冻结 policy 决定 retry |
| `effect_outcome_unknown` | 非幂等副作用可能已经发生；禁止自动 retry/redrive，要求显式处置 |
| `control_termination` | cancel、deadline、interruption；不允许降级为业务失败被 Catch/Collect 吞掉 |
| `invariant_corruption` | ledger/projection、scope、identity 或 hash 不一致；fail closed 并停止该 Run 自动推进 |

一般状态转移命令只有四类稳定结果：`committed`、`exact_replay`、`state_conflict`、
`stale_lease`。Worker task 的 heartbeat/result commit 还可以返回一个封闭的 authority decision：
`operation_deadline_elapsed`。它表示数据库按自己的时钟确认普通结果已越过 operation deadline，
且本次普通结果事务已经完整回滚；runtime 随后只能用私有构造器提交对应 timeout intent。它不是
通用错误、不能由 worker 伪造，也不得被映射成业务失败。

`committed` 与 `exact_replay` 都返回同一个权威 transition identity；intent conflict 是独立错误，
不得伪装成 exact replay。事务提交前发生的任何错误必须整体回滚，不留下 event、projection、
ValueRef/ArtifactRef 引用、timer、inbox 或 outbox 的子集。内部失败只进入 Internal Event；Public
Event/API 只能使用 closed、脱敏的公共错误映射，不能包含 SQL、prompt、provider body、secret、
任意用户 payload 或 Rust debug 文本。

## 7. 生命周期、版本与恢复

### 7.1 Wait、Pause、Cancel

- `wait` 是业务节点，等待 timer 或 durable signal，期间不占用 worker。
- `pause` 是运维 gate，停止派发新 Task；已运行 Task 按 policy 继续或 drain，signal/timer 继续记录。
- `cancel` 是终止意图，阻止新 admission，传播取消并等待必要 drain。
- `signal` 是持久化异步消息；重复 signal 通过 message ID 幂等。
- `query` 只读取 projection，不推进状态。
- `resume` 只把 admission 从 paused 恢复为 open，不结算任何业务 Wait。

WaitSignal/Timer 只能由匹配 Signal、Timer、取消，或单独授权且完整审计的 admin override
结算。普通 Resume 不得跳过人工审批、外部事件或时间条件。

Signal 与 timeout 同时到达时，数据库中第一个满足预期状态的事务胜出；另一个必须成为可审计
但不改变结果的 late event。Cancel 与 success 竞争的是 durable termination intent 与
completion CAS；intent 一旦先提交，必须 drain 后按 intent 终结，不能再被普通 success 覆盖。

### 7.2 Retry、Resume、Redrive、Fork、Migrate

| 操作 | 身份与定义 |
|---|---|
| Retry | 同一 Run、Activation、Definition/Deployment Revision；仅在 Activation 未终态时按 policy 创建新 Attempt |
| Resume | 同一 Run 与 Definition/Deployment Revision；仅恢复 paused admission |
| Redrive | 创建带 lineage 的新 Run，固定原 Definition/Deployment Revision，按控制重放复用结果 |
| Fork | 创建带 lineage 的新 Run；可选择 Revision、检查点、输入覆盖和兼容前缀结果 |
| Migrate | 为未完成 Run 创建新 Definition/Deployment Revision 上的 replacement Run，并验证节点与 Schema 映射 |

失败或取消 Run 的 durable terminal 不得被 Redrive 重新打开。Migrate 必须先停止旧 Run admission、
完成规定 drain，再以明确原因终结旧 Run；新 Run 保存 lineage。不得在 resume/retry 时静默使用
最新 workflow、Deployment Revision、Prompt alias、模型 alias 或 plugin 实现。

Activation terminal 后不存在“手工 Retry 原 Activation”；用户希望再次推进时必须使用 Redrive
或 Fork。

Retry policy 在首次 dispatch 时冻结。每次 retry checkpoint 必须绑定失败事实、原 Attempt、下一
Attempt/lease epoch/fencing token、完整下一 task envelope、`available_at`、剩余预算以及相关
projection lineage；Task claim 使用数据库时钟且不得早于 `available_at`。当前 task/activation/
attempt projection 与最新 retry checkpoint 不一致时必须 fail closed，不能通过改写 outbox 时间
绕过 backoff 或换用新的 policy。

#### Redrive 复用合同

Redrive 先持久化 `run_reuse_candidates`，不得预先创建 succeeded Activation。Candidate 集合必须
对数据依赖闭合并记录 source control provenance；新 Run 的 Branch/Merge/Join 按原固定 Plan
重新推进。只有控制流真正 admission 到对应 Node/scope 时，scheduler 才校验并原子物化一个
succeeded Activation。未选路径的 Candidate 永远不创建 Activation。

一个 source Activation 只有同时满足下列条件才可成为 Candidate：

1. source 状态是 succeeded，并且 durable output 与所有 Artifact 仍存在且 hash 验证通过；
2. Definition/Deployment Revision、Node ID、稳定 scope key、输入 Value hash 完全一致；
3. Node config、descriptor/plugin hash、output Schema 和 effect policy 完全一致；
4. 它的全部数据依赖已经复用，或由相同纯表达式重新计算出相同 hash；
5. Map/Loop/Subflow 的动态实例能通过稳定 item/iteration/invocation key 一一对应；
6. Attempt 没有 unknown/ambiguous external effect 标记。

Candidate 被实际 admission 时创建 succeeded Activation projection，记录
`reused_from_activation_id`，但不创建 worker Attempt。Timer、Wait 和 terminal 节点不进入
Candidate；旧 Signal 不自动重放。

映射到 source 的未完成/失败节点继承原 `effect_id`。具有 provider idempotency 的调用可以据此
安全重试；不具备 idempotency 且结果不确定的副作用默认阻塞 Redrive，必须由操作者显式 Fork
并确认创建新的 effect lineage。Artifact 缺失时不得假装复用成功。

#### Migrate 交接合同

1. Migrate 只允许在旧 Run admission=paused、lifecycle 为 active/waiting、没有 leased/running Attempt，并完成 child drain 后执行。
2. 映射和 Schema 必须在接管前验证；随后 durable migration intent 关闭 Task 与 Signal admission。
3. intent 之后发送到旧 Run 的 Signal 返回稳定 `RUN_MIGRATING`，不得静默转发。
4. 未消费 Signal、Timer 和 Wait 不自动迁移；迁移映射必须显式声明如何在新 Run 重建。
5. 一个最终事务同时把旧 Run 提交为 `cancelled`、原因 `MIGRATED`，创建 replacement Run、lineage 和允许复用的 Activation 引用。
6. 最终事务失败时旧 Run 保持 migration intent，可安全重试；不得出现旧 Run 已终态但 replacement 不存在。
7. 旧 lease 在 intent 时 fencing；late completion 不得推进旧 Run 或 replacement Run。

### 7.3 定义和依赖版本

1. Draft 可变；Published Definition Revision 永久不可变。
2. Plan 固定 compiler、表达式引擎、Prompt/resource hash 和所需 Node descriptor contract hash。
3. 每个 immutable Deployment Revision 把一个 Definition Revision 绑定到具体 ModelRef、provider adapter 和可执行 plugin/worker version，并生成 binding hash。
4. Run 同时固定 Definition Revision 与 Deployment Revision；有效执行身份是 `(plan_hash, binding_hash)`。
5. 模型 alias 只在创建 Deployment Revision 时解析；alias 更新必须创建新 Deployment Revision，且只影响新 Run。
6. Secret 值不进入 plan/binding hash；必须保存 SecretRef 及可用的非敏感版本元数据。
7. Public Agent `version` 必须标识有效 Deployment Revision，并在 plan hash 或 binding hash 改变时变化。
8. 仍被 Run 引用的 Definition/Deployment Revision 与 descriptor 不得删除。
9. 不兼容旧 descriptor 时必须保留旧 worker，或产生显式不可恢复错误；不得静默换实现。
10. Migrate/Fork 只有在输入输出 Schema、effect 与节点映射验证通过后才可复用结果。

### 7.4 长历史与保留

1. 长循环和长期会话可以由显式 continue-as-new 创建新 generation 并保留 lineage；本版本不
   承诺自动 history rollover。
2. 大 Map 和独立子工作流应使用 child run 隔离历史与取消边界。
3. Event、Artifact、幂等键、Public Event 和 Definition 使用不同保留策略。
4. 在引用它们的 Run、redrive 或审计窗口结束前，不得删除 Definition、Artifact 或 effect key。

### 7.5 结构化并发

1. 父 scope 记录所有已 admission 的 child Activation。
2. 父 scope 成功前，required child 必须 settle，取消中的 child 必须完成规定 drain。
3. Stop、root deadline、store error、panic 和 invariant failure 不得被 `all_settled` 转成业务数据。
4. Public root terminal 只由 Run terminal transaction 产生，child yield 不得直接提交 RunOutput。

## 8. 保留、替代与删除

| 保留并复用 | 替代或删除 |
|---|---|
| 类型、Schema、词法作用域验证 | authored `switch/case`、public `core.*` |
| Message、Prompt、自然 YAML、`$`、`{{ }}` | executable Region/SSA operation tree 与递归 scheduler |
| LLM、Action、HTTP、Tool executor | 进程内唯一中间值与 terminal-only recovery |
| Model registry、capability、response validation | AST path/order durable ID |
| Action effect、idempotency、cancel metadata | public journal 兼任 execution ledger |
| 隐私边界、取消/drain、唯一 terminal CAS | singleton runtime ownership 与 SQLite 生产对等承诺 |
| normalization、content hash、SourceMap | leaf node 任意 control edge 或 `goto` |

### 8.1 Cutover 原则

1. Leaf executor 可以通过 adapter 复用，旧 scheduler 不得包在新 scheduler 内长期运行。
2. 新旧 Plan 不得在同一个 Run 中混用。
3. 开发数据允许重建，不写兼容迁移来保留已声明可丢弃的旧执行数据。
4. 每个替代阶段必须先建立新合同测试，再删除旧组件。
5. 合并当前持久化内核前必须清除 checked-in Agent、positive fixture 和 active docs 中的旧控制流。

## 9. 九阶段交付顺序与状态

以下九阶段是有前置依赖的执行顺序，编号不得按局部实现完成度重排、合并或倒置；后阶段的测试存在
不能替代前阶段退出标准。它们记录实现依赖和验收边界，不是另起一份待办路线图；“已验证”只由
§11 的证据快照和对应门禁判定。

| 阶段 | 实施状态 | 主要验证锚点 |
|---|---|---|
| 1. 架构和状态机 | 已实现 / 已验证 | `engine_state_model`、`engine_control_model`：非法转移、终态不可变、stale fence、first-winner、跨 Run 隔离 |
| 2. Canonical Typed Plan | 已实现 / 已验证 | `plan_model`、`plan_schema`；不可执行 Policy、不可达节点、端口/终止合同在发布前拒绝 |
| 3. PostgreSQL durable kernel | 已实现 / 已验证 | `repository_postgres*`、`postgres_migrations`、`projection_rebuild`、非 skip 的 `ci_postgres_gate` |
| 4. 最小纵向切片 | 已实现 / 已验证 | `scheduler_core`、`scheduler_postgres`、真实进程 crash matrix、Artifact 提交/恢复、生产二进制重启恢复 |
| 5. 执行安全合同 | 已实现 / 已验证 | `scheduler_advanced`、SQLite/PostgreSQL boundary suites；retry/timeout 固化、cancel/drain、signal/timeout first-winner |
| 6. Structured Core | 已实现 / 已验证 | `dsl_core`；checked-in Agent/Markdown 编译、旧控制语法负向测试、等价 Plan normalized hash |
| 7. 删除旧内核 | 已实现 / 已验证 | residual scan 为零；format、Clippy、全量测试、真实 PostgreSQL 与 stock production binary gate 通过 |
| 8. 高级控制、恢复与保留 | 已实现 / 已验证 | `dsl_advanced`、AgentLoop、Recovery、Production Recovery/Subflow、Artifact/GC、Human Task first-winner |
| 9. Graph 作者合同与 API | 已实现 / 已验证 | `graph_author`、`graph_product`、`graph_repository`；Graph/View/trace authority 与 Policy edit fail-closed |

### 阶段 1：冻结架构和状态机

**前置条件**：本文进入 Approved；暂停在旧 `switch`/Region runtime 上增加控制流能力。

**产物**：

- Node、Activation、Attempt、Token、Value 与 Scope 的 Rust 状态模型；
- Run/Activation 状态转移表；
- Branch/Merge/Fork/Join 形式不变量；
- PostgreSQL 事务边界和错误 taxonomy；
- model-based/property 测试框架。

**退出标准**：非法转移、双终态、重复 completion、未 drain 父终态和 stale epoch 均由模型测试拒绝。

### 阶段 2：实现 Canonical Typed Plan

**前置条件**：阶段 1 的身份和状态合同冻结。

**产物**：

- Plan Node/Port/Edge/Scope/Policy/SourceMap；
- Branch、Merge、Fork、Join、Return、Raise verifier；
- 稳定 ID 与 semantic hash；
- programmatic Plan fixture builder，仅供 verifier、scheduler 和恢复测试使用。

**退出标准**：等价定义产生相同 Plan golden/hash；ID 不随无语义重排变化；错误端口、类型、作用域、Merge/Join 关联在运行前失败。

### 阶段 3：建立 PostgreSQL durable kernel

**前置条件**：Plan 与 Activation identity 可序列化并版本化。

**产物**：

- definition/deployment revision、Run、Scope、Activation、Attempt、Control Token、Event、Task/Public Event Outbox、Inbox、Timer、Fork/Join/reuse-candidate projection 与 Artifact migration；
- ledger + projection 同事务 repository；
- per-Run event sequence、CAS、idempotency unique key；
- SQLite 单进程最小实现或测试 double。

**退出标准**：事务回滚不留下半个 event/projection/outbox；ledger 明确登记的 projection 闭合集合可校验重建；outbox/inbox/receipt 权威不可被 repair 冒充；真实 PostgreSQL 并发测试通过。

### 阶段 4：完成最小纵向切片

**前置条件**：阶段 2 Plan 和阶段 3 durable kernel 可联调。

**范围**：

```text
Input -> Branch -> LLM/Action -> Merge -> Return
```

**产物**：新 scheduler、task outbox consumer、worker adapter、结果 checkpoint、恢复入口，以及
LLM 大输出所需的最小 content-addressed ArtifactRef 提交和 orphan sweep。

**退出标准**：在 Branch 前后、外部调用前后、结果提交前后、下游 Activation 创建前后杀进程，Run 都从最近 durable 边界恢复且只提交一个终态。

### 阶段 5：补齐执行安全合同

**前置条件**：最小纵向切片可恢复。

**产物**：

- task lease、heartbeat、lease epoch 和 zombie completion rejection；
- retry/backoff、timeout、cancel/drain；
- 静态 Fork/Join/Collect 的 `all_success` 与 `all_settled`；
- WaitSignal、Timer、pause/resume；
- public event projection；
- fault/crash injection harness。

**退出标准**：提交前崩溃、提交后 ack 前崩溃、lease 到期竞态、cancel/success 竞态、signal/timeout 竞态全部满足 first-winner 和幂等合同；静态 Parallel 的快慢分支、失败、all_success drain 和 all_settled typed output 均通过恢复测试。

### 阶段 6：实现 StructuredAuthorDocument Core

**前置条件**：新 Plan 和 runtime 已有稳定纵向合同。

**产物**：

- `if/elif/else`、`parallel`、`match`、`yield`、`wait`、`return/raise` parser/compiler；
- CEL 条件上下文、typed yield/Phi、词法 capture；
- checked-in Agent 与 canonical Markdown 示例迁移；
- 旧 `switch`、child result 和 `core.*` 的负向 parser fixture。

**退出标准**：所有 Agent 经当前 compiler 执行；旧控制语法在 positive surface 不可执行；文本 DSL 与手工等价 Plan 的 normalized hash 一致。

本阶段只发布 Core surface。Map、Loop、Subflow、Try/Catch、Human Task 等高级作者语法在
阶段 8 与其 verifier/runtime 一起开放，不允许 parser 先接受而 runtime 尚不能执行。

### 阶段 7：删除旧内核

**前置条件**：所有生产 Agent 和 E2E 已使用新 runtime，并确认没有 checked-in Agent 依赖阶段 8 尚未实现的高级能力。

**删除项**：

- recursive `scope_scheduler`；
- executable Region operation list 与旧 Branch/Phi runtime；
- runtime-local-only value store；
- startup `mark_incomplete_interrupted` 恢复策略；
- singleton PostgreSQL store ownership；
- 旧 Switch parser/compiler/runtime；
- 仅为旧控制节点保留的 registry 和测试。

**退出标准**：production source-symbol residual scan 为零；不存在旧新 scheduler feature flag；format、lint、全量测试、真实 PostgreSQL 和 real-process gate 通过。

### 阶段 8：交付高级控制、恢复与保留

**前置条件**：旧内核已删除，基础运行模型稳定。

**产物**：

- Map/Collect、Loop、Subflow/child run、AgentLoop；
- Try/Catch/Finally、Human Task；
- Redrive、Fork、Migrate，以及显式 continue-as-new 与 generation lineage；
- 上述能力的作者 parser、lowering、Plan verifier 与 durable runtime；
- Artifact 的 orphan/reference retention 与可恢复的有界 GC、非终态 Public Event 的有界清理，
  以及显式 generation/continue-as-new lineage。当前版本不提供 Execution Event、Definition 与
  幂等 authority 的自动清理或归档操作；这只是删除能力边界，不构成无限期保存、归档可恢复性
  或保留 SLA。自动 history rollover/归档、large Map 自动 child-run 分片及规模/性能承诺不在
  本阶段验收范围。

本阶段实际 retention/GC 合同是：

1. verified 但从未被 durable reference 提交的对象，只有在数据库记录的 orphan retention 截止后
   才可由有界批次 claim；删除必须有全局 object fence/claim、可恢复且幂等。
2. 已引用对象以 Run/lineage retention root 为准；terminal 只登记 forward-only release deadline，
   使用数据库时钟，调用者时钟或重复请求不得缩短期限。共享 content hash 仅在全部 root 释放后可删。
3. GC 必须先由 repository 判定并 claim，再删除共享 store 字节并提交结果；扫描目录、观察文件年龄
   或某个 runtime 单方面删除都不具有 authority。崩溃后可重复，stale claim 不得越过新引用。
4. 只清理已发布且超过期限的 nonterminal Public Event；terminal public event 及 exact replay/
   delivery receipt 受独立合同保护，不得由该 pruner 删除或从 Internal Event 伪造。
5. Execution Event、Definition 和 effect/idempotency authority 在本版本没有自动 GC；未提供删除操作
   不等于无限 retention SLA，也不得宣称已实现归档恢复。

**退出标准**：上述作者、Plan 与 runtime 合同均有对应合同测试；动态控制、Human Task、
Redrive/Fork/Migrate、显式 continue-as-new 及 retention/GC 具有适用的真实 PostgreSQL 门禁。
自动 rollover、归档、分片及规模/性能不在本阶段范围。

### 阶段 9：交付 Graph 作者合同与服务端 API

**前置条件**：Canonical Plan 和 SourceMap 已稳定，不再依赖 Region 容器执行。

本阶段仅指 GraphAuthorDocument、ViewDocument、trace 的 wire contract、
发布/读取 API 与原子 semantic edit primitives；不包含浏览器端可视化编辑器、拖拽交互组件
或前端产品实现。

Graph API 边界必须满足：GraphAuthorDocument 是唯一 semantic author payload，客户端不得上传
Canonical Plan、semantic hash 或编译结果作为 authority；服务端对 closed wire 解码、重编译、
Plan 验证和 revision/CAS 后才可发布。ViewDocument 使用独立版本与写入路径，只能引用稳定 Graph
ID；trace overlay 是按稳定 Run/Activation/Node ID 关联的只读观测，不得回写作者语义。单次 semantic
edit，尤其成组 topology upsert/delete，必须在完整 Graph 上原子验证，失败不得留下半个拓扑。

**产物**：

- GraphAuthorDocument schema；
- 独立 ViewDocument；
- 只读 execution graph 与 trace overlay；
- Branch/Parallel/Map/Loop 的原子 semantic edit primitives；
- structured/graph 显式转换器与不可归约诊断。

**退出标准**：ViewDocument 布局更新不改变 semantic hash；显式转换且保留稳定 ID 的两种作者格式编译为相同 Plan；业务节点在 Graph 中保持平级；不可归约图不会被伪装成结构化 YAML。

## 10. 验收测试矩阵

### 10.1 Parser 与 Compiler

- `if/elif/else` 顺序、必需 else 与无值分支的隐式 fallthrough；
- condition non-null boolean 与 expression failure；
- typed `yield`、Phi、optional/default/Result；
- `match` 不能包含步骤；
- 控制边与数据边类型错误；
- stable ID、semantic hash 和 SourceMap；
- 旧 `switch/core.*` positive input 稳定失败。

### 10.2 Plan Verifier

- 每个 Branch Activation 最多选择一个命名输出端口；
- Merge 不按静态入度等待；
- Join 必须关联一个 fork group；
- duplicate/missing leg、错误 scope instance、非法 cross-scope binding；
- 普通 leaf node 不能 goto 或创建 edge；
- Loop 缺少退出预算失败。
- Policy 可执行矩阵内组合通过，`Budget`、结构节点 timeout 和其他无 runtime consumer 的组合在发布前失败。

### 10.3 PostgreSQL 与幂等

- event/projection/outbox/activation 同事务；
- per-Run sequence 并发唯一且单调；
- duplicate transition/completion 幂等；
- lease 续期、到期、重领和 stale epoch；
- 多 scheduler 同时推进一个 Run；
- projection 校验重建；
- SQLite 明确只通过单进程子集测试。

### 10.4 Crash Matrix

- 状态提交前崩溃；
- 提交后、ack 前崩溃；
- 外部调用完成后、结果落库前崩溃；
- 下游 Activation 创建前后崩溃；
- lease 刚到期时旧 worker 返回；
- cancel 与 success 同时提交；
- signal 与 timeout 同时提交；
- scheduler/worker 全部停止后恢复。

### 10.5 控制流 E2E

- 排他分支未选路径不创建 Activation；
- Branch -> Merge 与 typed yield；
- nested parallel、all_success fail-fast + drain；
- all_settled 只输出 typed safe failures；
- Map 顺序、空集合和重复 key；
- Loop 每轮新 Activation；
- Subflow 取消和 terminal 隔离；
- Subflow default/optional 归一化在 SQLite/PostgreSQL 对等；仓储层拒绝类型正确但来源伪造的 input、
  optional 注入，以及错误 interface、timeout、output contract；
- Stop、deadline、panic、store failure 不被业务收集。

### 10.6 版本与恢复

- Retry/Resume/Redrive 使用原 Definition/Deployment Revision；
- alias 更新创建新 Deployment Revision，且只影响新 Run；
- 旧 descriptor 由匹配 worker 执行；
- Fork/Migrate Schema 不兼容时拒绝；
- continue-as-new 保留 lineage；
- Definition、Artifact 和 effect key retention。

### 10.7 双作者表面

- 由显式转换产生且保留稳定 ID 的 structured 与 graph 文档等价编译；
- layout 变更不影响 Plan；
- SourceMap 可定位两种作者源；
- reducible graph 成功转换；
- irreducible graph 保留 graph 并返回诊断；不可无损归约本身不是 Graph 校验或发布失败；
- Policy-bearing Graph 合法、保持 graph-only，Graph JSON round-trip 与 semantic hash 必须保留
  Policy；structured 转换不得静默丢弃 Policy；
- ViewDocument 损坏不影响已发布 Run。

### 10.8 安全、隐私和外部副作用

- Secret 不进入 Plan、ledger、SSE、trace 或日志；
- Public Event 不包含 Prompt、Message、图片或节点完整输出；
- Artifact hash、引用提交与 orphan GC；
- 不支持 idempotency 的 effect 产生风险诊断；
- token stream 丢失不影响 durable result；
- external exactly-once 不出现在 API 或文档承诺中。

## 11. 实现证据快照

**状态为 `Implemented / Verified`。** 2026-07-19 在同一工作树上完成静态门禁，并以 `CI=1`、
PostgreSQL 16.14 和全新测试数据库执行非 skip 的完整测试。下列测试名是关键合同的可定位证据；
阶段 1～9 的整体覆盖同时由 §9 状态表和最终全量门禁约束。

| 合同 | 关键测试名 |
|---|---|
| 状态机、Plan verifier 与 Policy 可执行矩阵 | `verified_plan_rejects_budget_policy_without_a_runtime_contract`；`reachability_policy_and_terminal_contracts_fail_at_build_time`；`verified_leaf_retry_and_timeout_policies_are_frozen_into_dispatch`；`graph_policy_edits_reject_unexecutable_contracts_before_publication` |
| 输入 required/optional/default、显式缺失及 Graph/hash round-trip | `graph_publication_uses_the_same_frozen_input_normalization_contract`；`native_graph_input_contract_round_trip_preserves_presence_defaults_and_hash`；`native_nullable_required_input_is_not_inferred_to_be_optional`；`run_input_paths_do_not_conflate_missing_optional_or_out_of_bounds_values` |
| Subflow 的仓储权威重推导与缺失值保持 | `postgres_subflow_admission_rederives_normalized_input_and_call_contract`；`sqlite_subflow_persists_normalized_child_input_without_collapsing_absence` |
| lease、取消、deadline 与 first-winner | `postgres_all_success_cancels_and_fences_sibling_before_internal_failure`；`postgres_cancel_run_fences_a_claimed_worker_and_allows_pre_admission_cancel`；`sqlite_wait_signal_and_timeout_share_one_durable_first_winner`；`postgres_wait_signal_and_timeout_share_one_durable_first_winner` |
| Human Task 恢复与并发唯一赢家 | `human_work_item_concurrent_claim_has_one_winner_and_cancel_closes_it`；`reserved_human_completion_is_replayed_after_runtime_restart_without_client_retry` |
| Policy-bearing Graph 合法但不可无损 structured 归约 | `native_linear_graph_with_policy_remains_graph_with_a_stable_diagnostic` |
| Graph wire、布局隔离、服务端验证和 graph-only 保留 | `graph_wire_contains_explicit_parts_and_never_serializes_plan_or_hash`；`layout_only_changes_cannot_change_graph_semantic_hash`；`authoritative_graph_decode_recompiles_and_rejects_invalid_tampering`；`valid_graphs_without_a_lossless_structured_inverse_remain_graphs` |
| Graph/View/trace durable authority 与产品 API 流 | `graph_author_view_and_trace_have_one_durable_authority_each`；`sqlite_graph_author_publish_run_trace_and_view_is_a_product_flow`；`postgres_graph_author_product_contract_matches_sqlite` |
| production shared Artifact store 身份与 fail-closed authority | `production_artifact_store_gate_precedes_publication_and_binds_shared_identity`；`sqlite_artifact_store_authority_is_atomic_immutable_and_fail_closed`；`postgres_artifact_store_authority_is_atomic_immutable_and_fail_closed_when_available` |
| Artifact 多引用 retention、orphan GC 和 Public Event 清理 | `sqlite_shared_content_hash_is_deleted_only_after_every_run_releases_it`；`postgres_shared_content_hash_is_deleted_once_after_global_release_when_available`；`production_artifact_gc_deletes_verified_unreferenced_object`；`production_public_event_pruner_keeps_terminal_and_expires_nonterminal_rows` |
| 真实进程恢复与 stock production binary | `postgres_real_process_crash_matrix_recovers_from_durable_state`；`stock_production_binary_recovers_postgres_run_across_restart` |

| 发布门禁 | 结果 |
|---|---|
| `CI=1` 且全部 PostgreSQL 测试 URL 指向全新 PostgreSQL 16.14 数据库，执行 `cargo test --all-targets --quiet` | 通过；包括非 skip PostgreSQL、real-process、恢复/crash、Subflow 与 stock binary gate |
| `cargo fmt --all -- --check`、`git diff --check` | 通过 |
| `bash scripts/check-cutover-residuals.sh` | 通过；production source-symbol residual 为零 |
| `cargo clippy --all-targets --all-features -- -D warnings` | 通过 |
| `cargo audit` | 通过；无安全公告命中 |
| `cargo deny check` | 通过；仅报告允许的重复依赖版本 warning |
