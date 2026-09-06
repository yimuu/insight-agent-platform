# ADR-0009：持久执行底座、Agent 领域与执行版本边界

| 属性 | 值 |
|---|---|
| 状态 | Proposed |
| 日期 | 2026-09-06 |
| 设计提案 | [架构重整 Spec](../specs/architecture-restructuring/README.md) |
| 接受条件 | 对应 owning types、边界合同与必要 migration 和设计完成共同交叉评审 |
| 当前行为 | 继续由 accepted ADR、现有机器合同、migration 和 owning Rust type 定义 |

## 背景

平台已采用 PostgreSQL current-state authority、shared Job、精确资源绑定和隔离的 Sandbox 执行。
随着功能扩展，跨领域事务与适配器知识集中在公共 repository 和编排应用层，作者入口与 Plan 能力不一致，
调度公平、配额拒绝、长 Run 版本演进以及事件投递的完整边界需要重新明确。

本决策的目标是保存现有正确性机制，减少重复执行基础设施与跨层耦合，并为数据保留和运行中升级建立明确语义。
本 ADR 是目标提案，不将未实施行为写成当前事实。

## 提议决策

保留自研的显式 Plan 执行与 PostgreSQL 持久化模型。定义、Run/Plan、Agent 领域、Job/调度、数据安全和物理适配器
分别拥有其事实与决策。领域函数保持纯决策；PostgreSQL adapter 通过同一 transaction 组合跨领域原子操作。
不新增中央 mutation 服务或第二个 durable engine，不通过 Provider、消息或事件重放建立 current-state authority。

逻辑调用身份跨重试稳定，物理 attempt/fence 随代次变化。Job 领取不等于外部副作用重做授权；不确定结果由领域
根据 effect、幂等与证据决定核对、失败或继续。取消先关闭新执行准入，再使子执行和物理清理收敛。

调度采用固定虚拟分区，PostgreSQL 保存唯一分区映射和分区内公平状态；配额在领取事务中决定可接受任务子集。
公平性保证明确限于调度分区，跨分区为近似公平；租户硬配额仍由现有账本原子裁决。
虚拟分区不代表数据库物理分表，也不随 worker 副本数改变。

将既有租户 deficit 从 `scheduler_state` 的集合载荷迁到一个租户调度状态表，并新增持久 Job 扫描 continuation；分区行保留轮转协调状态。
新表具有独立的租户并发边界及按分区/租户持续推进的核心查询，避免将无限增长的活跃租户状态塞入有界 JSON，或因窗口裁剪
遗失公平历史。该状态迁移后只有一个 owner，不能同时维护旧集合；候选仍读取 Job，不建立第二 ready queue。
有界扫描持续推进并环回，配额不满足的前排任务不能永久遮挡后续可准入任务。

Run 固定不可变定义与执行语义要求，worker 声明兼容语义版本，实际 build 作为证据记录。
安全修复可以发布保持语义兼容的新 build；改变解释语义须显式变更版本。运行定义冻结不免除当前权限撤销检查。
非 Run Job 使用自己的编译语义或领域维护操作 ABI，不借用虚构 Run 身份；编译语义及策略输入在验证任务创建时冻结。
授权撤销阻止新执行和正文披露，合法系统角色仍可按 exact effect/owner/fence 完成已有副作用证据、额度结算与清理，不能因此增加业务能力。
数据库采用受审查的向前迁移，支持窗口、排空和恢复规则显式定义；不得因目录改造重写已发运 baseline。

Event 保存已提交事实，Outbox 专注投递，业务清理复用 shared Job。公开读取以 owner 当前状态为准；必要的派生投影
可重建但不能裁决授权、额度或执行状态。保留与恢复覆盖数据库、对象 generation、密钥及已发生外部副作用。
Event 仅按连续前缀保留清理并原子推进 Run replay floor；Outbox worker 使用受限角色和部署拥有的 JetStream 投递合同。
PKCE cleanup 耗尽后通过显式领域恢复命令产生新的物理 Job 并原子替换 current pointer，逻辑删除身份和旧证据保持不变。

作者入口复用一个纯编译核心和唯一有界 Plan，Registry 仍拥有服务端验证与冻结。
Agent 配方组合现有 Model、Capability、Context、Skill 和 Task 语义；编译集成与黑盒框架集成明确各自恢复边界。
目录按依赖与职责组织，部署按信任域、权限和故障隔离组织，两者不一一对应。

## 与既有 ADR 的关系

- ADR-0001 的单一 authority、共享业务模型、事务与只读 runtime schema verification 继续保留。
  本提案若被接受，将调整其 fresh-only 演进边界以支持保留数据的向前迁移，并允许固定逻辑调度分区。
  同时按其新增表审查要求，允许上述租户调度状态表替代集合内租户状态，调整该项表上限；精确物理结构与新 schema contract
  由相应 forward migration 共同审查，已发运 baseline 保持不变。此例外不授权 PostgreSQL 物理 partition、第二配额账本或其他新状态表。
- ADR-0004 的 public `/v1`、无 BFF、无身份旁路和不可变发行路径继续保留；补充共享编译实现与完整作者能力。
- ADR-0007/0008 的唯一物理路径、两阶段激活、权限例外与保证边界保持有效；本提案不添加执行 provider 或降低隔离。

本 ADR 在 Proposed 状态不替代或修改上述 accepted 决策。接受时必须精确记录被修改条款及对应 authority，
不能仅修改 ADR 状态就认为相关合同和运行路径已经存在。

## 取舍与后果

保留自研内核使 Agent 授权、配额和执行结果可以在现有 PostgreSQL 事务边界组合，也继续承担状态转换、恢复与版本测试成本。
逻辑分区减少全局竞争，但放弃无协调条件下的全局严格公平。受控停写迁移减少双映射与混合解释风险，代价是明确维护窗口。
共享编译减少语义漂移，但浏览器/WASM、native 和公开验证仍需独立 conformance 证据。

拒绝以新增微服务数量、减少数据库表数量或目录移动完成度评价架构成功。
验收依据是隔离、liveness、配额守恒、first-winner、版本兼容、消息恢复、数据保留和真实用户组合行为。
精确合同目标与验收场景见 spec；通过后持久决定留在本 ADR，临时提案从工作树删除。
