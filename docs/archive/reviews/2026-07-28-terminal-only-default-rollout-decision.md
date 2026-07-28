# Terminal-only 默认模式 Rollout 决策

日期：2026-07-28

状态：**Accepted**

决策范围：平台、quickstart、Helm 的默认 persistence mode；Deployment Revision opt-in；
现有 revision/Run 的迁移策略；terminal-only feature flag

关联规范：
[Terminal-only Runtime 存储与 Conversation 规范](../specs/2026-07-27-terminal-only-runtime-and-conversations.md)

资格报告：
[Terminal-only Runtime 与 Conversation 资格报告](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)

## 1. 决定

平台默认 persistence mode **保持 `full`，不切换为 `terminal_only`**。

| 决策项 | Accepted decision |
|---|---|
| 平台默认值 | `full` |
| quickstart 默认值 | `full` |
| Helm 默认值 | `full` |
| terminal-only feature | 保持启用 |
| terminal-only 使用方式 | 兼容 Agent 通过 immutable Deployment Revision 显式 opt-in |
| 现有 Deployment Revision | 保持原 immutable persistence policy，不自动重写 |
| 已完成或正在运行的 Run | 不迁移、不原地转换 persistence mode |
| Gate 完成后的自动动作 | 不自动改变默认值；Gate 通过与默认切换是两个独立决定 |

该决定满足规范完成定义第 12 项，但不等于总体资格已经完成。作出本决定时，Phase 0、Gate A、
Gate C 和 Gate D 已通过，Gate B 正式两小时运行仍为 **Running / not yet decided**。Gate B
未通过时不得把 terminal-only 描述为整体 Qualified；即使 Gate B 后续通过，本决定仍保持有效。

后续结果注记（2026-07-28）：签署后 Gate B 正式报告以 72,000/72,000 closure、
347,968,068 WAL bytes 和 `passed=true` 关闭，Phase 0、Gate A～D 与完成定义 1～12 全部通过，
总体状态成为 Qualified；见
[资格报告](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)。
该后续结果不修改本记录的 Accepted decision，平台、Quickstart 与 Helm 默认值仍为 `full`。

## 2. 配置落点

| 配置入口 | 字段 | Accepted value | Feature |
|---|---|---|---|
| [`config/platform.yaml`](../../../config/platform.yaml) | `runtime.default_persistence_mode` | `full` | `runtime.terminal_only.enabled: true` |
| [`config/platform.quickstart.yaml`](../../../config/platform.quickstart.yaml) | `runtime.default_persistence_mode` | `full` | `runtime.terminal_only.enabled: true` |
| [Helm `values.yaml`](../../../deploy/helm/insight-agent-platform/values.yaml) | `runtime.defaultPersistenceMode` | `full` | `runtime.terminalOnly.enabled: true` |

配置文件、省略字段的 DSL 行为和 Helm 默认值必须保持同义。任何将默认值改为
`terminal_only` 的后续变更都需要新的显式 rollout 决策，不能只依赖 benchmark 报告或代码默认。

## 3. 为什么不切换默认值

### 3.1 能力是有意不同，不是同一 durability 等级的优化

`terminal_only` 用显著更小的热写路径换取以下能力缺失：

- runtime/Pod/PostgreSQL restart 后不恢复未完成 Run；
- 没有中间 event/projection replay；
- 没有 durable timer、signal、human wait；
- 没有 fork-from-checkpoint、redrive prefix reuse、migration、continue-as-new；
- 没有 durable task/effect fence 或外部副作用 exactly-once；
- v1 不支持多 runtime ownership handoff。

这不是可以对所有现有 Agent 静默替换的实现细节。默认保持 `full` 可以避免把已有用户从 durable
合同降级为 best-effort terminal execution。

### 3.2 已通过证据支持“可 opt-in”，不支持“全局默认”

当前正式证据表明：

- Phase 0 已将 full 模式物理 WAL 来源 100% 分类，证明降低写放大需要独立执行合同，而不只是
  PostgreSQL 调参；
- Gate A 证明 standalone Run 为 1 admission + 1 result，Conversation turn 再增加 2 条
  message，54 张 full durable 表 delta 为 0；
- Gate C 证明 50 active Run 在 hard kill 后准确中断、没有自动 recovery、same request
  不重放 effect，只有新 request 才显式 retry；
- Gate D 证明 10,000 turn 的消息原子性、幂等、分页、有界 context、stream scaling、
  百万 message query、tenant encryption 和 privacy delete；
- Gate B 的正式两小时 WAL 判定在本决定签署时仍未完成。

这些证据足以支持受控、显式、能力匹配的 opt-in。它们没有证明所有现有 workflow、真实 LLM、
retrieval、第三方 action 或多 runtime 部署都适合作为默认 terminal-only workload。

### 3.3 保守默认不妨碍新能力使用

feature 保持启用，兼容 Agent 不需要等待全局默认切换。显式 Deployment Revision policy 还能让：

- API discovery 准确暴露 `recovery_capability=none`、`event_replay=false`；
- compatibility validator 在发布时拒绝 unsupported workflow；
- 指标按 persistence mode 分开；
- 回退通过发布新的 `full` revision 完成，不修改旧 revision 的语义。

## 4. Opt-in 条件

只有同时满足以下条件的 Agent/Deployment Revision 才允许选择 `terminal_only`：

1. 调用方明确接受未完成 Run 在进程或数据库故障后变为 interrupted；
2. workflow 不依赖 durable timer、signal、human wait、fork、redrive、migration 或 recovery；
3. compatibility validator 对该 immutable revision 判定通过；
4. API/SDK 使用者能识别 `recovery_capability=none` 和 `event_replay=false`；
5. 外部 action/provider 使用业务级 idempotency key，或调用方接受显式 retry 可能重复副作用；
6. 当前部署满足 terminal-only v1 单 runtime owner 限制；
7. Conversation retention、tenant encryption、privacy delete 和 object lifecycle 已配置；
8. workload 容量不超出已资格化 profile；真实 LLM/retrieval/第三方 API 需要单独容量验证。

`allow_volatile_waits` 默认保持 `false`。未来即使显式开启，也不能把 volatile wait 描述为 durable。

## 5. Rollout 与回退

### 5.1 首次 rollout

1. 发布代码、schema、API 与 feature flag，但默认继续使用 `full`；
2. 只为经过 compatibility review 的新 Deployment Revision 设置 `terminal_only`；
3. 从低风险 tenant/Agent 开始，观察 mode-specific accepted、active、interrupted、terminal commit
   retry、summary failure、privacy delete 和 capacity rejection 指标；
4. 将外部 effect 的 request/idempotency key 与 Run request ID 纳入业务审计；
5. 扩大 opt-in 前使用真实 workload 重新验证延迟、容量、provider failure 和数据生命周期。

### 5.2 回退

- 停止新的 opt-in：发布新的 `full` Deployment Revision；
- 全局停止新 terminal-only 部署：设置 `runtime.terminal_only.enabled=false` /
  `runtime.terminalOnly.enabled=false`；
- 不修改旧 revision 的 immutable policy；
- 不把正在运行的 terminal-only Run 转成 full Run，也不尝试从其进程内状态生成 durable
  checkpoint；
- 已提交 admission/result/message 继续按 retention 与 privacy policy 管理；
- 回退 feature flag 不删除 terminal schema、Conversation 或历史结果。

回退不会为已被 runtime crash 中断的 terminal-only Run补造恢复成功。需要重试时必须使用新
request ID，并遵守外部 effect 的幂等策略。

## 6. 明确拒绝的替代方案

| 替代方案 | 拒绝原因 |
|---|---|
| Gate B 通过后自动把默认改成 `terminal_only` | 性能通过不等于 durability 合同等价；违背独立 rollout 决策要求 |
| 静默把未声明 policy 的 existing revision 解释为 terminal-only | 会改变 immutable deployment 语义并产生不可见能力降级 |
| 原地迁移 active full Run | terminal-only 没有可承接 full checkpoint/recovery lineage 的合同 |
| crash 后自动以新 request 重跑 | 可能重复外部副作用，并把中断伪装成恢复 |
| 因默认保持 full 而关闭 terminal-only feature | 会不必要地阻止已审查兼容 Agent 获得低写入路径 |
| 为获得低 WAL 关闭 PostgreSQL durability | 会破坏 admission/result/message 权威，资格无效 |

## 7. 复审触发条件

只有出现新的独立决策提案，并至少补齐以下证据时，才讨论是否改变默认值：

- Gate A～D 和全部规范完成定义在同一 immutable image/source snapshot 上关闭；
- 代表性真实 LLM、retrieval 和第三方 action workload 的容量与 failure evidence；
- 生产观察窗口内的 interrupted、effect retry、terminal commit、summary 与 privacy SLO；
- compatibility inventory 证明未声明 policy 的目标 Agent 不依赖 full-only 能力；
- 若要支持多 runtime，存在独立的 ownership/routing 规范与资格证据；
- key rotation、retention、backup 与 privacy delete 的生产运维验证；
- SDK/UI 已清晰呈现 terminal-only 的 capability downgrade；
- 明确的 tenant opt-out、回退和事故响应方案。

复审必须生成新的 review 文件。不得覆盖本记录来制造“默认值从未是 full”的历史。

## 8. 证据索引

- 资格报告：
  [`bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md`](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)
- Phase 0：
  [`phase0-full-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/phase0-full-report.json)
- Gate A standalone：
  [`gate-a-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-a-standalone-final2-pass/gate-a-report.json)
- Gate A Conversation：
  [`gate-a-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-a-conversation-final2-pass/gate-a-report.json)
- Gate B：
  [`gate-b-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/gate-b-report.json)
- Gate B 清理：
  [`cleanup-evidence.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/cleanup-evidence.json)
- Gate C：
  [`gate-c-suite-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/gate-c-suite-report.json)
- Gate D：
  [`gate-d-report.json`](../../../bench/results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/gate-d-report.json)
- 当前 API 合同：
  [`docs/current/api.md`](../../current/api.md)
- 当前架构合同：
  [`docs/current/architecture.md`](../../current/architecture.md)
- 当前运维合同：
  [`docs/current/operations.md`](../../current/operations.md)

## 9. 决策摘要

**Accepted：默认保持 `full`；terminal-only feature 保持启用；只有兼容 Agent 的新 immutable
Deployment Revision 可以显式 opt-in；不迁移现有 revision 或 Run；Gate 通过不会自动改变默认值。**
