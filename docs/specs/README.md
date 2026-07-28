# 活动设计规范

本目录保存已经形成明确实施与验收边界、但尚未完全成为当前可执行合同的设计规范。

- `specs` 描述目标状态、约束、实施阶段和验收门槛；
- `current` 只描述已经由 schema、实现和测试共同证明的当前行为；
- 规范完成实施并同步当前文档后，移入 `archive/specs` 留作决策记录。

发生冲突时，当前 schema、实现、conformance tests 和 [`current`](../current/README.md) 优先。
活动规范不能用于推断尚未交付的 API、配置项或容量承诺。

## 当前规范

当前没有尚在设计或实施的活动规范。

最近完成的规范：

- [Durable Runtime 50 活跃 Run 并发优化](../archive/specs/2026-07-26-durable-runtime-50-active-runs-optimization.md)；
- [Terminal-only Runtime 与 Conversation](../archive/specs/2026-07-27-terminal-only-runtime-and-conversations.md)。

尚未完成的 24 小时 release-candidate soak 属于资格验收，不是活动设计规范；见
[Durable Runtime 24 小时 RC 资格验收](../qualifications/durable-runtime-24h-rc.md)。
