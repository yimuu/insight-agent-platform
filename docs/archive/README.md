# 历史文档档案

本目录保存项目演进过程中产生的设计、实施计划、评审和迁移说明，仅用于追溯决策，不描述当前
生产合同。

| 目录 | 内容 |
|---|---|
| [`specs/`](specs/) | 已完成、被吸收、被替代或废弃的设计 |
| [`plans/`](plans/) | 已完成或失效的实施步骤与任务清单 |
| [`reviews/`](reviews/) | 特定时间点的代码、依赖和整改快照 |
| [`migrations/`](migrations/) | 旧 DSL 或运行时切换说明 |

归档文档保留原始标题、日期和大部分正文，以便理解当时为什么作出某项决定。文件中的状态、示例、
代码路径、依赖版本、验证结果和未完成事项不应直接用于当前实现。

## 最近完成记录

| 记录 | 状态 | 当前入口 |
|---|---|---|
| [Terminal-only Runtime 存储与 Conversation 规范](specs/2026-07-27-terminal-only-runtime-and-conversations.md) | Implemented / capacity-qualified（2026-07-28） | [资格指南](../current/terminal-only-qualification.md)、[资格报告](../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md) |
| [Terminal-only 默认模式 rollout 决策](reviews/2026-07-28-terminal-only-default-rollout-decision.md) | Accepted：默认保持 `full` | [部署与运维](../current/operations.md) |

当前入口：

- [文档首页](../README.md)
- [当前文档](../current/README.md)
- [架构概览](../current/architecture.md)
- [DSL v1 指南](../current/dsl.md)

如果归档记录与当前文档、实现或测试冲突，以[文档权威关系](../README.md#权威关系)为准。
