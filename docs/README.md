# Insight Agent Platform 文档

这里是仓库文档的唯一入口。`current` 包含当前可用行为、已生效规范和明确标记状态的活跃工程提案；
`archive` 保存已被替代或失效的历史设计与实施记录。未实现提案不描述当前行为事实。

## 阅读路线

第一次接触项目时，建议按以下顺序阅读：

1. [根 README](../README.md)：启动 Quickstart 并运行第一个 Agent；
2. [架构概览](current/architecture.md)：理解 durable graph execution；
3. [DSL v3 指南](current/dsl.md)：编写 Agent；
4. [HTTP 与 SSE API](current/api.md)：创建、观察和控制 Run；
5. [部署与运维](current/operations.md)：配置生产环境；
6. [开发指南](current/development.md)：修改和验证实现。

## 当前文档

| 文档 | 面向读者 | 内容 |
|---|---|---|
| [架构概览](current/architecture.md) | 架构师、开发者 | 运行模型、持久化边界与核心不变量 |
| [DSL v3 指南](current/dsl.md) | Agent 作者 | 作者语法、类型、表达式、控制流 |
| [HTTP 与 SSE API](current/api.md) | API 使用者 | 路由、幂等、响应流和人工任务 |
| [部署与运维](current/operations.md) | 运维、平台开发者 | 配置、数据库、Artifact、认证、迁移 |
| [开发指南](current/development.md) | 贡献者 | 代码导航、测试和 CI 门禁 |

规范性设计位于 [`current/specifications/`](current/specifications/)。其中：

- [DSL v3 持久化图执行架构规范](current/specifications/2026-07-18-dsl-v3-durable-graph-execution-design.md)
  是整体架构与执行语义的主规范；
- [Response 实时流与 LLM 发布控制规范](current/specifications/2026-07-19-response-streaming-and-llm-publication-design.md)
  是主规范的已实现窄增量。

已实施的内部架构规范：

- [Rust Workspace 与 Crate 边界拆分规范](current/specifications/2026-07-21-rust-workspace-crate-boundaries-design.md)
  当前为 `Implemented / Verified`：根兼容 facade 与七个内部 member 已按单向依赖 DAG 完成拆分并
  通过完整门禁；该规范只改变代码组织，不改变 DSL、runtime、API 或数据库合同。

## 权威关系

发生冲突时，按以下顺序判断：

1. 状态为 `Implemented / Verified` 的当前规范中的显式合同；
2. v3 schema、compiler、Plan verifier 与数据库约束；
3. PostgreSQL/SQLite、恢复和 real-process conformance tests；
4. checked-in Agent 与 positive fixtures；
5. `current` 中的使用者指南；
6. `archive` 中的历史记录。

标记为 `Proposed / Not Implemented` 的工程设计不改变上述行为权威顺序，也不能覆盖已实现合同；其
规范效力仅限于未来迁移的目标与验收条件。

使用者指南用于解释当前合同，但不能改变规范或实现。发现不一致时，应同时修正规范、实现证据与
使用者指南，不能从历史档案恢复已经删除的语义。

## 历史档案

[`archive/`](archive/README.md) 中的 specs、plans、reviews 和 migrations 只用于追溯决策。
归档文件中的状态、示例、路径和待办事项均以当时上下文为准，不是当前使用说明。
