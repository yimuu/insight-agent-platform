# Insight Agent Platform 文档

这里是仓库文档的唯一入口。`current` 包含当前可用行为的使用、运维和开发文档；`archive`
保存设计、实施、评审和迁移记录。

## 阅读路线

第一次接触项目时，建议按以下顺序阅读：

1. [根 README](../README.md)：启动 Quickstart 并运行第一个 Agent；
2. [架构概览](current/architecture.md)：理解 durable graph execution；
3. [DSL v1 指南](current/dsl.md)：编写 Agent；
4. [HTTP 与 SSE API](current/api.md)：创建、观察和控制 Run；
5. [部署与运维](current/operations.md)：配置生产环境；
6. [开发指南](current/development.md)：修改和验证实现。

## 当前文档

| 文档 | 面向读者 | 内容 |
|---|---|---|
| [架构概览](current/architecture.md) | 架构师、开发者 | 运行模型、持久化边界与核心不变量 |
| [DSL v1 指南](current/dsl.md) | Agent 作者 | 作者语法、类型、表达式、控制流 |
| [HTTP 与 SSE API](current/api.md) | API 使用者 | 路由、幂等、响应流和人工任务 |
| [部署与运维](current/operations.md) | 运维、平台开发者 | 配置、数据库、Artifact、认证、迁移 |
| [开发指南](current/development.md) | 贡献者 | 代码导航、测试和 CI 门禁 |

## 权威关系

发生冲突时，按以下顺序判断：

1. 当前 schema、compiler、Plan verifier 与数据库约束；
2. PostgreSQL/SQLite、恢复和 real-process conformance tests；
3. checked-in Agent 与 positive fixtures；
4. `current` 中的文档；
5. `archive` 中的历史记录。

当前文档用于解释可执行合同。发现不一致时，应同时修正实现证据与文档，不能从历史档案恢复已经
删除的语义。

## 历史档案

[`archive/`](archive/README.md) 中的 specs、plans、reviews 和 migrations 只用于追溯决策。
归档文件中的状态、示例、路径和待办事项均以当时上下文为准，不是当前使用说明。
