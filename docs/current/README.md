# 当前文档与规范

本目录包含当前 DSL v3 与 durable runtime 的使用者文档、已生效规范，以及明确标为未实现的活跃工程
提案。提案在状态切换前不描述当前实现事实。

## 使用者文档

- [架构概览](architecture.md)
- [DSL v3 指南](dsl.md)
- [HTTP 与 SSE API](api.md)
- [部署与运维](operations.md)
- [开发指南](development.md)

## 规范性合同

当前唯一的整体架构与执行语义规范是
[DSL v3 持久化图执行架构规范](specifications/2026-07-18-dsl-v3-durable-graph-execution-design.md)。
它定义作者 DSL、Canonical Typed Plan、持久化 Run/Scope/Activation/Attempt、控制 token、
Worker lease、恢复、Artifact 和发布门禁。

[Response 实时流与 LLM 发布控制规范](specifications/2026-07-19-response-streaming-and-llm-publication-design.md)
是已实现的窄增量，调整 Attached 用户响应流、LLM `stream`/`publish` 作者合同、工具 continuation、
RAG 公开结果和最终 response snapshot。

## 已实施的内部工程规范

[Rust Workspace 与 Crate 边界拆分规范](specifications/2026-07-21-rust-workspace-crate-boundaries-design.md)
记录单 package 向分层 workspace 的已完成迁移、依赖 DAG、兼容 facade 和验收证据。它当前为
`Implemented / Verified`：七个内部 member 与根 facade 已通过结构、兼容、SQLite/PostgreSQL 16、
real-process 和供应链门禁；该内部改造不改变上述已实现的 v3 行为合同。

仓库实现、数据库约束与测试是上述已生效规范的可执行一致性证据。完整的文档权威顺序见
[文档首页](../README.md)。历史设计与实施记录见[历史档案](../archive/README.md)。
