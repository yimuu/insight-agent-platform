# 当前文档与规范

本目录只包含当前 DSL v3 与 durable runtime 的使用者文档和规范性合同。

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

仓库实现、数据库约束与测试是规范的可执行一致性证据。完整的文档权威顺序见
[文档首页](../README.md)。历史设计与实施记录见[历史档案](../archive/README.md)。
