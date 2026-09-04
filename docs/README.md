# Insight Agent Platform 文档

`current` 描述当前 `insight.platform/v1` 产品；`contracts` 中的机器合同定义跨进程和公开边界；ADR 记录仍然有效的
架构决策；`qualifications` 只记录开发阶段验证边界。已完成、被替代和废弃的设计文档不在工作树保留，需要时从 Git 历史查看。

## 阅读路线

1. [根 README](../README.md)：安装预构建 CLI 并启动默认 `starter`；
2. [架构](current/architecture.md)：理解 authority 与隔离平面；
3. [`insight` CLI](current/cli.md)：发布、运行和观察 Agent；
4. [Public `/v1` API](current/api.md)与[原始 HTTP authoring](current/http-authoring.md)；
5. [运行控制台](current/console.md)；
6. [MCP、remote Capability 与 Sandbox](current/mcp.md)；
7. [部署与运维](current/operations.md)。

## 当前状态

产品化 P0～P4 与十条黄金场景已在同一 exact revision 通过；其上的 Agent 产品体验已经实现 shared compiler、
`insight agent`、Console、签名发行合同和 `starter + features` 开发闭包。Python SDK 已取消；Node 只用于 Console
构建、浏览器测试和独立 LangGraph.js reference，不是用户安装或 Rust 平台运行前置。

Platform v2 是架构代号，不会产生 `/v2` API。OpenSandbox 仓库实现和 L1～L3 已通过，本机 Kind L4 mechanics
曾完成 12/12 检查。项目仍处于开发阶段，生产级 L4～L6 当前不执行，也不声明 production-ready。

## 权威关系

公开或跨进程边界以 OpenAPI、JSON Schema、protobuf 和生成 registry 为 authority；持久化结构以 migration 为 authority；
进程内语义以 owning Rust type 与 domain test 为 authority；ADR 记录架构取舍。`docs/current` 解释当前行为，不建立第二套
字段或状态机。Git 历史只用于追溯，不能恢复旧 DSL、单进程 runtime、fallback 或双写。

## 设计与验证

- [`insight.platform/v1` 机器合同](../contracts/platform-v1/README.md)
- [Agent compiler conformance corpus](../contracts/product-experience/agent-compiler/v1/corpus.json)
- [架构决策](adr/)
- [开发阶段验证状态](qualifications/README.md)
