# Insight Agent Platform 文档

`current` 描述当前 `insight.platform/v1` 产品；`specs` 保存仍有效的设计合同；`qualifications` 只记录开发阶段
验证边界。已完成、被替代和废弃的文档不在工作树保留，需要时从 Git 历史查看。

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

发生冲突时依次以 owning spec、公开 OpenAPI/JSON Schema 或 protobuf、Rust nominal type、实现与 conformance evidence
判断。`docs/current` 解释已实现合同；Git 历史只用于追溯，不能恢复旧 DSL、单进程 runtime、fallback 或双写。

## 设计与验证

- [活动设计规范](specs/README.md)
- [开发阶段验证状态](qualifications/README.md)
