# Insight Agent Platform 文档

`current` 只描述当前 `insight.platform/v1` 产品；`specs` 保存合同与产品化记录；`qualifications` 跟踪尚未运行的
外部发布门禁；`archive` 保存已退出默认产品面的历史资料。

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

Platform v2 的架构代号不会产生 `/v2` API。CR-216 OpenSandbox仓库实现与L1～L3已通过；真实多节点
OpenSandbox/Kubernetes production topology、容量、混沌、restore、soak与GitOps promotion仍为外部L4～L6 Not run，
不被仓库内clean cut掩盖。

## 权威关系

发生冲突时依次以 owning spec、公开 OpenAPI/JSON Schema 或 protobuf、Rust nominal type、实现与 conformance evidence
判断。`docs/current` 解释已实现合同；`docs/archive` 只用于追溯，不能恢复旧 DSL、单进程 runtime、fallback 或双写。

## 历史与资格

- [历史档案](archive/README.md)
- [Platform v2 Production L4～L6](qualifications/platform-v2-production-l4-l6.md)
- [Productization 目标](specs/productization/00-goals.md)
- [Productization 实施与证据](specs/productization/implementation-plan.md)
