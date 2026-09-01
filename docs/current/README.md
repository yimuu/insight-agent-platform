# 当前产品文档

这里描述当前 `insight.platform/v1` 与 `/v1` Agent 产品，包括预构建 CLI、Console、默认 `starter` 和显式
feature 闭包。仓库资格证明覆盖 L1～L3；真实发行与生产 L4～L6 状态在对应文档中保持 `Not run`。

- [架构与 authority 边界](architecture.md)
- [`insight` CLI](cli.md)
- [公开 HTTP API](api.md)
- [原始 HTTP authoring](http-authoring.md)
- [运行控制台](console.md)
- [MCP、remote Capability 与 Sandbox](mcp.md)
- [部署与运维](operations.md)

旧 `insight.agent/v1` DSL、单进程 runtime、terminal-only 路径及其迁移文档已移入
[`docs/archive/current-dsl-v1`](../archive/current-dsl-v1/)，不再是默认产品合同。

当前行为以 `contracts/platform-v1/openapi.yaml`、public JSON Schema、protobuf、Rust nominal type、实现和
conformance tests 的交集为准。真实多节点 OpenSandbox/Kubernetes production topology 与 GitOps promotion 仍为外部门禁，
不因仓库 clean cut 自动通过。
