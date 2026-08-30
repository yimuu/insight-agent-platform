# 当前产品文档

这里仅描述已由 productization P0～P4 证明的 `insight.platform/v1` 与 `/v1` 产品：

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
conformance tests 的交集为准。真实多节点 Kubernetes/runsc 与 production GitOps promotion 仍为外部门禁，不因
仓库 clean cut 自动通过。
