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

旧 `insight.agent/v1` DSL、单进程 runtime 和 terminal-only 路径已经退出默认产品合同；历史设计可从 Git 查看。

公开与跨进程边界以 `contracts/platform-v1` 中的 OpenAPI、JSON Schema、protobuf和生成registry为authority；持久化结构以
migration为authority；进程内语义由owning Rust type与domain test约束。真实多节点OpenSandbox/Kubernetes production topology
与GitOps promotion仍为外部门禁，不因仓库clean cut自动通过。
