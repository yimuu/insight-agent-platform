# 当前产品文档

这里描述当前 `insight.platform/v1` 与 `/v1` Agent 产品，包括默认 Compose 安装、Console、多来源模型配置和共享安装所有者的 Native/Helm 入口。各项验证以对应修订的证据为准；此前修订的资格不自动适用于当前架构，真实发行与生产门禁状态见资格文档。

## 统一安装交付入口

- [安装、启动与恢复](installation.md)：仓库根目录直接使用 Compose 构建并启动；Native 冻结主机产物，Helm 消费同一配置所有者。模型端点在启动后通过模型配置页面管理；检索等外部依赖按安装指南配置。Sandbox 仍使用独立 Kubernetes/OpenSandbox 路径。
- [模型配置](model-configuration.md)：Console 和公共 CLI 支持 Responses、Anthropic Messages 来源，显式文件/环境变量导入、额度、默认模型、连接检测、轮换和撤销。连接检测不是完整 Agent 运行资格，未知厂商事实仍保持未知。
- [公共 CLI](cli.md)与[Console](console.md)：Console 首次创建管理员账号，之后使用邮箱和密码登录；CLI 使用安装交付的私有短期会话。Console 对象上传经同源转发，由服务端验证存储 TLS；浏览器无需导入存储证书。
- [文档检索与人工审阅样例](../../examples/productization/document-review/README.md)：完整源码及普通公开资源发布步骤已交付；实际检索端点、其 conformance Artifact、真实依赖和人工响应必须由相应操作提供，不能用本地夹具替代。

同身份恢复会核对原持久数据，不重置、续补额度或修复外来状态。首次 provider 自初始化结果未知时保留原卷并拒绝自动重启。
实际安装、模型运行、持久会话与流式输出的本地验证见[资格记录](../qualifications/README.md)。
浏览器交互、真实检索/人工旅程与生产资格仍以各自证据为准。
[统一验收索引](../specs/unified-installation/README.md)保留当前结果、范围及历史失败记录。

## 其他产品与工程文档

- [架构与 authority 边界](architecture.md)
- [共享作者入口、框架集成与评测](agent-authoring.md)
- [公开 HTTP API](api.md)
- [原始 HTTP authoring](http-authoring.md)
- [MCP、remote Capability 与 Sandbox](mcp.md)
- [部署与运维](operations.md)
- [可观测性与告警处置](observability.md)

旧 `insight.agent/v1` DSL、单进程 runtime、terminal-only 与 SQLite 业务状态路径已从活跃 workspace、image、配置、
Schema 和资格 harness 删除；历史只能从 Git 查看，不能作为兼容入口恢复。

公开与跨进程边界以 `contracts/platform-v1` 中的 OpenAPI、JSON Schema、protobuf和生成registry为authority；持久化结构以
唯一当前 `crates/adapters/platform-postgres/schema.sql` 为authority；进程内语义由owning Rust type与domain test约束。真实多节点OpenSandbox/Kubernetes production topology
与GitOps promotion仍为外部门禁，不因仓库clean cut自动通过。
