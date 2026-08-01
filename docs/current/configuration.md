# 平台配置

状态：Current

平台配置文件的顶层 `version` 仍为 `1`；MCP 子系统已经 clean-cut 到 `mcp.version: 2`。这两个版本
属于不同命名空间。配置解析严格拒绝未知字段，所有相对路径以配置文件目录为基准。

## MCP v2 权威边界

`mcp.client` 只定义全局安全策略，不包含第三方 Server 实例。Server、Draft、Discovery、Import、
Validation、Revision 和 active pointer 的唯一权威是 durable management store。旧
`mcp.client.servers` 会被严格解析器拒绝，不存在双读或 YAML 优先级。

管理面启用时必须同时满足：

- `mcp.client.enabled: true`；
- 至少一个互不重复的 Operator credential，token 只能来自 `token_env`；
- `secret_encryption` keyring 可解析；
- `secret_resolver.allowed_names` 非空，且管理对象只能引用该集合；
- discovery worker 数为 `1..=64`，pending 上限为 `1..=10000`；
- production 不允许 loopback 明文、private-network 或 redirect 例外。

Streamable HTTP Draft 只接受无 userinfo/query/fragment 的 HTTPS endpoint；单进程开发可由全局策略
允许精确 loopback HTTP。stdio Draft 只能引用 `stdio_launch_profiles` 中预批准的绝对 executable、
固定 argv、绝对工作目录、secret environment 槽位和 isolation profile。API 只允许提交 profile 的
`allowed_parameters`，不能提交 executable、环境变量名或 shell 字符串。

完整示例和各字段说明见 [MCP 使用、运行与安全合同](mcp.md)。生产/Quickstart 的可执行基线分别见
[`config/platform.yaml`](../../config/platform.yaml) 与
[`config/platform.quickstart.yaml`](../../config/platform.quickstart.yaml)。Helm values 使用
`mcp.client.*` 渲染同一合同，secret value 只能通过 `mcp.secretEnv[]` 或专用 existing Secret 注入。
可直接 lint/render 的启用示例见
[`values-mcp-management-example.yaml`](../../deploy/helm/insight-agent-platform/values-mcp-management-example.yaml)。

## Secret 与哈希

配置和管理 API 只持久化 secret reference。secret value 不进入 Draft/Revision canonical JSON、
fingerprint、日志、错误、metric label、审计 payload 或响应。轮换同一 ref 的值不会改变 Revision；
endpoint、ref 名、stdio profile fingerprint 或平台 policy fingerprint 改变会使旧 evidence stale 或
阻止重新激活。

## 配置验证

本地验证配置和完整 Schema 合同：

```bash
cargo test --locked --test platform_config_v2
cargo test --locked -p insight-storage --test schema_layout
helm lint deploy/helm/insight-agent-platform
helm template insight-agent-platform deploy/helm/insight-agent-platform
```

生产启用值还应先在隔离 namespace 渲染并检查 Operator token、MCP keyring 和 resolver 引用均来自
Kubernetes Secret；ConfigMap 中不得出现这些值。
