# 平台配置

状态：Current

平台配置文件的顶层 `version` 仍为 `1`；MCP 子系统已经 clean-cut 到 `mcp.version: 2`。这两个版本
属于不同命名空间。配置解析严格拒绝未知字段，所有相对路径以配置文件目录为基准。

## 共享 Management v1

Agent、Provider 和 MCP 管理 API 共用顶层 `management.version: 1`。启用任一管理路由时必须设置
`management.enabled: true` 并至少配置一个 environment-backed Operator credential；普通 `auth.mode`
不能让管理路由匿名，也不能把 Operator token 当作 tenant/user 提权。capability 是闭合集合且互不隐含。

```yaml
management:
  version: 1
  enabled: true
  operator_credentials:
    - identity: platform-author
      token_env: INSIGHT_PLATFORM_AUTHOR_TOKEN
      capabilities:
        - agent.read
        - agent.write
        - agent.validate
        - agent.publish
        - agent.deploy
        - agent.activate
        - agent.archive
        - agent.debug.sandbox
        - provider.read
        - provider.write
        - provider.discover
        - provider.test
        - provider.publish
        - provider.activate
        - provider.suspend
        - provider.retire
        - mcp.server.read
  provider_secret_resolver:
    type: environment_reference
    allowed_names: [COMPANY_LLM_TOKEN]
  limits:
    max_agent_draft_bytes: 4194304
    max_agent_prompt_files: 128
    max_provider_models: 4096
    max_pending_operations: 256
    operation_retention_days: 30
  debug_execution_profiles:
    author-sandbox:
      mode: sandbox
      max_concurrent_sessions: 4
      session_timeout: 10m
      retention: 24h
      allow_external_actions: false
      allow_live_provider_credentials: false
```

Sandbox profile 不能允许外部 action 或 live credential。Live profile 还要求 `agent.debug.live`；首次
请求只返回费用/副作用风险预览，创建请求必须带回 exact `risk_preview_hash` 和
`live_confirmation: true`。token value、debug input 和 secret value 都不能写入配置文件。

`management.provider_secret_resolver` 是 managed Provider credential reference 的唯一白名单；
`environment_reference.allowed_names` 只列环境变量名，Draft 中对应
`secret://environment/<NAME>`。Provider validation/publish 要求该名称在服务进程环境中可解析，但 API、
`providerctl`、Revision、hash 与审计都不读取或保存 value。它不再从旧 `providers.extensions` 推导。
activation readiness 和 runtime projection 都使用同一白名单；已发布 reference 后续被移出白名单或
value 不可用时，current/exact model resolution 均 fail closed。

## MCP v2 权威边界

`mcp.client` 只定义全局安全策略，不包含第三方 Server 实例。Server、Draft、Discovery、Import、
Validation、Revision 和 active pointer 的唯一权威是 durable management store。旧
`mcp.client.servers` 会被严格解析器拒绝，不存在双读或 YAML 优先级。

管理面启用时必须同时满足：

- `mcp.client.enabled: true`；
- 共享 `management.operator_credentials` 中至少一个具备相应 MCP capability 的 credential；
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
`management.*` 与 `mcp.client.*` 渲染同一合同，secret value 只能通过 `mcp.secretEnv[]` 或专用 existing Secret 注入。
可直接 lint/render 的启用示例见
[`values-mcp-management-example.yaml`](../../deploy/helm/insight-agent-platform/values-mcp-management-example.yaml)。

## Live Run Stream bus

`runtime.run_stream` 与 durable `history` 独立。单 Runtime 默认使用进程内 backend：

```yaml
runtime:
  run_stream:
    topology: single_runtime
    broker:
      type: in_memory
    body_queue_capacity: 256
    control_queue_capacity: 32
    max_frame_bytes: 4096
    max_item_bytes: 4194304
    max_run_bytes: 16777216
    terminal_barrier_timeout: 2s
    outbound_write_timeout: 10s
```

Worker 与 Attached SSE 可能位于不同 Runtime 时，必须显式选择 `distributed` + Core NATS：

```yaml
runtime:
  run_stream:
    topology: distributed
    broker:
      type: nats_core
      servers: [tls://nats-0.nats.svc:4222]
      namespace: prod_cn1
      credentials_env: INSIGHT_RUN_STREAM_NATS_CREDENTIALS
      tls:
        required: true
        root_certificates: [/var/run/secrets/insight-nats/ca.pem]
        client_certificate: null
        client_private_key: null
      connect_timeout: 5s
      subscription_ready_timeout: 2s
      reconnect_min_delay: 100ms
      reconnect_max_delay: 5s
      max_pending_messages: 4096
      max_pending_bytes: 16777216
      drain_timeout: 5s
    body_queue_capacity: 256
    control_queue_capacity: 32
    max_frame_bytes: 65536
    max_item_bytes: 4194304
    max_run_bytes: 16777216
    terminal_barrier_timeout: 2s
    outbound_write_timeout: 10s
```

`in_memory` 只允许 `single_runtime`；SQLite 也只允许 `single_runtime`。`nats_core` 可用于两种 topology，
但 production 必须启用 TLS 并提供 `credentials_env`。server URL 必须是无 userinfo、path、query、
fragment 的显式 `nats://host:port` 或 `tls://host:port`；namespace 只允许 1～64 个小写字母、数字、
`_`、`-`。旧字符串 `in_process`、`postgres_notify`、`nats_core` 和 backend object 未知字段都会被拒绝。

Helm 使用 `runtime.runStream.*` 渲染同一合同，默认仍为单副本 `in_memory`。外部 NATS 示例见
[`values-nats-core-qualification.yaml`](../../deploy/helm/insight-agent-platform/values-nats-core-qualification.yaml)；
credentials 和 TLS 必须来自两个既有 Kubernetes Secret，chart 不部署 NATS，也不把 secret value
渲染到 ConfigMap。

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
