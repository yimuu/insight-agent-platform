# Changelog

本文件记录 Insight Agent Platform 面向使用者的重要变更。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- SQLite/PostgreSQL 等价的 Agent 与 Provider Operator 管理面：Agent Draft、Validation、Definition、
  Deployment Resolution/Revision、Activate、Archive/Restore、Debug Session，以及 Provider Draft、
  Discovery、显式模型导入、Connection Test、Validation、Revision、Activate、Suspension/Resume、
  Retirement；附带严格 JSON Schema/OpenAPI、CLI 导入和 clean-cut migration 工具。
- 多 runtime Provider Revision registry 投影：PostgreSQL body-free opaque notification 只用于唤醒，
  generation poll 与 durable active/archive state 保持权威；Agent Deployment 固定 exact Provider、
  MCP、Action、Retrieval、Subflow 与 worker evidence。
- 完整 MCP `2026-07-28` modern Host/Client 与 Server profiles，覆盖 Tools、Resources、Prompts、
  Completion、Subscriptions、Elicitation、stdio、Streamable HTTP 和 HTTP Authorization；
  官方 Tasks extension 与 `2025-11-25` legacy client 作为独立协商 profile。
- 独立 `insight-mcp` 协议边界、`/mcp` Server endpoint、MCP catalog/context/connection API、
  durable interaction API 与 `run-stream/v1` interaction 事件。
- SQLite/PostgreSQL 等价的 MCP interaction、remote/server task、OAuth transaction 与加密
  credential 持久化，以及 TypeScript/Go 官方 SDK 双向互操作资格验收。
- 版本化只读 Provider Catalog，内置 `dashscope-cn` / `dashscope-intl` route、官方
  `DASHSCOPE_API_KEY` 凭据约定、最小模型 Profile，以及可选的 `providers` extension 与
  `model_policy` 治理配置。
- `run-stream/v1` 新增 `run.tool.progress`，Action 可通过独立闭合 Schema 和 scoped
  best-effort publisher 公开有界的 `output_text/output_json` 执行进度。
- `progress_tool_assistant` 与 `progress_counter` 示例，用于演示模型工具参数、两次进度、公开结果
  和 assistant continuation 的完整 Attached SSE 生命周期。
- 显式 opt-in 的 `terminal_only` 执行路径，只持久化 admission 与 terminal result；进程失败时
  未完成 Run 明确进入 `interrupted`，不伪装成可恢复执行。
- tenant/user 隔离的 Conversation、message、summary、cursor pagination、archive 与 privacy
  delete API。
- tenant-scoped Artifact encryption、retention/deletion maintenance，以及 Phase 0、Gate A～D
  fail-closed qualification harness。

### Changed

- Graph/YAML authoring 统一进入 managed Agent Draft；semantic edit、Definition publish、Deployment
  create 和 active route 切换不再隐式合并。普通历史 Deployment Run 路由与旧
  `/v1/graph-agents/**` clean-cut 删除。
- 静态 Agent 目录与 `providers.extensions` 只作为显式导入源；服务启动不再用文件状态覆盖 durable
  Agent/Provider head。MCP/Provider discovery 均不提供 wildcard 或 auto-import。
- Agent `llm.model` clean-cut 为严格的 `{provider, id}` selector；删除模型业务别名、必需的
  `models.yaml` / `models.config` 和公共 `json_object_output` 配置。结构化 `response` 现在始终
  使用平台 Prompt 策略与本地 JSON/Schema 校验；Provider Catalog 和扩展不再声明或自动启用
  原生 `response_format`，对应模型 worker 身份更新为 `openai-chat-adapter-2.1.0`。
- Attached SSE 已 clean-cut 到统一的 `run-stream/v1`：27 个闭合事件全部以 `run.*` 命名，
  lifecycle terminal 只携带一个按状态闭合的 `run` 快照；Full 与 Terminal-only 共享同一
  wire shape。
- durable terminal snapshot 改为 canonical `run_payload`，协议哈希域显式包含
  `run-stream/v1`。Run terminal transaction 以 first-winner 将未闭合 interaction 转为
  `run_terminal`，将完整安全摘要与 terminal 事实一起冻结进 `run_payload`，并纳入
  `snapshot_hash`；旧 `response_snapshots` 结构不支持原地升级。
- Attached HTTP 不再返回 `X-Response-ID`，`run_id` 成为公开执行身份；`running` 只在执行
  authority 确认后发出，terminal replay 可直接从 `created` 进入 terminal。
- 运行时配置键从 `response_stream` clean-cut 为 `run_stream`，不提供旧键别名。
- `run.tool.completed/failed` 现在包含 logical tool call 的 `duration_ms`；full runtime 使用
  durable 首次执行时间并覆盖 retry/backoff，terminal-only 使用同边界的进程内计时。
- terminal `run.tool_results` 现在保留公开 status-only 成功调用并使用空 `content` 校准；
  `current_time`、`text_metrics`、`integer_calculator` 公开安全结果，`text_replace` 结果继续私有。
- `run-stream/v1` 的闭合事件集合最终固定为 27 个：原有 25 个运行事件加上
  `run.interaction.required` / `run.interaction.closed`；未发布期直接 clean-cut，不提供
  `response-stream/v1` 或第二个 run-stream 版本兼容层。
- terminal `interactions[]` 最多完整冻结 1024 项，超限 fail closed 且不静默截断；
  live `run.interaction.required` / `run.interaction.closed` 只是通知，durable terminal snapshot 是恢复与
  终态校准权威。
- Deployment Revision identity 冻结 `full|terminal_only` persistence policy；Run DTO 显式返回
  recovery、event replay 与 wait capability。
- Quickstart、Helm chart 和未声明 Deployment Revision 的默认 persistence mode 继续为 `full`；
  `terminal_only` 仍需兼容的 Deployment Revision 显式选择。

### Security

- Agent/Provider/MCP 管理路由使用共享但 capability 闭合的 Operator credential，mutation 强制
  request ID/ETag/idempotency，并在同一事务提交 body-free audit/outbox；Provider credential 只接受
  独立 `management.provider_secret_resolver` 白名单中的 reference。
- MCP secret 与 OAuth token 仅通过 Secret 引用和版本化 envelope encryption 使用；远程 metadata、
  resource/prompt/tool content、stderr、request state 与用户响应均经过边界校验、隔离和脱敏，
  不成为 Plan、revision identity 或默认日志中的安全权威。
- Provider extension 只保存 secret reference；Provider/model、endpoint、adapter、Catalog 与
  extension digest 进入 Deployment Revision，secret 值和 URL query 不进入 Plan、identity 或日志。
- 工具参数、进度和结果继续独立双重授权；progress 在冻结 Schema、公共结构/大小限制和频率限制
  后才进入 live broker，且不会进入 terminal result、Conversation 或默认正文日志。
- Conversation principal、tombstone 后公共读取、跨 tenant object 删除和密文明文泄漏均由
  repository/API 合同与资格测试覆盖。

## [0.1.0] - 2026-07-25

首个公开开发版本。`0.1.x` 阶段的公开合同仍可能发生不兼容调整。

### Added

- `insight.agent/v1` 结构化 Agent DSL，支持类型、约束、模板、分支、并行、Map、Loop、
  AgentLoop、Subflow、Try/Catch/Finally、等待、人工任务和显式终止。
- YAML 与 Graph authoring 统一编译为不可变、类型化并经过验证的 Canonical Plan。
- PostgreSQL 16 生产持久化内核与 SQLite 单进程开发实现，覆盖 lease/fence、重试、超时、
  signal/timer first-winner、恢复、redrive、fork、migrate 和 continue-as-new。
- Model、Action 与 Retrieval 资源注册、严格 schema 校验、OpenAI adapter、工具 continuation
  和公开结果投影。
- `/v1` HTTP API、Attached live-only SSE、durable terminal snapshot、人工任务和 Artifact 读取。
- 内容寻址 Artifact store、保留策略、GC、公开事件投影和响应流边界。
- PostgreSQL/SQLite 权威完整 Schema、启动前显式 provisioning、只读 contract gate，以及业务
  服务运行时零 DDL 权限边界。
- 分层 Rust workspace、依赖策略检查、真实 PostgreSQL 合同测试、binary smoke 和恢复门禁。

### Security

- 配置、DSL、Plan、任务和事件 wire 默认拒绝未知字段，并在持久化边界重新验证权威 hash。
- Secret 值不进入 Plan、数据库、Graph、trace、错误、日志或 Debug 输出。
- 外部 HTTP、TLS、redirect、schema reference、响应大小和流式输入均使用显式安全策略与上限。

### Compatibility

- 本版本采用 clean-break 发布，只接受 `insight.agent/v1`。
- 不提供旧 DSL、旧运行内核、旧 Plan、旧 migration 路径或开发数据库兼容层。
