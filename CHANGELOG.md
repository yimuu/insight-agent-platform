# Changelog

本文件记录 Insight Agent Platform 面向使用者的重要变更。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- `response-stream/v1` 新增 `workflow.tool.progress`，Action 可通过独立闭合 Schema 和 scoped
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

- `workflow.tool.completed/failed` 现在包含 logical tool call 的 `duration_ms`；full runtime 使用
  durable 首次执行时间并覆盖 retry/backoff，terminal-only 使用同边界的进程内计时。
- terminal `workflow.tool_results` 现在保留公开 status-only 成功调用并使用空 `content` 校准；
  `current_time`、`text_metrics`、`integer_calculator` 公开安全结果，`text_replace` 结果继续私有。
- `response-stream/v1` 闭合事件集合由 24 个原地切换为 25 个；这是 `0.1.x` 受控客户端同步升级，
  不提供旧/新 v1 混合部署兼容层。
- Deployment Revision identity 冻结 `full|terminal_only` persistence policy；Run DTO 显式返回
  recovery、event replay 与 wait capability。
- Quickstart、Helm chart 和未声明 Deployment Revision 的默认 persistence mode 继续为 `full`；
  `terminal_only` 仍需兼容的 Deployment Revision 显式选择。

### Security

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
