# Changelog

本文件记录 Insight Agent Platform 面向使用者的重要变更。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

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
