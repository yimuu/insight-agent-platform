# 已完成资格验收

本目录保存已经完成并签署的容量、故障、持久化和发布资格验收记录。记录中的复现环境、工作负载、
门槛和证据路径属于对应日期的验收快照，不是当前功能使用指南；当前运行合同以
[`docs/current`](../../current/README.md) 为准。

| 日期 | 记录 | 结果 | 正式报告 |
|---|---|---|---|
| 2026-08-01 | [Agent 与 Provider 管理 Control Plane v1 资格验收](2026-08-01-agent-provider-management-v1-qualification.md) | Qualified；双数据库控制面、exact binding、Debug、迁移与多 runtime Provider 投影通过 | 本记录 |
| 2026-07-31 | [MCP 管理 API v1 资格验收](2026-07-31-mcp-management-api-v1-qualification.md) | Qualified；durable 管理面、显式导入、双数据库、Agent/Run fence 通过 | 本记录；外部 SDK 原始报告由 qualification harness 生成 |
| 2026-07-30 | [MCP 完整支持资格验收](2026-07-30-complete-mcp-qualification.md) | Qualified；modern client/server、Tasks 与 legacy profile 通过 | 本记录；外部 SDK 原始报告由 qualification harness 生成 |
| 2026-07-28 | [Terminal-only 验收与 WAL 资格](2026-07-28-terminal-only-qualification.md) | Qualified；Phase 0、Gate A～D 通过 | [资格报告](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md) |
