# 活动设计规范

本目录保存已经形成明确实施与验收边界、但尚未完全成为当前可执行合同的设计规范。

- `specs` 描述目标状态、约束、实施阶段和验收门槛；
- `current` 只描述已经由 schema、实现和测试共同证明的当前行为；
- 规范完成实施并同步当前文档后，移入 `archive/specs` 留作决策记录。

发生冲突时，当前 schema、实现、conformance tests 和 [`current`](../current/README.md) 优先。
活动规范不能用于推断尚未交付的 API、配置项或容量承诺。

## 当前规范

当前没有活动规范。

最近完成的规范：

- [Agent 与 Provider 管理 Control Plane v1](../archive/specs/2026-08-01-agent-and-provider-management-control-plane.md)：
  已交付 durable Agent Draft/Definition/Deployment/Activate/Debug 与 Provider
  Draft/Discovery/Revision/Activate/Suspension 生命周期、双数据库状态机、exact binding、Operator API、
  clean-cut migration 和多 runtime Provider 投影；
- [MCP 管理 API v1 与显式导入规范](../archive/specs/2026-07-31-mcp-management-api-v1.md)：
  已以 durable 管理控制面替代 YAML Server 权威，交付 Draft、异步 Discovery、显式
  Tool/Resource/Prompt 导入、不可变 Revision、CAS 生命周期、Operator 权限和 Agent 精确 binding；
  不包含运行时 Tool 通配符或自动授权；
- [MCP 2026-07-28 完整支持规范](../archive/specs/2026-07-30-complete-mcp-support.md)：
  已交付 modern Host/Client、Server、Tasks、独立 legacy profile、双标准传输、OAuth、
  durable Elicitation、Resources/Prompts/Completion/Subscriptions 与包含 interaction 事件的
  `run-stream/v1`；
- [Provider Catalog 与直接模型选择优化](../archive/specs/2026-07-30-provider-catalog-and-direct-model-selection.md)：
  已删除必需的模型文件与别名层，交付结构化 selector、版本化 Catalog、可选 Provider extension
  和统一的结构化输出本地校验；
- [Run Stream v1 统一事件模型优化](../archive/specs/2026-07-29-run-stream-v1-unified-event-model.md)：
  已将 `/runs/stream` clean-cut 为闭合的 `run.*` 事件，并以单一 canonical durable Run snapshot
  统一 terminal authority；
- [Response Stream v1 工具活动可视化优化](../archive/specs/2026-07-29-response-stream-v1-tool-activity-visibility-optimization.md)：
  原地增强 `response-stream/v1`，增加安全工具进度、耗时、结果展示和 terminal 校准合同；
- [Durable Runtime 50 活跃 Run 并发优化](../archive/specs/2026-07-26-durable-runtime-50-active-runs-optimization.md)；
- [Terminal-only Runtime 与 Conversation](../archive/specs/2026-07-27-terminal-only-runtime-and-conversations.md)。

尚未完成的 24 小时 release-candidate soak 属于资格验收，不是活动设计规范；见
[Durable Runtime 24 小时 RC 资格验收](../qualifications/durable-runtime-24h-rc.md)。
