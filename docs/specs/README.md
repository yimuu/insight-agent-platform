# 活动设计规范

本目录保存已经形成明确实施与验收边界、但尚未完全成为当前可执行合同的设计规范。

- `specs` 描述目标状态、约束、实施阶段和验收门槛；
- `current` 只描述已经由 schema、实现和测试共同证明的当前行为；
- 规范完成实施并同步当前文档后，移入 `archive/specs` 留作决策记录。

发生冲突时，当前 schema、实现、conformance tests 和 [`current`](../current/README.md) 优先。
活动规范不能用于推断尚未交付的 API、配置项或容量承诺。

## 当前规范

- [产品化收敛阶段目标](productization/00-goals.md)及其
  [实施计划](productization/implementation-plan.md)：下一阶段停止横向扩展平台内核，交付一个命令的本地
  平台、薄 Python SDK、最小运行控制台、十条黄金场景和最终仓库 clean cut。当前状态为 Proposed，尚未改变
  `docs/current` 描述的产品行为；
- [Platform v2 clean-cut 规范集合](platform-v2/00-overview.md)：重新定义 Agent、Skill、Capability、
  Context、MCP、Subagent、Model、Sandbox 与 Artifact 的目标边界。00～18 与
  [四阶段实现计划](platform-v2/implementation-plan.md)已按 CR-201 仓库范围关闭为 Verified；真实多节点
  Kubernetes、runsc、production telemetry、容量/混沌/恢复/soak 与人工 GitOps promotion 仍是可选部署
  资格，不属于未完成的 spec 实现任务。“v2”是架构代号，目标公共合同仍为 `insight.platform/v1` 和
  `/v1`，不会提供 `/v2` 或兼容双栈；这些目标尚未完成对当前旧产品入口的 clean cut。

最近完成的规范：

- [Agent 调用、Conversation 与 S3 文件合同规范](../archive/specs/2026-08-05-agent-invocation-conversation-and-s3-files.md)：
  已交付 `query/messages/files/inputs` 调用信封、无会话 Run、平台托管 Conversation、File Service、
  S3-only Artifact、图片附件、引用生命周期，并通过
  [真实 RustFS/S3 资格验收](../archive/qualifications/2026-08-06-agent-invocation-rustfs-s3-qualification.md)；
- [Run Stream 可插拔实时消息总线与 NATS Core 优化](../archive/specs/2026-08-02-pluggable-run-stream-bus-and-nats-core.md)：
  已将 PostgreSQL 限定为 durable authority，clean-cut 删除 Run Stream `postgres_notify` backend，
  为当前单 Runtime 交付 `in_memory`，为跨 Runtime fan-out 交付 Core NATS，并通过零 per-SSE
  PostgreSQL listener、单 NATS data connection、严格拓扑、安全、30 分钟混合负载与 2 小时 soak；
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
