# Insight Agent Platform 文档

这里是仓库文档的唯一入口。`current` 包含当前可用行为的使用、运维和开发文档；`specs`
保存尚在设计或实施的活动规范；`qualifications` 跟踪尚未完成的正式验收；`archive`
保存已完成或失效的设计、实施、评审、资格验收和迁移记录。

## 阅读路线

第一次接触项目时，建议按以下顺序阅读：

1. [根 README](../README.md)：启动 Quickstart 并运行第一个 Agent；
2. [架构概览](current/architecture.md)：理解 durable graph execution；
3. [DSL v1 指南](current/dsl.md)：编写 Agent；
4. [HTTP 与 SSE API](current/api.md)：创建、观察和控制 Run；
5. [MCP 使用、运行与安全合同](current/mcp.md)：连接或暴露 MCP；
6. [部署与运维](current/operations.md)：配置生产环境；
7. [开发指南](current/development.md)：修改和验证实现。

## 当前文档

| 文档 | 面向读者 | 内容 |
|---|---|---|
| [架构概览](current/architecture.md) | 架构师、开发者 | 运行模型、持久化边界与核心不变量 |
| [DSL v1 指南](current/dsl.md) | Agent 作者 | 作者语法、类型、表达式、控制流 |
| [HTTP 与 SSE API](current/api.md) | API 使用者 | 路由、幂等、响应流和人工任务 |
| [MCP 使用、运行与安全合同](current/mcp.md) | 集成者、平台开发者 | MCP profiles、传输、授权、交互与运维 |
| [部署与运维](current/operations.md) | 运维、平台开发者 | Schema 预置、数据库、Artifact、认证 |
| [开发指南](current/development.md) | 贡献者 | 代码导航、测试和 CI 门禁 |

## 活动设计规范

[`specs/`](specs/README.md) 描述已经形成实施和验收边界、但尚未完全成为当前可执行合同的目标设计。
规范中的新 API、配置、schema 和容量数字在实现及 conformance evidence 完成前均不是当前行为。

下一活动阶段为 [产品化收敛目标](specs/productization/00-goals.md)及其
[实施计划](specs/productization/implementation-plan.md)：停止横向扩展平台内核，集中交付本地开发入口、
稳定的 CLI/HTTP 开发者入口、最小运行控制台、十条黄金场景和仓库 clean cut。本阶段明确延期语言 SDK；
该计划当前为 Proposed，尚未改变
`docs/current`。

[Platform v2 clean-cut 规范集合](specs/platform-v2/00-overview.md)的 00～18 与
[四阶段实现计划](specs/platform-v2/implementation-plan.md)已按 CR-201 仓库范围关闭为 Verified。真实多节点
Kubernetes、runsc、production telemetry、容量/混沌/恢复/soak 与人工 GitOps promotion 未在本项目执行，
仍是部署方可选的 production release gate；它们不再是 spec00～18 或实现计划的剩余任务。Platform v2 尚未
对当前旧产品入口执行 clean cut；“v2”是架构代号，目标公共合同仍使用 `/v1`，不会提供 `/v2` 或兼容双栈。最近完成的
[MCP 完整支持规范](archive/specs/2026-07-30-complete-mcp-support.md)及其
[资格验收](archive/qualifications/2026-07-30-complete-mcp-qualification.md)已归档；当前合同见
[MCP 使用、运行与安全合同](current/mcp.md)。Provider Catalog 优化见
[归档规范](archive/specs/2026-07-30-provider-catalog-and-direct-model-selection.md)。

## 活动资格验收

[`qualifications/`](qualifications/README.md) 只跟踪已经定义门槛但尚未取得有效正式结果的验收工作。
它不改变当前运行合同，也不能用不完整结果扩大现有能力承诺。

| 验收 | 状态 | 剩余工作 |
|---|---|---|
| [Durable Runtime 24 小时 RC](qualifications/durable-runtime-24h-rc.md) | Pending / requires always-on runner | 以 10 arrival/s 完成不中断的 24 小时 Gate D 并保存正式证据 |
| [Platform v2 Production L4～L6](qualifications/platform-v2-production-l4-l6.md) | Not run / optional deployment release gate | 仅在部署方需要 production-ready 声明时执行真实 runsc、容量、恢复与 GitOps clean cut |

Terminal-only 已于 2026-07-28 完成 Phase 0、Gate A～D 与完成定义 1～12，状态为
`Implemented / capacity-qualified`。设计记录已移入
[归档规范](archive/specs/2026-07-27-terminal-only-runtime-and-conversations.md)，验收方法与判定已保存为
[归档资格记录](archive/qualifications/2026-07-28-terminal-only-qualification.md)，完整数字见
[资格报告](../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)。
独立 rollout 决策仍保持平台默认 `full`，Gate 通过不自动修改默认值。

## 权威关系

发生冲突时，按以下顺序判断：

1. 当前 schema、compiler、Plan verifier 与数据库约束；
2. PostgreSQL/SQLite、恢复和 real-process conformance tests；
3. checked-in Agent 与 positive fixtures；
4. `current` 中的文档；
5. `specs` 中尚未完成的目标设计；
6. `qualifications` 中尚未完成的验收门槛与证据状态；
7. `archive` 中的历史记录。

当前文档用于解释可执行合同。发现不一致时，应同时修正实现证据与文档，不能从历史档案恢复已经
删除的语义。活动规范用于指导未来实现，资格验收用于限制正式声明；两者都不能覆盖当前实现证据。

## 历史档案

[`archive/`](archive/README.md) 中的 specs、plans、reviews、qualifications 和 migrations
只用于追溯决策与已完成验收。
归档文件中的状态、示例、路径和待办事项均以当时上下文为准，不是当前使用说明。
