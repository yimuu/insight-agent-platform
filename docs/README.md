# Insight Agent Platform 文档

根据使用目的选择入口。当前行为、目标设计和验证状态分别维护，避免把提案或合同要求误认为已经交付。

## 使用平台

- [根 README](../README.md)：安装 CLI 并完成首次 Run。
- [当前产品文档](current/README.md)：CLI、HTTP、Console、MCP 与运行机制。
- [当前架构](current/architecture.md)：authority 与隔离边界。
- [部署与运维](current/operations.md)、[可观测性与处置](current/observability.md)。

## 理解与修改设计

- [架构决策](adr/README.md)：accepted 决定与 proposed 变更分别列出。
- [活跃 spec](specs/README.md)：尚未完成实施的目标与交叉评审。
- [Agent 平台架构重整](specs/architecture-restructuring/README.md)：本轮完整目标、目录设计及合同影响。
- [仓库工程规则](../AGENTS.md)：贡献与架构变更的共同约束。

## 核对合同与证据

- [`insight.platform/v1` 机器合同](../contracts/platform-v1/README.md)。
- [Agent compiler conformance corpus](../contracts/product-experience/agent-compiler/v1/corpus.json)。
- [开发阶段验证状态](qualifications/README.md)：实际执行范围、证据强度和未运行的生产门禁。

公开及跨进程边界由 OpenAPI、JSON Schema、protobuf 和 owning Rust type 定义，持久化结构由 migration 定义。
ADR 保存架构决定；`current` 解释已实现行为，不维护平行字段表、状态机或通过报告。
项目的 `/v1` 产品入口与具体内部架构代号分别管理；目标 spec 不自动改变公开协议或生产资格。

已完成、被替代和废弃的临时提案从工作树删除，通过 Git 查阅。历史不能作为恢复旧 DSL、单进程 runtime、fallback 或双写的入口。
