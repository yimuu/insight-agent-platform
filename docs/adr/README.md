# 架构决策导航

每份 ADR 的状态与适用范围以其正文为准。Proposed 不替代已接受决定，也不表示目标已经实现。

## Accepted

- [ADR-0001：PostgreSQL baseline 与 authority](0001-platform-v2-postgres-baseline.md)
- [ADR-0004：产品入口共享 authority](0004-product-surface-boundaries.md)
- [ADR-0007：OpenSandbox Kubernetes 与两阶段执行](0007-opensandbox-execution-provider.md)
- [ADR-0008：runner capability 与镜像闭包](0008-sandbox-runner-capability-boundary.md)
- [ADR-0009：持久执行底座、Agent 领域与执行版本](0009-durable-kernel-and-agent-domain-boundaries.md)
- [ADR-0010：统一安装与多来源模型配置](0010-unified-installation-and-model-configuration.md)
- [ADR-0011：Console TypeScript 构建与结构化编辑](0011-console-typescript-and-authoring-experience.md)

被替代和废弃的决策通过 Git 历史查阅，不恢复旧实现或兼容路径。接受提案前必须将架构与对应机器合同、当前 schema 和证据要求共同审查。
