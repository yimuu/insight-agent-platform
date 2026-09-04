# 活动设计规范

本目录只保留仍有效的设计合同。

- `specs` 描述目标状态、约束、实施阶段和验收门槛；
- `current` 只描述已经由 schema、实现和测试共同证明的当前行为；
- 已完成、被替代或废弃的过程文档从工作树删除，需要时从 Git 历史查看。

发生冲突时，当前 schema、实现、conformance tests 和 [`current`](../current/README.md) 优先。
活动规范不能用于推断尚未交付的 API、配置项或容量承诺。

## 当前规范

- [Platform v2 合同集合](platform-v2/00-overview.md)：Agent、Skill、Capability、Context、MCP、Subagent、
  Model、Sandbox、Artifact、API、部署与验证边界；
- [Agent 产品体验](product-experience/00-overview.md)：`agent.yaml`、CLI、Console、渐进披露、发行物和开发 profile。

实现过程、cross-review 流水账、阶段报告和旧合同不再作为活动文档保留。
