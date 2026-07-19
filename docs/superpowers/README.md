# 设计文档权威关系

当前唯一的整体架构与执行语义规范是 [DSL v3 Durable Graph Execution](./specs/2026-07-18-dsl-v3-durable-graph-execution-design.md)。它定义作者 DSL、Canonical Typed Plan、持久化 Run/Scope/Activation/Attempt、控制 token、Worker lease、恢复、artifact 和发布门禁。

仓库根目录的 [README](../../README.md)、checked-in Agent、v3 positive fixtures、schema、实现与测试是该规范的可执行一致性证据。它们不能各自发明与规范冲突的新语义；发现冲突时，应先修正规范或证据并明确说明原因。

本目录中更早日期的 specs 与 plans 仅作为历史决策记录保留。除非当前 v3 规范显式引用某项跨领域合同，否则旧文档中的作者语法、执行计划、控制节点或恢复模型都不是当前生产合同，也不能作为重新引入已删除实现的依据。

判断优先级：

1. 当前 v3 规范中的显式合同；
2. v3 schema、verifier 与数据库约束；
3. PostgreSQL/SQLite、恢复和 real-process conformance tests；
4. checked-in Agent 与示例；
5. 历史 specs/plans（只提供背景）。

切换残留由 `scripts/check-v3-cutover-residuals.sh` 在 CI 中阻止。历史 spec 和明确命名的 negative fixture 可以记录被拒绝的旧输入，但 production source、checked-in Agent、positive fixture 和当前入口文档不得重新依赖已删除内核。
