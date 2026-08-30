# 架构与 authority 边界

平台分为三个隔离平面：Gateway 负责认证、校验与协调；Orchestration Worker 通过 durable Run、Invocation、Job、
Task、Event、Receipt 与 Outbox 推进工作；Sandbox Execution Plane 执行不可信代码。Gateway 不执行用户代码，消息系统
只传递 wake hint，业务事实以 PostgreSQL aggregate 为准。

Agent、Skill、Capability、Context、MCP、Model、Policy 和 Sandbox 复用
Resource -> immutable ResourceVersion -> Deployment -> Binding 生命周期。Run admission 冻结 exact binding；active head
变化不会改变既有 Run。Capability 是唯一通用可调用合同，Native、Remote HTTP/gRPC、MCP Tool 与 Sandbox 只是实现后端。

代码仅在 restricted WASI 或每 Job gVisor container 中运行。普通 Sandbox Capability 会原子创建 durable Sandbox Job，
由独立 Executor 领取并以 fence 合并结果。Python、Node、Shell 或 WASM 不在 Gateway/Worker 内 spawn。

完整规范入口为 [`../specs/platform-v2/00-overview.md`](../specs/platform-v2/00-overview.md)。spec00～18 仍为
Accepted/In Progress，因为多节点 Kubernetes、runsc 和发布资格尚未执行。
