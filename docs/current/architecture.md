# 架构与 authority 边界

平台分为三个隔离平面：Gateway 负责认证、校验与协调；Orchestration Worker 通过 durable Run、Invocation、Job、
Task、Event、Receipt 与 Outbox 推进工作；Sandbox Execution Plane 执行不可信代码。Gateway 不执行用户代码，消息系统
只传递 wake hint，业务事实以 PostgreSQL aggregate 为准。

Agent、Skill、Capability、Context、MCP、Model、Policy 和 Sandbox 复用
Resource -> immutable ResourceVersion -> Deployment -> Binding 生命周期。Run admission 冻结 exact binding；active head
变化不会改变既有 Run。Capability 是唯一通用可调用合同，Native、Remote HTTP/gRPC、MCP Tool 与 Sandbox 只是实现后端。

不可信代码只有一条物理路径：Sandbox Dispatcher -> internal OpenSandbox Server -> Kubernetes API -> BatchSandbox Controller ->
containerd/runc。普通 Sandbox Capability 原子创建 shared durable Job；Dispatcher领取并续租同一Job，持久化physical evidence，
通过immutable fixed Armed runner最多启动一次Package，并在提交terminal结果前重新验证current Job lease fence。OpenSandbox、
Controller和runner不修改Job、Run、Invocation或其他Platform业务状态。Python、Node和Shell不在Gateway或普通Worker内spawn。

完整规范入口为 [`../specs/platform-v2/00-overview.md`](../specs/platform-v2/00-overview.md)。CR-216仓库L1～L3已通过；
真实production topology、capacity/soak、restore与promotion的L4～L6仍为Not run，因此不作production-ready声明。
