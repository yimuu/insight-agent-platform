# ADR-0007：OpenSandbox Kubernetes Provider 与两阶段 Sandbox 执行

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-09-04 |
| 取代 | ADR-0002（历史记录见 Git） |

## 问题

OpenSandbox 的 create API 没有可依赖的客户端幂等键；metadata list 只能发现对象，不能证明一次 create 是否已经生效。
如果 create 立即执行 Package，响应丢失或 Dispatcher 重启后的自动重试可能重复产生外部副作用。
进程内锁、`list -> create` 检查和 provider object 都不能替代 PostgreSQL 中的 durable authority 与 fence。

## 决策

唯一物理路径是 Sandbox Dispatcher 经内部 OpenSandbox Server 使用 Kubernetes provider 创建 BatchSandbox，由 Controller 管理 Pod，
并由 containerd/runc 中的固定 runner 执行 Package。Docker provider、自建 WASI/gVisor 及其他 backend 不允许作为 fallback。

- shared Job 是 attempt、lease、fence、control、terminal 和 bounded physical evidence 的唯一业务 authority；
- Dispatcher 是唯一可以 claim Sandbox Job、调用 provider、选择候选、授权激活、提交结果和驱动清理的 Platform role；
- OpenSandbox、Controller、Pod 和 runner 只拥有物理生命周期与证据，不得修改 Job、Run、Invocation 或其他业务状态；
- provider observation 不能直接决定业务终态；terminal commit 必须在 PostgreSQL 中重新验证 current Job fence；
- 一个 selected sandbox 只服务一个 Job physical attempt，不跨 Job 复用，也不开放交互式或持久 session。

## 两阶段 provisioning 与激活

### Inert candidate

Dispatcher 从 immutable execution request 与 physical attempt 导出稳定 provisioning identity；接管时会变化的信息不属于该 identity。
候选 metadata 只携带恢复和校验所需的 bounded identity。

每次外部 create 前，Dispatcher 必须在 current Job fence 下原子授权下一个 durable ordinal；次数、静默窗口和总时长等
冻结上限不会因重启而恢复。repository 返回 `Applied` 或 `Replayed`：只有 `Applied` 可在事务外调用一次 provider；
`Replayed` 不得调用。授权后、调用前崩溃会消耗该 ordinal，只能在约束允许时授权下一 ordinal。

create 只启动 immutable runner；runner 进入 inert `Armed` 状态，不启动 Package。Dispatcher 可按稳定 identity 发现有限个
inert candidates，并在 Job fence 下选择唯一候选；未选候选只能清理，永不激活。

### One-shot activation

Dispatcher 保有激活签名私钥，runner 只得到验证材料。激活帧绑定 selected candidate、runner boot 和冻结的 execution request；runner
验证签名并以 durable one-shot latch 接受它。同帧重放返回既有状态，冲突帧拒绝，一个 boot 最多启动一次 Package。

一旦 Job 记录 activation authorized 或 potentially started，响应是否丢失都不得创建、选择或激活 replacement；恢复只能查询
同一候选的 state/result，并由 current Job fence 裁决提交。

### Boot rollover

授权激活后若观察到不同 runner boot，旧激活帧不能发送给新 boot。Dispatcher 必须先在 current Job fence 下持久化 boot-rollover
摘要，将结果裁决为 `UnknownOutcome`，再执行 cleanup/absence reconcile，不得启动 replacement。相同 observation 可重放，矛盾则 fail closed。

## Control、cleanup 与 orphan

Cancel 和 timeout 的 durable control fact 位于同一 shared Job。owner 事务原子推进 Job/Invocation 到 `Cancelling`；Dispatcher 的
reserved control scan 以数据库时间和既定锁序提交终态与 cleanup intent，但不在事务内执行 provider I/O。迟到结果由 terminal/version fence 拒绝。

Terminal commit 清除业务 lease，并在同一 Job 中生成受版本与 generation 保护的 cleanup intent。Terminate、delete 和 absence proof
只在 terminal 后执行；cleanup fence 不能改变业务结果，也不构成第二 lease、aggregate、queue 或 table。provider 不可达只保留 pending 与告警。

Orphan metadata 必须区分业务候选与 readiness probe，并只包含定位 owner 所需的非敏感标识。业务 orphan 先 point-read 唯一
shared Job，再验证 current token、attempt 与冻结绑定；OpenSandbox 不获得 repository 权限。orphan sweep 跳过 readiness；metadata
缺失、歧义、损坏或 repository 不可用时保留并告警，不得通过全表 payload 扫描、猜测 owner 或默认删除来恢复。

## 安全与权限

OpenSandbox Server 仅通过 authenticated internal endpoint 暴露给 Dispatcher。它不得获得激活签名私钥、Platform PostgreSQL、
NATS、Artifact store、public API 或 workload credential。固定 runner 与 Package 使用隔离身份；Package 不继承 runner 的管理权限，
不能读取 runner-owned state、控制 runner 或逃离其 execution process group。

Sandbox 网络 profile 只有显式 `Direct` 与 `Disabled`：两者默认拒绝 ingress，`Disabled` 拒绝 egress，`Direct` 仅允许受限外网并
拒绝 Platform、节点、服务和云 metadata 网段。Pod 禁止 privileged、host namespace、hostPath、device、runtime socket、
service-account token 与 Platform credential，并冻结 non-root、read-only filesystem、seccomp 和资源限制。
部署无法证明 CNI deny 与所需权限边界时，readiness 必须 fail closed。

## 保证与明确不保证

平台保证 command Receipt 的逻辑去重、selected physical attempt 上的 Package 最多激活一次，以及 Job terminal 的 fenced first-winner。
平台不保证历史上只创建一个 inert candidate，也不为 Package 对网络、数据库、消息或第三方 API 的副作用提供 exactly-once；
这些副作用的事务与幂等属于 Package 和目标服务。

本决策不声明 `Direct` 为 production-grade egress control，不声明单节点 runc 为强多租户隔离，也不自动证明 HA、capacity、
restore 或 production readiness；未完成的资格门禁必须保持 `Not run`。

## 否决方案

- fork OpenSandbox 增加 create 幂等扩展：两阶段协议已隔离 workload 副作用，不承担长期上游分叉；
- create 时直接执行 Package，再靠 metadata list 恢复：无法关闭响应丢失后的重复执行窗口；
- Dispatcher 直接创建 Kubernetes CR：绕过唯一 provider lifecycle 边界；
- 保留 Docker、WASI/gVisor 或其他 fallback：形成第二执行路径并带来不可预测降级；
- Pool 或跨 Job 复用：把 tenant residue、boot identity、quota、cleanup 与长期 session 耦合；
- 让 OpenSandbox claim Job、修改业务状态或面向 public client：形成第二业务 authority。

## Authority 与证据

当前机器合同与拥有类型优先于本文：[`contracts/platform-v1`](../../contracts/platform-v1/README.md)、
[`sandbox.rs`](../../crates/platform-contracts/src/sandbox.rs) 与 [`sandbox_policy.rs`](../../crates/platform-contracts/src/sandbox_policy.rs)。
持久化裁决见 [`sandbox_repository.rs`](../../crates/platform-postgres/src/sandbox_repository.rs)，runner 协议见
[`platform-sandbox-runner`](../../crates/platform-sandbox-runner/src/lib.rs)，部署边界见 [`Sandbox Helm chart`](../../deploy/helm/insight-platform-sandbox/)；
当前行为与资格状态见 [`architecture.md`](../current/architecture.md)、[`mcp.md`](../current/mcp.md) 和 [`operations.md`](../current/operations.md)。
