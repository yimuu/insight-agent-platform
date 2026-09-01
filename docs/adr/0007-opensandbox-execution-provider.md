# ADR-0007：OpenSandbox Kubernetes Provider 与两阶段 Sandbox 执行

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 revision 2 |
| 日期 | 2026-09-02 |
| 取代 | [ADR-0002](0002-gvisor-kubernetes-launcher.md) |
| 影响规范 | 00、01～04、07、09、10、14、15、17、18、cross-review、implementation-plan、product-experience 00/06 |

## 背景

首版自建 restricted WASI 与 per-Job gVisor 链路包含 Wasmtime Executor、Kubernetes Pod Launcher、
`RuntimeClass=runsc`、admission/RBAC、process attestor、guest bootstrap 与多套资格矩阵。CR-216 要以 clean-cut
方式替换这些物理执行实现，同时保留 shared Job、Invocation、Run、Artifact 与 terminal transaction 的业务authority。

上一版决策选择 OpenSandbox Docker provider，并要求上游新增持久化 `Idempotency-Key` 扩展。对 OpenSandbox 0.2.x
文档、Lifecycle API、Kubernetes deployment、BatchSandbox controller、官方镜像与 provider 实现完成部署级审计后，确认：

- 公开 create API 没有可依赖的 client create idempotency key；metadata list 是发现接口，不是原子唯一性原语；
- Kubernetes provider 已把 lifecycle metadata 落到 BatchSandbox/Pod label，Kubernetes API 可作为可恢复 physical store；
- BatchSandbox controller 已提供 TTL、Pod reconciliation、delete 与 absence 基础，不需要平台维护另一套 launcher/guest；
- Docker provider 需要 Docker socket，且默认把 sandbox 服务随机发布到 `0.0.0.0`，不适合作为平台目标拓扑；
- create 立即启动有副作用的 workload 时，response-loss 后若既不能证明 create 未发生、又没有 provider 原子幂等，任何自动重试
  都可能重复执行。这个问题不能靠 Dispatcher 内存锁或 `list(metadata) -> create` 正确关闭。

因此，本修订不修改 OpenSandbox 源码，而是把“physical candidate 创建”与“Package workload 激活”拆成两个阶段。

审计输入固定为官方 [Kubernetes overview](https://open-sandbox.ai/kubernetes/)、
[deployment guide](https://open-sandbox.ai/kubernetes/deployment)、
[Lifecycle API](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/sandbox-lifecycle.yml)、
[server configuration](https://github.com/opensandbox-group/OpenSandbox/blob/main/server/configuration.md) 与
[official image repository](https://hub.docker.com/u/opensandbox)。外部 `main`、mutable tag 和网页当前内容都不是运行时选择；
实施 BOM 必须解析为本 ADR 第“部署与恢复”节的 exact source commit、schema 与 OCI manifest digest。

## 决策

首版唯一物理链是：

```text
Capability owner transaction
        |
        | shared Sandbox Job
        v
Sandbox Dispatcher ---- internal ClusterIP/auth ----> OpenSandbox Server
  PostgreSQL Job authority                            Kubernetes provider
  claim/fence/evidence/cleanup                              |
                                                           v
                                              BatchSandbox Controller
                                                           |
                                                           v
                                              Pod + containerd/runc
                                              immutable Armed runner
```

不使用 OpenSandbox Docker provider、agent-sandbox、Pool、task-executor、snapshot、image-committer、node-agent、
RuntimeClass/runsc 或任何备用 backend。Kubernetes/containerd-runc 是显式的 developer-profile runtime，不是隔离级别
fallback。OpenSandbox Server 和 Controller 都是 physical provider；它们不得成为 Platform 业务状态authority。

边界固定如下：

1. shared Job 是唯一业务 work authority，保存 attempt、lease、fence、retry、cancel、terminal 与 bounded physical evidence；
2. Sandbox Dispatcher 是唯一可 claim Sandbox Job、调用 OpenSandbox、选择 physical candidate、激活 runner、提交结果并清理的
   Platform role；
3. OpenSandbox 只拥有 BatchSandbox/Pod/runner 的 physical lifecycle，不得获得 Platform PostgreSQL、NATS、Run、Invocation、
   Receipt、Event、Outbox、Artifact store 或 public API credential；
4. OpenSandbox lifecycle、Pod phase、runner state 与 result 都只是证据。terminal commit 必须重新读取并 CAS 验证 current Job owner、
   lease generation/token、worker process generation、physical attempt 与 request digest；
5. 每个 selected sandbox 只服务一个 Job physical attempt。首版不跨 Job 复用 sandbox，不开放 interactive exec、PTY、code context、
   mutable filesystem API、snapshot、persistent session 或 public endpoint。
6. shared Job JSON 只保存 bounded execution plan、RunValue identity/digest、physical evidence 与 terminal summary，不复制 input/result
   body。Dispatcher 在 claim/recovery 时从 exact immutable input RunValue 重建 execution request；terminal first-winner 在同一事务验证
   result frame并写 output RunValue、Job、Invocation、quota、Event 与 Outbox。
7. CR-216 Inline input/output 的有效 hard ceiling 固定为既有 RunValue inline ceiling `1_048_576` bytes，并同时受冻结 Profile
   ceiling约束；大于任一 ceiling 都 fail closed，Artifact port 保持 inactive。Job/Event/Outbox/Receipt 不保存 workload 正文。
8. terminal commit 清除业务 Job lease并在同一 Job physical evidence 内产生 cleanup intent。之后的 delete/absence 写入使用独立
   `SandboxCleanupFenceV1` 的 expected Job version、cleanup generation、process generation 与 database-time expiry CAS；它不允许改变
   Job terminal outcome、Invocation、RunValue、quota、Event 或 Outbox，也不是第二业务 lease/aggregate/table。

## 两阶段 provisioning 与激活

### 1. 稳定 provisioning identity

`SandboxProvisioningTokenV1` 由 `tenant_id + job_id + physical_attempt + execution_request_digest` 的 domain-separated
canonical digest 产生。它不含 lease generation、worker generation、trace 或超时等会在接管时变化的字段。

Dispatcher 用该 token 的 digest 作为 OpenSandbox metadata。Kubernetes provider 把 metadata 转换成 operator-owned label，
因此 Dispatcher 在 create response 丢失或进程重启后，可以直接通过 Kubernetes-backed list 找回候选。

### 2. 惰性候选

OpenSandbox create 只启动 immutable fixed runner。runner 进入 `Armed`，校验固定 closure/digest frame 并等待一次性激活，绝不在
create/boot 时启动 Package。并发 create 或 response-loss 可能产生有限个、没有 workload 副作用的 inert candidates；这不是
“一个 key 历史上只能出现一个 physical object”的保证。

Dispatcher 对同 token 的候选执行有界发现与静默窗口，在 PostgreSQL current Job fence 下 CAS 选择唯一 candidate，并把
OpenSandbox ID、runner boot identity 与 request digest 持久化为 physical evidence。未选候选只允许 cleanup，永不激活。
只有在激活前、并证明现有候选均未启动 Package 时，才允许按 Profile 的 candidate count/time limit 再次 create。

### 3. 一次性激活

fixed runner 暴露 closed、private、bounded 协议：

```text
GET  /v1/state
POST /v1/activate
GET  /v1/result
```

provisioning intent 事务生成并持久化一个 opaque 256-bit activation token；create frame 只向 runner 提供其 digest，不泄露 token。
Dispatcher 先读取 runner boot identity，再在 PostgreSQL current Job fence 下把 selected candidate 记录为
`ActivationAuthorized/PotentiallyStarted`，然后发送包含 activation token、request digest 与 input digest 的 activate frame。
runner 必须先以 create-exclusive + fsync 的 durable latch 接受 token，再启动 Package：同 token 重放返回相同状态；不同 token
返回 conflict；一个 boot identity 最多启动一次 Package。

一旦 Job 持久化 `ActivationAuthorized/PotentiallyStarted`，无论 activate response 是否丢失，都不得创建新 key、candidate、sandbox
或自动重跑 Package。恢复只能查询同一 runner 的 state/result；runner boot identity 变化、Pod 被 controller 重建或无法证明结果时，
进入 `UnknownOutcome` 并执行 cleanup/absence reconcile。

## 幂等边界

平台明确保证三个不同边界：

1. Platform command Receipt：同一 command 只创建一个逻辑 Invocation；
2. runner activation：selected physical attempt 的 Package 最多激活一次；
3. Job terminal commit：current lease fence first-winner，迟到结果不能覆盖新 generation 或既有 terminal。

平台不保证历史上只创建一个 inert candidate，也不为 workload 对网络、数据库、消息或第三方 API 的副作用实现幂等或
exactly-once。目标服务的 transaction、idempotency key 与业务语义属于 Sandbox Package 和目标服务责任。

## 网络与安全基线

单节点 developer Profile 默认 `Direct`，并支持显式 `Disabled`。二者都使用 operator-owned、静态、closed CNI NetworkPolicy：

- 默认拒绝 sandbox ingress，只允许 Dispatcher 到 fixed runner protocol，以及 OpenSandbox Server/Controller 所需的 lifecycle 流量；
  不创建 Ingress/LoadBalancer/NodePort/host port；
- `Disabled` 拒绝全部 egress；
- `Direct` 允许 DNS 与外部网络，但拒绝 Platform namespace、Pod/Service/Node CIDR、cloud metadata 与内部管理面；
- sandbox Pod 禁止 privileged、host PID、host network、hostPath、device、runtime socket、service-account token 与 Platform credential；
- runner non-root、read-only root filesystem、capability drop、seccomp、resource/pids/ephemeral-storage limit 均由 fixed template 冻结。

NetworkPolicy 语义依赖 CNI，部署必须固定并完成对应 L3 资格；无法证明所需 deny 时 readiness fail closed。`Direct` 只是开发
Profile 的受限直接出网，不得宣称 production-grade egress control 或强多租户隔离。

## 部署与恢复

- 固定 OpenSandbox Server `v0.2.3`、BatchSandbox Controller `v0.2.0`、execd `v1.0.22` 与 Platform runner OCI digest；
- 官方独立 Server chart 尚未按所需版本发布，因此从审核 commit
  `c39b814f36ded4c61d5ac6f9332ee4dfbab86c00` vendor/source-pin 最小 chart，并冻结 CRD、模板和镜像 manifest digest；
- developer Profile 使用 1 个 Server replica、1 个启用 leader election 的 Controller；Server `informer_enabled=false`，让恢复
  list 直接读取 Kubernetes API；
- `/health` 只证明 Server 进程存活。Dispatcher readiness 必须做 authenticated create/list/delete capability probe 或等价的
  provider contract probe，并核验 CRD、controller、runner、network policy 与 exact digest closure；
- physical persistence 位于 Kubernetes API/BatchSandbox CR，不引入 OpenSandbox SQLite 或 Platform 业务表作为 provider store；
- TTL 是最后保护；Dispatcher 仍负责 terminal/cancel/timeout delete、未选 candidate 回收、orphan decision 与 absence proof；terminal
  后由 bounded cleanup claim/fence 在 shared Job row 上接管，不复用已清除的业务 lease。

## 否决方案

- 修改或 fork OpenSandbox 增加 provisioning extension：两阶段 runner 已关闭 workload 重复执行，不需要承担上游分叉；
- create 时直接执行 Package，再靠 metadata list 恢复：无法关闭 response-loss 后的重复副作用窗口；
- Dispatcher 直接创建 deterministic-name BatchSandbox CR：绕过 OpenSandbox lifecycle API，违反唯一 provider 边界；
- 保留 Docker provider 或 WASI/gVisor fallback：引入第二条 runtime、socket/public-port 风险与不可预测降级；
- 使用 Pool 或跨 Job 复用 sandbox：会把 tenant residue、cleanup、boot identity、quota 与 lease fence 耦合为长期 session；
- 使用 agent-sandbox：其交互式 agent/session 定位大于 CR-216 所需面；
- 让 OpenSandbox claim Job、修改 Run/Invocation 或直接对 public client 开放：形成第二业务authority并绕过 admission/audit。

## 结果与资格

目标实现删除 active composition 中的 Wasmtime、gVisor launcher/guest、attestor、RuntimeClass/runsc、相关 RBAC/admission/
manifests/preflight、host-process execution、backend selector 与 fallback。实现完成前当前行为仍以 `docs/current` 为准。

最低资格覆盖：候选并发与 response-loss、provider/controller 重启、Dispatcher kill/reclaim、PostgreSQL candidate CAS、runner activation
重放/conflict、runner-start uncertainty、lease rollover、stale result、cancel/timeout、TTL/delete/absence、orphan cleanup、Direct/Disabled
网络、固定 result frame、wrong credential/audience 与 OpenSandbox 零 Platform 权限。L4～L6 未运行时必须标记 `Not run`；CR-216
不构成 production-ready、strong isolation 或 HA 声明。
