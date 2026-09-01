# ADR-0007：OpenSandbox 作为唯一 Sandbox 物理执行 Provider

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 |
| 日期 | 2026-09-01 |
| 取代 | [ADR-0002](0002-gvisor-kubernetes-launcher.md) |
| 影响规范 | 00、01～04、07、09、10、14、15、17、18、cross-review、implementation-plan、product-experience 00/06 |

## 背景

首版自建restricted WASI与per-Job gVisor链路包含Wasmtime Executor、Kubernetes Pod Launcher、
`RuntimeClass=runsc`、admission/RBAC、process attestor、guest bootstrap与多套资格矩阵。它提供了较强隔离，
但显著扩大了首条可用执行链的实现与运维面。

CR-216的目标是先跑通`Capability Invocation -> durable Job -> code -> result -> cleanup`，允许首版使用
Docker/runc和直接出网。OpenSandbox已经提供sandbox lifecycle、Docker provider、TTL与
metadata查询，因此可以替代平台自建的物理执行实现。它不能替代平台的业务authority、tenant授权、Invocation、
Job lease、terminal commit或Artifact authority。

本决策基于以下上游公开合同：

- [OpenSandbox Lifecycle API 0.1.0](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/sandbox-lifecycle.yml)；
- [OpenSandbox Execd API 1.0.0](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/execd-api.yaml)；
- [OpenSandbox server configuration](https://github.com/opensandbox-group/OpenSandbox/blob/main/server/configuration.md)。

外部项目的`main`分支不是运行时版本选择。发行物必须固定经审核的OpenSandbox commit、镜像digest、API schema digest和
本ADR定义的扩展digest。

## 决策

首版Sandbox物理执行只有一个provider：内部OpenSandbox Server及其Docker/runc runtime。restricted WASI、
自建gVisor Launcher/guest、host process和任何备用backend都不进入目标composition。Docker/runc是显式首版选择，
不是安全runtime失败后的fallback。

```text
Capability owner transaction
        |
        | shared Sandbox Job
        v
Sandbox Dispatcher  ---- authenticated internal API ----> OpenSandbox Server
  PostgreSQL Job authority                              Docker/runc + fixed runner
  fence/commit/cleanup                                  physical lifecycle only
```

边界固定如下：

1. shared Job仍是唯一业务work authority，保存attempt、lease、fence、retry、cancel和safe terminal result；
2. Sandbox Dispatcher是唯一可调用OpenSandbox的Platform role，也是唯一可把physical evidence提交给Job owner事务的role；
3. OpenSandbox只拥有sandbox/container/runner/file的physical lifecycle，不获得Platform PostgreSQL、Run、Invocation、
   Receipt、Event、Outbox或Artifact store credential；
4. OpenSandbox status、runner exit和日志只是证据。Dispatcher提交结果时必须重新读取并CAS验证current Job owner、
   lease generation/token、worker process generation、physical attempt和request digest；
5. OpenSandbox API不对public client、Agent、Skill、Model、MCP或普通Capability Worker开放；
6. 每个physical attempt创建一个ephemeral sandbox，执行一个固定published entrypoint，terminal/cancel/timeout后删除；
   首版不使用pause、resume、snapshot、persistent session或public endpoint。

## Provisioning 幂等扩展

当前公开Lifecycle create合同没有可依赖的client idempotency key。平台不得用`list(metadata) -> create`模拟原子唯一性，
因为并发和response-loss窗口仍可产生重复container。目标OpenSandbox发行物必须在Server内部实现并持久化以下扩展：

```text
POST /v1/sandboxes
Idempotency-Key: <SandboxProvisioningKeyV1>
X-Insight-Request-Digest: sha256:<canonical-create-request-digest>
```

- key由`tenant_id + job_id + physical_attempt + provisioning_lease_generation + execution_request_digest`的
  domain-separated canonical digest产生；
- 同key、同request digest的并发或重放只创建一个physical sandbox，并返回同一`sandbox_id`；
- 同key、不同digest返回`409 provisioning_idempotency_conflict`且零创建；
- key/result必须在OpenSandbox重启后仍可恢复，TTL不得早于对应sandbox absence proof与Platform reconciliation窗口；
- lifecycle metadata同时保存key digest、Job digest、provisioning lease generation、physical attempt、execution request digest和
  create request digest，供有界orphan reconcile；
- 若所固定的上游版本不支持该合同，必须在固定OpenSandbox fork/plugin内补齐并通过conformance；不得在Dispatcher旁边再建
  第二套sandbox runtime或以盲重试绕过。

Platform Job保存`provisioning_key_digest + sandbox_id + physical_attempt`作为bounded external evidence，不复制OpenSandbox
lifecycle state。OpenSandbox自己的幂等记录与SQLite/其他store只属于physical provider state，不成为业务数据库或第二Job authority。

## 执行与不确定结果

OpenSandbox create以array entrypoint直接启动immutable image内的fixed runner；input是create request中的bounded canonical frame。
runner只执行published package argv一次，随后把typed result frame原子写入image合同冻结的固定路径并保持存活直到delete/TTL。
Dispatcher只可通过OpenSandbox内部execd read-only file API有界读取该固定路径；平台不调用execd shell command、code context、PTY或
filesystem mutation API，也不允许runtime package manager、mutable image tag或调用方shell字符串。execd command API及其幂等语义不进入
首版Platform合同。

- create response loss只允许用同一provisioning key重放并取得同一sandbox，不发第二条执行命令；
- recovery先按已持久化sandbox ID或key metadata observe；lease接管不重算旧provisioning key，runner可能已开始时不得创建
  新key/新sandbox重跑；
- 可证明result时以current Job commit fence提交既有physical evidence；不可证明时进入`UnknownOutcome`并delete/absence reconcile；
- 新lease可以接管observe/cleanup，但不能把“可能执行过”解释为“未执行”；新用户Run是新的业务执行。

平台只保证Sandbox provisioning和Job terminal commit幂等。Sandbox workload通过网络、数据库、消息或第三方API产生的副作用，
以及目标API自身的幂等、事务和exactly-once语义，属于Sandbox Package及目标服务责任。平台不解析HTTP method、不区分读写、
不注入第三方idempotency key，也不提供跨外部系统exactly-once。

## 网络与安全基线

首版OpenSandbox Docker provider使用显式bridge network，允许Sandbox Profile声明`Disabled | Direct`出网；单节点developer profile
默认冻结`Direct`，以跑通联网workload，其他Deployment不能继承隐式default。`Direct`允许普通DNS/IP/URL访问，不要求经过Platform Egress Broker。
平台不开放OpenSandbox ingress/public endpoint，且仍禁止privileged、host PID、
host network、hostPath、Docker/runtime socket、device和Platform/Kubernetes credential注入。

这是developer-preview隔离基线，不得声明为强多租户、production-qualified或等价于gVisor/microVM。需要Secret的Deployment只有在
OpenSandbox安装并通过独立secret-injection合同后才能activate；CR-216首条无Secret流程不得用明文env或调用方输入绕过Secret policy。

## 否决方案

- 保留WASI/gVisor并在失败时fallback到OpenSandbox：产生多backend分支和不可预测安全降级；
- 让OpenSandbox直接claim Job或写Run/Invocation：把physical provider升级为第二业务authority；
- 只在Dispatcher内存或metadata list中去重create：不能关闭并发与response-loss窗口；
- create后再调用execd shell-string执行接口：形成第二次dispatch与不必要的幂等/注入边界；
- 首版继续要求gVisor/Kubernetes资格：与“先跑通Docker流程”的目标不一致；
- 让public client直接使用OpenSandbox API：绕过Capability admission、tenant policy、quota、Job fence与audit。

## 结果与资格

目标实现会删除或退出首版composition的Wasmtime Executor、gVisor Launcher/guest、attestor及其Helm/RBAC/admission资产，
保留薄Sandbox Dispatcher、OpenSandbox Server和Docker provider。实现完成前当前行为仍以`docs/current`为准。

最低资格必须覆盖：create同key并发/response-loss/restart、不同digest冲突、Job lease rollover后的late result、runner可能启动后
response-loss不新建sandbox重跑、Dispatcher/OpenSandbox强杀恢复、TTL/delete/orphan cleanup、network enabled/disabled、fixed-path
result read与bounded diagnostics、
wrong API key/audience及OpenSandbox零Platform DB权限。L4强隔离、production HA、gVisor/Kata/Firecracker和恶意逃逸测试推迟。
