# Platform v2 Sandbox Execution Plane 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-21 |
| 依赖 | 03、04、07、09、10、15 |
| 直接下游 | 17、18 |

## 1. 决策摘要

Sandbox Execution Plane是独立的不受信任代码执行服务。API、Scheduler、Model Worker、Capability Worker和
MCP Host不得spawn Python、Node.js、WASM或Shell。它们只提交带durable Job fence的closed execution request。

首版backend闭集只有：

- restricted WASI：无network、无Secret、无host filesystem的纯计算；
- single-Job gVisor container：需要Python/Node/trusted Shell、冻结文件系统、受控network或Secret的执行。

plain runc/OCI、host process、privileged container、microVM、Firecracker和KVM不在首版合同中。

## 2. 物理边界

```text
Capability/Runtime service
        |
        | authenticated Submit/Cancel/Observe
        v
Sandbox Controller  -- durable Job authority through repository port
        |
        | closed fenced executor protocol
        v
WASI Executor pool | gVisor Pod Launcher pool
                            |
                            +-- admission-locked single-Job Pod (`RuntimeClass=runsc`)
        |
        +-- Artifact Data Worker
        +-- Egress/Secret Broker (gVisor only, policy gated)
```

Sandbox Controller、WASI Executor和gVisor Pod Launcher与API/Model/MCP分别使用独立Deployment、ServiceAccount、queue、
DB/storage connection pool、permit和NetworkPolicy。Executor无数据库凭据，不得直接改写Run、NodeExecution、
Invocation或Job。

只有gVisor Pod Launcher可通过专用ServiceAccount访问单一execution namespace的闭集Pod API；它不得读取Secret/ConfigMap、
修改RBAC/ServiceAccount/Node/RuntimeClass或使用exec/attach/port-forward。Controller、WASI Executor与guest Pod不获得该权限。

## 3. Runtime Catalog 与发布

可执行代码必须在发布时成为immutable Sandbox Package Artifact，并由exact Capability Deployment绑定。
Skill package中出现脚本文件不会使其自动可执行。

Runtime Catalog ResourceVersion至少冻结：

- runtime kind/version/image/module digest和CPU architecture；
- entrypoint、ABI、input/output schema与maximum bytes；
- dependency lock、SBOM、provenance、malware/license scan evidence；
- filesystem、network、Secret、Artifact ports和resource hard limit；
- required isolation class、executor protocol和cleanup policy。

Python/Node dependency resolution、image build、package manager、signature与scan全部发生在publication pipeline。执行时禁止
`pip install`、`npm install`、mutable image tag、未锁定download和字符串拼接shell command。

## 4. Isolation 选择

```rust
enum IsolationClass { WasiRestricted, GvisorContainer }
```

selection只由published Sandbox Profile根据trust、Effect、runtime、Secret、network、filesystem与resource需求决定；
调用方不能降级。

| 需求 | WASI | gVisor |
|---|---:|---:|
| 纯函数WASM、无network/Secret | 允许 | 允许但非首选 |
| Python/Node/trusted Shell | 拒绝 | 允许 |
| 受控network egress | 拒绝 | 允许 |
| Secret injection | 拒绝 | 允许 |
| writable scratch filesystem | 有界memory | 有界ephemeral volume |
| privileged/device/Kubernetes API | 拒绝 | 拒绝 |

## 5. Execution Request

```rust
struct SandboxExecutionRequestV1 {
    schema_version: ConstU16<1>,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    job_id: JobId,
    lease_generation: u64,
    package_artifact: ArtifactRef,
    runtime_catalog_version_id: ResourceVersionId,
    sandbox_profile_version_id: ResourceVersionId,
    isolation: IsolationClass,
    entrypoint: Entrypoint,
    input: RunValueId,
    output_schema_digest: Digest,
    artifact_ports: Vec<ArtifactPortBinding>,
    secret_grants: Vec<SecretGrantRef>,
    egress_policy_digest: Option<Digest>,
    limits: SandboxLimits,
    deadline_at: Timestamp,
    request_digest: Digest,
}
```

request是closed、canonical、有size limit的immutable snapshot。Controller重新验证tenant、Invocation、Job lease、package digest、
Deployment/Profile/runtime binding和hard limits。旧lease generation、未发布package、未声明port、未授权Secret或
isolation降级全部fail closed。

## 6. Job、lease 与Executor protocol

Sandbox执行复用shared Job，`JobId`是唯一物理work identity。不存在SandboxJob aggregate、ID别名、
ManagementOperation或隐式child Job。

Controller为每个attempt创建一次性executor lease，关键字为
`(JobId, lease_generation, worker_process_generation_id)`。Executor只能通过mTLS closed RPC报告：

```rust
enum SandboxPhysicalOutcome {
    Started { evidence: StartEvidence },
    Progress { bounded: Progress },
    Succeeded { output: SandboxOutputEvidence },
    Failed { failure: SandboxFailure },
    Terminated { evidence: TerminationEvidence },
}
```

Controller只有在repository复核current Job/owner fence后才能commit业务outcome。Executor response不是Run或Invocation authority。

## 7. WASI ABI v1

WASI module必须：

- 只使用发布profile允许的stable ABI/import集；
- 有界32-bit memory、fuel、table、instance和epoch deadline；
- 以canonical JSON bytes读取input并输出canonical JSON bytes；
- 无socket、host filesystem、environment Secret、clock/random外的未声明nondeterminism；
- output在Controller/Artifact Data Worker信任边界再做schema、size与digest验证。

fuel exhausted、memory limit、ABI violation、invalid UTF-8/JSON/schema和epoch interrupt分别是stable failure class。只有guest call
实际退出后才生成terminated evidence。

## 8. gVisor 执行合同

gVisor使用签名且按digest固定的OCI image与节点安装的`runsc` RuntimeClass。Launcher为每个fenced Job attempt创建全新
single-Job Pod，执行后删除；
warm pool只复用无tenant状态的image/cache，不复用tenant filesystem、process、Secret或network namespace。

guest Pod必须禁止privileged、host PID/network、hostPath、device、Kubernetes API、metadata endpoint、Docker socket和
runtime socket。root filesystem只读，scratch/tmpfs有byte/inode与lifetime上限。seccomp、capability、user namespace、
cgroup、PID、CPU、memory、I/O和pids limit由发布profile固定。fail-closed admission固定RuntimeClass、image digest、
ServiceAccount、resource/volume/network closure；plain runc fallback是否定合同。

Launcher只拥有`create/get/watch/delete` Pod、`get` status/log的namespace-scoped RBAC，不能改变admission policy。每个Pod UID、
Job/attempt/lease generation、request digest、runtime/image digest和resource closure必须相互绑定。Pod phase、exit与log只是外部
physical evidence；Executor复核fence、canonical result与Artifact grant后才能向Controller报告。watch中断、delete不确定、node loss
或runtime evidence漂移进入bounded reconcile/absence proof，不能伪造terminal success。

## 9. Artifact、network 与Secret

Executor不持有object-store credential。package/input Artifact由Artifact Data Worker按exact tenant、Job generation、port、
digest和size读取；output file以预分配Artifact identity进行stage/verify，只有owner terminal事务形成Ready引用。

gVisor network默认deny。允许时只能调用Egress Broker的catalog target，不能自由DNS/IP/URL、proxy、redirect或
raw socket。响应按host、request、byte、time和rate limit受限。

Secret只能通过one-shot grant注入gVisor sandbox的memory/tmpfs，绑定tenant、Job generation、package、name、purpose、
deadline和max reads。它不进入DB、log、Event、Artifact、output或crash dump；terminal/cancel/lease loss后撤销并清理。

## 10. Resource、timeout、cancel 与恢复

SandboxLimits至少包含wall time、CPU、memory、pids、stdout/stderr、input/output bytes、Artifact count/bytes、
network requests/bytes和filesystem bytes/inodes。调用方只能在published hard limit以内缩小。

cancel先写durable intent，Controller再以current fence发terminate。超时、取消或lease loss时Executor必须停止进程、
撤销network/Secret/Artifact grant并删除scratch。无termination/absence evidence不得重用相同物理位置。

Controller崩溃后通过Job lease、executor process generation和bounded reconciliation scan恢复。旧Executor的late result被fence拒绝；
不确定外部Effect进入reconciliation，不盲目重试。

## 11. 容量与平台保护

- WASI和gVisor各有独立queue、permit、executor pool和autoscaling signal；
- Sandbox总体与API、Scheduler、Model、MCP、native Capability和Artifact Maintenance隔离；
- claim前同时预留local executor slot与durable tenant/WorkClass quota；
- queue按weighted resource units受限，大任务不能以一个item冒充小任务；
- 该平面只有界执行，重计算/长任务必须变为独立remote Capability服务。

## 12. 可观测性与安全

metric至少包含queued weighted units、oldest age、startup latency、active executions、limit violations、termination latency、
cleanup failure和executor loss。tenant、package、path、URL和Secret identity不进label。stdout/stderr默认敏感，只保存有界、
脱敏的diagnostic Artifact。

image/module必须签名、SBOM、provenance和scan验证；node/runtime policy漂移使readiness fail closed；所有RPC
进行mTLS identity和exact audience校验。

## 13. 验收标准

- API/Model/MCP/Capability Worker无spawn/import runtime能力；
- 同一Job只有一个Executor generation可commit，旧generation被拒绝；
- WASI import/fuel/memory/ABI/output schema负向fixture fail closed；
- gVisor guest不存在runc fallback、hostPath、privileged、device、metadata或Kubernetes API通路；Launcher只有admission锁定的
  namespace-scoped Pod lifecycle权限，所有越权verb/resource/subresource被RBAC与admission双重拒绝；
- package/input/output Artifact都复核exact digest、length、tenant、port和Job fence；
- Secret/network未声明时默认deny，terminal后grant可证明撤销；
- cancel、timeout、process kill和Controller restart后不留存活process、credential或scratch；
- Sandbox队列和DB pool饱和不影响API、Scheduler、Model或critical-control资格指标。

## 14. 分层证据

- domain/protocol：closed schema、selection、limit与fence tests；
- runtime：real Wasmtime和真实Kubernetes `RuntimeClass=runsc`的gVisor conformance/escape/cleanup/watch/restart tests；
- repository/process：PostgreSQL lease、crash、cancel和recovery tests；
- release：production-equivalent node/runtime/NetworkPolicy/saturation qualification。

开发期fake adapter或单进程fixture不能声明gVisor发布资格。

## 15. 明确推迟

- microVM、Firecracker、KVM、GPU、privileged workload与host process；
- Managed MCP stdio与persistent sandbox session；
- 重计算、长任务和任意runtime installer；
- cross-region execution和tenant-warm sandbox复用。

## 16. 未决问题

首版WASI + gVisor合同无未决设计问题。gVisor具体版本与运行参数由发布profile按供应链证据固定。
