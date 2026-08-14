# Platform v2 Sandbox Execution Plane 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / Implementation In Progress |
| 日期 | 2026-08-11 |
| 依赖 | [`04-tenancy-security-and-policy.md`](04-tenancy-security-and-policy.md)、[`07-scheduler-workers-and-concurrency.md`](07-scheduler-workers-and-concurrency.md)、[`09-capability-model-and-registry.md`](09-capability-model-and-registry.md)、[`10-capability-invocation.md`](10-capability-invocation.md)、[`13-mcp-host.md`](13-mcp-host.md)、[`15-artifacts-and-files.md`](15-artifacts-and-files.md) |
| 直接下游 | 17、18 |

> Persistence ruling：Runtime/Profile/Package 使用共享 Resource；build/run/cleanup 使用 Invocation/Job/Event，executor lease
> 属于 Job。Sandbox 不建立自己的 attempt、lease、result 或 transition authority。

## 1. 决策摘要

所有 Python、Node.js、WASM、Shell 和 MCP stdio server 都在独立 Sandbox Execution Plane 执行。Control API、
Runtime API、Scheduler、Model Worker、Capability Worker 和 MCP Host 不创建脚本进程，也不与执行任务共享 CPU、
内存、PID、文件系统、连接池或并发 permit。

隔离底座采用 policy-selected tier：WASM 优先使用受限 WASI runtime；平台审核、无 Secret/无网络的不可变包可用
gVisor sandbox；模型生成代码、tenant 上传依赖、Shell、Secret 或网络访问必须使用 microVM。普通 runc/OCI
container、宿主 shell 和共享进程池不允许执行用户代码。每次执行使用全新 sandbox identity；warm pool 只复用
无 tenant 状态的预启动 base snapshot，执行后销毁。

Sandbox 不是大型计算平台。任务必须有短硬 deadline、CPU/memory/PID/disk/output/network 上限；重计算交给独立
外部 Capability service。此边界首先保护平台并发和安全，而不是追求无限语言兼容。

## 2. 目标与非目标

### 2.1 目标

- 将动态/不可信代码从平台服务并发、内存和故障域中完全隔离；
- 给 runtime、package、entrypoint、resource、network、secret、input/output 和 Artifact 固定机器合同；
- 支持 Python、Node.js、WASM/WASI 与 Reviewed Shell 的最小明确集合；
- 让短执行低延迟完成，长于请求窗口的执行 durable defer 并释放平台 Worker；
- 对重复提交、Executor crash、node loss、timeout、cancel 和迟到结果提供一致恢复；
- 按信任和能力选择 WASM、gVisor 或 microVM，不允许调用方降低 isolation；
- 通过独立 Pod/node pool、队列和配额保证 Sandbox 饱和不拖垮 API/Scheduler/Model/MCP；
- 对代码供应链、依赖、网络、Secret、文件和输出进行端到端约束。

### 2.2 非目标

- 不支持任意 Docker image、privileged container、Docker socket、hostPath 或 Kubernetes API；
- 不提供 SSH、交互终端、桌面、浏览器 GUI、后台 daemon 或无限 notebook session；
- 不支持 GPU、分布式计算、大数据批处理、训练任务或长期服务托管；
- 不在执行时运行 `pip install`、`npm install`、`apt`、`curl | sh` 或动态包管理器；
- 不允许 Model/Skill 选择 runtime image、系统调用、网络目标、Secret 或资源上限；
- 不承诺有外部网络 Effect 的脚本 exactly-once；
- 不把 stdout/stderr 当成无限事件流或最终结构化输出；
- 不让 Sandbox Executor 直接更新 Run、NodeExecution 或 CapabilityInvocation 表。

## 3. 外部隔离基础

本规范不把平台协议绑定到某个产品，但生产实现需要通过相同 isolation conformance。初始参考底座：

- [Firecracker architecture](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md)
- [Firecracker production host setup](https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md)
- [gVisor security architecture](https://gvisor.dev/docs/architecture_guide/intro/)
- [gVisor security model](https://gvisor.dev/docs/architecture_guide/security/)

microVM 实现必须使用硬件虚拟化和独立 guest kernel；gVisor 实现必须使用独立 application kernel/runtime
boundary。二者之外的新 backend 需要 threat-model review、escape/resource/network conformance 和新的
IsolationBackend Revision。普通 namespace/seccomp/runc 组合不满足用户代码隔离基线。

## 4. 信任与物理边界

```text
Platform Orchestration Plane
  -> authenticated Sandbox API
Sandbox Gateway / Controller
  -> durable SandboxJob + bounded queue
Dedicated Executor Node Pool
  -> WASI instance | gVisor sandbox | microVM
Egress Proxy + Artifact Broker + Secret Broker
```

| 组件 | 信任 | 所有权 |
|---|---|---|
| Sandbox Gateway | 可信 | 请求认证、schema、idempotency、admission |
| Sandbox Controller | 可信 | Job state、placement、lease、callback |
| Executor Agent | 受限可信 | 创建/销毁 sandbox、资源监督、guest channel |
| Guest Agent | 不信任边界内最小 TCB | 接收 request、启动 entrypoint、上传结果 |
| User code/dependency | 不受信任 | 只在 sandbox 内执行 |
| Egress Proxy | 可信 | DNS/target/method/byte/rate enforcement |
| Artifact/Secret Broker | 可信 | 一次性 scoped grant，不暴露平台 credential |

Executor node 不运行 API、Scheduler、Model、MCP Host 或数据库。Sandbox namespace/VM 不获得 Kubernetes service
account、node credential、container runtime socket、cloud metadata 或平台内部 DNS 通用访问。

## 5. Runtime Catalog

```rust
struct SandboxRuntimeRevision {
    runtime_revision_id: RevisionId,
    runtime_id: SandboxRuntimeId,
    family: RuntimeFamily,
    runtime_version: ExactVersion,
    image_or_module_digest: Digest,
    guest_kernel_digest: Option<Digest>,
    guest_agent_digest: Digest,
    supported_isolation: BTreeSet<IsolationClass>,
    supported_abi: SandboxAbiVersion,
    builtin_modules_manifest_digest: Digest,
    sbom_artifact_id: ArtifactId,
    provenance_evidence_id: EvidenceId,
    semantic_digest: Digest,
}

enum SandboxRuntimeFamily {
    Python,
    NodeJs,
    WasmWasi,
    ReviewedShell,
    ManagedMcpServer,
}
```

Machine wire固定为`python | node_js | wasm_wasi | reviewed_shell | managed_mcp_server`；supported isolation固定为
`wasm | sandboxed_container | micro_vm`，Sandbox ABI v1固定为`v1`。数组按wire value排序、无重复，未知值不得由
runtime manifest、Draft或Worker动态扩展。

- Runtime Catalog 只接受平台构建、签名、扫描并 attested 的 exact digest；
- Runtime authority按tenant保存可引用的exact发布投影，但tenant author不能上传或替换runtime binary/image；只有
  installation受信导入流程可把已签名Worker/runtime manifest投影到tenant，投影仍必须保留tenant外键和完整审计；
- `latest`、floating tag、用户 image、runtime URL 和 host interpreter 被拒绝；
- Python/Node 版本由 Revision 固定，升级创建新 Revision；
- WASM 支持的 WASI world/ABI 和 capability set 必须 exact；
- ReviewedShell 只提供固定最小工具集，不等于完整 Linux 用户空间；
- ManagedMcpServer 是固定 server package 的专用 runtime profile，不共享 Host 进程；
- 紧急 CVE suspension 阻止新 Job，历史证据保留。

## 6. Code Package 与 Build

```rust
struct SandboxPackageRevision {
    package_revision_id: RevisionId,
    package_id: SandboxPackageId,
    source_artifact_id: ArtifactId,
    source_digest: Digest,
    runtime_revision_id: RevisionId,
    entrypoint: Entrypoint,
    dependency_lock_digest: Digest,
    runtime_bundle_artifact_id: ArtifactId,
    build_evidence_id: EvidenceId,
    trust_class: CodeTrustClass,
    package_digest: Digest,
}
```

Entrypoint kind固定为`python_module | node_module | wasm_export | reviewed_executable | managed_mcp_server`，并分别使用
closed字段保存module/export或规范化relative path；不存在generic command、shell string、任意argv prefix或未知kind。

Package publish pipeline 在独立受限 Build Sandbox 中完成：archive validation、source scan、lock validation、离线
dependency fetch、license/malware policy、build、SBOM、签名和 execution conformance。Runtime Job 只挂载生成的
immutable bundle，不访问 package registry。

规则：

- Python 依赖解析为完整 wheel bundle/hash lock，禁止 runtime setup hook 网络；
- Node 依赖解析为 immutable bundle/lock，生命周期脚本默认禁用；
- WASM module 必须通过 validator、import allowlist、fuel/memory analysis；
- Shell entrypoint 必须是 package 内固定 relative path，人工 ReviewedPublished；
- `sh -c`、`eval`、字符串拼命令、动态 loader path 和任意 interpreter flag 禁止；
- 参数通过 closed JSON、argv array 或 ABI memory 传递；
- ModelGenerated code 使用临时 CodeArtifact + 专用 DynamicCode profile，不发布为可复用 Package；
- Skill package 永不自动变成 SandboxPackage。

## 7. Sandbox Profile Revision

Sandbox Profile把可复用执行安全策略发布为immutable Revision；它不是容器/microVM实例，也没有独立Deployment：

```rust
struct SandboxProfileRevision {
    profile_revision_id: RevisionId,
    profile_id: SandboxProfileId,
    allowed_trust_classes: BTreeSet<CodeTrustClass>,
    allowed_runtime_families: BTreeSet<RuntimeFamily>,
    minimum_isolation: IsolationClass,
    isolation_policy_revision_id: RevisionId,
    resource_profile_revision_id: RevisionId,
    network_policy_revision_id: RevisionId,
    artifact_io_policy_revision_id: RevisionId,
    secret_policy_revision_id: Option<RevisionId>,
    cleanup_policy: SandboxCleanupPolicy,
    max_job_duration: Duration,
    semantic_digest: Digest,
}
```

Profile policy closure固定Resource/Network/ArtifactIO/Isolation四个exact Policy Revision，SecretResolution为唯一可选
第五项；minimum isolation不得弱于Isolation Policy。cleanup policy首版machine wire只有`single_use_destroy`，明确表示
每个Job sandbox在collection后销毁，不能把warm base snapshot解释为tenant执行实例复用。

Capability Deployment的`ExactSandboxBinding`必须固定exact Package、Runtime和Sandbox Profile Revision，并验证ABI、
runtime family、code trust、entrypoint、Effect、Artifact ports、Secret purpose、network和minimum isolation兼容。Profile
只能收紧04/14平台硬策略；DynamicCode、ReviewedShell、任意Secret或external network都不能通过Profile降到低于本规范
矩阵的IsolationClass。Profile active head只帮助未来authoring/deploy resolution，Run/Job不追随head。

## 8. Isolation Class 选择

```rust
enum IsolationClass {
    Wasm,
    SandboxedContainer,
    MicroVm,
}
```

Machine wire使用`wasm | sandboxed_container | micro_vm`；安全强度顺序固定为
`wasm < sandboxed_container < micro_vm`，比较时使用该显式rank而不是字符串或enum声明顺序。

最小隔离矩阵：

| 条件 | 最低 IsolationClass |
|---|---|
| 已验证 WASM、无 Secret、默认无网络 | Wasm |
| 平台审核 immutable Python/Node、无 Secret、无网络、固定依赖 | SandboxedContainer |
| ModelGenerated/tenant source 或 dependency | MicroVm |
| ReviewedShell | MicroVm |
| 任意 Secret grant | MicroVm |
| 任意 external network egress | MicroVm |
| Managed MCP stdio server | MicroVm |
| 高敏数据或 policy 强制 | MicroVm |

`SandboxedContainer` 初始实现为 gVisor，不允许 plain OCI/runc。最终 class 由 Package trust、Capability Effect、
Secret、network、data classification、tenant policy 和 platform hard policy 的最强要求决定；调用方只能要求
更强隔离，不能降级。MicroVM 不意味着允许更高权限，它仍使用相同最小 capability policy。

## 9. Execution Request

```rust
struct SandboxExecutionRequest {
    schema_version: u32,
    protocol_version: SandboxAbiVersion,
    tenant_id: TenantId,
    sandbox_job_id: SandboxJobId,
    invocation_id: CapabilityInvocationId,
    job_id: JobId,
    expected_invocation_version: u64,
    attempt_no: u32,
    lease_generation: u64,
    capability_deployment: ExactDeploymentRef,
    capability_binding: CapabilityBackendBinding,
    runtime_revision: ExactVersionRef,
    runtime: SandboxRuntimeResourceSpec,
    package_revision: ExactVersionRef,
    package: SandboxPackageResourceSpec,
    profile_revision: ExactVersionRef,
    profile: SandboxProfileResourceSpec,
    isolation_class: SandboxIsolationClass,
    executor_worker_manifest_digest: Sha256Digest,
    isolation_backend_contract_digest: Sha256Digest,
    effect: Effect,
    classification: DataClassification,
    input_value_id: RunValueId,
    input_schema_digest: Digest,
    input_ref: ValueRef,
    output_value_id: RunValueId,
    output_schema_digest: Digest,
    artifact_grants: Vec<ScopedArtifactGrant>,
    secret_grants: Vec<ScopedSecretGrant>,
    network_mode: SandboxNetworkMode,
    resources: SandboxResourceEnvelope,
    deadline: DateTime<Utc>,
    callback: ScopedSandboxCallback,
    trace_context: SafeSandboxTraceContext,
    request_digest: Sha256Digest,
}
```

Gateway根据exact Capability Deployment/RunBindings派生Package、Runtime、Sandbox Profile及其resolved policy和grant。
客户端input不能覆盖
tenant、runtime、entrypoint、resources、network、Secret、callback 或 isolation。Request 使用 mTLS workload
identity 和 canonical body digest；Executor RPC仅接受leaf certificate中恰好一个
`spiffe://insight.platform/workload/sandbox-executor.<backend>` URI SAN，WASI v1的exact identity为
`spiffe://insight.platform/workload/sandbox-executor.wasi`。CN、DNS SAN、自报header或payload中的Worker ID都不能替代该传输身份；
业务命令还必须独立通过WorkerProcessGeneration与lease fence。重放绑定Invocation expected version、sandbox
job/attempt/epoch/deadline。`SandboxJobId`、`JobId`与
预留的output `RunValueId`使用同一个UUID的不同typed prefix，使Job主键同时预留唯一输出identity；input Value/Schema、output
Schema和ValueRef必须与Invocation frozen admission及已提交input RunValue逐字段一致。

`executor_identity_digest`不是WorkerManifest或镜像摘要。Executor启动后必须先向同节点node/runtime attestor登记
`worker_process_generation_id`、WorkerManifest digest和isolation backend contract digest。登记端点只能通过node-local Unix socket
访问并仍要求mTLS exact Executor workload identity；attestor必须从不可进入wire的Unix peer-credential上下文取得宿主PID，不能接受payload、
header或环境变量自报PID。attestor再从只读node/runtime authority直接观察并相互核对Pod UID、node UID、runtime sandbox/cgroup locator、boot identity、
PID namespace与process-start tick，返回覆盖登记请求、workload identity及全部观察值的sealed `executor_identity_digest`。Executor不接收或
选择这些观察值，只可把回执摘要提交到首次`Preparing` phase，后续phase沿用同一摘要；未登记、跨节点socket、重复绑定到不同generation、
由Executor自报后未被runtime观察、或digest与attestor回执不一致时均不得claim或启动Job。登记是attestor的有界短期证明索引，不是
Job/lease/current-state authority；进程存活期间不得因TTL删除，确认generation消失后至少保留到平台hard maximum wall + cleanup窗口结束，
随后必须有界回收。

提交时`lease_generation`必须为0；generic Job claim成功后才把返回的exact generation绑定到Executor command。Runtime、
Package与Profile同时携带exact ref和closed document，Executor不允许按mutable current head重新解析。Artifact read grant固定
Ready `ArtifactRef`，可选exact byte range；write/commit grant固定Staging Artifact ID、port、generation、byte ceiling和expiry，
两种形状不能混用。grant、Secret和callback到期时间不得晚于Job deadline，也不得在admission时已经过期。

## 10. Admission 与 Job 状态机

```rust
enum SandboxJobState {
    Accepted,
    Preparing,
    Starting,
    Running,
    Collecting,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}
```

```text
Accepted -> Preparing | Cancelling | TimedOut | Failed
Preparing -> Starting | Cancelling | TimedOut | Failed | Lost
Starting -> Running | Cancelling | TimedOut | Failed | Lost
Running -> Collecting | Cancelling | TimedOut | Failed | Lost
Collecting -> Succeeded | Cancelling | TimedOut | Failed | Lost
Cancelling -> Cancelled | Failed | TimedOut | Lost
```

SandboxJob 是 03 共享 Job 的 `work_class=sandbox` typed view，复用统一 lease/epoch/fence；它不是Capability Job的child，也没有
第二个attempt、lease或terminal authority。`Lost` 表示
guest/executor结果不可恢复；owner Invocation 必须依据 Effect、cleanup 和 recovery 事实显式决定逻辑状态。

这里的“不是Capability Job的child”同时约束MCP Managed stdio operation：其Capability Implementation逻辑kind仍为`Mcp`，但exact
transport要求代码执行时，Gateway必须跳过`CapabilityRemote` Job并把同一个物理attempt直接表示为SandboxJob。request使用closed tagged
source区分普通`Sandbox` Implementation与`ManagedMcp` Implementation；后者额外冻结exact MCP Deployment、Discovery、Authorization、
protocol operation/continuation，且Package Runtime family必须为`managed_mcp_server`、entrypoint必须为`managed_mcp_server`、最终
isolation必须为`micro_vm`。不能用两个Capability/Sandbox Job、Runner内存状态或RPC request ID表达同一个物理attempt。

Managed stdio Resource subscription例外地同时存在逻辑MCP subscription Job与物理Sandbox session Job，因为notification/reconcile等待与
microVM process/cleanup是两个独立生命周期。Sandbox Job只能通过逻辑Job冻结的exact generation创建；逻辑Job保存关联Sandbox Job identity，
Sandbox payload反向保存逻辑subscription/generation digest。任一方向不匹配、旧session仍Running、或absence/quarantine证据不足都fail
closed。该例外不允许第二套session、lease或terminal表，仍只使用共享Job typed payload。

终态不可离开。`Lost` 表示执行边界无法证明代码是否完成或产生外部 Effect；Controller 把 safe uncertainty 回传
CapabilityInvocation，由 10 决定 retry/reconciliation。没有网络/Secret且 Interface 为 Pure 的 Job lost 可以按
平台 policy 创建新 Attempt，但不能修改旧 Job。

Admission 原子验证 tenant、Ready Invocation expected version、binding、input/output Value与Schema、package/runtime status、quota、
deadline、Artifact/Secret grant、resource、network、isolation capacity 和 idempotency，然后写一个共享Sandbox Job/outbox，
同时把Invocation推进为Deferred并绑定该Job。没有可用 execution capacity时Job保持durable Accepted；admission只受独立Sandbox
control-plane硬上限约束，不在Capability Worker内等待slot，也不创建`CapabilityRemote` Job。

数据库时间是admission与phase提交的唯一时钟。一个admission事务锁定Capability Invocation和exact Capability Deployment，
校验Deployment gate/closure以及active Runtime/Package/Profile revision document，建立Artifact grant link，reserve
`concurrent_jobs`、`sandbox_cpu_seconds`、`sandbox_memory_mebibytes`、`sandbox_output_bytes`四条quota line，并原子写Invocation、Job、
Command Receipt、Event和Outbox。任何一步失败都不留下部分Invocation transition、reservation、grant或Job。

## 11. Placement 与 Executor Lease

- Controller 按 isolation/runtime/architecture/resource/data region/tenant anti-affinity 选择 Executor pool；
- Job claim 使用 `FOR UPDATE SKIP LOCKED` 或等价 durable queue、lease epoch 和 owner fence；
- Executor 在创建 sandbox 前再次验证 signed Job envelope 与 runtime/package digest；
- 一个 sandbox 只执行一个 execution ID；不同 tenant/Run 不共享 guest kernel/process namespace；
- microVM 分配独立 jail、tap/vsock、rootfs overlay、cgroup 和 device policy；
- gVisor 每 Job 新 sandbox，不将多个 tenant container 放入同一 sandbox；
- WASM store/instance 每 Job 新建，host imports 使用最小 allowlist；
- lease heartbeat 由 Executor agent 发出，user code progress 不能延长硬 deadline；
- stale Executor 的 result、artifact finalize 和 callback 被 epoch fence 拒绝。

Executor不能直接写Job。Preparing、Starting、Running、Collecting和terminal各使用不同的Worker `JobCommit` Receipt/Event/Outbox
identity，通过`SandboxExecutionAuthority`提交；提交者WorkerProcessGeneration必须等于Job fence中的generation owner，phase evidence、
expected Job version、lease generation和lease token任一不匹配都整体回滚。prepare后的首次durable phase提交失败时必须先destroy已创建
sandbox，且不得启动user code。

## 12. Warm Pool 与清理

允许 warm 的对象只有平台构建的 base runtime/microVM snapshot：

- snapshot 在任何 tenant code、input、Secret、network 或 Artifact 注入前创建；
- clone 启动后注入新的 entropy、execution identity、overlay filesystem 和 network identity；
- 一个 clone 执行一次后销毁，不能 reset 后给另一个 Job；
- package layer 可以只读 content-address cache，但 cache 不含 tenant input/output/credential；
- gVisor/WASM instance 不跨 Job 复用；
- cleanup 删除 overlay、tap rule、cgroup、grant、memory mapping 和 temporary key，并写 cleanup receipt；
- cleanup 失败隔离 node/sandbox identity，不能立即回池；
- warm pool 有独立内存/slot 上限，不能挤占 running/control capacity。

execution deadline不截断必要的销毁取证；cleanup只允许在`deadline + resources.cleanup_milliseconds`的有界grace内完成。
成功结果必须证明`Destroyed`且grant已撤销、ephemeral storage已销毁；无法证明时不能伪造成功，按failure/uncertainty进入
terminal authority。

## 13. Guest ABI 与 Entrypoint

Guest 内只暴露逻辑目录/通道：

```text
/opt/runtime        read-only runtime
/opt/package        read-only immutable package
/work/input         read-only materialized inputs
/work/output        empty quota-controlled output
/run/grants         ephemeral broker sockets/tokens
vsock/control       request, cancel, heartbeat, result envelope
```

Entrypoint 接收单个规范化 request JSON path 或固定 ABI handle。结构化结果写到唯一 result channel/file，并由 Guest
Agent 读取；额外文件只有在声明 Artifact output port 下收集。禁止依赖当前目录、host UID、环境 hostname、系统
时间精度、共享 `/tmp` 或未声明 env var。

### 13.1 WASI ABI v1

首个`wasm_wasi` backend固定为Wasmtime `42.0.0`上的受限core WebAssembly ABI；“WASI”表示该runtime family的发布与
隔离层级，不表示v1默认开放WASI Preview 1/2系统调用。v1 module必须满足：

- binary Artifact长度不超过16 MiB，Executor从Artifact Broker取得后重新验证exact length和SHA-256；
- module不允许任何import，因而不能访问filesystem、socket、clock、random、environment、stdio或host callback；
- 只能导出一份32-bit、非shared且有显式maximum的`memory`，不能导出table；minimum和maximum均不得超过Job的
  `wasm_memory_pages`，且`wasm_memory_pages * 64KiB <= memory_mebibytes * 1MiB`；
- engine feature set固定为WebAssembly 1.0（WASM1）；export集合必须恰好为`memory`、`insight_alloc`和Package固定的`run`；
  额外export、component model、threads、memory64、multi-memory、reference/function-reference、GC、SIMD、bulk-memory、
  multi-value、tail-call和exception均不属于v1；
- `insight_alloc(i32 input_len) -> i32 input_ptr`返回guest memory中的可写区；Executor写入canonical input envelope；
- `run(i32 input_ptr, i32 input_len) -> i64`返回`(output_ptr << 32) | output_len`，两个字段按unsigned 32-bit解释；
- input envelope固定为`{"schema_version":1,"request_digest":...,"input_value_id":...,
  "input_schema_digest":...,"value":...}`；Artifact-backed input必须由唯一`read_whole` exact grant读取，
  重新核对content digest/length、strict JSON和exact input schema后按同一形状传入；不得把ArtifactRef
  metadata作为正文通过input schema validator；
- output必须是UTF-8 strict canonical JSON `{"schema_version":1,"value":...}`，不得含unknown/duplicate field；
  `output_len`必须为正且不超过`result_bytes`，随后由exact output-schema validator生成validation evidence；
- 每个Job新建Engine/Store/Instance，启用fuel、Store memory/instance/table limiter和epoch interruption。fuel耗尽形成稳定
  resource failure；cancel/timeout先发exact generation epoch interrupt，只有guest call实际退出、grant已撤销且Store被drop后才允许
  生成termination/cleanup evidence；
- ABI输入、输出和guest linear memory在读取完成后scrub；module、compiled code和不含tenant数据的runtime实现可以由未来
  CandidateManifest允许的content-addressed cache复用，但Store/Instance/tenant bytes不能复用。

任何WASI syscall、host import、filesystem、network、Secret、stdout/stderr或Artifact output扩展都必须定义新ABI version及
import/resource conformance，不能静默加入v1 allowlist。当前WASI backend因此只接收Pure/ReadOnly、无Secret、无网络、最多一个
输入read grant且无output grant的执行；其他请求必须由policy选择更强且具备相应broker的backend。

环境变量只含无敏感 runtime metadata。Secret 不进入 env/argv；需要时由 Secret Broker 以一次性内存/tmpfs file
或受限 fd 提供，读取后 grant 失效。DynamicCode profile 永不获得 Secret grant。

## 14. 文件系统与 Artifact I/O

- rootfs/runtime/package 全部只读，唯一 writable overlay 是 per-Job、quota-controlled、noexec（entrypoint 必需
  路径除外）；
- 不挂载 hostPath、workspace、Docker socket、`/proc` host view、`/sys` device、tenant bucket 或共享 PVC；
- input Artifact 通过 15 的 grant broker materialize，验证 tenant/digest/media/size；结构化主输入还必须在
  Executor/Controller trust boundary使用exact Interface input schema重新验证；
- output 先写 staging grant，Guest 不能指定 object key、Artifact ID、classification 或 public URL；
- symlink/hardlink/device/FIFO/socket/path traversal 和 sparse quota bypass 被拒绝；
- result commit 前执行 file count/size/media/malware/archive/content policy；
- stdout/stderr 分别限 byte/rate，截断形成 private diagnostic，不作为 success output；
- sandbox 销毁后所有 ephemeral disk/key 必须不可恢复，持久输出只存在 Ready Artifact。

## 15. Network Egress

默认网络为 `None`。启用时所有流量经过平台 Egress Proxy：

```rust
struct SandboxNetworkPolicyDocument {
    mode: NetworkMode,
    destinations: Vec<CanonicalDestination>,
    methods_or_protocols: BTreeSet<AllowedProtocol>,
    max_connections: u32,
    max_request_bytes: u64,
    max_response_bytes: u64,
    dns_policy: DnsPolicy,
    tls_policy: TlsPolicy,
}
```

- 禁止 raw socket、listen/inbound、peer-to-peer、arbitrary DNS、UDP（除 brokered DNS）和 CONNECT tunnel；
- hostname 在 proxy 解析并检查 private/link-local/metadata/Kubernetes/internal ranges；
- redirect 每 hop 重新验证，默认禁止跨 origin；
- TLS 证书/SNI/hostname 固定 policy，不允许关闭验证；
- egress 不携带平台 ambient credential/cookie；
- response bytes/rate/decompression ratio 有硬限制；
- allow destination 不等于授权 Effect，Capability Effect/Approval 仍独立执行；
- external write 断线时返回 Uncertain，不因代码进程退出自动重试。

## 16. Resource 与时间限制

每个 ResourceProfile 固定：

```text
vCPU quota/period and max cores
memory hard limit and swap disabled
PID/thread/process limit
ephemeral disk/inode/file count
read/write bytes and IOPS
stdout/stderr/result/artifact bytes
network connections/bytes/rate
WASM fuel/epoch/memory/table limits
wall clock, CPU time, startup and idle deadline
```

平台只提供 small/medium 等经过容量验证的短任务 profile；没有 unlimited/custom host resources。tenant/Agent
policy 只能收紧。达到 memory/PID/disk/output/fuel/network 限制时强制终止 sandbox 并返回稳定 failure class；
resource violation 不允许转成普通脚本 catch 后继续无限运行。

## 17. Result 与 Callback

```rust
enum SandboxExecutionOutcome {
    Completed(SandboxCompletedOutput),
    Failed(SafeSandboxFailure),
    Cancelled(SandboxCancelEvidence),
    TimedOut(SandboxTerminationEvidence),
    Uncertain(SandboxUncertainty),
}
```

Completed 需要：guest exit policy 满足、result schema 合法、所有 Artifact 已验证/finalize、当前 Job epoch 有效、
resource accounting 完整。Controller 通过 03 的 scoped callback/inbox 提交 outcome。重复 callback 返回同一 receipt；
独立Capability owner controller再执行Interface output/data policy validation，并以同一Job的terminal evidence把Deferred
Invocation推进为逻辑terminal/retry/reconciliation；该归并不claim或修改已经terminal的Job，也不创建第二个物理Job。
terminal Event只是低延迟wake hint。controller必须同时运行bounded、sharded PostgreSQL keyset safety scan，只选择仍由当前
Deferred/Cancelling Invocation绑定、Job current version与terminal Event aggregate version/payload digest精确一致的候选；扫描不写第二份
pending状态。merge使用source Event作为Receipt dedupe owner并重新锁定全部事实，重复或并发first-winner稳定收敛。

Sandbox `Succeeded` 只表示受控执行完成，不直接把 Invocation/Run 置成功。raw exit code、signal、stderr、VMM error
映射为 stable safe failure；私有 diagnostic 可形成短 retention encrypted Artifact。

## 18. Timeout、取消与终止

- deadline 是平台绝对 UTC deadline；startup、execution、collecting 都计入 wall time；
- soft cancel 先向 Guest Agent 发出，只有短 grace；随后 kill sandbox/microVM；
- hard deadline 到达立即撤销 egress/Secret grant、终止 VMM/runtime、冻结 output finalize；
- 对active Sandbox的timeout先提交`control_kind=timeout`的Capability `Cancelling` Event；只有Sandbox terminal evidence归并后
  才把Invocation置`TimedOut`，若可能已产生Effect则进入`ReconciliationRequired`；
- process tree、child namespace 和 daemon 必须与 sandbox 一起终止；
- cancel/timeout 与 result 并发由 Job generation/epoch first-winner；
- `Cancelled/TimedOut` 不证明外部 Effect 未发生，CapabilityInvocation 依据 network/Effect evidence reconcile；
- Run terminal 后迟到 result 不进入 Plan，但 cleanup、Artifact staging GC 和 effect incident 继续；
- kill 不依赖 user code cooperation。

## 19. 幂等与恢复

- Gateway 以 `(tenant, execution_id, request_digest)` 幂等，digest 冲突 fail closed；
- 同一 Invocation Attempt 重复 submit 返回已有 Job/terminal receipt；
- safe retry到期后，Gateway在单事务内执行`RetryScheduled -> Deferred`并创建新的Sandbox Job；旧terminal Job永久保留，
  request中的全局`attempt_no`必须恰好递增且不得超过Invocation冻结的attempt limit；
- safe retry的相对退避必须从Capability admission snapshot复制进每个Sandbox request，范围为`1..=60000ms`。owner controller不得提交
  绝对时间或本地退避配置；repository使用transaction database clock派生`retry_at`并与Invocation deadline求交；
- Controller/Executor crash 后由bounded expired-lease scan推进：仍为`Accepted`且deadline未到的Job清除旧lease并回到Ready，
  因为Executor必须先提交`Preparing`才允许调用backend；仍为`Accepted`但deadline已到的Job以无执行证据的`TimedOut`终结；
  已提交`Preparing`或更晚phase的Job只有在旧sandbox已销毁或对应node identity已隔离、grant已撤销并提交closed recovery
  evidence后才能标记`Lost`。旧generation随Job version/lease generation fence失效，迟到结果不能覆盖恢复结果；
- 可证明的既有sandbox可以由实现了相同exact generation owner-token协议的新owner重连；不能证明时必须走上述`Lost`路径，
  不能把`Preparing`回退为`Accepted`或在同一Sandbox Job内重跑用户代码；
- package/runtime/cache 只按 verified digest 命中，cache corruption 隔离 node 并重新拉取；
- Callback 丢失由平台 poll/safety scan 查询 Job terminal receipt；
- terminal Event wake丢失、重复或乱序由Capability owner controller的bounded PostgreSQL scan与source-Event Receipt归并收敛；
- Controller DB commit 后 response 丢失由相同 execution ID 重试读取；
- artifact staging orphan 由 15 GC，不能被其他 Job adopt；
- node loss 后 Pure/no-network Job 可以由新 Attempt 重跑，有 Effect Job 进入 reconciliation；
- NATS/queue wake 可丢失，durable Accepted/expired lease scan 保证最终推进。

## 20. 并发、背压与平台保护

Sandbox 使用独立层级 permit：

```text
global isolation class
 -> executor pool/runtime
   -> tenant
     -> package/capability
       -> Job resource units
```

- queue、preparing、starting、running、collecting、callback、cleanup 分别有 bounded capacity；
- 在获得 execution slot 前不分配 microVM memory，不持有 Capability Worker permit；
- Sandbox API 接受 Job 后立即返回 durable handle，Invocation 进入 Deferred；
- per-tenant fair queue 与 hard quota 防止单租户耗尽 node pool；
- control/cancel/cleanup/reservation 使用保留 capacity，running saturation 不能阻止终止；
- image/package pull 使用独立 bandwidth/IO permit，避免 cache miss 拖垮执行；
- autoscaling 依据 weighted queued resource units、startup latency、slot utilization 和 oldest age；
- 队列超过硬门槛时 admission 返回稳定 capacity failure/defer，不在 API/Scheduler 内排无限 future；
- Sandbox 全部不可用时 API、Scheduler、Model、Native Capability 和 MCP readiness 必须保持正常。

admission按ceiling reserve四维quota；terminal必须在同一事务释放concurrent/memory reservation，并以可信usage结算CPU/output。
失败或不确定且缺少完整usage时按已冻结ceiling保守结算，不能以0掩盖已消耗资源。`artifact_links`是本Job grant撤销的唯一
durable authority：Broker在生成`Destroyed`/cleanup evidence前，以exact tenant、Sandbox Job、request digest、attempt、
WorkerProcessGeneration和lease generation幂等地把本Job的active grant推进为released；相同物理attempt重放必须得到等价evidence。
terminal事务释放任何尚未released的剩余grant，并断言本Job恰有request冻结的全部released grant；数量或owner不匹配时整笔
terminal commit失败。Broker先撤销与terminal收口不是两套authority，也不得因重复撤销把合法terminal拒绝。

## 21. Secret

- Package 声明 SecretPurpose，Capability Deployment/Policy 决定是否绑定；
- Job 使用一次性 ScopedSecretGrant，绑定 tenant、execution、purpose、runtime digest、deadline 和 max reads；
- Executor/Guest 通过 mutually authenticated broker 取值，controller/database 不保存 value；
- Secret 不进入 source/package digest、env、argv、filesystem image、snapshot、stdout、stderr、result 或 Artifact；
- Secret file 位于 tmpfs/内存，权限最小，Job 结束或 cancel 立即 revoke/scrub；
- ModelGenerated code、unreviewed package、WASM 未声明 import 和 shared warm snapshot 禁止 Secret；
- egress destination 与 Secret purpose 必须联合批准，防止任意 exfiltration；
- canary detector 发现输出/日志包含 Secret 时隔离结果、revoke credential 并产生 incident。

## 22. 供应链与节点安全

- runtime/kernel/rootfs/guest agent/VMM/gVisor/package/builtin tool 都是 exact digest、签名、SBOM 和 provenance；
- Executor 启动报告 node/runtime attestation，Controller 只向合格 pool placement；
- microVM VMM 使用 jailer、最小 device、seccomp、独立 UID/cgroup/network namespace；
- Executor node 最小 OS、自动补丁、secure boot/IMA（能力允许时）、磁盘加密和无通用管理员工作负载；
- KVM/device/plugin 权限只给 Executor Agent，不给 guest/controller/其他 Pod；
- no privileged sidecar in guest Pod、no host PID/network、no service account token；
- runtime CVE/escape incident 可按 digest suspension、drain node、revoke grants 和批量阻止新 Job；
- node forensic 数据按隐私 policy 保存，不包含无授权 tenant source/Secret。

## 23. Persistence 与 Artifact 映射

Sandbox Runtime、Profile 与 Package 使用共享 Resource/ResourceVersion；环境可执行绑定使用 Deployment。Build、execution、
callback、cleanup 与 reconciliation 是共享 Invocation/Job/Receipt/Event，executor placement/lease/fence 保存在 Job 热字段，
guest/backend detail 保存在 Job 的 bounded typed payload。Source、bundle、SBOM、input、output、diagnostic 与 build stage 使用
Artifact/ArtifactLink。Sandbox Controller 只能通过 repository command 推进 owner aggregate，不能建立第二套 attempt、lease、
result、cleanup 或 executor-node authority。

## 24. 所有权接口

```rust
trait SandboxGateway {
    async fn submit(&self, request: SandboxExecutionRequest) -> SandboxSubmitReceipt;
    async fn get(&self, request: GetSandboxJob) -> SandboxJobSnapshot;
    async fn cancel(&self, request: CancelSandboxJob) -> SandboxCancelReceipt;
}

trait SandboxExecutorBackend {
    async fn prepare(&self, job: LeasedSandboxJob) -> PreparedSandbox;
    async fn start(&self, sandbox: PreparedSandbox) -> RunningSandbox;
    async fn terminate(&self, command: TerminateSandbox) -> TerminationEvidence;
    async fn destroy(&self, command: DestroySandbox) -> CleanupEvidence;
    async fn abort(&self, command: AbortSandboxExecution) -> SandboxAbortEvidence;
}

trait SandboxGatewayAuthority {
    async fn accept_sandbox_execution(&self, command: AcceptSandboxExecution)
        -> CommandOutcome<SandboxPhaseDecision>;
}

trait SandboxExecutionAuthority {
    async fn commit_sandbox_phase(&self, command: CommitSandboxPhase)
        -> CommandOutcome<SandboxPhaseDecision>;
    async fn commit_sandbox_outcome(&self, command: CommitSandboxOutcome)
        -> CommandOutcome<SandboxPhaseDecision>;
}
```

WASI Artifact读取由独立Artifact Broker实现15的受信物化合同。Sandbox Controller只把Executor的
credential-free请求转交该Broker并返回已经过exact generation/length/digest复验的bounded bytes；它不得
自行解释密文locator，也不得把PostgreSQL/S3/KMS client或credential链接进Executor进程。read authority的
两条授权路径是闭合且互斥的：

- `runtime_bundle`不携带grant，必须精确匹配当前active leased Sandbox Job冻结的published Package revision及
  `runtime_bundle_artifact`；
- `input_value`必须携带该Job request中完全相等的`read_whole` grant，并匹配仍为active、未过期、generation/
  payload digest/owner/source Artifact均一致的`artifact_links`行。

两条路径都必须重验tenant、SandboxJob/Job同UUID投影、request digest、WorkerProcessGeneration、lease
generation、active physical phase、deadline、Ready Artifact、Verified Blob及ArtifactRef全部字段。ArtifactRef
本身、已释放grant、Executor提交的deadline或当前Job payload任一单独事实都不足以授权读取。该路径不创建
read状态表；body-free read audit使用已有Event/telemetry策略，grant的durable状态仍只属于`artifact_links`。

Backend 对 IsolationClass exhaustive。Domain contract 不依赖 Kubernetes/Firecracker/gVisor SDK；adapter 层负责具体
实现。所有 outcome 为闭合枚举，未知 agent/VMM state fail closed。

## 25. 公共 API 与事件

Sandbox API 只供平台 workload identity 使用，不向终端用户提供任意 `exec` endpoint。用户通过已发布
Capability/Agent 提交 typed input。管理面提供 Runtime/Package/Profile publish、validate、suspend 和 evidence
query。

内部事件：

```text
sandbox.job.accepted
sandbox.job.started
sandbox.job.completed
sandbox.job.failed
sandbox.job.cleanup_required
sandbox.runtime.suspended
```

公共 Run projection 默认只显示 Capability waiting/completed/failed，不暴露 isolation backend、node、image、
package path、exit code 或 logs。Progress 是 bounded coarse milestone，不能发送任意 stdout。

## 26. 可观测性与隐私

```text
sandbox_jobs_total{runtime_family,isolation,outcome}
sandbox_job_duration_seconds{runtime_family,isolation,outcome}
sandbox_queue_age_seconds{isolation}
sandbox_slots{isolation,state}
sandbox_startup_duration_seconds{isolation}
sandbox_resource_termination_total{resource}
sandbox_executor_lost_total{isolation}
sandbox_cleanup_total{outcome}
sandbox_egress_total{outcome,reason_class}
```

tenant/Run/Invocation/package/destination/node/filename 不进入 metric label。Trace 记录受控 revision hash、resource
profile、phase、bytes 和 failure class，不记录 code/input/output/Secret。stdout/stderr 默认不进入集中日志；只有
经 redaction 的 platform agent logs 可观测。

## 27. 配置与部署

- `sandbox-gateway/controller`、`sandbox-builders`、`sandbox-executors-wasm`、`sandbox-executors-gvisor`、
  `sandbox-executors-microvm` 使用独立 Deployment/node pool/queue/DB pool；
- microVM pool 需要专用 KVM-capable Linux nodes、taint/toleration 和最小设备权限；
- gVisor 使用固定 RuntimeClass，admission policy 阻止降级为 runc；
- WASM Executor 可以无 KVM，但仍在 Sandbox node pool；
- NetworkPolicy 默认 deny，只有 controller、Artifact/Secret Broker、Egress Proxy 必需路径；
- Controller内部gRPC强制client CA验证，并在统一RPC前置门exact匹配Executor workload URI SAN；证书轮换沿用同一URI
  identity，不能使用证书指纹、CN或客户端metadata作为角色授权；
- API/Scheduler namespace 不具备调用 Executor low-level API 或访问 KVM 的身份；
- rolling deploy 先停止 admission、drain/terminate bounded Jobs、接管 durable receipts，再替换节点；
- readiness 区分 controller 可用、各 isolation capacity 和依赖健康，不因单 pool 饱和全局失败。

## 28. 测试矩阵

隔离 conformance 至少覆盖：

- host filesystem、PID、network namespace、Kubernetes API、metadata、device、KVM、socket escape；
- fork/thread bomb、CPU spin、memory bomb、disk/inode/sparse file、stdout/stderr/result flood；
- symlink/hardlink/path traversal/archive bomb、malicious media、Artifact scope swap；
- DNS rebinding、redirect、private IP、raw socket、listen、TLS bypass、response bomb；
- Secret env/argv/proc/log/result/snapshot/cache exfiltration；
- runtime/package/image digest swap、mutable tag、unsigned build、cache corruption；
- Gateway duplicate submit、Controller crash、Executor kill、node loss、callback loss、stale epoch；
- cancel/timeout/completed race 与完整 process tree termination；
- warm snapshot entropy、single-use clone、tenant residue和 cleanup failure；
- WASM import/fuel/memory、gVisor runtime enforcement、microVM VMM/jailer policy；
- Sandbox saturation 时 API/Scheduler/Model/Native/MCP latency 与 admission 不受其连接/permit 影响。

安全测试需要独立 red-team escape suite、节点基线扫描和定期故障注入，不只依赖单元测试。

当前CR-133只交付了closed domain、backend registry/host、五阶段worker authority和PostgreSQL adapter。Executor cancellation/wall
timeout会按exact request/attempt/lease generation调用backend abort；取消阶段与终态分别使用fenced commit，abort和取消阶段提交并发启动，
不因数据库提交等待而推迟process kill。terminal可在execution deadline后的有界cleanup grace内提交，但Worker identity、version、generation
和token仍必须完全匹配。Capability cancel/timeout/reconciliation Event现在显式携带`control_kind`。bounded Controller从已提交Event或分片
safety scan解析source Event/version/payload digest和leased request；查询只返回目标WorkerProcessGeneration拥有的lease，并且只有tenant、
Sandbox Job、Invocation、request digest、attempt、lease generation与WorkerProcessGeneration全部匹配才触发Executor token。重复、missing与
stale投递不修改数据库，Capability Invocation/Event仍是唯一逻辑控制权威。未领取Job由独立controller command锁定source Event、
Invocation和Ready Job后原子走`Accepted -> Cancelling -> Cancelled`或`Accepted -> TimedOut`，以0 usage释放四维reservation与Artifact
grant；payload不伪造Executor identity、process termination或cleanup evidence。Sandbox不能再经generic Job claim执行；专用claim先锁定并
复核parent Invocation仍为`in_flight|deferred`、exact Capability/Runtime/Package/Profile gate仍有效，再返回绑定lease generation的request
和fence，因此已提交control与claim竞态保持数据库first-winner。
跨Pod控制现由Core NATS request/reply adapter投递到
`insight.platform.v1.sandbox.control.<WorkerProcessGenerationId>`；每个Executor generation只订阅自己的exact subject，不使用queue group。
请求与应答共同受HardLimitProfile的`control_data.nats_payload_bytes`约束，应答必须回绑原始`signal_digest`。malformed、oversize、
错误generation和one-way消息不被ack；断连/超时返回transport unavailable并由相同committed Event的bounded safety scan重试。
该adapter和本地router都不写current state，也不把NATS reply升级为完成证据。
expired-lease safety scan返回exact request、旧Job version/generation、旧WorkerProcessGeneration和物理phase但不暴露lease token；
恢复command以稳定semantic key提交`JobCommit` Receipt/Event/Outbox。`Accepted`且deadline未到只清除lease并回Ready，deadline已到以
0 usage TimedOut；`Preparing`及以后必须附带destroy或node-quarantine、grant撤销和closed uncertainty evidence才进入Lost并保守结算，
同一恢复可由不同recovery process以新mutation ID重放。`Preparing -> Lost`已加入closed state contract，因为该phase提交后backend prepare
可能正在进行，不能回退到Accepted。
58个contract fixture、22个Sandbox domain/worker fixture、3个NATS control wire/config fixture、strict Clippy及真实PostgreSQL 16
Sandbox transaction/resolver/scan/recovery测试均已实际通过。生产WASI与Firecracker adapter及其独立进程/Helm拓扑已经交付；gVisor adapter、
authenticated NATS real-process/control ACL fixture、生产backend reconnect/abort/quarantine实现、完整Artifact/Secret/Egress broker组合以及
escape/process-kill/saturation conformance仍是Phase 4退出门禁，不能由mock backend、Helm渲染或编译结果替代。

运行时`SandboxRecoveryDriver`使用Sandbox WorkerManifest的独立critical-control permit循环执行分片scan；每个已进入backend的候选先经
exact backend contract执行destroy或node quarantine并生成sealed evidence，再由不同的PostgreSQL连接阶段提交恢复。业务Sandbox permit
饱和不阻止该scan，单候选backend失败不伪造数据库终态，数据库不可用触发bounded backoff。当前只具备port、host验证和runtime unit
fixture，不表示任一生产WASI/gVisor/microVM backend已实现该操作。

CR-146已交付独立`insight-platform-sandbox-wasi` Executor adapter；Wasmtime/Cranelift只存在于该crate，API、Scheduler、
Orchestration和generic Worker依赖图不包含JIT/runtime。adapter通过Artifact Broker读取并重验module、以bounded local semaphore
限制并发、执行13.1的closed ABI、调用exact output-schema validator与grant revoker，并实现prepare/start/collect/terminate/destroy/
abort/expired-lease recovery。engine关闭全部proposal后仅开启本合同实际需要的`FLOATS | MUTABLE_GLOBAL`特征位；不得把Wasmtime的
parser-only `GC_TYPES`默认位或任何post-WASM1 proposal误当成本ABI的一部分。重启后的recovery若没有进程内execution，只能在独立
process-generation isolation authority证明旧WorkerProcessGeneration已终止后提交Destroyed，当前generation缺失本地状态必须fail closed。
10项真实Wasmtime conformance覆盖engine feature contract、canonical JSON成功、任意import与post-WASM1指令拒绝、fuel耗尽、I/O超限、
memory-page与Job memory envelope耦合、运行中epoch interrupt等待guest退出、process-generation absence proof以及single-use cleanup/grant
revoke。该开发期证据仍未包含生产Artifact/
Secret broker实现、独立Executor进程/Pod、gVisor/microVM、Linux escape suite或CandidateManifest，不能声明Phase 4/6通过。

production Firecracker Provider现已接入Controller Artifact Broker：每个请求按exact tenant、Sandbox Job/request、Executor与Provider
process generation、sandbox identity、lease、deadline读取runtime bundle及可选Artifact-backed输入，Provider再次核对完整ArtifactRef长度和
SHA-256；主逻辑输入必须持有exact `read_whole` grant。通过guest Ready fence后，Provider以不超过1 MiB的canonical、digest-bound chunk在
private vsock依序交付一次性runtime/input materialization，最后才发送同一request fence的execute command；materialization与execute envelope
digest共同进入start evidence。已安装且合同闭合的`managed_mcp_server` runtime可由该Provider选择。定向domain/config/protocol/socket fixture
实际通过，但尚无真实Linux KVM/jailer/guest-agent互操作、进程终止/恢复、escape或饱和证据，因此不把microVM backend或Phase 4/6标记为完成。

生产部署合同现增加独立`executor-microvm` DaemonSet，只调度到带专用label与taint的KVM Linux node pool。非root Executor与root
Firecracker Provider在同一node-local Pod内仅共享mTLS保护的Unix socket；只有Provider挂载`/dev/kvm`、host cgroup、持久化jail/state
目录并取得逐项allowlist的Linux capability，Executor不挂载这些路径或Provider credential，Provider也不挂载Executor/NATS/attestor
credential。两者使用不同mTLS Secret，Pod不挂service-account token；默认deny NetworkPolicy只开放Controller、NATS、DNS以及为Managed
Secret预留的Egress Broker边。ValidatingAdmissionPolicy逐容器拒绝KVM、hostPath、capability或credential边界漂移，静态部署门禁同时验证
四个workload、六条NetworkPolicy、immutable image、closed JSON及`max_concurrency <= maximum_instances`。这只是production-equivalent
topology合同，未在CandidateManifest绑定的真实KVM node上运行，不能替代上述Linux资格证据。

Managed stdio Resource subscription的durable authority现已复用共享23表交付到Ready提交，不增加表或migration。closed `SandboxJobPayload`以
`capability_execution | managed_mcp_subscription_session`区分两种物理workload；Managed variant冻结逻辑subscription/Job、session
generation、MCP/Discovery/Auth closure、Runtime/Package/Profile/Policy、Artifact/Secret grant、四维资源reservation、deadline与callback。
PostgreSQL在一个事务内验证并锁定全部exact authority，双向写入逻辑session link与唯一Sandbox Job，将逻辑MCP Job停回Waiting，并提交
Receipt/Event/Outbox。专用claim按closed workload discriminator领取，普通Sandbox claim对其不可见；并发claim只有一个lease winner。
fenced `Preparing`提交Executor/Attestor evidence，`Starting`原子推进逻辑session为`Initializing`，Ready事务同时推进逻辑
`Active/Ready`与物理`Running`。加密opaque session只存逻辑Invocation，物理Job只保留credential-free ready binding，避免复制current
session状态。全新PostgreSQL 16 fixture覆盖admission唯一winner/replay/idempotency drift、双向identity、grant/quota/canary、队列隔离、
claim first-winner、phase replay/stale fence与双状态Ready的Receipt/Event/Outbox原子性。普通MCP subscription Worker同时已fail closed为
Streamable HTTP only。新增的Sandbox establishment Worker以closed Provider port强制`Preparing提交 -> prepare -> Starting提交 ->
initialize但不放行通知 -> Ready提交 -> 同一prepared instance activation`；所有provider evidence绑定request、lease、Worker、Executor及
sandbox identity，任一post-prepare合同、authority或provider失败都必须先调用exact destroy。两个unit fixture覆盖成功顺序和Ready提交失败
销毁且不activation。cleanup port现可在prepare响应丢失时按exact request/fence、无prepared evidence执行销毁。独立Managed session
authority internal gRPC已在Controller组合，先由node attestor校验登记，再只允许exact microVM Executor URI SAN执行claim/phase/Ready；
Executor library的专用claim driver与普通Sandbox共享同一`LocalWorkerPools`，先保留本地容量再claim，并在长生命周期command future结束前
保持permit。microVM Artifact RPC现逐字转发closed请求，不再转换成会丢失Provider/sandbox/workload identity的WASI请求；统一
`BrokeredSandboxArtifactBroker`为WASI与microVM共享object store、KMS unseal、两阶段authorization和单一in-flight bulkhead，但分别调用
各自typed PostgreSQL authority。Managed session只可在`Starting`用exact active package grant读取runtime bundle；grant revoker按closed
workload区分有限Capability与长生命周期session，并对Managed Job/request/attempt/lease/Executor及Ready sandbox identity执行幂等回收。
全新PostgreSQL 16 Managed fixture及既有Sandbox回归fixture均实际通过，不增加表或migration。mTLS authority、Executor pool和Sandbox
domain定向测试分别9、3、33项通过。真实Managed microVM session Provider、guest Artifact/Secret注入和同实例activation现已进入独立
Provider进程；Managed authority新增非事件化exact-fence heartbeat，domain/RPC测试通过，fresh PostgreSQL fixture已编译但因本机Docker
daemon无响应未取得实际运行证据。Sandbox domain establishment Worker现会在Provider prepare/initialize/activate等待期间按profile
续租，并把最新version fence串行传给下一phase；heartbeat失败后先等待Provider调用收敛，再对任何已创建实例执行exact destroy，避免
取消中的RPC留下孤儿microVM。长期liveness heartbeat、terminal supervisor仍未组合进Executor进程，也没有terminal/session-loss recovery
或真实进程资格，
因此不关闭Phase 4。

共享Sandbox Job表上的有限Capability expired-lease scan现强制按closed `workload_kind=capability_execution`过滤，不会把Managed session
payload误送入有限执行解码器；Managed session仍须由其专用terminal/absence recovery扫描处理。

Managed session新增独立fenced lost commit：exact Provider cleanup evidence、最新物理Job fence、usage reservation和四个terminal quota
ledger identity共同进入请求摘要；PostgreSQL同一事务把物理Job写为`Lost/ReconciliationRequired`并清除lease，保守结算未知CPU/output、
确认Artifact grant已释放，随后才清除逻辑session/link、设置full reconcile并重排逻辑MCP Job，Receipt/Event/Outbox与双状态变化原子提交。
该authority已贯穿domain、PostgreSQL与internal gRPC且不增表/migration；Provider liveness/cleanup RPC、长期Executor supervisor和expired
lease absence worker仍Open。

Managed session的一次性Secret交付现已实现为两阶段、双平面协议。microVM Provider只以exact workload URI SAN调用Egress；Egress以自身
workload identity调用Sandbox Controller执行reserve与commit，并在两者之间通过既有Security Authority、KMS和Secret Provider解析材料。
Controller在reserve和commit均锁定并复验同一Managed Sandbox Job、request/attempt、当前lease、Executor、Provider process generation、
sandbox identity、完整prepared canonical digest和active ScopedSecretGrant。只有fresh reserve后紧接fresh commit的调用可以释放bytes；
任何重放、响应丢失、过期、generation/fence漂移或已达`maximum_reads`都fail closed。read次数使用共享`receipts` authority计数，commit写入
同一Receipt及Event/Outbox，且不推进Job version，避免使正在执行的Worker fence失效。该设计保持Controller不见明文、Egress无数据库
credential、Provider无数据库/KMS/Secret Manager权限，并继续保持23表与单一`0001` migration。guest内注入与真实Managed session
Provider的生产组合仍未交付，因此本切片不关闭Phase 4。

process-generation isolation authority是独立于PostgreSQL lease、NATS和Controller进程的node/runtime attestor；数据库lease过期、NATS
断连、Pod deletion request、Controller本地cache miss或对旧generation RPC超时都不是absence proof。其closed请求必须精确绑定
`tenant_id`、`sandbox_job_id`、`request_digest`、旧`worker_process_generation_id`和已提交到Sandbox Job phase evidence中的
`executor_identity_digest`。成功回执固定包含同一组字段、`observed_at`、CandidateManifest安装的`attestor_identity_digest`、
`process_absent | node_quarantined`二选一的处置和覆盖全部字段的canonical digest。`process_absent`表示attestor从对应runtime/cgroup
authority确认旧execution process tree不存在；`node_quarantined`表示该exact node identity已由placement、network和storage authority
共同隔离且不能继续或重新取得任何Sandbox grant。仅Kubernetes API对象不存在、节点NotReady或网络不可达不能单独产生任一处置。
回执字段漂移、未来时间、unknown处置、attestor未安装/不可用均fail closed并保留reconciliation；该回执不是新的current-state或
持久化表，只作为既有Sandbox recovery Event中的bounded evidence digest。

absence请求只有在`executor_identity_digest`能反查到上述attestor登记，且该登记的generation、node、runtime/cgroup与请求完全
一致时才可评估。attestor必须从同一node/runtime authority检查登记的process-start identity已经不存在，防止PID/cgroup复用；
`node_quarantined`还必须引用同一登记中的node UID并验证网络、存储和grant隔离共同完成。Controller、Executor或数据库都不能创建、
补写或推断这份登记。

registration与Controller verify/absence使用两个独立listener。registration listener是同节点Unix socket上的mTLS HTTP/2，服务端将
TLS URI SAN和内核peer credentials共同写入不可序列化的本地观察上下文后才调用attestor authority；wire envelope中不存在PID/UID/GID、
Pod UID、node UID、cgroup或process-start字段。Controller listener是集群内mTLS TCP，只允许
`spiffe://insight.platform/workload/sandbox-controller`，不能注册或刷新generation。Unix socket文件必须由attestor创建在只读挂载给
Executor的专用目录，owner/group/mode固定，不能暴露为Service、host-wide writable socket或跨节点volume。

DaemonSet每节点只拥有本节点登记，因此sealed登记回执还必须携带`attestor_route`：它是canonical、无path/query/credential的
`https://<private-node-ip>:<fixed-host-port>`，与`executor_identity_digest`及完整evidence digest共同封印。Executor在claim携带该route，
Controller只在CandidateManifest冻结的Sandbox node CIDR和fixed host port内建立mTLS连接；DNS、public/link-local/loopback IP、端口漂移、
普通Service负载均衡和不在allowlist的route一律拒绝。首次`Preparing`把route随现有Sandbox Job payload原子保存，expired-lease scan从
同一payload返回，verify/absence请求必须逐字段回绑；不增加route表、中心registry或Kubernetes API查询权威。node replacement使旧
route不可达时保持reconciliation，不能尝试另一节点或把unreachable解释为absence；attestor Pod重启继续复用同一node route及本地registry。

## 29. 验收标准

- 任意脚本/MCP stdio server 都无法在平台服务 Pod 创建进程；
- ModelGenerated、Shell、Secret、network workload 必定进入 microVM，无法由 request 降级；
- plain runc、privileged、hostPath、Docker socket、service-account token fixture 全部被 admission 拒绝；
- Sandbox Job durable defer 后释放 Capability Worker permit；
- Sandbox 队列/CPU/memory 饱和不降低 API/Scheduler/Model/MCP 的准入能力；
- Job duplicate/crash/timeout/cancel/late result 只有一个 terminal receipt；
- Pure lost Job 可安全新 Attempt，有 Effect lost Job 进入 reconciliation；
- package dependency 在执行时不访问 registry/network；
- Artifact/Secret grant 只对单 Job/port/purpose/deadline 有效；
- resource/network/file/output 所有硬限制可由可复现 fixture 触发；
- warm clone 不携带前一 Job 数据，cleanup 失败自动隔离 node；
- escape、Secret、network 和 output canary 不进入宿主或其他 tenant；
- Runtime suspension 能停止新 Job，drain/kill/recovery 有 runbook 和审计。

## 30. 明确推迟的工作

- GPU、训练、分布式/HPC 和超过平台短任务上限的计算；
- 任意用户 Docker image 与 package manager runtime install；
- SSH、交互 shell、notebook kernel 和后台 service；
- Windows/macOS guest runtime；
- live migration 与跨地域 Job continuation；
- 执行后复用同一 tenant microVM；
- speculative multi-executor racing；
- 用户可定义 seccomp/device/kernel 参数。

## 31. 未决问题

没有阻止 Artifact、API 或 Qualification 设计的未决问题。具体 microVM VMM 可以在通过同一 conformance 后替换，
但 ModelGenerated/Shell/Secret/network 的最低 MicroVm 等级、独立 node pool 和平台服务不执行代码的边界不可
降低。
