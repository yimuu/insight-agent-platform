# Platform v2 Sandbox Execution Plane 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 |
| 日期 | 2026-09-01 |
| 依赖 | 03、04、07、09、10、15、[ADR-0007](../../adr/0007-opensandbox-execution-provider.md) |
| 直接下游 | 17、18 |

> CR-216 clean-cut：首版physical provider只有OpenSandbox Server + Docker/runc。restricted WASI、Wasmtime Executor、
> gVisor Pod Launcher/guest、process attestor与其Kubernetes admission/RBAC不进入目标composition。shared Job、Invocation、
> Artifact与terminal transaction authority不变。

## 1. 决策摘要

Sandbox Execution Plane是独立的不受信任代码执行服务。API、Scheduler、Model Worker、Capability Worker和MCP Host不得
spawn Python、Node.js、WASM或Shell；它们只创建带durable shared Job的Sandbox Capability Invocation。

首版只有一条物理链：

```text
Capability Invocation owner
        |
        | shared Job: attempt / lease / fence / cancel / terminal
        v
Sandbox Dispatcher
        |
        | internal authenticated OpenSandbox lifecycle API
        v
OpenSandbox Server
        |
        +-- Docker/runc ephemeral sandbox
        +-- immutable image + fixed runner entrypoint
```

OpenSandbox完整替代平台自建的physical runtime implementation，但不替代Platform business authority。Docker/runc是显式
first-release provider，不是其他runtime失败后的fallback。目标隔离等级是developer preview；实现完成前不得宣称强多租户或
production-qualified。

## 2. 目标与非目标

### 2.1 目标

- 跑通`Job -> provision/start -> observe -> validate -> fenced commit -> cleanup`；
- Python、Node、WASM和published trusted Shell统一使用immutable OCI image；
- create并发、response loss和OpenSandbox重启时不重复创建/启动同一physical attempt；
- Dispatcher迟到结果不能越过Job lease fence；
- 允许exact Sandbox Profile选择direct outbound network；
- 以TTL、delete与orphan sweep最终回收container和scratch；
- 保持Sandbox与API、Orchestration、Model、MCP及其他Capability容量隔舱。

### 2.2 非目标

- 不提供WASI/gVisor/Kata/Firecracker/microVM或host process backend；
- 不提供interactive PTY、code interpreter context、pause/resume/snapshot、persistent session或public endpoint；
- 不在运行时安装dependency、解释任意shell command或执行Skill目录文件；
- 不解析workload对第三方API的HTTP method、读写或事务语义；
- 不为workload网络/数据库/消息副作用提供idempotency或exactly-once；
- 不让OpenSandbox或public client修改Run、Invocation、Job、Receipt、Event或Outbox。

## 3. 术语与信任边界

| 术语 | 定义 | authority |
|---|---|---|
| Sandbox Job | `WorkClass::Sandbox`的shared durable Job | 唯一业务work/attempt/lease/terminal authority |
| Sandbox Dispatcher | 唯一claim Sandbox Job并调用OpenSandbox的Platform Worker | current Job fence验证与owner terminal transaction入口 |
| OpenSandbox Server | lifecycle API、physical store与Docker provider | sandbox/container/runner/log physical state only |
| Physical attempt | 一个provisioning key对应的单个ephemeral sandbox | 外部evidence，不是业务aggregate |
| Sandbox Package | publication产生的immutable OCI image与package evidence | ResourceVersion/Artifact authority |
| Fixed runner | image内按digest冻结的Platform runner与argv entrypoint | 只执行published package contract |

PostgreSQL是Job/Invocation/Run唯一业务authority。OpenSandbox store可以持久化sandbox lifecycle、metadata与provisioning receipt，
但不能投影为业务current state。Dispatcher拥有最小Sandbox Job repository port；OpenSandbox没有Platform数据库、NATS、Artifact store、
Run/Invocation RPC或public API credential。

OpenSandbox lifecycle/execd API只绑定内部network和认证key/token。仅Dispatcher的provider adapter可在内存中持有它们；不得持久化到
Job、Event、Outbox、log或trace。Agent、Skill、Model、MCP、普通Capability Worker和public client不得获得endpoint或credential。

## 4. 领域模型

```rust
enum SandboxProviderKind {
    OpenSandbox,
}

enum SandboxNetworkMode {
    Disabled,
    Direct,
}

struct SandboxRuntimeContractV1 {
    schema_version: ConstU16<1>,
    provider: Const<SandboxProviderKind::OpenSandbox>,
    opensandbox_release_digest: Sha256Digest,
    lifecycle_schema_digest: Sha256Digest,
    provisioning_extension_digest: Sha256Digest,
    execd_read_schema_digest: Sha256Digest,
    docker_runtime_digest: Sha256Digest,
    runner_contract_digest: Sha256Digest,
}

struct SandboxProfileDeploymentClosureV1 {
    schema_version: ConstU16<1>,
    profile_revision: ExactSandboxProfileRevisionRef,
    runtime_revision: ExactSandboxRuntimeRevisionRef,
    provider_binding_digest: Sha256Digest,
    network_mode: SandboxNetworkMode,
    limits: SandboxLimitsV1,
    secret_injection: DisabledOrQualifiedBinding,
    qualification_evidence: ArtifactRef,
}
```

`SandboxProviderKind`是首版closed singleton。unknown provider、WASI/gVisor/microVM/host variant、runtime fallback、mutable release、
schema drift或provider binding drift在Job claim前fail closed。

Sandbox Runtime与Package是02的definition-only ResourceVersion；Sandbox Profile通过immutable Deployment固定可执行环境。
Skill中的文件不会自动成为Package，Package也不能绕过Capability Deployment被直接执行。

## 5. Rust 所有权接口

```rust
trait SandboxJobRepository {
    async fn claim(&self, request: SandboxClaim) -> LeasedSandboxJob;
    async fn heartbeat(&self, fence: SandboxJobFence) -> SandboxJobFence;
    async fn record_physical_evidence(
        &self,
        command: RecordSandboxEvidence,
    ) -> SandboxJobFence;
    async fn commit_terminal(&self, command: CommitSandboxTerminal) -> CommitReceipt;
    async fn recover(&self, observation: SandboxRecoveryObservation) -> RecoveryDecision;
}

trait OpenSandboxProvider {
    async fn create_idempotent(
        &self,
        request: OpenSandboxCreate,
    ) -> OpenSandboxInstance;
    async fn observe(&self, sandbox_id: OpenSandboxId) -> OpenSandboxObservation;
    async fn read_bounded_result(
        &self,
        sandbox_id: OpenSandboxId,
        exact_path: FixedResultPath,
        limit: ResultFrameLimit,
    ) -> BoundedResultFrame;
    async fn terminate(&self, sandbox_id: OpenSandboxId) -> TerminationObservation;
    async fn prove_absent(&self, sandbox_id: OpenSandboxId) -> AbsenceObservation;
    async fn list_orphans(&self, cursor: OrphanCursor) -> BoundedPage<OpenSandboxMetadata>;
}
```

`OpenSandboxProvider`实现不能依赖Platform repository或领域service。`SandboxJobRepository`不能包含Docker/OpenSandbox SDK类型。
两者只通过本规范closed DTO组合。

## 6. Execution Request 与持久化合同

```rust
struct SandboxExecutionRequestV1 {
    schema_version: ConstU16<1>,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    job_id: JobId,
    lease_generation: u64,
    physical_attempt: u32,
    worker_process_generation_id: ProcessGenerationId,
    package_version_id: ResourceVersionId,
    image_digest: OciImageDigest,
    runtime_version_id: ResourceVersionId,
    sandbox_profile_deployment_id: DeploymentId,
    runner_argv: BoundedArgv,
    package_argv: BoundedArgv,
    input: InlineCanonicalJson,
    input_schema_digest: Sha256Digest,
    output_schema_digest: Sha256Digest,
    network_mode: SandboxNetworkMode,
    limits: SandboxLimitsV1,
    deadline_at: Timestamp,
    trace: TraceIdentityV1,
    request_digest: Sha256Digest,
}
```

request是closed、canonical、有size/depth/field count limit的immutable snapshot。`request_digest`是semantic execution closure
摘要，排除自身、`lease_generation`、`physical_attempt`、`worker_process_generation_id`、lease token/expiry/version等current fence
字段以及trace correlation identity；heartbeat、lease接管与每一物理hop的trace变化都不改变它。

首条CR-216流程的input/output为有界canonical JSON。input由OpenSandbox create environment中的固定runner frame注入，
不得包含Secret；runner把一个domain-separated result frame原子写入Runtime合同冻结的固定路径。文件与大输出仍必须走15的Artifact服务，但
Sandbox Artifact port在本修订的首条实现批次中不可activate，超过Inline上限返回`sandbox_output_too_large`而不静默截断。

Platform业务数据库只在shared Job的bounded binding/evidence中保存request digest、provisioning key digest、OpenSandbox ID、
physical attempt、可选runner observation digest与cleanup状态；不复制OpenSandbox lifecycle state、log正文或container配置。

## 7. OpenSandbox provisioning 幂等机器合同

```rust
struct SandboxProvisioningKeyV1 {
    schema_version: ConstU16<1>,
    tenant_id: TenantId,
    job_id: JobId,
    physical_attempt: u32,
    provisioning_lease_generation: u64,
    execution_request_digest: Sha256Digest,
}
```

wire key是`sha256("insight.sandbox.provision/v1\0" || canonical_cbor(key))`的lowercase opaque encoding。
Dispatcher调用OpenSandbox create必须携带：

```text
Idempotency-Key: <opaque provisioning key>
X-Insight-Request-Digest: sha256:<canonical create request digest>
```

OpenSandbox发行物必须实现：

- 同key + 同digest（包括并发与response-loss重放）只创建一个sandbox并返回同一`sandbox_id`；
- 同key + 不同digest返回`409 provisioning_idempotency_conflict`且零新建；
- sandbox create与receipt唯一性在同一provider transaction/atomic operation中成立；
- OpenSandbox重启后仍能按key恢复result；receipt TTL不得早于sandbox absence proof和Platform最大reconcile窗口；
- metadata保存opaque Job digest、provisioning lease generation、physical attempt、key digest、execution request digest与create request
  digest，用于有界reconcile；
- `list(metadata) -> create`、Dispatcher内存map或“create失败就换key”均不满足合同。

当前上游公开Lifecycle API 0.1.0没有足以依赖的client-create幂等合同。固定发行物必须在OpenSandbox自身fork/plugin/provider
层补齐上述扩展并发布schema/conformance digest；该扩展是实现前置，不是可选优化。

## 8. OpenSandbox create 映射与runner合同

每个physical attempt使用一个OpenSandbox create：

- `image.uri`必须是publication冻结的OCI digest，拒绝tag；
- `entrypoint`必须等于Runtime证据中的`runner_argv` array，不允许调用方字符串或`sh -c`；
- `package_argv`来自published Package evidence，由runner以argv直接启动，不经shell解析；
- `timeout`不晚于Job deadline且不超过Profile wall-time hard limit；
- CPU、memory、pids和filesystem限制只能在Profile以内缩小；
- `networkPolicy`由Profile的`Disabled | Direct`规范映射，Invocation不能覆盖；
- metadata只含opaque、bounded、非Secret correlation digest；
- snapshots、volumes、lifecycle hooks、secure/public access和ingress endpoint首版拒绝。

fixed runner从单一bounded environment frame读取canonical input，验证schema/digest，按`package_argv`执行一次，并产生：

```rust
enum SandboxRunnerFrameV1 {
    Started { request_digest: Sha256Digest },
    Succeeded { output: InlineCanonicalJson, output_digest: Sha256Digest },
    Failed { class: SandboxFailureClass, diagnostic_digest: Sha256Digest },
}
```

frame有magic/version/length/digest并原子rename到固定result path；runner随后保持存活直到delete/TTL。Dispatcher只能通过内部execd
`GET /files/download`读取该exact path，并强制HTTP Range/result hard limit；不能列目录、接受调用方path或使用upload/delete/rename/replace/
permission API。未匹配frame、重复terminal frame、尾随bytes、非法UTF-8/JSON、schema mismatch、digest mismatch或越界全部失败。
raw stdout/stderr只是有界敏感diagnostic，不是结果authority。

OpenSandbox execd的shell command、PTY、code context、filesystem mutation和public endpoint API不进入首版Platform调用面；execd command
API的请求幂等、command ID与重试语义由OpenSandbox provider自己负责，本规范不复制也不依赖。Platform只使用上述exact read-only result
method，避免把上游shell-string API提升为可调用Capability合同。

## 9. 状态机与Job terminal commit

```text
Job Ready
  -> Dispatcher claim/lease
  -> validate exact Deployment/Package/Profile/provider manifest
  -> persist provisioning intent/key digest
  -> OpenSandbox create-idempotent (container starts fixed runner)
  -> persist sandbox ID/physical evidence under current fence
  -> observe lifecycle + read exact bounded runner result frame
  -> validate output/schema/digest
  -> owner terminal transaction rechecks current Job fence
  -> Job + Invocation + RunValue + quota + Event/Outbox terminal
  -> terminate/delete -> absence proof
```

OpenSandbox `Running/Terminated/Failed`、container exit与diagnostics都只是physical evidence。Dispatcher提交terminal时必须重新加载并
CAS验证tenant、Invocation owner、Job ID/state/version、lease generation/token、worker process generation、physical attempt、request digest、
deadline/cancel intent和quota reservation。任一漂移零业务写入；late success不能覆盖新generation或已terminal owner。

terminal business commit可以先于physical absence，但必须立即形成durable cleanup intent。cleanup失败不能撤销已提交业务结果；它由
bounded recovery继续，且会阻止相同physical location复用。

## 10. 幂等、并发与背压

首版只承诺以下三项Platform范围幂等：

1. public/internal command Receipt：同一Platform命令只创建一个逻辑Invocation；
2. OpenSandbox provisioning：同一physical attempt只创建并启动一个sandbox；
3. Job terminal commit：current fence first-winner，重复提交返回既有safe result或stable stale conflict。

Sandbox workload经网络、数据库、消息或第三方API执行的读写、副作用、transaction与idempotency属于Package及目标服务责任。
平台不检查HTTP method、不注入目标API idempotency key、不维护外部写receipt、不提供cross-system exactly-once。

Dispatcher在claim前预留local slot与tenant/WorkClass quota。OpenSandbox API、Docker active container、启动中container及diagnostic bytes
分别有hard concurrency/queue/byte limit。OpenSandbox饱和只返回Sandbox capacity/backpressure，不使API、Scheduler、Model、MCP、
Artifact或critical-control readiness失败。

## 11. 超时、重试、取消与恢复

- create response loss只用同一provisioning key重放并取得同一sandbox；
- 在provider证明原key没有创建sandbox且runner未开始前，owner policy可以创建新的physical attempt；
- 一旦sandbox存在或runner可能开始，不自动创建新key/新sandbox执行相同workload；
- Dispatcher crash/reclaim先按已持久化sandbox ID或key metadata observe，不能先create；新lease接管时复用旧provisioning key和
  physical evidence，不以current lease generation重算旧key；
- 结果可证明时，新Dispatcher以自己的current Job commit fence提交既有physical evidence；旧provisioning lease只用于绑定该physical
  attempt，不要求等于提交时current lease；结果不可证明时进入`UnknownOutcome`；
- cancel/timeout先写durable intent，再调用terminate/delete；物理终止是best effort，不把已发生workload副作用改写为未发生；
- OpenSandbox不可达时保留current Job/reconcile状态并做bounded probe，不盲目重发workload；
- orphan sweeper分页扫描opaque metadata，回到Platform重验Job后才delete；OpenSandbox自行TTL是最后保护，不推进业务状态。

只有provisioning和pre-start dependency失败是自动retry安全区。任何可能已运行的workload都不因其Capability Effect声明而自动重跑。
新用户显式提交的Run是新的业务执行，不复用旧provisioning key。

## 12. Artifact、网络、Secret与安全

### 12.1 Artifact

OpenSandbox没有object-store credential。首条实现只处理Inline canonical input/output；未来Sandbox Artifact port必须通过共享Artifact Data
Worker，在owner Job开始前预分配identity/quota，并在terminal owner事务形成Ready Link。不得把OpenSandbox volume、snapshot或host bind
mount当作Artifact authority。

### 12.2 网络

`Disabled`拒绝outbound；`Direct`使用Docker bridge允许普通DNS/IP/URL访问。Direct流量不经过Platform Egress Broker，因此平台不提供
endpoint allowlist、DNS pinning、TLS root、redirect、request/response byte accounting或第三方credential语义。Profile必须显式记录这一
风险；首版不开放inbound/public endpoint，禁止host network、metadata endpoint与Docker/runtime socket。

### 12.3 Secret

默认`secret_injection=disabled`。需要Secret的Deployment只有在固定OpenSandbox发行物实现独立、qualified、one-shot secret injection
binding后才能activate；否则返回`secret_injection_unsupported`。Secret value不得进入create environment、metadata、input、diagnostics、
result、OpenSandbox store或public error。CR-216不以“允许网络”为理由放宽Secret authority。

### 12.4 Container security minimum

即使首版允许runc，也必须禁止privileged、host PID、host network、hostPath、device、Docker/containerd socket、Platform/Kubernetes
credential和任意capability追加；启用`no-new-privileges`、Docker default seccomp/capability drop、non-root runner、resource/pids limit与
ephemeral scratch。满足这些minimum不等价于gVisor/microVM隔离。

## 13. 可观测性与隐私

最低fixed metrics：

- Job ready/expired-lease count与oldest age；
- Dispatcher claim、provision、observe、commit、terminate outcome和latency；
- provisioning replay/conflict、stale fence、unknown outcome；
- OpenSandbox API capacity、active sandbox、TTL expiration、delete/absence/orphan cleanup；
- runner exit/failure class、stdout/stderr/result bytes与limit violation。

metric label只使用component role、operation、outcome和safe failure class。tenant、Job、Invocation、sandbox ID、image、URL、package、
path与Secret不进label。log/trace只记录opaque digest和bounded safe code，不记录input/output、stdout/stderr、environment或external URL。

Dispatcher从durable Job恢复trace ID并生成per-hop span；内部OpenSandbox请求可以携带新span correlation，但不得把Platform trace header
注入workload environment或向workload第三方请求传播。

## 14. 配置与部署

目标最小composition：

- 独立Sandbox Dispatcher workload identity、Sandbox Job DB role、NATS consumer、DB pool、permit与NetworkPolicy；
- 独立OpenSandbox Server identity/API key、persistent physical metadata volume、Docker provider与API capacity；
- Docker daemon仅对OpenSandbox Server可达；Dispatcher、Gateway、Scheduler及普通Worker不得挂载socket；
- OpenSandbox只接受Dispatcher source/audience，且出站仅用于Docker/provider所需控制；
- sandbox container使用bridge network；Direct/Disabled由exact Profile create request映射；
- startup固定OpenSandbox release/image/schema/extension/runner/Docker config digest，漂移使readiness fail closed。

单节点developer profile可以使用持久SQLite作为OpenSandbox physical store，但必须持久化volume并通过restart幂等测试。它不支持
OpenSandbox control-plane HA声明。该profile的exact built-in Sandbox Profile冻结`network_mode=Direct`以跑通联网workload；需要断网的
Capability必须发布并绑定显式`Disabled` Profile，不能依赖runtime default。未来共享provider store或多副本方案不得复用Platform业务表。

## 15. 测试矩阵

| 层级 | 必须证明 |
|---|---|
| L1 contract/unit | closed request/profile、digest、fixed argv、result frame、limits、network mapping、unknown field/provider rejection |
| L2 PostgreSQL | concurrent claim、lease rollover、stale terminal、cancel/timeout、evidence CAS、quota settlement、orphan decision |
| L3 process/provider | same-key concurrent create、response loss、OpenSandbox restart、Dispatcher kill/reclaim、runner started窗口、delete/TTL/absence |
| L3 network/security | Direct可出网、Disabled零出网、无public ingress、wrong API key/source、无Platform DB/socket/credential、bounded diagnostics |
| L4～L6 release | production topology、strong isolation、capacity/chaos/soak/restore/promotion；CR-216均Not run |

负向fixture必须包括：same key/different digest、metadata-list race、mutable tag、wrong image/runner/schema digest、shell-string override、
privileged/host mount/socket、oversized input/result/log、duplicate terminal frame、OpenSandbox late success、old lease cleanup与orphan误删保护。

## 16. 验收标准

- API、Scheduler、Model、MCP和普通Capability Worker没有spawn runtime或Docker/OpenSandbox credential；
- OpenSandbox是唯一首版provider，WASI/gVisor/microVM/host fallback不存在；
- 同一provisioning key在并发、response loss和OpenSandbox restart后只有一个sandbox ID与一次runner start；
- Dispatcher terminal提交重验current Job lease fence，旧generation/错owner/错request digest零业务写入；
- OpenSandbox没有Platform DB、Run/Invocation mutation、Artifact store或public API权限；
- runner只执行published fixed argv；Dispatcher除exact result read外不能访问runtime installer、shell-string execd、filesystem mutation、PTY、snapshot或public endpoint；
- Direct network通过且Disabled network fail closed；二者都不获得host network/socket/credential；
- workload第三方副作用不被描述为Platform幂等，可能已开始的执行不自动重发；
- cancel、timeout、Dispatcher/OpenSandbox kill与TTL后最终取得absence proof或持续告警；
- Sandbox容量饱和不影响API、Scheduler、Model、MCP、Artifact或critical-control准入。

## 17. 明确推迟的工作

- restricted WASI、gVisor、Kata、Firecracker、microVM、KVM、GPU、privileged workload与host process；
- strong multi-tenant isolation与production OpenSandbox HA；
- Sandbox Artifact input/output port首条实现、large result与direct object-store access；
- OpenSandbox Credential Vault/Secret injection；
- Platform-managed Sandbox egress allowlist、TLS/SSRF/redirect broker；
- interactive command/code context、PTY、pause/resume/snapshot、persistent/warm tenant sandbox与public endpoint；
- managed MCP stdio、heavy compute、cross-region execution和runtime installer；
- workload外部API idempotency与cross-system exactly-once。

## 18. 未决问题

没有阻塞架构的问题。实现前唯一硬前置是固定OpenSandbox发行物必须提供第7节的atomic provisioning extension；若上游版本缺失，
在OpenSandbox自身fork/plugin内补齐并通过conformance，不得降级为Dispatcher内存去重或metadata list/create。

具体OpenSandbox commit、OCI/Docker版本、digest、limits与developer profile资源数值由18的实现/资格产物冻结；在这些门禁通过前，
本规范Accepted只表示目标合同完成cross-review，不表示当前WASI/gVisor实现已被替换或OpenSandbox production-ready。
