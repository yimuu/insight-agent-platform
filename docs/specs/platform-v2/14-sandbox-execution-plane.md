# Platform v2 Sandbox Execution Plane 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 revision 2 |
| 日期 | 2026-09-02 |
| 依赖 | 03、04、07、09、10、15、[ADR-0007](../../adr/0007-opensandbox-execution-provider.md) |
| 直接下游 | 17、18 |

> CR-216 clean-cut：首版 physical provider 只有 OpenSandbox Kubernetes provider + BatchSandbox Controller +
> containerd/runc。restricted WASI、Wasmtime Executor、自建 gVisor Launcher/guest、process attestor、Docker provider、
> host process 与相关 fallback 不进入目标 composition。shared Job、Invocation、Run、Artifact 与 terminal transaction
> authority 不变。OpenSandbox 源码不需要修改。
>
> revision 2关闭实现前P1：Job JSON不复制input/result body；exact RunValue负责正文，terminal transaction原子写output；业务
> Job lease清除后，Dispatcher只用同一Job physical evidence中的独立cleanup generation fence继续delete/absence，不新增表或aggregate。

## 1. 决策摘要

Sandbox Execution Plane 是独立的不受信任代码执行服务。API、Scheduler、Model Worker、Capability Worker 和 MCP Host 不得
spawn Python、Node.js、WASM 或 Shell；它们只创建带 durable shared Job 的 Sandbox Capability Invocation。

首版只有一条物理 provisioning 链：

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
        v
Kubernetes API -> BatchSandbox Controller -> Pod -> containerd/runc
                                                immutable Armed runner
```

OpenSandbox 替代平台自建 physical runtime implementation，但不替代 Platform business authority。Kubernetes/containerd-runc
是显式 developer-profile runtime，不是其他 runtime 失败后的 fallback。目标隔离等级是 developer preview；CR-216 不宣称
strong multi-tenant、HA 或 production-qualified。

公开 OpenSandbox create 不提供 client-side atomic idempotency key。本规范不 fork 上游，而是把物理创建与有副作用的 Package
启动分开：create 只生成 inert `Armed` candidate，PostgreSQL 在 current Job fence 下选择唯一 candidate，随后 fixed runner
以 durable one-way activation latch 最多启动一次 Package。

## 2. 目标与非目标

### 2.1 目标

- 跑通 `Job -> inert candidates -> select -> activate -> observe -> validate -> fenced commit -> cleanup`；
- Python、Node、WASM 和 published trusted Shell 统一使用 immutable OCI image 与 fixed runner；
- 在 create 并发、response loss、provider/controller 重启和 Dispatcher reclaim 后，最多激活一次 Package；
- runner 可能启动后绝不创建新 token、candidate、sandbox 或自动重跑 workload；
- Dispatcher 迟到结果不能越过 current Job lease fence；
- 单节点 developer Profile 默认 `Direct`，同时支持显式 `Disabled`；
- 以 delete、TTL、orphan decision 与 absence proof 最终回收 BatchSandbox、Pod 和 scratch；
- 保持 Sandbox 与 API、Orchestration、Model、MCP 及其他 Capability 容量隔舱。

### 2.2 非目标

- 不保证同 token 历史上只创建一个 inert candidate；候选只能有限、无 Package 副作用并最终清理；
- 不提供 Docker provider、WASI、gVisor、Kata、Firecracker、microVM 或 host-process backend；
- 不提供 Pool、跨 Job sandbox reuse、interactive PTY、code context、pause/resume/snapshot、persistent session 或 public endpoint；
- 不在运行时安装 dependency、解释任意 shell command 或执行 Skill 目录文件；
- 不解析 workload 对第三方 API 的 HTTP method、读写或事务语义；
- 不为 workload 网络、数据库、消息副作用提供 idempotency 或 exactly-once；
- 不让 OpenSandbox 或 public client 修改 Run、Invocation、Job、Receipt、Event、Outbox 或其他 Platform 业务状态。

一个 Sandbox Package 可以在同一 Job 内按发布合同读取多个文件、启动多个受限子进程并完成一个业务计算；同一个 sandbox
连续执行多个 Job 不属于 CR-216。跨 Job 复用会把 residue、tenant identity、quota、deadline、lease fence、boot identity 和 cleanup
耦合成长期 session，并使“上一 Job 是否已完全清除”成为新的业务状态机。

## 3. 术语与信任边界

| 术语 | 定义 | authority |
|---|---|---|
| Sandbox Job | `WorkClass::Sandbox` 的 shared durable Job | 唯一 business work/attempt/lease/terminal authority |
| Sandbox Dispatcher | 唯一 claim Sandbox Job 并协调 provider 的 Platform Worker | candidate selection、current fence、terminal commit、cleanup |
| OpenSandbox Server | internal lifecycle API 与 Kubernetes provider | physical lifecycle only |
| BatchSandbox Controller | reconciliation、Pod create/delete、TTL | physical lifecycle only |
| Physical attempt | 一个 provisioning token 下的一组 bounded inert candidates 与一个 selected candidate | bounded external evidence，不是业务 aggregate |
| Candidate | OpenSandbox 创建且尚未启动 Package 的 sandbox | `Armed` 前提下可安全删除 |
| Fixed runner | image 内按 digest 冻结、由官方 execd init 监督的唯一 workload command 与 closed protocol | 验证输入、一次性激活、写入结果 |
| Sandbox Package | publication 产生的 immutable OCI image/package evidence | ResourceVersion/Artifact authority |

PostgreSQL 是 Job/Invocation/Run 唯一业务 authority。Kubernetes API 保存 BatchSandbox/Pod physical state，但不能投影为业务
current state。Dispatcher 拥有最小 Sandbox Job repository port；OpenSandbox Server、Controller、execd 和 runner 都没有 Platform
数据库、NATS、Artifact store、Run/Invocation RPC 或 public API credential。

OpenSandbox lifecycle API 只绑定 internal ClusterIP 并使用独立 credential。runner protocol 只在 sandbox internal endpoint 暴露，
由 NetworkPolicy 限制为 Dispatcher；不得创建 public Ingress、LoadBalancer、NodePort 或 host port。Agent、Skill、Model、MCP、
普通 Capability Worker 和 public client 都不得获得 endpoint 或 credential。

## 4. Closed 领域模型

```rust
enum SandboxProviderKind {
    OpenSandboxKubernetes,
}

enum SandboxNetworkMode {
    Disabled,
    Direct,
}

struct SandboxRuntimeContractV1 {
    schema_version: ConstU16<1>,
    provider: Const<SandboxProviderKind::OpenSandboxKubernetes>,
    opensandbox_server_release_digest: Sha256Digest,
    lifecycle_schema_digest: Sha256Digest,
    batchsandbox_crd_digest: Sha256Digest,
    batchsandbox_controller_digest: Sha256Digest,
    kubernetes_provider_template_digest: Sha256Digest,
    runner_protocol_digest: Sha256Digest,
    container_runtime_digest: Sha256Digest,
    network_policy_digest: Sha256Digest,
}

struct SandboxProfileDeploymentClosureV1 {
    schema_version: ConstU16<1>,
    profile_revision: ExactSandboxProfileRevisionRef,
    runtime_revision: ExactSandboxRuntimeRevisionRef,
    provider_binding_digest: Sha256Digest,
    network_mode: SandboxNetworkMode,
    limits: SandboxLimitsV1,
    provisioning_limits: SandboxProvisioningLimitsV1,
    secret_injection: Const<Disabled>,
    qualification_evidence: ArtifactRef,
}

struct SandboxProvisioningLimitsV1 {
    maximum_candidates: BoundedNonZeroU8<1, 4>,
    candidate_page_items: BoundedNonZeroU8<1, 16>,
    candidate_quiescence_milliseconds: BoundedU32<100, 5_000>,
    provisioning_timeout_milliseconds: BoundedU32<1_000, 120_000>,
    orphan_page_items: BoundedNonZeroU16<1, 100>,
    runner_header_bytes: BoundedU32<1_024, 65_536>,
    diagnostic_bytes: BoundedU32<1_024, 65_536>,
}
```

`SandboxProviderKind` 是首版 closed singleton。unknown provider、Docker/WASI/gVisor/microVM/host variant、runtime fallback、
mutable release、schema drift、template drift 或 provider binding drift 在 Job claim 前 fail closed。

Sandbox Runtime 与 Package 是 02 的 definition-only ResourceVersion；Sandbox Profile 通过 immutable Deployment 固定执行环境。
Skill 中的文件不会自动成为 Package，Package 也不能绕过 Capability Deployment 被直接执行。

## 5. Rust 所有权接口

```rust
trait SandboxJobRepository {
    async fn claim(&self, request: SandboxClaim) -> LeasedSandboxJob;
    async fn heartbeat(&self, fence: SandboxJobFence) -> SandboxJobFence;
    async fn record_provisioning_intent(
        &self,
        command: RecordProvisioningIntent,
    ) -> SandboxJobFence;
    async fn select_candidate(
        &self,
        command: SelectSandboxCandidate,
    ) -> SelectedSandboxCandidate;
    async fn authorize_activation(
        &self,
        command: AuthorizeSandboxActivation,
    ) -> AuthorizedSandboxActivation;
    async fn record_physical_observation(
        &self,
        command: RecordSandboxObservation,
    ) -> SandboxJobFence;
    async fn commit_terminal(&self, command: CommitSandboxTerminal) -> CommitReceipt;
    async fn claim_cleanup(&self, request: SandboxCleanupClaim) -> SandboxCleanupFence;
    async fn record_cleanup_observation(
        &self,
        command: RecordSandboxCleanupObservation,
    ) -> SandboxCleanupFence;
    async fn recover(&self, observation: SandboxRecoveryObservation) -> RecoveryDecision;
}

trait OpenSandboxProvider {
    async fn create_candidate(&self, request: OpenSandboxCreate) -> OpenSandboxCandidate;
    async fn list_candidates(
        &self,
        token_digest: SandboxProvisioningTokenDigest,
        cursor: CandidateCursor,
    ) -> BoundedPage<OpenSandboxCandidate>;
    async fn observe(&self, sandbox_id: OpenSandboxId) -> OpenSandboxObservation;
    async fn runner_state(&self, sandbox_id: OpenSandboxId) -> SandboxRunnerState;
    async fn activate(
        &self,
        sandbox_id: OpenSandboxId,
        frame: SandboxActivationFrameV1,
    ) -> SandboxRunnerState;
    async fn read_result(
        &self,
        sandbox_id: OpenSandboxId,
        limit: ResultFrameLimit,
    ) -> BoundedResultFrame;
    async fn terminate(&self, sandbox_id: OpenSandboxId) -> TerminationObservation;
    async fn prove_absent(&self, sandbox_id: OpenSandboxId) -> AbsenceObservation;
}
```

`OpenSandboxProvider` 不得依赖 Platform repository 或领域 service；`SandboxJobRepository` 不得包含 Kubernetes/OpenSandbox SDK
类型。两者只通过本规范的 closed、typed、bounded DTO 组合。调用方不能指定 runner path、port、URL、HTTP method 或任意 argv。

## 6. Execution Request 与 digest

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
    input_value_id: RunValueId,
    output_value_id: RunValueId,
    classification: DataClassification,
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

durable Job payload保存的是不含正文的closed `SandboxExecutionPlanV1`：它冻结上述RunValue identity、classification、input/output
schema/content digest、Package/Runtime/Profile/image/argv/network/limits/deadline与semantic request digest。Dispatcher只在claim/recovery事务
快照中加载exact immutable input RunValue、复核tenant/run/node/value identity与digest，然后补入current lease generation、worker generation、
physical attempt和trace，重建本次`SandboxExecutionRequestV1`。OpenSandbox create env/metadata与Job evidence都不保存business input。

request 是 closed、canonical、有 size/depth/field-count limit 的 immutable in-memory dispatch snapshot。`request_digest` 是 semantic execution
closure 摘要，排除自身、`lease_generation`、`physical_attempt`、`worker_process_generation_id`、lease token/expiry/version 与
trace correlation；heartbeat、lease 接管或 hop trace 改变不能改变它。

CR-216首条Inline input/output的有效hard ceiling分别为`min(frozen Profile ceiling, 1_048_576)` bytes；这与既有RunValue inline
authority一致，Profile中更大的Sandbox transport额度不能绕过该ceiling。runner frame额外header不得超过`runner_header_bytes`。
所有边界先检查declared length，再做bounded read和一次decode；unknown field、duplicate
field、non-canonical number、非法 UTF-8、depth/field/byte 超限都 fail closed。Package argv 是 string array，不接受 shell string。
首条实现超过 Inline output ceiling 返回 `sandbox_output_too_large`，不截断也不把 stdout 当结果；large Artifact port 保持 inactive。

## 7. Provisioning token 与候选机器

```rust
struct SandboxProvisioningTokenV1 {
    schema_version: ConstU16<1>,
    tenant_id: TenantId,
    job_id: JobId,
    physical_attempt: u32,
    execution_request_digest: Sha256Digest,
}

struct SandboxCandidateMetadataV1 {
    schema_version: ConstU16<1>,
    provisioning_token_digest: Sha256Digest,
    execution_request_digest: Sha256Digest,
    runtime_contract_digest: Sha256Digest,
    profile_deployment_digest: Sha256Digest,
    network_mode: SandboxNetworkMode,
}
```

wire token 是 `sha256(canonical_jcs({"domain":"insight.sandbox.provision/v1","token":token}))` 的 lowercase opaque digest。它不含 lease
generation、worker generation、trace 或 deadline。OpenSandbox metadata 只携带 token/correlation digest 与 operator-controlled
closed labels，不含 tenant、Job、Invocation、input、Secret 或用户字符串。

`record_provisioning_intent` 的 current-fence CAS 同时生成一个 256-bit `OpaqueActivationToken`；CAS loser 读取 winner 的既有 token，
不得另生成。token 是 runner idempotency identity，不授予 Platform API/DB/Kubernetes 权限；它以 sensitive Job evidence 持久化供 restart
后重放，禁止进入 metadata、Event、Outbox、log、trace 或 metric。create frame 只包含 `sha256(token)`，runner 在 activate 时用 constant-time
comparison 验证；未选 candidate 永远收不到 token。

OpenSandbox create 不是原子唯一性 primitive。合同允许同 token 出现多个 candidate，但必须满足：

1. create 只启动 `Armed` runner，绝不启动 Package；
2. 每次 create 前后都执行 Kubernetes-backed bounded list；`informer_enabled=false`，恢复不依赖进程内 cache；
3. candidate discovery 有 Profile 固定的 page、count、quiescence 与 total-time hard limit；超限进入 reconcile，不无限 create；
4. Dispatcher 在 current Job fence 下通过 PostgreSQL CAS 选择唯一 OpenSandbox ID；CAS loser 只能 observe/cleanup；
5. selected candidate 之外的候选永不 activate；response-loss 后先发现，不能先 create；
6. 只有 Job 尚未 `ActivationAuthorized`、所有已发现候选均可证明 `Armed`/未启动、且 count/time budget 仍有余量时，才允许
   再 create inert candidate；
7. metadata list 只用于发现与 orphan reconcile，唯一 selected owner 来自 PostgreSQL，不把 list/create 描述为原子幂等。

Platform 只承诺 “selected physical attempt 的 Package 最多激活一次”，不承诺 “一个 token 只产生一个 Kubernetes object”。

## 8. OpenSandbox create 映射

每个 candidate 的 create mapping 是 closed template：

- `image.uri` 必须是 publication 冻结的 OCI manifest digest，拒绝 tag；
- `entrypoint` 必须是 Runtime evidence 中的 fixed runner argv，不允许调用方覆盖或 `sh -c`；
- create frame 只含 input/schema/output/package/runtime/profile digest、published package argv、limits、activation-token digest 与 fixed
  runner protocol config；不把 business input body 写入 BatchSandbox/Pod spec 或 environment；
- Package 在 runner 接受 activate 前不得启动；
- timeout/TTL 不晚于 Profile ceiling 与 reconciliation budget；CPU、memory、pids、ephemeral storage 只能在 Profile 内缩小；
- `Disabled | Direct` 只映射到两个预安装、operator-owned policy label；Invocation 不能给出 raw policy；
- service-account automount false；拒绝 volumes、host mounts、devices、lifecycle hooks、snapshots、secure/public access 与 ingress；
- OpenSandbox 返回的 ID/endpoint 都是不可信 physical evidence，必须验证 scheme、namespace、label closure 和 bounds。

Dispatcher 不创建 BatchSandbox CR、Pod、Service 或 container，也不访问 Kubernetes write API。所有 physical create/delete 都经过
OpenSandbox lifecycle API；只读 provider readiness 与资格探针可以核验 CRD/controller/NetworkPolicy 状态。

## 9. Fixed Armed runner 协议

OpenSandbox Kubernetes provider 固定通过官方 `bootstrap.sh` 注入 execd；Profile 必须冻结
`execd_run_as_init=true`，因此 execd 是 PID 1 并只监督 image 内 immutable、非 root fixed runner 这一个 workload
command。runner 是 Package 唯一 activation authority，但不得被描述为容器 PID 1；Platform 不调用 execd 的 general
exec/file API。runner 先验证 create frame 并生成每次 container start 唯一的 bounded `boot_id`，然后进入 `Armed`。它只暴露
fixed internal port 与三个 closed operation：

```text
GET  /v1/state
POST /v1/activate
GET  /v1/result
```

协议由 OpenAPI/schema digest 冻结，禁用 redirect、content negotiation、chunked/unbounded body、caller path 和其他 method。所有 request/
response 都有 magic、version、declared length、canonical body digest 与 hard timeout。fixed NetworkPolicy 把 route 限制给 Dispatcher；
activate 还必须证明 provisioning intent 中持久化的 256-bit token 与 create-time digest 相符。该 token 不作为 Platform credential，
不能调用任何其他接口；state/result 仍受 namespace/service-account flow 与 sandbox-scoped endpoint isolation 限制。

```rust
enum SandboxRunnerPhaseV1 {
    Armed,
    ActivationLatched,
    Started,
    Succeeded,
    Failed,
    UnknownPriorActivation,
}

struct SandboxActivationFrameV1 {
    schema_version: ConstU16<1>,
    activation_token: OpaqueActivationToken,
    boot_id: RunnerBootId,
    execution_request_digest: Sha256Digest,
    input_digest: Sha256Digest,
    input: InlineCanonicalJson,
}
```

Dispatcher 必须先读取 `Armed + boot_id`，再在 PostgreSQL current Job fence 下持久化 selected ID、boot ID、activation token、
request digest 与 `ActivationAuthorized/PotentiallyStarted`，之后才可调用 activate。activation token 一经持久化不得轮换。

runner 在 spawn Package 之前必须：验证 boot/request、activation token digest、input schema/digest/size；以 create-exclusive 写入固定
activation latch；fsync file 和 parent directory；
原子发布 `ActivationLatched`。同 token 重放返回已有状态；不同 token 返回 `409 activation_conflict`；一个 boot ID 最多 spawn 一次。
runner 不把新 lease token 传给 Package。

若 runner/container 在 latch 后重启，新的 boot ID 读取到旧 latch 且没有完整 terminal result 时必须进入
`UnknownPriorActivation`，不得自动 restart Package。Dispatcher 对同 boot ID 可以安全重放同 token；boot ID 改变且无完整 result 时只能
记录 `UnknownOutcome`、cancel/terminate/cleanup，不能 activate 或 create replacement。

Package 可以在一个 Job 内启动 published contract 允许的多个子进程，但 runner 只调用 `package_argv` 一次，不经 shell 解析。Package
terminal 后 runner 将 result 写临时文件，fsync，校验长度/digest，再 atomic rename 到 fixed result path；路径和 maximum bytes 由 runner
contract 冻结。

`GET /v1/result` 是唯一结果读取面，只读 fixed path，未 terminal 返回 stable not-ready。Dispatcher 不使用 execd shell command、PTY、
code context、file upload/download/list/delete/rename、runtime installer 或任意 OpenSandbox endpoint。result 必须完整校验 magic、version、
request digest、terminal phase、output schema、output digest、declared/actual size 和 zero trailing bytes。

result frame正文只在runner fixed result与Dispatcher bounded memory中存在。`commit_terminal`先按request/boot/schema/digest/size完整验证，
再把成功output写入预分配的exact RunValue ID；shared Job payload只保存frame/output digest、declared bytes与safe terminal class，Event、
Outbox、Receipt、log和metric都不复制正文。

## 10. Job/physical 状态机

```text
Job Ready
  -> claim current lease
  -> validate exact Deployment/Package/Profile/runtime closure
  -> persist ProvisioningIntent(token, physical_attempt)
  -> discover/create bounded inert candidates
  -> PostgreSQL CAS CandidateSelected(sandbox_id)
  -> observe Armed(boot_id)
  -> PostgreSQL CAS ActivationAuthorized/PotentiallyStarted(token, boot_id)
  -> replay-safe activate same runner
  -> Started -> Succeeded | Failed | UnknownOutcome
  -> validate fixed result evidence
  -> owner terminal transaction rechecks current Job fence
  -> Job + Invocation + RunValue + quota + Event/Outbox terminal + cleanup intent
  -> claim SandboxCleanupFence
  -> delete all candidates -> fenced absence proof
```

OpenSandbox status、BatchSandbox/Pod phase、runner phase、exit 与 diagnostics 都只是 physical evidence。terminal transaction 必须重新
加载并 CAS 验证 tenant、Invocation owner、Job ID/state/version、lease generation/token、worker process generation、physical attempt、
request digest、deadline/cancel intent 与 quota reservation。任一漂移零业务写入；late success 不能覆盖新 generation 或既有 terminal。

terminal business commit 可以先于 physical absence，但必须同时形成 durable cleanup intent。标准 Job lease 在 terminal 被清除；后续
cleanup 使用只允许修改同一 Job physical evidence 的独立 generation fence：

```rust
struct SandboxCleanupFenceV1 {
    schema_version: ConstU16<1>,
    tenant_id: TenantId,
    job_id: JobId,
    expected_job_version: u64,
    physical_attempt: u32,
    cleanup_generation: u64,
    process_generation_id: ProcessGenerationId,
    expires_at: Timestamp,
}
```

claim/reclaim以PostgreSQL数据库时间和`FOR UPDATE SKIP LOCKED`争用；每次delete/absence observation都用完整fence CAS并递增Job version。
它不得改变terminal Job state/result/terminal_at、Invocation、RunValue、quota、Event、Outbox或Receipt。cleanup失败不撤销业务结果；由
bounded recovery继续并告警。OpenSandbox不得直接推进Job、Invocation或Run。

## 11. 幂等、并发与背压

CR-216 只承诺：

1. command Receipt 唯一逻辑 Invocation；
2. PostgreSQL candidate selection first-winner；
3. fixed runner activation token 在同 boot identity 上 replay-safe，Package 最多启动一次；
4. current Job fence 的 terminal commit first-winner。

候选 object 可以短暂重复，但必须 inert、bounded、可枚举、可删除。Sandbox workload 通过网络、数据库、消息或第三方 API 产生的
副作用、transaction 与 idempotency 属于 Package 和目标服务责任；平台不注入外部 idempotency key，也不提供 cross-system
exactly-once。

Dispatcher 在 claim 前预留 local slot 与 tenant/WorkClass quota。候选数、OpenSandbox API in-flight、active BatchSandbox、starting Pod、
runner state/result bytes、diagnostics 与 cleanup backlog 均有 hard limit。provider 饱和只使 Sandbox readiness/backpressure 失败，不能
拖垮 API、Scheduler、Model、MCP、Artifact 或 critical-control admission。

## 12. 超时、重试、取消与恢复

- create response 丢失：先按同 token bounded list；已有候选保持 inert，CAS 只选一个；不得把 metadata list 称为原子唯一性；
- Dispatcher crash/reclaim：先读取 Job physical evidence，再 list/observe；旧 lease 不能写业务状态，新 lease 不重算 token；
- `CandidateSelected` 后：不得选择第二个 candidate，除非 current fence 在激活前以明确 pre-start failure 创建新的 physical attempt；
- `ActivationAuthorized/PotentiallyStarted` 后：只能对相同 sandbox、boot ID、activation token 查询或重放；禁止新 token、candidate、
  sandbox、physical attempt 或自动 workload retry；
- runner `Succeeded/Failed` 且 result 完整：新 lease 可以在 current fence 下提交既有证据；boot 变化、result 不完整或 start 不确定则
  `UnknownOutcome`；
- cancel/timeout：先写 durable intent，再 terminate/delete；physical kill 是 best effort，不能把已发生外部副作用改写为未发生；
- provider 不可达：保留 Job/reconcile 状态并做 bounded probe，不盲目重发 workload；
- orphan sweeper 分页读取 operator label，回到 PostgreSQL 重验 tenant/job/token/selected/activation/terminal/cleanup 后才决定 delete；
- controller TTL 是最后保护，只删除 physical object，不推进业务状态；Dispatcher 必须最终取得 absence proof 或持续告警。
- terminal后cleanup worker不得伪造或复用已清除的Job lease；它只能claim current cleanup generation，过期generation的late absence写入
  stable stale-fence，且任何cleanup CAS都不能改写terminal business columns。

只有 create/boot/validation 明确发生在 `ActivationAuthorized` 之前，才属于自动 retry 安全区。Capability Effect 声明不改变这一规则。

## 13. Artifact、网络、Secret 与 container security

### 13.1 Artifact

OpenSandbox/runner 没有 object-store credential。首条实现只处理 Inline canonical input/output；未来 Sandbox Artifact port 必须通过
共享 Artifact Data Worker，在 owner Job 开始前预分配 identity/quota，并在 terminal owner transaction 形成 Ready Link。不得把
BatchSandbox volume、snapshot、emptyDir 或 host mount 当作 Artifact authority。

### 13.2 网络

两个 network mode 都由 operator-owned label 选择预安装 policy，默认拒绝 ingress：

- runner protocol 只允许 Dispatcher namespace/service account 到 fixed ClusterIP/port；OpenSandbox Server/Controller 只获得其 lifecycle
  所需的最小流量；
- `Disabled` 拒绝全部 egress，包括 DNS；
- `Direct` 只允许 DNS 与外部目标，拒绝 Platform namespaces、Pod/Service/Node CIDR、Kubernetes API、cloud metadata 与管理网段；
- 不创建 public ingress、host network、host port、NodePort 或 LoadBalancer。

Profile 必须固定 CNI、CIDR inventory 和 policy digest，并通过真实 L3；CIDR/policy 漂移或 CNI 无法表达所需 deny 时 readiness fail
closed。`Direct` 不提供 Platform egress broker 的 allowlist、DNS pinning、TLS、redirect 或 byte-accounting 语义。

### 13.3 Secret

CR-216 固定 `secret_injection=disabled`。Secret value 不得进入 create env、metadata、input、runner protocol、diagnostics、result、
OpenSandbox/Kubernetes store 或 public error。需要 Secret 的 Deployment 不可 activate；未来需独立 Reviewed 合同。

### 13.4 Container minimum

Pod 固定 non-root、read-only root filesystem、`allowPrivilegeEscalation=false`、capability drop、qualified seccomp、resource/pids/
ephemeral-storage limits、service-account automount false。禁止 privileged、host PID/IPC/network、hostPath、device、Docker/containerd/CRI
socket、Kubernetes token 与 Platform credential。满足这些 minimum 不等价于 gVisor/microVM isolation。

## 14. Physical evidence 与持久化

Platform 在 shared Job 的 bounded binding/evidence 中保存：

- 不含正文的execution plan、exact input/output RunValue ID、schema/content digest与semantic request digest；
- physical attempt 与 provisioning token digest；
- selected OpenSandbox ID、candidate discovery count/quiescence decision；
- runner boot ID、sensitive activation token/its digest 与 activation state；
- result frame/output digest、declared bytes、safe failure/unknown-outcome code；
- cleanup required/generation/owner/expiry、delete observation 与 absence proof digest。

activation token 只保存在 protected Job physical evidence，不进入任何 projection/observability；不保存 runner bearer、OpenSandbox
lifecycle snapshot、Pod spec/status、input/result body、log/stdout/stderr 正文或 container config。input唯一正文来自exact immutable
RunValue，成功output唯一正文由terminal transaction写入预分配RunValue；Job/Event/Outbox/Receipt都只保存digest/evidence。没有独立业务
lifecycle，因此不新增第二Job aggregate/table；Job lease前的执行与terminal后的cleanup generation都由shared Job同一row/version CAS。

Kubernetes API/BatchSandbox CR 是 restart-safe physical store。OpenSandbox Server 本地 memory/cache 不是恢复authority；developer
Profile 固定 `informer_enabled=false`，candidate list 直接读取 Kubernetes API。Controller 重建 Pod 会改变 runner boot ID，并触发第 12
节的不确定结果规则。

## 15. 可观测性与隐私

最低 fixed metrics：

- Job ready/expired-lease count 与 oldest age；
- claim、candidate create/list/select、activation authorize/replay/conflict、observe、commit、terminate latency/outcome；
- candidate duplicate/count/quiescence limit、stale fence、runner boot rollover、unknown outcome；
- OpenSandbox API capacity、BatchSandbox/Pod active、TTL、delete/absence、orphan cleanup backlog；
- runner phase/failure class、input/result/diagnostic bytes 与 limit violation。

metric label 只使用 component role、operation、outcome 与 safe failure class。tenant、Job、Invocation、sandbox ID、image、URL、package、
path 与 Secret 不进 label。log/trace 只记录 opaque digest 和 bounded safe code，不记录 input/output、stdout/stderr、environment 或外部
URL。Platform trace header 不注入 workload 或第三方请求。

## 16. 配置、部署与 readiness

目标最小 composition：

- 独立 Sandbox Dispatcher workload identity、Sandbox Job DB role、NATS consumer、DB pool、permits 与 NetworkPolicy；
- internal ClusterIP OpenSandbox Server `v0.2.3`，只允许 Dispatcher source/audience；
- BatchSandbox CRD 与 Controller `v0.2.0`，developer Profile 单 replica、leader election enabled；
- execd `v1.0.22` 作为上游 pod component，固定 `execd_run_as_init=true` 作为 PID 1 监督 fixed runner；Platform 不调用其
  general exec/file API；
- immutable Platform Armed runner OCI manifest digest；
- Kubernetes/containerd-runc 与 CNI exact closure；无 Docker socket、RuntimeClass/runsc、host runtime socket 或 public ingress；
- 两套预安装 NetworkPolicy：`Direct` 与 `Disabled`；Invocation 不生成任意 policy；
- controller TTL 与 Dispatcher cleanup/reconcile 同时启用。

官方 all-in-one chart 不能替代 exact closure。所需独立 Server chart 从审核 commit
`c39b814f36ded4c61d5ac6f9332ee4dfbab86c00` source-pin/vendor，安装前冻结 chart、CRD、Server/Controller/execd/runner 镜像
manifest digest 与 values schema；不得把 mutable tag 或未发布 artifact 描述为官方 release。

单节点 developer Profile 默认 `Direct`，并提供显式 `Disabled` Profile。Server 与 Controller 都为 1 replica；这只用于 L3，不能
声称 control-plane HA。`/health` 只作 process liveness。Dispatcher/provider readiness 必须认证核验 API、CRD/controller、create/list/
delete、runner protocol、network policy 与 exact digest closure；探针产生的 inert candidate 必须删除并取得 absence proof。

## 17. Error taxonomy

stable safe classes 至少包括：

- `sandbox_contract_invalid`、`sandbox_profile_drift`、`sandbox_provider_unready`；
- `sandbox_candidate_limit`、`sandbox_candidate_selection_conflict`、`sandbox_candidate_not_armed`；
- `sandbox_activation_conflict`、`sandbox_runner_boot_changed`、`sandbox_unknown_outcome`；
- `sandbox_result_not_ready`、`sandbox_result_invalid`、`sandbox_output_too_large`；
- `sandbox_stale_fence`、`sandbox_cancelled`、`sandbox_timed_out`、`sandbox_capacity_exhausted`；
- `sandbox_cleanup_pending`、`sandbox_absence_unproven`、`sandbox_network_policy_unqualified`。

provider/Kubernetes raw error、Pod/container ID、endpoint、path、input/output 和 logs 不进入 public safe error。retryability 由 Job 状态与
activation boundary 决定，不能只根据 HTTP status 或 error class 推断。

## 18. 测试矩阵

| 层级 | 必须证明 |
|---|---|
| L1 contract/unit | closed schemas、canonical digest、fixed argv/runner/result frame、size/depth/count limits、illegal config/provider/network rejection |
| L2 real PostgreSQL | concurrent claim/candidate CAS、lease rollover、stale result、terminal first-winner、cancel/timeout、quota settlement、orphan decision |
| L3 real provider | OpenSandbox+Kubernetes+containerd/runc concurrent create、response loss、Server/Controller restart、Dispatcher kill/reclaim |
| L3 runner/recovery | activation replay/conflict、runner-start uncertainty、boot rollover、fixed result validation、TTL/delete/absence/orphan cleanup |
| L3 network/security | Direct external access、Disabled zero egress、internal CIDR deny、no public ingress/socket/credential、wrong API credential/source |
| L4～L6 release | production topology、strong isolation、capacity/chaos/soak/restore/promotion；CR-216 未实际运行时均为 `Not run` |

负向 fixture 必须包括：candidate list race、late duplicate candidate、mutable tag、wrong image/runner/schema/policy digest、shell override、
privileged/host mount/socket、oversized input/result/diagnostic、duplicate/trailing terminal frame、activation token conflict、boot identity rollover、
OpenSandbox late observation、old lease cleanup、orphan误删保护与 provider 直接业务写入尝试。

## 19. 验收标准

- active composition、配置、测试与非历史文档只剩 OpenSandbox Kubernetes physical provider；
- WASI/Wasmtime、自建 gVisor launcher/guest、attestor、RuntimeClass/runsc、相关 RBAC/admission/manifests/preflight、host execution、
  backend selector/fallback 均被删除；
- concurrent/response-loss create 只产生 bounded inert candidates，PostgreSQL 只选择一个，Package 最多 activation 一次；
- `ActivationAuthorized/PotentiallyStarted` 后没有新 token/candidate/sandbox/automatic retry；
- terminal commit 重新验证 current Job fence，旧 generation/错 owner/错 request digest 零业务写入；
- runner 只执行 published fixed argv，result 只从 fixed read-only operation 获取并完整校验；
- OpenSandbox/Controller/runner 没有 Platform DB、Run/Invocation、Artifact store、NATS 或 public API credential；
- Direct 和 Disabled 均通过真实 CNI 资格且无 public ingress、host network/runtime socket/Platform credential；
- cancel、timeout、Dispatcher/provider kill、TTL 与 orphan recovery 最终取得 absence proof 或持续告警；
- workspace、contract、integration、CLI/profile、deployment 与 docs checks 全部通过；
- L4～L6 未运行时明确 `Not run`，不宣称 production-ready。

## 20. 明确推迟与实施门禁

明确推迟：Docker provider、restricted WASI、gVisor/Kata/Firecracker/microVM、GPU/privileged workload、production HA、strong
multi-tenant isolation、Artifact large I/O、Secret injection、Platform egress broker、interactive exec/PTY、Pool/persistent sandbox、public
endpoint、managed MCP stdio 与 workload external-effect idempotency。

没有需要修改 OpenSandbox 源码的架构前置。实施仍必须在 deployment BOM 中解析并固定 source-pinned chart、CRD、Server、Controller、
execd、runner、CNI 与 container runtime 的真实 manifest digest；未通过 exact L3 的 CNI/runtime 组合不得 activate。Accepted 表示目标合同
完成 cross-review，不表示当前代码已经切换或系统 production-ready。
