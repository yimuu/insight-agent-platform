# Platform v2 Tenancy、Security 与 Policy 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-196 |
| 日期 | 2026-08-26 |
| 依赖 | 01、02、03 |
| 直接下游 | 05～18 |

> 2026-08-24 implementation feedback（CR-181）：05的external leaf允许一个slot冻结多个exact candidate，但此前04只定义
> Selection Policy identity，没有可执行decision/evidence合同。CR-181冻结候选选择输入、输出和提交时重验边界，禁止Scheduler/
> Worker自由选择Deployment。

> CR-182：CandidateSelectionEvidence本身不能定义选择语义；Selection Policy Revision必须持有下述closed executable document，
> 空document或自由表达式均拒绝。

> CR-189：Remote Context Deployment与Run snapshot冻结canonical endpoint、exact Network/TLS/Trust Policy及SecretBinding refs。
> Context Worker只把这些opaque exact refs交给Egress；Egress在最后一跳重验Policy/gate并解析Secret值。进程本地URL、默认trust store、
> 自由header或明文Secret都不能补全缺失closure。

> CR-195：MCP Streamable HTTP同样禁止默认trust store。Egress startup catalog必须把exact Trust Policy编译为bounded显式PEM trust
> bundle并纳入配置digest；每次请求以exact Deployment/Policy refs选择该entry，TLS只信任该bundle并校验canonical endpoint hostname。
> bundle缺失、PEM无效、entry漂移或调用方尝试覆盖trust material时，必须在HTTP dispatch前fail closed。

> CR-196：OAuth token exchange也是remote HTTPS adapter。Egress OAuth installed binding必须把MCP Deployment closure的exact Trust Policy
> 编译为bounded PEM roots并绑定exact Auth Policy/profile/token endpoint；不得使用系统默认CA，也不得由Callback/RPC提供trust正文。

## 1. 决策摘要

Tenant是所有业务state、authorization、policy、quota、Artifact、Secret reference和Event的最高scope。平台不使用
nullable/fake tenant表示安装级authority，不在业务DB维护Installation Operator/Release current state。

authorization是`principal + tenant binding + resource/action + exact Run/Deployment context + published Policy + current security gate`
的closed决策。Secret只保存opaque reference，在最后一跳解析。代码只在独立Sandbox Execution Plane运行。

## 2. Principal 与tenant binding

```rust
enum PrincipalKind { Human, ServiceIdentity }

enum TenantRole {
    TenantAdmin,
    ResourceAuthor,
    RuntimeOperator,
    Approver,
    Auditor,
    ArtifactUser,
    Service,
}

struct PrincipalContext {
    principal_id: PrincipalId,
    kind: PrincipalKind,
    tenant_id: TenantId,
    roles: BoundedSet<TenantRole>,
    scopes: BoundedSet<Permission>,
    authentication_strength: AuthStrength,
    credential_id_digest: Digest,
    issued_at: Timestamp,
    expires_at: Timestamp,
    trace_id: TraceId,
}
```

OIDC/JWT/mTLS主体映射为稳定Principal，tenant binding以`(tenant_id, principal_id)`唯一。tenant来自credential和
routing authority，不信任body/query/header中自由声明的tenant/principal/role。绑定变化使用Receipt、expected version和
Event/Outbox同事务。

平台运维人员不自动获得tenant数据权限。support/break-glass使用外部特权访问系统、强认证、工单/
审批、有效期和不可变运维审计；如需调用tenant API，必须获得显式short-lived tenant binding，不使用
impersonation header。

tenant级默认policy选择由tenant current aggregate中的closed `TenantConfigV1`拥有：

```rust
struct TenantConfigV1 {
    scheduling_policy: Option<ExactPolicyDeploymentRef>,
    artifact_retention_policy: Option<ExactPolicyDeploymentRef>,
    artifact_io_policy: Option<ExactPolicyDeploymentRef>,
}
```

三个slot彼此独立且只能指向该tenant内enabled、active Resource的exact immutable Policy Deployment；Deployment closure再冻结唯一
Policy Revision及其digest。绑定command使用Tenant strong
version CAS、Receipt、Event与Outbox原子更新，只改变对应slot而保留其他slot。Artifact public prepare要求后两个slot均存在并分别验证
`PolicyKind::Retention`与`PolicyKind::ArtifactIo`，缺失、错kind、digest不符或同revision复用均fail closed。不得扫描多个active Policy后任取一条，
也不以进程config、请求body或排序约定替代tenant current authority。

## 3. Authorization

permission是closed registry，例如：

```text
resource.read/write/publish/deploy/activate
run.create/read/control
task.read/respond/approve
artifact.upload/read/delete
audit.read
```

每个command先认证、确定tenant、解析nominal target，再评估RBAC + published Policy + resource security gate。
list/query在repository predicate层应用tenant与可见性，不在读取后内存过滤。`not_found`/`forbidden`映射避免
成为cross-tenant存在性oracle。

authorization decision的缓存key必须包含tenant、principal binding version、permission、target kind/ID/version、policy digest、
security gate generation和auth strength，并有短TTL。revocation/suspension不依赖缓存自然过期，而通过version/gate使旧key失效。

## 4. Policy lifecycle

Policy复用Resource -> immutable ResourceVersion -> Deployment/Binding。首版PolicyKind至少包含：

```rust
enum PolicyKind {
    Authorization,
    Scheduling,
    Retry,
    Approval,
    DataHandling,
    Retention,
    NetworkEgress,
    SecretAccess,
    SandboxIsolation,
    ModelSafety,
    Logging,
}
```

```rust
struct PolicyDeploymentClosure {
    policy_revision: ExactPolicyRevisionRef,
    environment: CanonicalEnvironment,
    applicability_digest: Digest,
    qualification_evidence: ArtifactRef,
}
```

Policy Deployment不执行代码；它是tenant可绑定的exact applicability/qualification closure。Resource active binding指向Deployment，
TenantConfig和Run snapshot同时冻结Deployment与其中Revision，禁止跳过Deployment直接追随Revision head。

每个Policy payload是closed、versioned、canonical、有size limit的nominal type。发布时执行syntax/semantic/cross-policy/hard-limit
验证。Run/Invocation/Job冻结exact Policy Deployment及其Policy ResourceVersion/digest，不在历史工作上自动换成current head。

CR-187冻结Model admission首版必须消费的三个nominal document：

```rust
struct ModelSafetyPolicyDocument {
    schema_version: 1,
    contract_id: BoundedCode,
    platform_instruction: BoundedUtf8,
    instruction_content_digest: Digest,
    instruction_byte_budget: u32,
    instruction_token_budget: u32,
    pre_dispatch_rules_digest: Digest,
    post_response_rules_digest: Digest,
}
struct ModelBudgetPolicyDocument {
    schema_version: 1,
    maximum_attempts_per_turn: u32,
    maximum_input_tokens_per_turn: u64,
    maximum_output_tokens_per_turn: u32,
    maximum_total_tokens_per_turn: u64,
    cost_ceiling_microunits_per_turn: u64,
}
struct ModelPublicProjectionPolicyDocument {
    schema_version: 1,
    reject_prompt_overflow: true,
    retain_source_map: true,
    retain_sensitive_prompt_body: false,
}
```

Safety instruction最多65536 UTF-8 bytes/16384 estimated tokens且raw SHA-256必须匹配；Budget各值非零、attempt不超过32且
input+output不超过total；PublicProjection首版只接受上述fail-closed组合。三者都由外层`rules_digest == canonical(document)`绑定，
unknown field/version、宽松overflow、敏感正文持久化或digest漂移在publication拒绝。

current emergency gate可以阻止尚未开始的外部leaf、撤销grant/Secret/network或触发cancel，但不修改冻结snapshot、
不改写已提交结果。决策保存policy ID/version/digest、input evidence digest、decision/reason和time，不保存Secret/正文。

## 5. Data classification 与encryption

```rust
enum DataClassification { Public, Internal, Confidential, Restricted }
```

classification在ResourceVersion、RunValue、Artifact、Context、Model/MCP request和Sandbox port之间只能按published rule保持或升级。
降级需要显式declassification Capability、approval和evidence，不由调用方改metadata。

PostgreSQL、Artifact store、backup和transport均加密。Artifact首版只使用部署时`ArtifactStorageBindingManifestV1`，不定义
tenant Add/Rebind/Revoke、EncryptionDomainId、installation fence或dynamic KMS API。旧Artifact保留exact binding identity，运维迁移按
18 runbook/GitOps处理。

Model output首版Inline-only，没有Model-output Artifact policy、Producer storage binding或专用Artifact quota。

## 6. Secret

```rust
struct SecretBindingRef {
    binding_id: SecretBindingId,
    tenant_id: TenantId,
    provider_id: SecretProviderId,
    reference_ciphertext: BoundedCiphertext,
    reference_digest: Digest,
    purpose: SecretPurpose,
    allowed_workloads: BoundedSet<ComponentRole>,
    region: CanonicalRegion,
    projection_version: u64,
}
```

数据库不保存Secret value、access token、OAuth code/verifier或可逆的plain reference。Secret Provider是由GitOps/startup manifest
安装的closed trusted client catalog，不是tenant可上传的plugin，不接受调用方endpoint。

解析发生在Egress/Secret Broker最后一跳，每次复核tenant、binding/current gate、purpose、workload identity、region、
owner/Job fence、deadline和read count。返回值只在bounded memory/tmpfs中存活，不回传普通Worker、DB、Event、log、trace、
Artifact或error body。

rotation创建新SecretBinding version/reference，对未来Deployment/Run生效。emergency revoke通过current gate阻止新解析并
撤销活跃grant；已发生external effect按Effect/reconciliation处理。

## 7. Egress

NetworkEgressPolicy冻结catalog target identity、scheme/port、DNS/TLS/redirect/proxy policy、region/classification、request/response
method/header/body合同、byte/time/rate limit和SecretBinding requirement。调用方不能提供自由URL、IP、Host header、
proxy或redirect target。

Egress Broker复核exact Deployment/Job/tenant/policy/auth binding并做DNS pinning、private/metadata/address deny、TLS hostname/pin、
redirect重验、request/response限制和sanitized error mapping。普通Worker不获得raw credential或直出网路。Sandbox gVisor
也只能通过declared Egress Broker port；WASI无network。

首版remote HTTPS adapter不得读取操作系统默认CA集合来补全缺失Trust Policy。process-installed entry中的显式trust bundle是唯一证书根输入，
受bounded parse、startup config digest和exact Deployment/Policy匹配保护；运行时request/protobuf不携带PEM正文。
该要求覆盖Capability HTTP、Remote Context、MCP Streamable HTTP及OAuth token exchange；OAuth本地JWKS验证材料与TLS roots职责不同但必须在同一
installed binding中分别exact冻结。

## 8. Code 与Skill trust

Skill是不受信任方法包，不是可执行主体。脚本只有在publication pipeline中成为immutable Sandbox Package，
并由exact Capability Deployment绑定后才能执行。Python/Node/WASM/trusted Shell都只在Sandbox Plane运行。

```text
Pure WASM, no network/Secret -> restricted WASI
Python/Node/Shell or network/Secret/filesystem -> gVisor
```

plain runc/host process/privileged container禁止，microVM/Firecracker/KVM推迟。调用方不能降级isolation。运行时不安装
package、下载依赖或使用mutable image。

## 9. Quota

quota是durable tenant-scoped aggregate，复用scope/kind/dimension registry。至少覆盖active/waiting Runs、WorkClass concurrency、
Model tokens/cost、Sandbox weighted resources、Artifact staging/ready bytes/count/grants/ingress/egress、MCP sessions/subscriptions和
external request rate。不为每个domain建quota table。

Job claim/I/O前以一个generation-owned QuotaReservation bundle原子预留全部lines，terminal/recovery锁定实际bundle原子
consume/release。调用方只携带reservation identity/fence，不重述或删减scope lines。local process permit是物理容量，
不durable quota是不同authority。

subscription refresh只预留`WorkClass::Context`并发与适用的tenant/remote-host request lines，不预留Run/ContextQuery item quota，
也不复用MCP connection Job的reservation。Context Worker发出的internal request不得携带raw session、authorization header、token、
Secret value或自由endpoint；MCP Host以tenant + subscription + Context Job fence重载exact MCP Deployment、Discovery与Auth binding，
Secret仍只在Egress最后一跳解析。wrong workload audience、owner/fence/closure或撤权必须在外部I/O前拒绝。

Artifact只有普通staging/ready/physical/grant/traffic quota，无Model Artifact Producer专用bundle。

## 10. Exact candidate selection

Model、Capability、ChildAgent和Skill slot的`selection_policy`是唯一candidate selection语义authority。Policy Revision必须包含
closed、deterministic selector program；输入只允许RunBindings中该slot的规范排序candidate列表、05 node可选的exact route RunValue、
已冻结principal/policy snapshot及剩余budget，不得读取active head、网络、Secret、随机数或进程时钟。

```rust
struct CandidateSelectionPolicyDocument {
    schema_version: u32, // 固定1
    mode: OnlyCandidate | OrderedFirst | RouteHash,
    route_schema_digest: Option<Digest>,
}
```

document canonical digest由外层`PolicyResourceSpec.rules_digest`唯一保存并在publication重验；document内部不得保存自身digest，
Selection Policy Deployment/Binding再通过exact Revision semantic digest冻结该published document。

`only_candidate`要求exact candidate count为1且没有route；`ordered_first`要求没有route并选择规范排序后的第一个candidate；
`route_hash`要求route schema digest完全匹配，计算route canonical JSON bytes的SHA-256，将digest作为大端无符号整数对candidate count
取模。candidate排序键固定为`(resource_kind, deployment_id, deployment_digest)`；不得读取health/active head或在失败时跳到下一项。
增加mode必须提升Selection Policy document `schema_version`并同步owner schema/evaluator/negative fixture。

```rust
struct CandidateSelectionEvidence {
    schema_version: u32,
    slot_id: SlotId,
    policy_revision_id: ResourceVersionId,
    policy_semantic_digest: Digest,
    ordered_candidate_deployment_digests: Vec<Digest>,
    route_value: Option<ExactRunValueRef>,
    selected_deployment: ExactDeploymentRef,
    result_digest: Digest,
    canonical_digest: Digest,
}
```

零candidate在publication/admission拒绝；单candidate仍产生可重验evidence，多candidate必须执行exact Policy Revision。相同inputs必须
产生相同selected deployment与digest；selector无法唯一决定、选择集合外对象、route schema不匹配或policy/binding漂移均fail closed。
Scheduler可以在事务外执行closed selector，但创建Invocation/ContextQuery/child Run的owner transaction必须锁定Run/Node/Job，重新加载
exact slot与Policy Revision，重验route RunValue identity/schema/content digest和evidence，再接受selected deployment。Evidence只进入
Receipt/Event bounded detail，不建立selection current-state aggregate或表。

## 11. Audit 与隐私

安全相关Event至少记录principal/workload、tenant、action、target kind/ID/version、decision/reason、policy/evidence digest、
auth strength、occurred/committed time和trace ID。不记录Secret/token、prompt/response、tool arguments、code/file body、URL query、
object key或raw provider/runtime error。

metric label只使用low-cardinality role/operation/outcome/reason class，tenant/principal/resource ID不进label。受控diagnostic Artifact
有额外authorization、retention、encryption和audit，不与普通Run result权限自动等同。

## 12. 安全与恢复不变量

- tenant只能通过typed binding访问自身对象，无cross-tenant generic admin API；
- active policy/Deployment变化不改写已存Run snapshot，emergency gate只阻止未开始工作/撤销grant；
- Secret/network/Artifact/code权限一律默认deny、exact purpose/audience/fence授权和terminal撤销；
- authorization/cache/quota/grant/revoke的旧version/generation不得提交；
- external dependency不确定时保存Unknown/Reconcile，不降级security或伪造success；
- break-glass不依赖应用内的永久超级tenant role。

## 13. 验收标准

- cross-tenant get/list/mutation、ID prefix/kind、tenant header/body spoof全部fail closed且不泄漏存在性；
- revoked/suspended principal/policy/deployment/Secret/grant在current gate后不能新准入；
- Run仍使用冻结Policy/Deployment，emergency gate不改写历史snapshot/result；
- raw Secret/OAuth token/code/verifier不进DB/Event/log/trace/problem/Artifact；
- Egress SSRF/DNS rebinding/redirect/private/metadata/proxy/header注入负向fixture通过；
- Skill script不能被直接执行，API/Model/MCP/Capability Worker无spawn runtime；
- WASI/gVisor选择不能由调用方降级，runc/host/microVM不在首版runtime composition；
- quota concurrent reserve/settle/recovery不超卖、不泄漏、不重复consume；
- Artifact static binding在新旧object上可读/删，公开API无dynamic binding；
- single/multiple candidate selection均从exact frozen Policy确定；伪造route、乱序candidate、集合外结果和旧policy digest全部拒绝；
- security log/metric/Event脱敏和high-cardinality门禁通过。

## 14. 明确推迟

- tenant self-service KMS/storage/Secret Provider plugin；
- cross-tenant collaboration/federation和cross-region active-active；
- microVM/Firecracker/KVM、confidential computing和heavy compute；
- 应用内永久platform super-admin或Installation Release权限模型。

## 15. 未决问题

CR-181 cross-review已确认candidate selection无第二current authority并恢复Accepted；实现与L2/L3 evidence仍待完成。
