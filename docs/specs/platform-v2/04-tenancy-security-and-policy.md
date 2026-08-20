# Platform v2 Tenancy、Security 与 Policy 规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-20 |
| 依赖 | 01、02、03 |
| 直接下游 | 05～18 |

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

每个Policy payload是closed、versioned、canonical、有size limit的nominal type。发布时执行syntax/semantic/cross-policy/hard-limit
验证。Run/Invocation/Job冻结exact Policy ResourceVersion/digest，不在历史工作上自动换成current head。

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

Artifact只有普通staging/ready/physical/grant/traffic quota，无Model Artifact Producer专用bundle。

## 10. Audit 与隐私

安全相关Event至少记录principal/workload、tenant、action、target kind/ID/version、decision/reason、policy/evidence digest、
auth strength、occurred/committed time和trace ID。不记录Secret/token、prompt/response、tool arguments、code/file body、URL query、
object key或raw provider/runtime error。

metric label只使用low-cardinality role/operation/outcome/reason class，tenant/principal/resource ID不进label。受控diagnostic Artifact
有额外authorization、retention、encryption和audit，不与普通Run result权限自动等同。

## 11. 安全与恢复不变量

- tenant只能通过typed binding访问自身对象，无cross-tenant generic admin API；
- active policy/Deployment变化不改写已存Run snapshot，emergency gate只阻止未开始工作/撤销grant；
- Secret/network/Artifact/code权限一律默认deny、exact purpose/audience/fence授权和terminal撤销；
- authorization/cache/quota/grant/revoke的旧version/generation不得提交；
- external dependency不确定时保存Unknown/Reconcile，不降级security或伪造success；
- break-glass不依赖应用内的永久超级tenant role。

## 12. 验收标准

- cross-tenant get/list/mutation、ID prefix/kind、tenant header/body spoof全部fail closed且不泄漏存在性；
- revoked/suspended principal/policy/deployment/Secret/grant在current gate后不能新准入；
- Run仍使用冻结Policy/Deployment，emergency gate不改写历史snapshot/result；
- raw Secret/OAuth token/code/verifier不进DB/Event/log/trace/problem/Artifact；
- Egress SSRF/DNS rebinding/redirect/private/metadata/proxy/header注入负向fixture通过；
- Skill script不能被直接执行，API/Model/MCP/Capability Worker无spawn runtime；
- WASI/gVisor选择不能由调用方降级，runc/host/microVM不在首版runtime composition；
- quota concurrent reserve/settle/recovery不超卖、不泄漏、不重复consume；
- Artifact static binding在新旧object上可读/删，公开API无dynamic binding；
- security log/metric/Event脱敏和high-cardinality门禁通过。

## 13. 明确推迟

- tenant self-service KMS/storage/Secret Provider plugin；
- cross-tenant collaboration/federation和cross-region active-active；
- microVM/Firecracker/KVM、confidential computing和heavy compute；
- 应用内永久platform super-admin或Installation Release权限模型。

## 14. 未决问题

首版tenant/security/policy/Secret/quota合同无未决设计问题。
