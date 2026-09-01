# Platform v2 系统架构与领域边界规范

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-216 revision 1 |
| 日期 | 2026-09-01 |
| 依赖 | [`00-overview.md`](00-overview.md) |
| 直接下游 | 02、03、04、05、18 |

> CR-216 revision 1 impact：Sandbox 物理边界 clean-cut 为
> `Sandbox Dispatcher -> OpenSandbox Server -> Kubernetes/BatchSandbox -> containerd/runc`。
> shared Job 仍是唯一业务 work authority；Dispatcher 拥有 claim/candidate selection/activation/fence/terminal，OpenSandbox 与
> Controller 只拥有 BatchSandbox/Pod/runner physical lifecycle，
> 不获得Platform PostgreSQL或Run/Invocation mutation权限。ADR-0007取代ADR-0002。

> CR-197 impact：跨plane correlation由03的durable `TraceIdentityV1`和每hop ephemeral span组成。Gateway、Scheduler、Worker、MCP、
> Sandbox、Artifact、Security/Egress只传播opaque trace ID和新span parent；不得把trace header当作tenant/owner authority，也不得向首版第三方
> provider/MCP/Context endpoint转发内部`traceparent`。该共享transport concern不改变plane ownership或增加服务。

## 1. 决策摘要

Platform v2 使用一个模块化领域内核和五个可独立扩缩的执行隔舱：Model、Capability、Context、MCP 与
Sandbox。平台不采用
“所有概念都是 Tool”或“每种后端都是 DSL 节点”的设计。

CR-195明确Egress process-installed catalog是exact Deployment/Policy到可执行TLS trust material的唯一运行时适配边界；普通Worker、MCP Host
和调用方不能提供CA bundle或触发默认trust store。该material是启动配置，不是业务current state，也不改变Deployment owner。
CR-196确认该边界同时拥有OAuth token endpoint的trust material；Callback/Host只发送credential-free typed contract，不成为TLS authority。

```text
Control Plane
  Agent / Skill / Capability / Context / MCP / Policy Registry
                         ↓ immutable Deployment
Orchestration Plane
  Run / NodeExecution / Invocation / ChildRun / HumanTask
                         ↓ durable dispatch
Execution Plane
  Model Executor | Capability Worker | Context Worker | MCP Host | Sandbox Dispatcher -> OpenSandbox
                         ↓
Data Plane
  PostgreSQL | S3-compatible Artifact Store | NATS wake/event transport
```

这些是逻辑边界，不要求每个模块都成为微服务。只有会运行不可信代码、持有独立协议会话或需要独立并发
预算的边界才必须物理分离。

## 2. 目标与非目标

### 2.1 目标

- 为 Agent、Skill、Capability、ContextSource、Subagent、MCP 和 Sandbox 给出互斥、稳定的定义；
- 让管理面动态发布能力，同时保证每个 Run 可重现、可恢复、可审计；
- 让代码执行、MCP I/O 和模型调用互不耗尽对方的并发预算；
- 为原生 Rust、HTTP/gRPC、MCP Tool 和脚本提供同一个 Capability Interface；
- 保留 Retrieval 的来源、引用、分页、过滤和权限语义；
- 把跨进程暂停、远程任务、审批和输入请求建模为 durable state，而不是阻塞线程；
- 允许先以模块化单体交付控制面和编排面，再按负载独立部署 Worker。

### 2.2 非目标

- 不兼容 v1 `ActionRegistry`、`RetrievalRegistry` 或现有 DSL 节点名称；
- 不设计 UI、Marketplace、计费结算或跨地域主动容灾；
- 不把 Skill 设计为进程、容器、Run 或聊天 Agent；
- 不把 Subagent 伪装为普通 JSON Tool；
- 不让 MCP 成为平台内部唯一工具协议；
- 不承诺外部副作用 exactly-once；
- 不允许 API、Scheduler、Model Worker 或 MCP Host 执行任意用户脚本。

## 3. 规范术语

### 3.1 Agent

Agent 是版本化、具有类型化输入输出的执行定义。它拥有 Typed Plan、允许的 Skill/Capability/Context
策略、Model 策略和最终结果责任。Agent 不拥有具体后端连接和 Secret value。

### 3.2 Subagent

Subagent 是由父 Run 创建的 child Run。它具有独立 Run ID、状态机、绑定、预算、日志和结果；父 Run
通过 typed child interface 收到结果。父子之间只交换结构化 task、progress、input request、result、
failure 和 cancellation，不共享可变调用栈。

### 3.3 Skill

Skill 是不可变方法包，包含指令、匹配元数据、参考资料、资产、示例和 Capability Requirements。
Skill 不拥有线程、连接、Secret、数据库事务或运行状态。Skill 中的脚本只有发布为 Capability
Implementation 后才能执行。

### 3.4 Capability

Capability Interface 是可调用业务能力的类型化合同，包含输入、输出、Effect、幂等、取消、审批和
Artifact ports。Capability Implementation Revision 是后端协议/映射的不可变实现合同；Capability Deployment
再固定实际endpoint/runtime、SecretBinding和环境策略。

```text
Capability Interface
  ├─ Native Rust implementation
  ├─ Remote HTTP/gRPC implementation
  ├─ MCP Tool implementation
  └─ Sandboxed Script implementation
```

调用者只声明Interface需求；Agent/Capability Deployment冻结exact Implementation Revision与环境backend binding。

### 3.5 ContextSource

ContextSource 是只读上下文来源，返回带 provenance、citation、score、cursor 和 policy evidence 的结果。
数据库 schema search、向量检索、MCP Resource、知识库和受控文件索引属于 ContextSource。普通 Capability
不能冒充 ContextSource 并丢失这些语义。

### 3.6 MCP Host

MCP Host 拥有 MCP wire、transport、OAuth、session、discovery、Tasks、Subscriptions 和 interactions。
它将 MCP 对象显式投影为平台对象：

| MCP 对象 | 平台对象 |
|---|---|
| Tool | Capability Implementation |
| Resource | ContextSource Implementation |
| Prompt | 不受信任候选Artifact；显式纳入Agent/Skill Revision后成为其Prompt Asset |
| Task | Remote CapabilityInvocation continuation |
| Elicitation | InputRequired / ApprovalRequired |
| Subscription | Context invalidation/event source |

### 3.7 Sandbox

Sandbox 是不可信或动态代码的执行环境。Sandbox Execution Plane中的OpenSandbox拥有进程、文件系统、runtime、network和
physical lifecycle；Platform Sandbox Dispatcher只提交结构化Execution Request、观察bounded evidence并以current Job fence提交结果。
OpenSandbox状态不能成为Run、Invocation或Job current state。

### 3.8 Artifact

Artifact 是大值、二进制文件或跨服务文件的不可变对象。业务表只保存 tenant-scoped ArtifactRef，
正文保存在 S3-compatible store。Artifact 的安全、生命周期和内容验证由平台负责。
受信producer可以在外部I/O后留下不可读的Staging/Verified candidate，但只有业务owner的单一PostgreSQL事务可以把它变为
Ready并同时建立Reference/RunValue/terminal事实；producer本身不能成为第二个业务current-state authority。

## 4. 平面与物理边界

### 4.1 Control Plane

Control Plane 管理：

- Agent、Skill、Capability、ContextSource、Model、MCP 和 Policy 的 Draft/Revision/Deployment/Head；
- discovery、validation、publication、activation、suspension 和 retirement；
- Operator API、审计和配置验证。

Control Plane 不执行 Run，不调用用户 Capability，不读取 Secret value 回传给客户端。

### 4.2 Orchestration Plane

Orchestration Plane 管理：

- Run admission 和 immutable bindings；
- Typed Plan 状态推进；
- NodeExecution、CapabilityInvocation、ChildRun、HumanTask、timer 和 signal；
- lease、fence、retry、cancel、suspend、resume 和 terminal result；
- transactional outbox。

Scheduler 只根据已提交事实生成下一步工作。它不得执行模型请求、外部 I/O 或脚本。

### 4.3 Execution Plane

Execution Plane 分为以下 bulkhead：

| 隔舱 | 所有权 | 必须独立并发预算 |
|---|---|---|
| Model Executor | Provider adapter、stream、tool-call response | 是 |
| Capability Dispatcher | Native/HTTP/gRPC 调用与结果归一化 | 是 |
| MCP Host | MCP session、OAuth、remote Task、subscription | 是 |
| Sandbox Dispatcher / OpenSandbox | Dispatcher拥有 Job claim/candidate CAS/activation/fence/commit；OpenSandbox/Controller 拥有 BatchSandbox/Pod/runner physical lifecycle | 是且必须与API/普通Worker物理分离 |
| Context Worker | 检索、重排、citation assembly | 是 |
| Artifact Gateway | public upload/download、授权与流控 | 是 |
| Artifact Data Worker | Context、MCP、Capability与Sandbox的受信读写、验证与派生 | 是 |
| Artifact Maintenance | scan、retention、quarantine与GC | 是 |

一个隔舱饱和时必须通过有界队列和 durable backpressure 停留在自己的工作类别，不能获取其他隔舱的
permit，也不能使 API readiness 失败。

MCP subscription refresh是跨两个执行隔舱的typed协作，不合并所有权：Context Worker claim并拥有shared Context Job的
lease、retry、terminal与tenant quota；MCP Host仅在收到带exact Job fence的内部Resource Refresh请求后执行MCP wire I/O。
Host不claim Context Job、不自行从notification执行Context工作，也不写Context result/current state；Context Worker不获得MCP
session或Secret value。调用与结果都必须是有界、credential-free机器合同，任一侧饱和只保留自身permit。

### 4.4 Data Plane

- PostgreSQL：所有管理和运行状态的唯一事务权威；
- S3-compatible store：Artifact 正文权威；
- NATS：wake hint、live observation 和 committed outbox fan-out，不拥有业务状态；
- Secret Manager：Secret value 权威，平台数据库只保存不可逆 reference identity。

## 5. 逻辑组件

| 组件 | 拥有 | 不拥有 |
|---|---|---|
| `control-api` | 管理命令、认证、DTO、错误映射 | Runtime 状态推进 |
| `runtime-api` | Run admission、query、signal、cancel、stream | Scheduler 决策 |
| `registry-domain` | Entity、Revision、Deployment、Binding | I/O adapter |
| `orchestrator-domain` | Plan、Run state machine、纯决策 | 数据库和网络 |
| `scheduler` | claim、lease、outbox、recovery drive | 叶节点执行 |
| `model-worker` | Model Invocation | Capability 和 Sandbox |
| `capability-worker` | Capability Invocation dispatch | Script process |
| `context-worker` | Context Query、授权过滤、检索、citation assembly及subscription refresh Job terminal | MCP wire/session/Secret、Capability 副作用与 Agent Plan |
| `mcp-host` | MCP 协议、连接状态及fenced Resource Refresh adapter | Context Job/Result current authority与 Agent Plan |
| `sandbox-dispatcher` | Sandbox Job claim、OpenSandbox request、candidate CAS、runner activation、physical evidence校验、fenced terminal commit与cleanup | 在自身进程执行用户代码、绕过Job fence、selected后另选sandbox、自由第三方API语义 |
| `opensandbox-server` | internal lifecycle API、Kubernetes provider、BatchSandbox physical metadata | Platform PostgreSQL、Run/Invocation/Job mutation、public API与业务重试/selection决策 |
| `opensandbox-controller` | BatchSandbox/Pod reconciliation、TTL/delete | Platform业务状态、Dispatcher credential、Package activation与terminal决策 |
| `artifact-gateway` | public upload/download、grant与边界限流 | 业务owner状态推进、内部Worker凭据 |
| `artifact-data-worker` | exact owner绑定的stage/read/verify/derive | public API、Run/Invocation current state |
| `artifact-maintenance` | scan、retention、quarantine、delete和GC | 新业务引用、public upload/download |
| `egress-broker` | exact endpoint catalog、DNS pinning、SSRF/TLS/redirect、late Secret resolution、bounded HTTP | Run/Invocation/Job current state、Secret 持久化、Provider adapter 语义 |
| `secret-broker`（Egress内部可信组件） | current SecretBinding gate、opaque reference解封、startup manifest安装的Provider选择、exact version evidence校验 | Secret value/reference持久化、公共API、业务current state、任意Provider加载 |
| `storage-postgres` | repository、CAS、lease、outbox | 领域决策 |
| `artifact-store` | blob I/O、integrity、GC | Run 状态机 |

物理部署可以复用同一个 Rust workspace，但不同 Worker role 必须使用独立 Deployment、连接池、并发
配置和readiness。Sandbox Dispatcher消费固定OpenSandbox lifecycle与Armed runner state/activate/read-result schema；OpenSandbox可以独立发布，但只能通过
ADR-0007版本化internal protocol接入，不能获得Platform repository port。
Artifact 首版只有Gateway、Data Worker和Maintenance三个物理role，分别使用独立
Deployment、ServiceAccount、数据库连接池、storage identity与permit。业务owner通过同一事务形成Ready引用，
Artifact进程不直接改写Run、Invocation或Job。Model输入输出首版都是Inline-only，因而不建设Model专用Broker或Producer。
`egress-broker`是独立基础设施角色，使用专用workload identity、连接池、并发/字节预算和NetworkPolicy；它只接收
credential-free closed request，并只返回sanitized metadata与bounded byte stream。它不得保存业务current state、把Secret
返回给Worker，或接受调用方提供的URL、header、proxy和redirect target。
`secret-broker`是该角色内的独立Rust所有权边界，不是第二个持久化服务：它只组合PostgreSQL的只读SecretBinding authority、
KMS/AEAD reference unsealer和进程安装的外部Secret Provider client。普通Management/Runtime/Host代码不得依赖受信resolution
projection。需要Secret的Sandbox Deployment只有在OpenSandbox安装并通过独立secret-injection合同后才能activate；首条CR-216
无Secret流程不得通过明文environment或调用方input绕过该边界。

OpenSandbox物理实现由[ADR-0007](../../adr/0007-opensandbox-execution-provider.md)固定：首版显式使用 Kubernetes provider、BatchSandbox
Controller 与 containerd/runc，每个 physical attempt 只有一个 selected ephemeral sandbox。create 可产生 bounded inert candidates，
PostgreSQL CAS 选择唯一 candidate，fixed runner activation 最多启动一次 Package。OpenSandbox/Pod/runner status 只是外部 evidence；
Dispatcher terminal transaction 仍重验 current Job lease fence。OpenSandbox 没有 Platform DB、Artifact store、Kubernetes write authority
以外的编排权限，也不能直接调用 Run/Invocation mutation 接口。

## 6. 依赖规则

允许的依赖方向：

```text
API / Worker adapters
        ↓
Application services
        ↓
Domain contracts
        ↑
Storage / transport implementations
```

以下依赖被禁止：

- Domain crate 依赖 Axum、SQLx、NATS、Docker SDK、OpenSandbox SDK或具体Provider SDK；
- Storage crate 依赖 API 或 Runtime composition；
- Skill crate 依赖 Sandbox、MCP transport 或 Secret resolver；
- MCP wire crate 依赖 Agent DSL；
- OpenSandbox Server直接写Job、Run、NodeExecution或Invocation，或Sandbox Dispatcher绕过owner terminal transaction；
- Model 输出直接构造数据库 command；
- 任一 Execution Plane 组件移动 active head 或发布 Revision。

## 7. 核心所有权接口

以下接口只定义所有权形状，具体 DTO 由后续规范冻结：

```rust
trait RegistryStore {
    async fn publish_revision(&self, command: PublishRevision) -> PublishReceipt;
    async fn compare_and_activate(&self, command: ActivateRevision) -> ActivateReceipt;
}

trait OrchestrationRepository {
    async fn admit_run(&self, command: AdmitRun) -> RunReceipt;
    async fn claim_work(&self, claim: WorkClaim) -> Vec<LeasedWork>;
    async fn commit_outcome(&self, command: CommitOutcome) -> CommitReceipt;
}

trait CapabilityBackend {
    async fn invoke(&self, request: InvocationRequest) -> InvocationOutcome;
    async fn cancel(&self, request: CancelRequest) -> CancelOutcome;
}

```

接口返回值必须是闭合枚举，不允许以任意 JSON 状态字符串扩展状态机。
Context backend port不在本架构总览重复声明；其唯一trait、request/outcome与continuation/cancel合同由12 `ContextBackend`拥有，组合层只依赖该port。

## 8. 端到端调用

```text
1. Client 提交 Run
2. Runtime API 在一个事务中固定 Deployment 与 RunBindings
3. Scheduler claim Ready NodeExecution
4. 纯 Plan 决策产生 Model、Capability、Context 或 ChildRun command
5. Repository 持久化 command 与 outbox
6. 对应执行隔舱领取工作
7. 后端返回 Completed，或返回 Accepted/InputRequired/ApprovalRequired
8. Repository 以 fence 提交 outcome
9. Scheduler 从已提交 outcome 继续推进
10. terminal result 与 ArtifactRefs 原子关联后，Run 进入终态
```

任一步的进程内内存、HTTP 连接或 NATS 消息丢失，都不能成为恢复所需的唯一事实。

## 9. 全局不变量

- 一个 Run 只绑定一个不可变 Agent Deployment；
- 一个逻辑 NodeExecution 可以有多个 Attempt，但只能有一个有效 committed outcome；
- 每个跨进程调用都有稳定 Invocation ID、idempotency key、deadline 和精确 Deployment binding；
- Skill、Prompt、Context 和外部 discovery 内容一律视为不受信任输入；
- 代码执行永远不发生在 Control Plane、Scheduler、Model Worker 或 MCP Host；
- ContextSource 默认只读；需要副作用的操作必须成为 Capability；
- active head 只影响未来 Deployment/Run，不能修改历史绑定；
- emergency suspension 可以阻止尚未开始的外部 leaf，但不能改写已提交结果；
- 所有列表、队列、递归、并行、正文、输出和 Artifact 都有配置上限与平台硬上限；
- 平台不从 timeout 推断外部非幂等操作“没有发生”。

## 10. 故障边界

| 故障 | 预期行为 |
|---|---|
| API 重启 | 已提交 admission 可查询和继续；未提交请求由 idempotency key 重放 |
| Scheduler 重启 | 从 PostgreSQL 已提交事实重新推导 work |
| Worker 丢失 | lease 到期后新 epoch 接管，旧 worker 无法提交 |
| NATS 丢消息 | safety scan 发现未闭合工作 |
| Sandbox 饱和 | Sandbox Invocation 保持 Ready/Queued，不占 Model permit |
| MCP 断线 | Invocation 等待重连、remote Task 或明确失败 |
| Artifact 上传失败 | 不提交业务引用；暂存对象由 GC 回收 |
| active head 变化 | 已存在 Run 不受影响 |

## 11. 可观测性边界

所有组件必须传播 `trace_id`、`run_id`、`node_execution_id` 和 `invocation_id`，但 tenant/user identity、
Secret reference、URL query、Prompt、代码正文和模型正文不得进入 metric label。日志默认记录稳定 ID、
状态、时长、字节数、attempt 和错误类别，不记录输入输出正文。

## 12. 验收标准

本规范进入 Accepted 前必须满足：

- 02～17 的术语表不重新定义本文件中的核心对象；
- 每个 durable state、外部协议、Secret 和 Artifact 都有唯一 owner；
- crate/service 依赖图不存在环；
- 至少有一个 sequence test 证明 Run 可跨 API、Scheduler、Worker、Sandbox 进程恢复；
- Sandbox、MCP 和 Model 任一隔舱饱和不会占用其他隔舱 permit；
- 架构测试禁止 Domain 对 transport/storage SDK 的反向依赖；
- 公开文档不会把 Skill、Subagent、MCP Tool 或 Script 互称为同一种对象。

## 13. 明确推迟的工作

- 具体 Plan 节点代数与 authoring syntax：05；
- durable Run 状态枚举：06；
- 并发数值和调度算法：07；
- Capability schema：09；
- Sandbox backend 选择与协议：14；
- 物理 Kubernetes topology 和 SLO：18。

## 14. 未决问题

基础领域边界没有未决问题。下游只能选择实现细节，不能合并本规范已经分离的核心对象。
