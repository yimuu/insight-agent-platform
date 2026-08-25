# Platform v2 四阶段实现计划（CR-187）

| 属性 | 值 |
|---|---|
| 状态 | In Progress / CR-187 contracts accepted; L3～L6 pending |
| 日期 | 2026-08-25 |
| 合同输入 | 00～18、cross-review CR-181/实现反馈复核、ADR-0001、ADR-0002、AGENTS.md |

> 2026-08-24：production external leaf接线发现Plan v3缺少可执行payload及candidate selection evidence，CR-181已重新打开04～18与cross-review。
> CR-181 cross-review已经关闭并恢复实现授权；Leaf/Task/Subagent dispatch必须直接实现Plan v4与exact selection/owner transaction，
> 不得恢复Plan v3或caller-supplied completion。已通过的CR-180 terminal authority实现和证据保留，不回退。

> CR-182 cross-review已关闭并恢复实现授权；candidate selection必须使用04 schema v1三种closed mode与共享纯evaluator。

> CR-183确认Selection document自身不含digest，publication只接受外层`rules_digest == canonical(document)`。

> CR-184关闭external leaf terminal重复dispatch缺口：Model/Capability/Context terminal owner必须原子终结当前leaf Node、写exact output
> RunValue并激活Plan `resume`目标Node/Job；不得重新Ready同一leaf Node。cross-review已恢复实现授权。

> CR-185已关闭：Skill package首版物理合同为11定义的无压缩`insight.skill-package/1` frame；实现必须先完成
> dedicated Artifact verification/materialization，再接入InstructionSection与deterministic Model request assembler。

> CR-186已关闭：deterministic Model request assembler使用七阶段canonical block和完整source map；Skill/Context/User固定为非特权
> role，ordinal冲突与byte/token overflow fail closed，首版没有隐式truncation或恢复时重排。

> CR-186 L1 pure assembler、Model/provider contract tests以及fresh PostgreSQL 16 ModelTurn持久化/tool continuation/first-winner
> 回归已通过；从exact Agent/Plan/Skill/Context材料构造这些block并接入production Scheduler admission仍待后续批次，不能把本证据
> 宣称为完整ModelLoop L3。

> CR-187已关闭production Model admission的Policy来源缺口：Safety/Budget/PublicProjection均为closed nominal Policy document，
> Model Deployment冻结exact Safety Revision；production provider必须从这些exact documents推导安全指令、attempt/token/cost与overflow，
> 不接受测试默认值或caller自由字段。

> CR-187 exact Model Policy loader已在fresh PostgreSQL 16 r177验证Model Deployment digest、profile Revision、三个enabled published
> Policy的exact semantic/payload/rules digest与PolicyKind；wrong Deployment digest fail closed。该证据只覆盖admission facts读取，完整
> canonical request与Skill/Capability投影接线仍待完成。

> 2026-08-25 implementation evidence：ModelTurn admission现已把Plan列出的Skill/Capability slots逐项冻结进snapshot与Receipt replay，
> 并在owner transaction用各slot的exact Selection Policy重新执行共享selector；请求投影即使引用候选集合内的合法但未选中
> Capability Deployment也会整批回滚。fresh PostgreSQL 16 r178已覆盖两候选`ordered_first`正/负路径及既有tool continuation、quota和
> first-winner回归。该证据仍不等于production canonical request provider或完整ModelLoop L3。

> CR-183已实现ChildAgent exact input/route/Selection Policy facts、SERIALIZABLE owner事务重算及PostgreSQL durable Plan store dispatch；HumanTask exact Plan owner/store、response Scope binding及owner-derived resume/failure事实已接线；Timer与Signal wait均由exact Plan及数据库时间的owner事务派生。Signal owner验证exact key、可选payload schema/摘要，将payload写为immutable RunValue并绑定当前Scope；Timer due与Signal timeout使用Job typed scheduling列和critical-control bounded scanner，普通wake/timeout deadline窗口互斥。上述first-winner、Receipt replay与扫描恢复已在fresh PostgreSQL 16 r88通过；Timer另已在fresh PostgreSQL 16 r113完成真实协调器L3链路（claim、durable park、独立safety到期唤醒、continuation claim、Return物化与Run终态）。Child/Task/Signal的独立进程kill/recovery L3、Timer kill-window、Signal认证ingress及Model/Capability/Context external leaf仍待完成。
| 公开协议 | `insight.platform/v1`、`/v1`，clean cut |

## 1. 计划原则

本计划只描述目标实现顺序，不宣称任一Draft API、schema、topology、capacity或runtime已是current behavior。
只有当phase的code、migration、manifests、tests、runbooks和evidence同时通过适用门禁，才能标记完成。

CR-173的恢复顺序已经完成到public route/schema与L1～L3开发门禁：closed Deployment matrix → exact binding/Run snapshot →
public route/schema。production qualification与cutover仍必须继续执行L4～L6，不能用开发fixture、静态manifest检查或合同Accepted状态替代。

CR-174进一步冻结Scheduler执行Branch/Map/Loop/Compute所需的closed typed expression IR、exact RunValue evidence与
materialize/evaluate/first-winner提交边界。此前手工注入`ControllerObservation`的fixture不能计为production handler证据；Phase 2
必须增加expression evaluator、owner transaction和不接受外部observation的多进程fixture。

CR-175补齐`HardLimitProfile v5`的expression instructions/input ports/stack depth三项typed limit。Phase 1 machine schema、fixture、
startup validation与Phase 2 Plan publication/evaluation必须消费同一profile字段；不得借用其他Plan limit或只依赖代码常量。

CR-176冻结Scope aggregate内的exact data-port environment。Phase 1 baseline须对齐Inline JSONB结构hard max；Phase 2必须实现
root input binding、bounded lexical lookup、Compute/Map/Loop原子RunValue+Scope CAS，并证明同一Plan port在动态Scope间不串值。

CR-177冻结expression-derived classification：Phase 2 repository必须从提交事务内重验的external input RunValue计算lattice join，
空input closure固定`Internal`，并禁止command/Worker/Artifact metadata降低Compute output classification。

CR-178把Map item port收紧为exact NodeOutput ref并提升未发布Plan wire version 2；Phase 2必须为每个item创建immutable RunValue并与
MapItem Scope/Node/Job/batch cursor原子提交，wrong producer/schema与批次重放必须fail closed。

CR-179冻结Loop carried Scope生命周期：body settlement原子复制carried RunValue并预建下一iteration Scope，continuation condition与
body复用该open Scope，false exit原子关闭并回到固定词法父Scope；iteration Scope不得串成父链或读取terminal Scope。

CR-180冻结Run terminal authority：Plan v3的Return/Raise分别引用exact final value/failure port，publication对齐Agent
output/error schema；Phase 2必须实现bounded Scope解析、Inline/Artifact正文物化与owner transaction重验，证明terminal RunValue/
Reference、Scope/Node/Job/Run、quota、Receipt/Event/Outbox为单一first-winner原子提交，并拒绝Plan v1/v2与外部terminal注入。

实现遵循以下顺序：

```text
Phase 1 合同/持久化基线
        ↓
Phase 2 durable orchestration 与可调用内核
        ↓
Phase 3 协议/数据/不受信任执行平面
        ↓
Phase 4 public /v1、部署、资格与clean cut
```

每个phase拆成可review、单一目的commit；一个批次通过比例化检查后先commit，再开始下一批。

## 2. 当前证据边界

当前仓库有模块化Rust workspace、shared Job/Run/Artifact/MCP/Sandbox等候选实现、一个23表schema contract v7 baseline与大量
开发fixture；部分代码源自CR-171之前的candidate，曾包含已推迟的microVM、
Managed stdio session、Model Artifact或过度Artifact role拆分。

因此：

- 可复用的领域与仓储代码必须通过CR-171对照审计后才计入phase evidence；
- 仅存在crate、test、Helm manifest或migration object不等于目标功能完成；
- 已推迟代码需从default build、runtime composition、deployment和公开contract移除，可在后续独立研究分支保留；
- 本批规范修订不改代码，不改变current runtime behavior。

## 3. Phase 1：合同、ID 与minimal persistence baseline

### 3.1 目标

建立后续代码唯一依赖的machine contract和schema v7基线，先删除过度authority，不立即实现所有runtime。

### 3.2 实现批次

1. **Owner types 与registries**

   - 冻结nominal IDs、ResourceKind、WorkClass、JobKind/OwnerKind、TaskKind、EventKind、ReceiptKind和problem codes；
   - 删除`SandboxJobId`、独立`OperationId`、Installation Release/Gate types、ManagementOperation和Model Artifact output types；
   - 保证RunValueId、JobId、ArtifactId等不共享同UUID alias；
   - 从owner types/registries生成或校验JSON Schema/OpenAPI/protobuf/Receipt projection。

2. **Minimal migration v7**

   - 确认candidate schema尚未生产发布；
   - 用一个reviewed baseline migration替换未发布的1～35候选集；
   - 保持23张总表/22张业务表，无Installation Release、ManagementOperation、SandboxJob或Gate表；
   - 仅保留PK/FK、tenant、unique、CAS、lease、structural check和outbox durability等DB authority；
   - 不用trigger重复Rust业务状态机。

3. **Shared repository foundation**

   - Resource/Version/Deployment、Run/Invocation、Job、Task、Event、Receipt、Outbox、quota与Artifact domain repositories；
   - typed JSONB decode/validate/canonical digest和size limits；
   - 统一Receipt claim/replay、CAS、lock order、Job lease、Event/Outbox transaction helper；
   - Operation safe projection直接来自Job，无第二current state。

4. **Remove deferred composition**

   - 从workspace default-members/binaries/feature graph、runtime composition和Helm manifests移除microVM/Firecracker/KVM；
   - 移除Managed stdio runner/session child与Model Artifact Producer/Broker composition；
   - 收敛Artifact server roles为Gateway/Data Worker/Maintenance；
   - 不必在本批物理删除所有历史源文件，但它们不得参与default build/release，也不得被目标代码依赖。

### 3.3 Exit gate

- owner/ID/kind registries的正负矩阵和codegen/conformance通过；
- fresh PostgreSQL 16/17执行唯一baseline成功，tenant/FK/CAS/lease/Receipt/Event/Outbox fixture通过；
- schema v7表数与ADR目标一致，但行为测试而非表数作为完成证据；
- default build/deployment不包含五类推迟authority；
- `cargo fmt --check`、workspace `cargo check`、相关Clippy/tests和migration smoke通过。

## 4. Phase 2：durable orchestration 与可调用内核

### 4.1 目标

交付Resource lifecycle、Run/Node/Subagent、Scheduler/Worker、CapabilityInvocation、Task与最小Model loop的端到端durable闭环。

### 4.2 实现批次

1. **Resource lifecycle**

   - Resource current Draft -> publication创建immutable ResourceVersion -> Deployment -> tenant active binding；
   - validation Job、publication、activation/suspension和exact RunBindingsSnapshot；
   - active变化只影响未来Run，无Installation Release dependency。

2. **Run、Node 与Subagent**

   - Run admission、typed Plan、Node/Scope/token、pause/resume/cancel/timeout/terminal convergence；
   - Subagent是child Run，具有exact binding、quota、deadline和typed parent link；
   - parent/child crash、cancel propagation、late result和result schema。

3. **Scheduler/Worker**

   - deterministic tenant fairness、claim前RAII slot、durable quota bundle、lease/heartbeat/fence和retry_at；
   - deferred WakeContract、bounded safety scan、critical-control reserve和drain/handoff；
   - WorkClass隔舱，不引入Model materialization、stdio session或microVM专用容量。

4. **Capability、Task 与Model Inline loop**

   - Native/HTTP/gRPC/MCP Tool/Sandbox typed backend映射，本phase先以fake/port驱动外部backend；
   - approval/input Task、callback/poll Receipt、Effect-aware retry/reconcile/UnknownOutcome；
   - ModelTurn、deterministic request、tool intent -> CapabilityInvocation、structured response和Inline hard limit；
   - 超限返回`model_output_too_large`，不创建Artifact。

### 4.3 Exit gate

- fresh DB上从Run admission -> Model -> Capability/Task/Subagent -> terminal result的多进程端到端fixture通过；
- 并发activation/admission不改写已存Run binding；
- 并发claim/Receipt/callback/Task terminal只有一个winner，旧fence全部拒绝；
- NATS全丢、Worker kill、DB connection饱和与drain后可恢复；
- domain/L2/L3证据通过，不将其声明为production capacity qualification。

## 5. Phase 3：协议、数据与Sandbox Execution Plane

### 5.1 目标

交付Context、remote MCP、Artifact三role、Egress/Secret、真实Model adapter以及WASI + gVisor的物理隔离实现。

### 5.2 实现批次

1. **Artifact三role**

   - Artifact/Blob/Link/Grant、static storage manifest、prepare/upload/verify/Ready/read/delete/GC；
   - Gateway/Data Worker/Maintenance三个binary/Deployment、closed caller capability和mutual-deny权限；
   - Operation=Job projection，Link create-time fence/current release fence语义；
   - Capability/Context/MCP/Sandbox共享Artifact port，无Model专用路径。

2. **Context 与Text2SQL**

   - Context Interface/Implementation/Binding、冻结parser/chunker/embedding/ranking闭包的Dataset Generation build Job、query/citation/cache；
   - 首次build预留Dataset ID、成功时物化root+generation、重建归属验证与active generation CAS；
   - Text2SQL按read-only planning/validation/execution/result-boundary拆分，SQL execution通过exact read-only Capability/remote service；
   - dataset/query saturation不占用Orchestration或Sandbox pool。

3. **Remote MCP Host**

   - Streamable HTTP negotiation、显式authorization binding的discovery、Tool/Resource/Prompt projection、Task/Elicitation、OAuth和subscription；
   - Egress Broker last-hop Secret resolution、catalog endpoint、SSRF/TLS/redirect/DNS/rate/byte/time limits；
   - 无stdio process、persistent Sandbox session或session child Job。

4. **WASI + gVisor Sandbox**

   - publication-time package/dependency/image/SBOM/provenance/scan freeze；
   - shared Job fenced Controller protocol、Executor无DB凭据；
   - real WASI ABI/fuel/memory/interrupt；
   - 真实`RuntimeClass=runsc`、admission-locked single-Job Pod、受限Launcher RBAC、no-runc-fallback、filesystem/network/Secret/Artifact grant与cleanup；
   - process kill、Controller restart、timeout/cancel和orphan reconciliation。

5. **Real Model/provider path**

   - provider catalog/deployment、adapter normalization、stream、usage/budget、safety、retry/cancel/unknown；
   - request/response Inline-only和tool loop端到端；
   - Model Worker与MCP/Sandbox/Artifact/Egress隔舱。

### 5.3 Exit gate

- real PostgreSQL + NATS + S3/KMS-compatible + fake/real protocol endpoints的端到端fixture通过；
- Artifact三role权限矩阵、wrong tenant/owner/fence/digest/storage generation全部fail closed；
- MCP protocol/OAuth/subscription、WASI ABI、gVisor escape/cleanup和Model adapter contract tests通过；
- Sandbox/Artifact/MCP/Model单lane饱和时其他lane与critical-control可用；
- default artifact/image/runtime不包含microVM、Managed stdio、Model Artifact或dynamic installer。

## 6. Phase 4：public `/v1`、部署、资格与clean cut

### 6.1 目标

交付minimal public API、production-equivalent topology、observability/runbooks、完整资格和GitOps clean replacement。

### 6.2 实现批次

1. **Public/API contracts**

   - `/v1` Resource lifecycle、Run/control/result/SSE、Task、Artifact和Job Operation projection；
   - OIDC/tenant/principal、If-Match、Receipt replay、problem、cursor、rate/quota/backpressure；
   - OpenAPI/owner schema conformance，无`/v2`、Release/Gate、dynamic storage、internal RPC proxy。

2. **Production topology**

   - 按18部署隔舱创建ServiceAccount、DB role/pool、NetworkPolicy、PDB/HPA、startup manifest和digest门禁；
   - WASI/gVisor节点/runtime、Artifact三role和Egress/Secret隔离；
   - PostgreSQL/NATS/S3/KMS/Secret的backup、restore、rotation和failure runbook。

3. **Observability 与qualification**

   - low-cardinality metrics、trace/log redaction、SLI/dashboard/alerts/runbooks；
   - L1～L6 fixture manifests、mixed load、fault injection、security matrix、soak、restore和rollout/rollback rehearsal；
   - 根据实测冻结首个production CapacityProfile，不使用Draft数字。

4. **GitOps cutover**

   - 生成signed image/SBOM/provenance/migration/test artifacts；
   - 提交exact digest到GitOps environment repository，人工审批promotion；
   - 执行clean `/v1` replacement，无dual write/fallback；
   - 失败时GitOps回滚到上一已资格闭包，不读写Installation Release row。

### 6.3 Exit gate

- 17的minimal OpenAPI与internal RPC正负conformance全部通过；
- 18的L1～L6资格矩阵、backup/restore、rollout/rollback和soak通过；
- 隔舱饱和、跨tenant、Secret/log、runc fallback、旧fence、重放和不确定outcome负向测试通过；
- production GitOps只引用exact已签名digest与已资格profile；
- 实现、规范、ADR、runbook、deployment和evidence对齐，无P0/P1遗留。

## 7. 跨Phase实现规则

### 7.1 Commit hygiene

- 每个commit只包含一个可review行为闭包；
- 合同、实现、migration/manifest与tests在同一commit；
- 已通过的完整批次立即commit，不长期混在working tree；
- 不提交known failing、half-wired或只有schema没有owner handler的状态。

### 7.2 不允许的快捷方式

- 不为了编译保留旧Operation/Release/SandboxJob/Model Artifact compatibility layer；
- 不在API/Worker中spawn code或运行package manager；
- 不用通用JSON owner、自由URL/header/shell或raw Secret穿透类型边界；
- 不把NATS、object store list、metric或Event当作current-state authority；
- 不通过新增表/服务/队列来规避已有shared model的owner设计；
- 不用表数、trigger数、route count或单进程fixture声明功能/资格完成。

### 7.3 变更规则

如实现发现规范冲突，停止代码，先按upstream -> downstream修改owner规范，将受影响文档退回Draft，
执行00～18 cross-review，再更新本计划。实现不得自行发明第二current-state authority。

## 8. 总体完成标准

Platform v2只在Phase 1～4全部exit gate通过、CR-171影响规范推进为Accepted、schema v7与migration实际发布、
production CapacityProfile经L4～L6证明、GitOps clean cut完成后，才能宣称完成。

本计划本身不完成任何phase；它只是Reviewed合同的执行顺序。
