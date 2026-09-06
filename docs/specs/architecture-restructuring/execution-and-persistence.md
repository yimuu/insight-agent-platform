# 执行内核、调度与持久化目标设计

本文是架构重构提案，不是当前实现说明，也不授予修改 Rust、数据库或线上环境的权限。当前行为仍由
[`contracts/platform-v1`](../../../contracts/platform-v1/README.md)、拥有类型、已登记 migration 和
[`docs/current`](../../current/README.md) 定义。本文描述最终设计及其替换条件，不是按投入排序的修复清单。
架构 ADR 与上游合同必须一起完成交叉评审并被接受，才能开始实现或 schema 工作。

固定分区映射、兼容窗口和新增 Job identity 的提案值仅记录在
[`execution-contract-targets.json`](execution-contract-targets.json)。该附件明确标记 `proposal_only`，运行时、
发布校验和当前合同生成器不得消费它。实施时必须把决定移入对应拥有类型并生成机器合同；在实现、合同、当前文档
和证据一致后删除本提案及附件。附件中的分区数量和兼容窗口是设计约束，不是容量测量结果。

## 1. 基线与保留的架构选择

保留自研 durable kernel，且只保留一个 PostgreSQL current-state authority。不叠加 Temporal/Restate 的工作流状态机，
不引入以消息可见性、provider status 或事件重放为准的第二执行内核。受约束 Program 的解释器与模型、Capability、
Context、人工交互领域共享这个底座，但其业务语义不属于通用 Job lease 状态机。

本提案保留 [ADR-0001](../../adr/0001-platform-v2-postgres-baseline.md) 的业务事实归属、事务原子性和只读 runtime schema
校验；受控改变全局调度串行边界、fresh-only 后续演进、PKCE 清理的持久交付方式。保留
[ADR-0007](../../adr/0007-opensandbox-execution-provider.md) 与
[ADR-0008](../../adr/0008-sandbox-runner-capability-boundary.md) 的唯一 Sandbox 路径、one-shot activation、物理证据和
cleanup fence。PKCE 清理迁移不能被推广为把 Sandbox terminal cleanup 拆成另一 Job：后者仍属于原 physical Job。

已检查的基线包括：Run/Job/Task owning types、WDRR 选择器、shared schema、OAuth callback/expiry 事务与 cleanup
authority、worker manifest、Run binding、RuntimePlan 和 schema verifier。当前 repository 中的全局 WDRR、通用 Outbox
发布缺口及单版本执行约束是重构输入，不在本文被重新描述为目标能力已经可用。

## 2. 模块责任与持久化入口

模块边界不自动变成 RPC 或独立进程。共用一个 PostgreSQL 事务的 use case 必须能在同一可信进程中组合；不得把
repository 拆成远程 CRUD 服务后再用补偿事务重造当前原子性。crate 与目录的最终落点由本提案总目录清单统一指定。

| 目标模块 | 拥有的语义和持久化入口 | 不得承担的责任 |
| --- | --- | --- |
| Definition / Registry | immutable definition、发布验证、exact deployment closure、绑定解析 | 推进已 admission 的 Run |
| Program / Run domain | 程序 activation/scope、数据依赖、Run 控制与逻辑收敛 | 解释 provider 当前状态为业务终态 |
| Invocation domains | Capability/ModelTurn/ContextQuery/MCP operation 的逻辑结果、效果、重试与 reconciliation | 复制 Job 的物理 lease 或 attempt authority |
| Task domain | 人工响应资格、deadline、generation、first-winner；领域专属终态义务引用 | 用 cleanup 状态重开已响应的人工 Task |
| Durable kernel | Job claim/start/fence、Receipt、事务提交约束、调度决定、恢复机制 | 认识 Agent 节点、prompt、MCP OAuth 或业务效果分类 |
| PostgreSQL adapters | 各拥有域的 repository、共享 Unit of Work、锁序和 schema verification | 在 SQL trigger 中复写完整业务状态机 |
| Execution adapters | 验证输入、物理执行、观察与取消、返回有界证据 | 绕过拥有域写入 Run/Invocation/Job current state |

用 domain command 替换跨域万能 repository 方法。Unit of Work 提供同 tenant、数据库时间、固定锁序、Receipt claim、
Event/Outbox append 和 commit/rollback；每个 repository 只暴露自己拥有的读写能力，不暴露允许任意表 mutation 的接口。
领域决定是 typed decision，数据库 adapter 负责以当前 version/fence 持久化；不生成任意 SQL 的“通用 mutation DSL”。

## 3. Run、Invocation、Job、Task 的事实归属

Run 固定定义与执行语义，持有全局控制意图及终态。程序 activation/scope 保持当前局部推进事实，并继续复用 shared
`run_nodes`；不为每种控制结构新建 table。Run 不复制各 Invocation、Task、Job 的闭集状态。

Invocation 家族保持各自 nominal identity 和拥有 Rust 类型。CapabilityInvocation、ModelTurn、ContextQuery 与 MCP
operation 不能因为共用 `invocations` 表而被压成一个充满可选字段的公共类型。逻辑 Invocation 的 effect identity 与
idempotency identity 跨物理重试稳定；领域决定能否重试、是否等待人工处理以及如何收敛逻辑结果。

Job 是一个可调度物理工作的唯一 current authority。worker process generation、lease generation、token digest 和
optimistic version 共同约束提交。Job 的 immutable owner 是 back-reference；拥有行上的 current Job pointer 决定其当前
关系。保留现有合法 owner 对和 Sandbox 自拥有 Job 的特殊合同，不把 Job owner 改成任意 Resource，也不新增 Attempt 表。

Task 独立保存人工 first-winner；等待人工期间不占用物理执行 lease。回应必须绑定 exact Task generation、拥有对象快照、
response schema 与当前授权。Task 终态可原子创建其领域要求的后续 Job，但后续 Job 成功不是人工响应已经生效的前提。

Run counters 逐项按用途处理：仍参与并发 guard、quota、终态或公开 cursor 的值保留明确 authority；纯展示进度从读取
模型计算。不能先删除 guard counter 再以异步 Event 投影补回。本文不承诺无根锁：公开事件仍在 Run 行分配连续
`public_sequence`，因此发布公共事件的提交保持每 Run 的短暂串行点；不做范围预分配或第二 sequence service。

## 4. 事务、并发与命令协议

### 4.1 提交边界

一次领域命令在一个 PostgreSQL 事务中提交 current state、quota reserve/settle、Event、要求投递的 Outbox，以及 Receipt
completion。Receipt 重放返回此前的确定结果；同 key 不同 request digest 拒绝。收到提交结果不确定时以原 identity 查询/
重试，不生成新 effect key。纯决定只消费已锁定事实和 PostgreSQL 时间。

需要外部 I/O 的命令先提交可恢复意图，事务外调用 provider，随后以当前 authority 重新校验再提交结果。Artifact/Secret/
Egress 的预授权不能替代提交前的 fence 和当前安全检查。外部 I/O、解析大对象和等待远端都不能持有业务行锁。

授权判断必须携带目的：新业务 dispatch、正文披露与受限的系统 completion/reconciliation/cleanup 分别判定。用户、
资源或凭证撤销立即禁止前两类；不得因此拒绝已合法准入 effect 的 fenced 完成记录、quota settlement、取消和必要清理。
第三类只允许记录既有意图的结果、查证该 effect 或履行 exact cleanup，不能借恢复角色发起新业务动作、读取无关 Secret
或向原调用者披露正文。结果写入 authority 与将结果返回用户是两个授权检查。worker 身份本身撤销后不能继续提交，
由仍获授权的恢复角色取得新 lease/fence 接管；此权限不绕过 current owner、control generation 或 exact effect identity。

保持 `SERIALIZABLE` 为 Run/调度等跨 aggregate mutation 的默认隔离级别；不将 schema 重构与未经证明的隔离降级混做。
SQL serialization/deadlock 是有界可重试基础设施结果；domain conflict、quota saturated、unsupported runtime 和
dependency unavailable 必须在 typed error 中分开，不再把正常背压报告为数据库故障。重试复用完整命令 identity。

### 4.2 锁序与局部并发

同一命令先明确涉及的 identity 集，再按规范顺序取锁：Receipt，调度 partition（claim 与 tenant 公平行 provisioning），tenant scheduler state，
tenant/policy/quota，
Run 控制根，scope/activation/Invocation/Task 的父子拥有关系，Job，Event/Outbox。同级 identity 采用稳定 canonical
排序。不同命令不得先锁子对象后追溯取父锁；需要扩大锁集时回滚并重新开始。规范顺序应由共享 transaction API 与
并发测试约束，而不是复制注释到每个 repository。

Run admission 与取消/终态对根控制边界互斥；普通局部推进持有足以阻止 control generation 改变的共享根锁，只有更新
根状态、guard counter 或公共序列时需要排他锁。凡会分配公共序列的命令必须一开始取得对应根排他锁，不能先共享后
在并发事务中升级成死锁。scope/activation 是局部竞争边界，Job heartbeat 不触碰 Run 的展示统计。

一个 Run 的全局终态必须由 root convergence command 裁决。它关闭新的工作 admission，验证程序根与所有仍能产生后续
工作的当前拥有关系，并完成必要 quota settlement；不能仅靠一个缓存的“运行中 Job 数量为零”判成功。兄弟 scope 的
完成不会自行改写 Run 终态，迟到 Task/callback 不能重新创建关闭后的工作。

### 4.3 物理效果协议

物理 work 在首次可能触达外部效果前持久化 exact request 与 logical effect identity；attempt identity 不参与逻辑幂等键的
重新随机生成。领取只授予执行权，不授予重做不确定写入的业务许可。领域为每种 backend 明确可查询、可幂等重发和
只能 reconciliation 的路径，kernel 只执行该决定。

非幂等调用的“已执行但结果未持久化”必须保持 unknown/reconciliation 语义。取消确认、网络断开、进程退出和 lease
过期均不自动构成 no-effect proof。Sandbox 仍按照原 Job 的 selected candidate、activation evidence 和 boot identity
恢复；不能利用新 kernel 引入 replacement activation、host execution 或其他 provider fallback。

## 5. 取消、deadline 与恢复

Run cancel 首先原子提交控制意图和 control generation，禁止新的子工作 admission；domain cancellation driver 再按
当前 pointer 推进 Task、Invocation、child Run 与 Job。Job fence 控制物理结果可否提交，Run/Invocation 的 control policy
决定竞态中哪个结果仍可接受。已经提交的 success 不被迟到取消改写，已经赢得控制裁决的取消不被旧 attempt 结果覆盖。

等待、暂停与终态必须分开：durable wait 释放执行 lease；pause 阻止新业务 dispatch，但不停止 heartbeat、回调接收、
取消和 reconciliation。绝对 deadline 由数据库时间判断，滚动发布、停机迁移和恢复都不偷偷延长 deadline。

恢复不是另一组特权 mutation。扫描器读取 PostgreSQL current row，通过同样的 owner command 处理过期 lease、timer、
retry、Task expiry 与子 Run 收敛；每次提交重验 observed version/fence。扫描使用稳定 keyset cursor、partition 和
批次上限，竞争失败只重试受影响页；某个损坏对象产生有界诊断并进入其拥有域处理，不能永久阻塞整个分区。

保留独立 critical-control connection/permit bulkhead。业务配额耗尽不能阻止取消、lease 回收、结果 settlement 和 PKCE
清理；这些路径仍受独立系统容量与安全权限限制。NATS 的 wake 可以降低延迟，但没有 wake 也必须通过周期扫描推进。

## 6. 调度 partition 与配额子集

### 6.1 稳定路由及唯一队列

所有 tenant-owned Job 使用附件中固定、版本化的虚拟 partition 映射。同一个 tenant 的同一 work class 在一个 partition
内排队；worker/HPA、副本数量和 CapacityProfile 不改变该映射。相同 tenant identity 在 Rust、迁移和恢复工具中必须
产生相同结果。固定空间选择是可审查的工程上限，不证明任意 tenant 数量或吞吐量可达。

tenant 行保存 immutable partition 路由 authority。Job 保存服务索引所需的路由列，并以同 tenant 复合 FK 约束到 tenant
路由；它是不可独立更新的派生键，不是第二份调度决定。`scheduler_state` 只拥有每个 work class/partition 的 round、
冻结 tenant keyset 上界与枚举位置。新增 `scheduler_tenant_state`，把原 JSON 内的 tenant deficit 迁出到每 tenant/work class 的独立行；该行拥有
有界公平账务、policy version 和 Job 扫描 continuation，并有 partition 索引。附件记录两者的目标 authority key；
字段与约束由 scheduler owning type 和新 migration 定义，不保留 JSON 镜像。

这张表具有独立的公平账务保留期和并发查询边界，行数随实际 tenant 与闭合 work class 集合的组合增长，单行字段有界；
它不复制 ready Job、Job 状态、backlog 或 quota。创建 tenant 时在该新 tenant 对 claimant 可见前，同事务建立所需公平
行。provisioning 预先按稳定顺序取得涉及的 partition 锁，记录公平行最早参与的下一 round；不能在持有 tenant/owner
锁之后补取 partition 锁。这样即使新 UUID 排在当前 cursor 前方，也不会混入已开始 round 的成员集合。现有 tenant 经
受控 forward migration/backfill 建立。新增 work class 必须先完成该类公平行 provisioning，再启用
该类 Job admission/claim。普通 Job 创建只验证结构已存在，禁止在已持有 tenant/Run/owner 锁后延迟 UPSERT 公平行；
缺失时返回可诊断的 provisioning/schema 错误，不动态修补或重置 credit。新 tenant 的初始化事务不更新既有公平行，
现有 tenant 的迁移则在对应 claimant quiescence 下执行，避免与 claim 的公平行→tenant 锁序反转。
公平行不会因为离开 candidate window、暂时无 Job 或进程重启而删除；只在 tenant retirement 或
带审计的显式 policy reset 时退休或重置。不新增 ReadyQueue、tenant backlog 表或 durable worker assignment。

claimant 在现有 `scheduler_state` 中轮转选择 partition，使用 PostgreSQL 行锁排斥同 partition 的并发选择；其他
partition 可以独立领取。进程维护的轮询起点只是提示；进程退出不会丢工作或改变公平性事实。不增加外部 leader lease。
没有候选工作的 partition 也使用低成本、有界轮询，不能要求每次对全 Jobs 表发现活跃 partition。

partition 的 tenant round 同样跨事务持久：开始时冻结该轮 eligible tenant 的 keyset 上界，只枚举已具备该轮参与资格
的行，单次 tenant window 耗尽只 yield 并保留 cursor；完成冻结范围才开始下一 round。新 tenant 从下轮参与，持续
创建 tenant 不扩大本轮范围或阻止老 tenant 再访。round 和上界属于原 partition state 的新持久 cursor 合同，不增加
成员清单表；tenant 公平行仅保存有界的参与 round，不保存队列。跨 window 不能重置 deficit 或再次发放同轮 quantum。

候选查询先限定 partition，再以公平行的 partition 索引从 cursor 枚举有界 tenant window，最后用参数化 keyset 查询
读取各 tenant 的 Job。空闲 tenant 允许被访问，但必须推进 cursor；不能先对全体 ready work 排序再限制 tenant 数量。
每个 tenant 的 Job continuation 随访问提交，不得每次重新读取同一批队首；quota-blocked Job 之后的可准入 Job 必须
可被后续批次访问。主 sweep 使用数据库拥有的 immutable Job creation ordering，具体键与 cutoff 规则由附件提出，
不得用可变 due/priority 排序键推进主 cursor。开始 sweep 时冻结数据库 creation cutoff 与该范围的 keyset 上界，
跨事务保存 continuation；后续创建不进入该范围，也不能延长上界。cutoff 之前创建但迟到提交的行若已落在 cursor
后方，在下轮或独立重查中访问；不为等待此类事务持有 Job 锁或倒退 cursor。

只有走完冻结范围才 wrap 到最旧 creation key；单次事务的扫描预算耗尽只 yield 并保留位置，不能因为总访问条数
达到某 cap 就重置到头。current due、worker compatibility、priority policy 和 quota 每次访问重新判断 eligibility，
重试变更 due key 不改变 Job 在主 sweep 中的位置，也不能使同一行重复进入主扫描。独立有界 head/aging probe 可以
使用 due/priority 索引加快老工作和紧急工作的访问，但不重置主 cursor，不承诺所有 Job 的严格全局优先级顺序。
扫描位置只是公平行的枚举事实，不授予 execution authority；初始候选枚举不预先锁 Job，后续仍按父 owner→Job
锁序重验当前 identity、版本和 quota。从而超过多页预算的后部工作与此前跳过的老 Job 都有重查路径。

Job 索引分别支持 partition/work class 下的 immutable creation sweep，以及 typed execution requirement 和 due/priority
probe；具体列顺序由查询计划和真实 backlog 证据决定后写入 migration。候选窗口与总公平行数分别有界管理；不能
用旧 JSON state cap 删除不在窗口内的 credit，也不能把客户端返回行数上限充当数据库扫描成本证明。

### 6.2 WDRR 与配额的组合算法

WDRR 的精确核算范围是一个 work class/partition。权重、burst、aging 与既有成本单位继续由拥有 policy/type 定义；
它约束成功提交的物理领取量，不承诺 CPU 时间、模型 token 或每个租户端到端延迟同比例。跨 partition 只提供近似
服务公平，不能以分片后无全局锁为由继续宣称全局严格 WDRR。

一个领取事务按以下流程执行：

1. 锁 partition state，锁其 bounded tenant window 对应的公平行，读取持久 credit、policy 与 Job continuation，
   再枚举有界候选并验证 worker 支持的 typed execution requirement。
2. 按全局锁序锁定候选涉及的 quota accounts 与必要 parent rows，重验当前 eligibility；因并发已变化的候选不使用
   陈旧版本继续领取。
3. 按 WDRR 的提议顺序逐候选计算账户 shadow reservation。只有该候选所有适用账户均有余额时才加入 admissible
   subset；接受一个候选立即从 shadow balance 扣减，后续候选共享同一已锁定事实。
4. 满额、缺少兼容 worker 或当前暂停是跳过候选的正常原因；它们不创建 lease、attempt、reservation 或 successful
   service 记录。继续访问同 tenant 后续 Job 和其他租户，直到有界扫描或领取额度用完。
5. 仅对选中的子集原子提交 Job lease、真正需要的 parent mutation、quota reserve/ledger、Event 和 Outbox；WDRR
   deficit 只扣成功 claim 的成本。partition 位置、各 tenant credit/round 和 Job continuation 同事务提交；即使
   admissible subset 为空，已完成的有界扫描仍前进。

quota 饱和不得使整批回滚，也不能导致相同 cursor 永久重试。每个已提交 visit round 只增加一次有界 quantum；公平行
持久记录已计入的 round，restart 或跨 candidate window 不会重复赠予或丢失 credit，失败事务也不累积“服务次数”。
暂时无可执行 Job 的 tenant 不积累无界 idle credit，具体 accrual/burst/aging 由冻结 policy 裁决；改变 policy version
需要显式转换或审计 reset，不能以 decoder 默认值隐式清零。continuation 保留原 physical attempt 的语义，不能因 durable wake 再次算作
新业务 attempt。临时 eligibility 缓存至多减少扫描，不能覆盖 Job 或 quota current state。

quota accounts 继续持有严格额度守恒，ledger 仍是 reserve/settle evidence。不要另设分片 token ledger、shadow
business account 或先领取后异步扣款。一个极热 tenant 仍可能竞争其账户，这是保留精确 tenant quota 的明确代价。

### 6.3 分区迁移

固定虚拟 partition 不做在线重映射。初次改造以 forward migration 增补 tenant/Job 路由、scheduler key 和公平状态表，
校验所有 existing Job 与 owner 的路由一致；停用旧 claimant 后，将可证明的原 JSON 账务迁入公平行并删除原账务表示，
同一 cutover 只保留一个 authority。旧实现已裁剪或不能跨 policy 解释的 credit 不能凭空重建：迁移必须显式记录公平性
policy reset 及其覆盖范围，经验证后在单一维护窗口启用 claimant。禁止旧全局 WDRR 与新 partition WDRR 同时
领取，同一工作不能同时存在两套 queue authority。

如将来必须改变映射，先停止 admission/claim，让已开始的事务与物理 attempt 安全 handoff 或进入 reconciliation，
保留 waiting Run；在独立运维流程完成 backfill、FK/index 验证并使全部 runtime 使用同一映射后恢复。所有旧 worker
在改映射前被停止并撤销执行身份。未成功迁移保持停写，不能回退到旧算法扫描新布局。重新划分造成的 deficit reset
属于显式公平性变更，必须记录已接受 policy，不把历史服务计数伪装成可跨映射延续。

## 7. 定义、执行语义和部署版本

### 7.1 四类 identity 分开

定义 identity 是 immutable ResourceVersion/Deployment closure；IR ABI 决定已编译 Program 的结构和验证规则；semantic
runtime identity 决定控制结构、表达式、异常、取消与恢复如何解释；build digest 是实际运行的 binary/image 证据。
它们分别 version，不把 public API 版本或镜像 tag 当作共同版本。

Job 的 execution requirement 使用有 owning type 的 nominal family，不能假定每个 Job 都属于 Run。Run 工作从 admission
冻结的 exact definition、Program semantic identity 与 IR ABI 继承要求；Agent 编译/重编译 validation 绑定 compiler
semantic identity 与 compile policy digest；其他 Resource/Artifact validation 及 maintenance/control 使用 domain operation
family，绑定真实 validator/operation ABI 与其适用的冻结 validation policy、protocol/adapter identity。Skill、Model、
Capability 等验证不因使用 shared validation Job 而伪造 Agent compiler identity。不适用的要求由 nominal variant 明确
表达，不能填零值或 sentinel；非 Run 工作也不创建 fake Run。精确 variant 和字段进入附件指定的 owning type，再生成合同。

每个 family 的索引路由列必须与 typed payload、JobKind 和真实 owner 一致，由生成校验和持久化约束共同验证；不能以
方便查询为由维护另一份可独立改写的兼容性。worker manifest 列明被资格化支持的各 family requirement；claim 只选择
匹配工作，start/commit 继续验证同一绑定与 process generation。无匹配执行能力是可诊断的 non-dispatchable condition，
不能被标为普通 quota 饱和或偷偷降级。manifest 由受信任 workload identity 和签名发布闭包约束，不是任意 worker
提交的 JSON 自证。worker 自称“兼容”不能改写 Run binding、validation 编译输入或 cleanup protocol。

当前 `WorkerManifest.adapter_runtime_digest` 不能独立证明 Program 语义兼容。兼容映射属于签名发布产物和拥有 contract，
runtime 验证它，PostgreSQL 持有各领域与 Job 的要求；不新增 RuntimeRelease 业务 aggregate，不把 GitOps promotion 搬进
业务数据库。

### 7.2 旧 Run 与安全补丁

附件规定有限的并存语义窗口。当前 live Run、waiting Task、Invocation 和可恢复 Job 对某语义的引用尚未清零时，
该语义的执行能力不能被普通发布移除；若新版本会超出窗口，则发布前置检查稳定拒绝，而不是偷偷扩大窗口或重解释
旧 Run。此约束允许旧 Run 在同一 physical schema 中继续，由支持其 exact semantic identity 的 worker 领取。

retirement 按 requirement family 证明引用解除：Program 检查旧 Run 及其可恢复子工作，compiler 检查待完成 validation、
重放承诺与 exact source/policy 的编译义务，maintenance/control 检查未履行 cleanup、取消和 reconciliation。某个
Program semantic generation 退役不能顺带移除仍被非 Run Job 使用的 validator/adapter/operation ABI；非 Agent validation
按真实 Resource/Artifact owner 和对应 validation policy 判断退出条件。尚有引用时保留受限执行
能力或显式停止相关新 admission；不能把清理丢给最新通用 worker 猜测。各 family 的实际 build evidence 都需保留。

同一语义可由经过兼容性语料验证的安全修补 build 实现；Run 不需要永久钉住一个有漏洞的 binary。实际 attempt
记录执行 build 和 process generation 供审计。会改变冻结语义的安全修复必须建立新 semantic identity：立即停止受影响
的后续 dispatch，保留控制/清理能力；由现有受授权的取消或 reconciliation 命令处理旧 Run，不就地改 binding。

不提供在原 Run 上升级程序语义的 mutation。需要新语义时，必须创建单独授权的新 Run 并显式提供输入；旧 Run 保留
原审计与结果。跨 Run 的业务补偿是领域功能，不能由版本迁移工具自动重做外部效果。

### 7.3 Payload 与物理 schema

payload decoder 按显式 schema version 选择拥有类型；不能去掉版本字段后用最新 struct 猜测历史 payload。受支持读窗
由附件按 owning payload family 限定，新增版本必须附独立旧样本和负向输入语料。尚存的 live 工作、待投递 Outbox、
Receipt replay 和承诺的 public replay 窗口会阻止相应 reader 退出；不能只统计 running Job。历史归档由独立版本化
读取工具解释，不作为 runtime fallback。

可证明语义不变的 current payload 表示转换可以 backfill，并重算新表示的 payload digest、记录迁移证据。被引用的
immutable definition、Artifact 和已提交 Event 保持原字节及 digest，不能原地重编码后假装其身份未变；不支持这些历史
对象的 runtime reader 必须按上述窗口保留，直到相应引用与公开读取承诺解除。

物理 schema 选择单一 active 版本，不设计在线 dual-write 或双 schema runtime。每次物理演进使用新 forward migration，
在写入 quiescence 中更新并验证；长 Run 可以处于 durable wait，不要求所有 Run 结束。新旧语义 worker 共用同一
已验证 schema，旧 binary 只有声明并通过该 schema 的验证才能启动。启动检查依然 fail closed；它验证实际 DDL
inventory、migration 集和受支持 payload，而不是仅匹配一个手写版本数字。

已 shipped migration 不重写；本提案也不重写当前 baseline。部署回滚只允许回到支持当前 physical schema 和仍存活
语义集合的 build；不支持时走 forward fix。任何停机导致的 deadline 流逝按冻结合同处理，不宣称零停机升级。

## 8. Event、Outbox 与 PKCE 清理

### 8.1 Outbox 只负责投递

Event 是不可变的已提交记录；current aggregates 不从 Event 重放重建。Event append helper 按拥有事件的 delivery
contract 决定是否同时产生 Outbox：需要对外投递的 committed projection/wake 有同事务 Outbox，纯内部审计记录可
不建投递义务。这个选择必须先由上游 owning type/generated contract 定义，不能由消费者临时挑 Event 后补写。

Outbox 继续引用 Event，不复制其原始 payload。内部 dispatcher 按固定、closed 的投递合同生成经过脱敏的投影，通过
附件指定的 durable transport 获得服务端持久确认；之后在 PostgreSQL 以当前 Outbox claim fence 标记完成。发送完成
但标记前崩溃可重投同一 Event identity，因此交付语义为 at least once。bus 不允许直接驱动任意业务 mutation，业务
消费者必须 point-read authority 或提交原有幂等命令。

逻辑顺序来自 Run public_sequence 或 aggregate version，transport 允许重复及乱序，不宣称网络顺序等于提交顺序。
现有 public SSE 从 PostgreSQL 读取已提交序列，不能改成依赖 NATS 到达顺序。未来派生消费者必须检查版本/水位，
发现缺口从 authority 读取或重建；不把“收到了下一条”当作前序投递完成。跨 aggregate 无全局全序承诺。

Outbox worker 使用独立 permit/连接预算、有界批次、claim epoch、lease expiry 和重试政策；unknown event kind 保留并
报告拥有合同不兼容，不能静默丢弃或无限阻塞其他投递。该独立部署角色仅获得读取安全投影、claim 与确认 Outbox
所需端口和权限，不能领取业务 Job、结算业务 quota 或写入领域终态。删除投递行之前必须满足明确的确认与保留窗口。NATS 故障
不阻塞 Run current state 提交，但会形成真实 backlog；存储达到 admission 水位时按固定容量政策拒绝新业务输入，
不能无限接受后寄希望于 queue 追上。

JetStream stream 的创建/升级/删除、subject mapping、存储与消息上限、保留策略、ACL 和 publish ACK 语义由部署
contract 拥有，随 dev、Helm 与 release 配置共同验证。投递器只接受目标 stream 的成功持久 publish ACK；普通 NATS
发送成功、超时、错误 ACK、stream 不存在或磁盘耗尽都不能标为 published。stream 管理不得在仍承诺交付时静默丢弃
已确认数据；容量达到限制时使用明确拒绝策略，让 Outbox 保留义务并产生背压。改变 stream 或 subject 必须验证现存
Outbox 和消费者水位的连续性。publisher ACK 仅证明 transport 已接受投递，不证明消费者完成，更不构成 consumer
exactly-once；消费者依旧按 stable Event identity 去重，并以 authority 处理缺口。JetStream 资格不能由 mock ACK
替代，必须在实际 dev/Helm/release 配置中覆盖 restart、存储压力、ACL 拒绝及确认丢失。

### 8.2 PKCE cleanup 的 authority 与引用方向

PKCE Secret cleanup 从 Outbox 移到 shared Job，使用附件指定的既有 work class/owner pair 和新的 closed JobKind。
OAuth Task 仍由当前 OAuth/MCP authorization binding 所有，不改 `tasks.owner_kind/owner_id`；cleanup Job 的 owner
是这个 exact Task。Task 的人工状态不因清理 pending/失败而改变，授权完成 Event 不再等待物理 Secret 删除才能发布。

Task row 增加 optional current cleanup Job 引用，作为普通列和同 tenant FK，置于 `TaskProjection`/持久化 record 中；
不能塞进 immutable `McpOAuthTaskBinding`，也不能复制 cleanup state。Job 沿用 immutable polymorphic owner back-reference，
不增加反向 Job-to-Task FK。这样插入顺序为已存在 Task → 新 Job → 更新 Task pointer，只有单向可检查的 FK，
不存在要求 deferred cycle 的新闭环。Task pointer 是当前关系 authority，Job 是物理清理状态 authority。

仅 OAuth Task 的终态转换可以首次设置该 pointer；正常物理重试使用同一 Job，只有下述显式 cleanup recovery command
可以替换已失败终态的 current Job。Task generation、人工 resolution 与 exact deletion effect identity 均不随替换改变。
若历史数据存在该 Task 所有的 live Interaction Job，转换必须先在同一事务按其 owner policy terminalize 它并结算，
再建立 cleanup Job，满足已有 live-owner 唯一约束；不绕开约束、改旧 JobKind 或同时保留两个 current works。当前
OAuth 路径没有为人工等待创建这种 Job，此分支只允许显式迁移审计后支持的数据形状。

### 8.3 OAuth 原子性和物理删除

OAuth callback winner 或 expiry command 在原有事务中同时完成：Task terminal resolution、必要 authorization binding
更新、确定的 cleanup Job 和 Task pointer、Event/Outbox、Receipt completion。任何部分失败全部回滚。callback/expiry
重放不再创建 Job；同时到达时由现有 Task generation/version 与 Receipt first-winner 决定唯一结果。cleanup Job
payload 仅携带 exact Task generation、terminal cause 和冻结的非 Secret identity，不保存 PKCE value、token 或 provider
credential。

cleanup worker 使用专用 critical-control 容量领取 shared Job，并在调用 Secret port 前验证：Task terminal resolution
与 cause 一致、Task pointer 指向当前 Job、Job owner 指回该 Task、PKCE purpose 与 pinned binding generation 均匹配。
继续复用当前 `McpOAuthPkceCleanupAuthority` 的 exact-binding 校验，不能降低为只看 Outbox/Job payload。Secret port
还校验当前 Job fence 和受限 cleanup-purpose 授权；上游用户/业务凭证撤销不阻止该义务，但不能成为新业务访问许可。

Secret port 只删除 exact pinned generation；`Deleted` 或已验证该 exact generation 不存在才构成完成。删除成功后
进程崩溃由同一 Job 查询/重试 exact generation，绝不能删除同名当前 generation。Job terminal、相关审计与 quota
settlement（若适用）仍 fenced 原子提交；Secret 不可达保持可恢复义务并告警，不回滚已经生效的 OAuth 授权。
此清理义务的 deadline/重试政策来自专用受限 policy，不继承已经过期的人工 Task deadline；重试预算耗尽保留未证实
删除的诊断和引用，不能因 Job terminal 便让 retention 忽略仍存在的 Secret。

### 8.4 清理预算耗尽后的显式恢复

Job terminal unsuccessful 且 exact deletion 尚无有效证明时，Task 保留未完成的清理义务。仅拥有专用 cleanup recovery
权限的操作者或受限系统角色可提交幂等 recovery command，携带当前 Task generation/version、预期旧 Job、恢复原因与
受 policy 限制的预算授权。该权限不能重开 Task、改变 OAuth authorization 或指定任意 Secret；默认扫描器不能自动
授予新的预算。恢复次数、每次预算、许可有效期与审计要求由 owning recovery policy 有界约束，不接受任意无限预算。

命令在同一事务锁定 Task 与旧 current Job，重验其 terminal unsuccessful、仍无 exact deletion proof 和 immutable
binding，核销有限恢复授权，创建新的 physical cleanup Job，并以 CAS 更新 Task current pointer；同时提交 Receipt、
审计 Event 和要求的 Outbox。任何失败全部回滚，重放返回同一个新 Job。新旧 Job 共享稳定的 deletion effect identity，
新 Job 的 immutable payload 保留其 predecessor identity 和恢复授权 evidence。旧 Job 不改回 ready，不清除失败历史，
Task 不重开，也不自动重置旧 Job 的 attempt budget；live-owner 唯一约束仍成立。

退休或非 current Job 不能获得新的 effect 授权，也不能提交完成。Secret gateway 必须在实际发起 exact-generation
操作前校验 current pointer/fence，不能复用指针切换前取得的无限期 permit。已在切换前合法发出的请求可能迟到返回；
它不能经旧 Job 写入成功，新 current Job 通过同一 logical identity 查询 exact absence 后完成 reconciliation。因此
此协议承诺唯一当前执行许可和 fenced 提交，不把无法撤回的在途 I/O 误称为从未发生。相关新旧 Job、恢复授权和删除
证明共同保留，直到该清理义务及审计保留条件解除；仍无法证明删除时继续告警并阻止对应引用退休。

### 8.5 替换当前 cleanup Outbox

停用旧 cleanup claimant，等待或撤销旧 claim identity，再运行受审查的 bounded backfill。以 terminal Task 的 exact
binding 为 root，把现有 cleanup evidence 归入其 shared Job；已证明 exact-generation 删除的记录建立相应终态 Job，
不能仅凭普通 Outbox“已处理”推断 Secret 消失。其余建立待清理 Job，保留原 Event/Receipt identity。

迁移在每个 Task 的同一事务设置 pointer、创建 Job 并去除 Outbox 的业务清理责任；Outbox 留作普通投递。无法从
当前 Task/Secret evidence 证明 identity 的行保持停用和可审查，不能猜测或丢弃。完整验证无旧 owner 仍可 claim 后
才启用新 worker；不允许旧 cleanup Outbox 与新 Job 双执行。清理 worker、permissions、部署配置和观测指标同步切换，
旧 cleanup-specific Outbox 状态/接口按已接受替换合同删除，不留 fallback。

## 9. 读取、保留与联合恢复

### 9.1 读取模型

管理详情、权限、mutation precondition、quota 和运行控制读取 current authority；列表、统计和可视化进度优先通过
有界索引/SQL view 计算。本次目标不预建新的 projection table、搜索集群或 Event-sourced current model。

若后续核心查询确实需要物化，必须单独证明其查询收益与生命周期，再由 ADR 决定持久化位置。派生记录必须携带
source version/watermark、可丢弃重建，更新不得回写 authority；API 明确其新鲜度，不拿它判断取消能否提交或某人
是否有权限。Console/CLI 的本地缓存继续只作显示，不持有 durable execution。

### 9.2 分开的保留义务

Run terminal 不等于所有相关记录都可删除。保留政策至少分别裁决业务历史、幂等 Receipt、待投递 Outbox、审计 Event、
外部清理、Artifact/Blob/RunValue 引用与 hold；实际种类和限制由 owning policy 维护，不在 prose 复制完整枚举。

Receipt 的 promised replay window 覆盖其命令的允许重试/回调窗口；仍有 uncertain effect 或未完成 delivery 时不能
机械按 TTL 删除去重证据。完成的 Outbox 达到重放窗口后才能删除；Event 仅在所有投递、审计与引用义务解除后清理。
Run 的短期公开 SSE cursor 与更长审计保留可不同；TTL 只界定 cursor 本身的有效期，不保证底层历史仍存在。

Run 拥有单调的 public replay floor，表示已永久清除的连续 public_sequence 前缀边界。其初始值、范围与 cursor
比较规则由拥有 Run/read contract 定义；清理命令必须持有 Run 控制根的排他锁，在同事务删除完整连续前缀并推进 floor。
hold、未完成投递或审计义务挡住前缀中的某条 Event 时，不能越过它挖洞。floor 不倒退，不重新编号，也不能按剩余
Event 的最小序号猜测；最后一条事件被删后仍保留 floor，后续 append 继续使用原 public_sequence。

初始读取和每一页 continuation 都在一致数据库快照内读取 floor、序列边界和返回事件。签名正确且 TTL 未过期的 cursor
若落后 floor，必须返回明确 history-gap 结果，不能静默跳到当前最早事件；expired cursor 也返回自身的闭合错误。
初始无 cursor 且存在已截断前缀时，响应必须说明 truncation 和有效起点，再按合同返回可用历史，不能表现为从 Run
起点完整读取。分页获得快照之后的 purge 不改变已物化页，下一页重新核验 floor；全删、空页和并发 append 均遵循
同一规则。live replay 不向归档 fallback，归档读取是独立权限与版本工具。

清理采用 bounded scanner 选 candidate → owner transaction 重验 holds/current pointers → shared Job（存在外部 I/O 时）
执行 → verified completion 的协议。没有外部 I/O 的纯历史删除可在 owner transaction 中直接完成。scanner cursor 是
可重扫提示，不增加 durable retention queue 或第二 lifecycle table。禁止靠宽泛 cascade 删除跨域证据。

Task cleanup pointer 在 Job 仍待执行、结果不确定或 exact Secret 删除未获证明时阻止 Task/绑定历史清理。解除物理 FK
的顺序必须由 retention transaction 明确执行：pointer 保持到 Task 本身达到 retirement 条件，归档必要证据后先删除
Task，再删除也已符合自身政策的 Job；这些操作在同一事务完成。发生显式 cleanup recovery 时，沿已有 Job payload/
Event evidence 保留 predecessor、恢复授权和 exact deletion proof，不能只保留最后成功 Job 而删除关联失败历史。
Task 尚需保留时也保留其引用 Job，不能临时置空
pointer 让其他事务误认为从未有清理义务。Sandbox 的原 Job terminal cleanup 采用其现有独立 fence/absence
proof，不能因为业务结果终态而提前删除 Job。

ArtifactBlob/Artifact 保持现有 digest/length 与 retention authority。归档如需 object I/O，复用现有 Artifact/Blob/hold
能力，不把备份/归档报告塞成 fake-tenant Resource。保留窗口与容量水位要有持续增长 workload 的证据，不能以行数
上限或新建索引替代 retention 闭环。

### 9.3 PostgreSQL、对象与 Secret 的联合恢复

恢复单位不是一个 PostgreSQL dump。部署系统生成内容寻址、签名的 recovery manifest，绑定数据库恢复点、可恢复
Artifact 对象版本/引用清单、仍需要的 Secret/KMS key versions、运行语义与 runner/package/release digest、恢复工具和
验证报告。manifest 是运维产物，不是新的业务 release aggregate；PG 仍持有恢复后 current state。

备份窗口内被恢复点引用的 object/key generation 必须受独立 backup hold 保护。只有数据库备份成功而对象或密钥不可
恢复时，该恢复集不可发布为可用。并非所有 Secret 都应恢复：PKCE/token 的过期、吊销与已删除状态须由原拥有域重新
验证，不能以数据库回滚使旧凭证重新获得权限。

恢复先隔离旧环境并撤销旧 worker/provider/control identity，终止旧数据库 sessions 并轮换受影响的数据库凭证，确保旧
writer 不可继续使用旧 fence；在关闭新 admission、claim 与外部 effect 的隔离环境安装匹配 schema，恢复数据和所需
对象，验证 tenant FK、digest、Job owner/current pointer、
quota settlement 与未完成 cleanup。新 worker 使用新 process generation，仍由正常 recovery command 重新领取，
不得直接清空 lease 或把所有工作批量改成 ready。

PITR 不能撤销第三方已发生的效果。恢复点之后的 callback、write、Outbox delivery 和 Secret 删除可能已经生效，因此
所有可能跨过恢复点的外部 effect 必须先按原 logical identity 查证并 reconciliation；无法证明未执行时禁止自动重发。
恢复点缺失某 Invocation/Job 时也不能从旧 Run 重建一个新随机 effect identity 重做。缺失 owner 的 physical object
按既有 orphan policy 保留、隔离并报告，不能通过“DB 没有行”推导可激活或可删除。

可重放的已提交投影保持 Event identity；消费者可能早已观察到恢复点之后的数据，必须完成订阅水位/版本与 authority
的对账，才能继续对外提供声称 current 的派生视图。业务恢复依靠 PG current state 与外部效果证明，不能用 NATS
消息历史反向覆盖 database。只有联合验证完成、不可判定工作已保留到 reconciliation、身份隔离生效后才恢复 admission；
报告逐项记录未恢复范围，不能把“PostgreSQL 启动成功”称为平台恢复成功。

## 10. 上游合同、migration 与行为验收映射

下表指定需要改变的 authority 和证据，不预写完整字段、状态闭集或 migration DDL。确切 schema、limits、wire values
由拥有 Rust 类型生成，物理变化追加新 migration；本次提案不修改它们。

| 设计面 | 当前拥有来源 / 目标变化 | 物理演进与独立行为验收 |
| --- | --- | --- |
| Authority / Unit of Work | [Job](../../../crates/platform-jobs/src/lib.rs)、[Task](../../../crates/platform-tasks/src/lib.rs)、[Run](../../../crates/platform-orchestrator/src/lib.rs)、Invocation owning crates；拆出领域 command/repository 能力与统一 transaction API | 对应事务的 rollback fault、同 key replay/digest conflict、跨 tenant FK/permission、serialization retry；任何失败不得只提交 quota/Event/Receipt 的一部分 |
| 按目的授权 | owning authorization-purpose types 与领域 effect/read/control ports；区分新动作、正文披露和受限系统履约 | effect 准入后撤权仍能由合法角色 fenced 完成/settle/cancel/cleanup，但新 dispatch 与 result/Artifact/事件正文读取拒绝；撤销旧 worker 后其提交拒绝，新恢复角色不获得通用业务权限 |
| 局部并发 / control | [repository](../../../crates/platform-postgres/src/repository.rs) 的 Run/Node/control owner；typed control generation 与锁模式 | 明确哪些 guard 必须保留或成为索引列；cancel 与新 child/Task response/terminal result 并发、公共 sequence 顺序、scope 大 fan-out 不死锁 |
| partition / WDRR | [scheduler](../../../crates/platform-scheduler/src/lib.rs)、[machine registries](../../../crates/platform-contracts/src/machine.rs)、worker/capacity types；新增版本化 partition 和持久公平行类型 | 新 forward migration 添加 tenant/Job 路由、复合 FK、scheduler_tenant_state 及索引；原 JSON deficit 迁出，tenant round 上界/参与资格与 Job continuation 明确新增；超过旧 window/state cap 仍保留 credit，跨轮 restart 不重赠/丢失；不断创建 tenant 仍完成冻结 round、旧 tenant 再访，HPA 不重映射，不同 partition 可并行，同 partition first-winner；claim 与 tenant/Job 创建并发无反向锁，公平行缺失不 late UPSERT |
| quota subset / scan | 现有 quota account/ledger 与 scheduler decision；新增正常 skip/backpressure、immutable creation key/cutoff 的 bounded sweep 和独立 aging probe 合同 | 不新增 queue/account mirror；可接受子集、跨租户及同租户队首饱和后仍访问可准入 Job；backlog 超过多页预算时能到后部，持续创建及任意新 Job ID 不扩展冻结范围，due/priority 反复变更不使同行重复进入主 sweep，cutoff 前创建但迟提交的行在下轮/重查访问，head probe 不重置 cursor，候选扫描不倒置 owner/Job 锁序；多个 account 共同约束，零可接受批次仍推进，仅 admitted 计费，reserve/settle 守恒 |
| 执行版本 | [RunBindingsSnapshot](../../../crates/platform-contracts/src/resource.rs)、[RuntimePlan](../../../crates/platform-orchestrator/src/lib.rs)、[WorkerManifest](../../../crates/platform-contracts/src/worker.rs)、Job requirement family 与签名发布闭包 | Run/Agent 编译 validation/domain operation 分族索引与 payload、JobKind、真实 owner 一致；非 Agent Resource/Artifact validation 使用真实 validator ABI/policy，无假 compiler/Run/sentinel；不兼容 worker 永不领取/提交，旧 wait 跨兼容 patch 恢复，pending validation/cleanup 阻止对应能力退役，窗口超限拒绝发布，未知 payload/IR fail closed |
| schema upgrade | [schema verifier](../../../crates/platform-postgres/src/lib.rs)、[baseline](../../../crates/platform-postgres/migrations/0001_platform_baseline.sql)、schema-contract generator | 新 forward migration，migration不可变、quiesced upgrade 原子验证、失败停写、rollback build 不支持当前schema时拒绝；保存旧Run语义与绝对deadline |
| Outbox delivery | Event delivery owning types、[public events](../../../contracts/platform-v1/events/public-run-events.json)、独立 Outbox repository 与受限 worker 角色、deployment JetStream contract | commit 后发送前崩溃、持久 ACK 后 DB 标记前崩溃、旧 claim ACK 拒绝、重复乱序、unknown kind；实际 dev/Helm/release stream 持久配置、disk full/错误 ACK/ACL 拒绝不能 published，consumer 重投不宣称 exactly-once，broker 故障时 Run 扫描仍推进 |
| PKCE cleanup | [OAuth cleanup authority](../../../crates/platform-mcp-host/src/oauth_cleanup.rs)、[callback/expiry repository](../../../crates/platform-postgres/src/mcp_repository.rs)、[McpOAuthTaskBinding](../../../crates/platform-contracts/src/mcp.rs)；新增 JobKind/payload 与 Task current pointer | tasks单向同tenant FK，无反向FK；callback/expiry竞态只创建一个Job、rollback全撤销、Secret精确generation删除/absence、轮换后不删新值、旧outbox worker彻底不能claim |
| PKCE exhausted recovery | MCP cleanup recovery command/policy、Task pointer 与 immutable Job predecessor evidence；新的 Receipt/error/security 合同 | 预算耗尽后有限授权可恢复；两个 recovery 竞争仅一个 CAS 成功，同 Receipt 重放不增 Job，越权/无限预算拒绝；失败任一步全回滚，Task 不重开、旧 Job 保留终态、effect identity 不变，退休 Job 无新 effect/commit，在途删除由新 Job 查证，历史与引用不能提前 purge |
| retention / replay floor | 现有 Retention Policy、Artifact/hold、Run owning record 与 public cursor/read 合同；新 floor 列及原子 purge 命令 | 只删除连续前缀，hold 阻塞不挖洞；有效 TTL cursor 低于 floor 返回缺口，初始无 cursor 显示 truncation，全删保留 floor，并发 purge/append/分页一致且序号不重置；cleanup 未证明完成不能 purge，外部 delete 后提交失败、重复 scan 安全，长期负载下空间可解释 |
| joint restore | production qualification / signed recovery manifest / provider reconciliation contracts | PG 恢复点与对象/keys/release 联合校验；旧 writer/identity/DB session 被隔离且凭证轮换，新 process generation 正常恢复；cutoff 前后外部 write/Outbox ACK/Secret delete、未知 effect 不重发、孤儿不默认删除、消费者已领先恢复点时能对账 |

测试入口以现有 [PG Run kernel](../../../crates/platform-postgres/tests/phase2_run_kernel.rs)、
[PG coordinator](../../../crates/platform-runtime/tests/orchestration_coordinator_pg.rs)、
[OAuth PG](../../../crates/platform-postgres/tests/phase4_mcp_oauth.rs)、
[timer/signal restart journey](../../../tests/productization/timer_signal_restart_recovery.rs) 为基线拆成独立行为场景，
而不是把新的大闭集 snapshot 加到同一个巨型测试中。schema 合同检查不能替代并发测试；本地 Kind 通过不能替代
多故障域、容量、持续增长、restore 或生产资格。

## 11. 交叉评审必须关闭的问题

评审逐一验证：父子 current pointer 的唯一性与合法 owner；锁序及公共序列的根锁；持久 tenant credit 和有界扫描；
同 tenant 队首背压与 quota admissible subset；按目的授权及各 Job requirement family 的退役；旧语义与 schema upgrade
的失败关闭；PKCE exact identity、单向 FK 和有限恢复授权；Outbox 的 ACK/部署责任和 Run replay floor；恢复后的外部
效果与身份隔离。每项必须能指向上游合同、物理变化和行为验收，不能只签署本文。

本提案选择有限语义并存、停写 schema migration、近似跨 partition 公平和无现成物化读表。若实施要求零停机 schema
升级、全局严格公平、就地迁移 Run 语义或第三方事件订阅，则已经改变本设计边界，必须重新接受对应 ADR；不能把它们
作为实现细节加入。所有生产容量、HA、soak 和联合恢复声明仍待专门资格证据，本提案不改变当前 `Not run` 状态。
