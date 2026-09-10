# 架构与 authority 边界

控制、持久编排、不可信执行与数据访问有各自的边界。Gateway 负责认证与用例接入；Orchestration Worker 通过
Run、Invocation、Job 和 Task 推进工作；Sandbox 执行不可信代码；Artifact 服务控制对象内容的访问和验证。
PostgreSQL 保存当前业务事实、租约 fence、并发版本与事务收据。NATS 传递唤醒提示或已提交事件的安全元数据；
消息、远端 provider 状态与进程内缓存都不能替代当前数据库状态。

Agent、Skill、Capability、Context、MCP、Model、Policy 和 Sandbox 复用
Resource -> immutable ResourceVersion -> Deployment -> Binding 生命周期。Run admission 冻结 exact binding；active head
变化不会改变既有 Run。Capability 是唯一通用可调用合同，Native、Remote HTTP/gRPC、MCP Tool 与 Sandbox 只是实现后端。

Remote Context 的安装目录只约束物理目的地址、信任根、区域、容量与凭据用途映射，不重复保存 Registry 的业务身份。
每次派发前，Security 用同一个只读数据库快照核对当前 Query、Run、Job 的稳定租约身份、principal 与 exact 依赖；
Egress 单独绑定实际 Inline 输入摘要，并在解析凭据、DNS 和打开 HTTP 前检查许可。权限变化或租约失效拒绝旧请求，
心跳更新 Job 版本不会单独使同一有效租约失效。此短期许可不保证撤回已打开的 HTTP 请求，也不是新的派发表或事务收据。
外部 Remote Search 协议的正文编解码不携带这些业务授权事实；协议资格不能替代运行时授权或人工响应证据。
HTTP 正文、完整 RPC frame 与最终 Inline 观察分别受限，前一层能接收的字节不保证后一层能容纳。
超过后续容量时保留真实请求/响应证据并走既有 fenced failure settlement，不截断或伪造 Artifact。
RPC 提交后的连接错误属于派发结果不确定，不表示提供方未被调用，也不会触发自动重发；
安全诊断只保留闭合阶段和摘要，当前 Query/Job、额度与 deadline owner 决定最终收口。

不可信代码只有一条物理路径：Sandbox Dispatcher -> internal OpenSandbox Server -> Kubernetes API -> BatchSandbox Controller ->
containerd/runc。普通 Sandbox Capability 原子创建 shared durable Job；Dispatcher领取并续租同一Job，持久化physical evidence，
通过immutable fixed Armed runner最多启动一次Package，并在提交terminal结果前重新验证current Job lease fence。OpenSandbox、
Controller和runner不修改Job、Run、Invocation或其他Platform业务状态。Python、Node和Shell不在Gateway或普通Worker内spawn。

领域类型和决策位于 `crates/domains`、`crates/definitions` 与 `crates/execution`。应用用例位于 `crates/application`，
只依赖领域类型和窄端口；`crates/adapters/platform-postgres` 实现端口并保留跨领域事务，应用层不反向依赖 SQLx、
PostgreSQL 或 RPC 编码。服务入口、物理 provider 和交付工具分别在 `apps`、`crates/adapters` 与 `tools/rust` 组合。
目录和依赖规则见[工程入口](../engineering/README.md)。

Agent 作者包保留原始源文件，CLI、WASM 和 Registry 使用同一个有界编译核心。Registry 验证任务读取确切源 Artifact，
独立重编译并比对 Plan。Plan 携带完整值 Schema；内部标量和数组使用值 Schema，公共对象输入保持独立的封闭对象约束。
编译期形状检查在原生与 WASM 一致，物化值在执行及提交边界按冻结文档校验。执行要求绑定 Program 或领域操作的语义身份，
进程可执行文件 digest 单独记录，重新构建可执行文件不会自动变成另一种业务语义。

ModelLoop 的响应使用该节点输出端口绑定的封闭对象 Schema；Agent 的最终输出可以在后续计算或人工任务后另行组成。
AgentContract 保留外部接口身份，模型响应 Schema 的摘要与所需文字指令归属 PlanNodeInstruction，并计入同一提示容量。
首次准入和工具续轮在原有事务中再次核对 exact Plan；续轮不能更换响应合同。其他内部端口仍可使用标量或数组。

普通 Job 领取共享有限分区和持久公平状态。物理扫描只略过完全没有租户公平记录的分区；已有租户是否绑定 Policy、是否有到期工作或是否能通过配额检查，不影响其分区继续获得扫描。新租户与公平记录原子创建后即进入后续扫描。
每轮选择一个分区内的有限租户集合，再在本地剩余容量、当前 Policy、配额
和实际执行能力约束下领取。尚未绑定 Scheduling Policy 的租户保持明确的未绑定状态。领取事务按分区、租户公平记录、配额、稳定排序的
领域根和 Job 加锁；业务命令与完成事务遵循对应 owner 的锁序。当前状态、配额结算、Event、Outbox 与 Receipt 在同一
PostgreSQL 事务提交。一次物理尝试由版本、进程 generation、lease generation 和 token 共同防止过期提交。
Job 引用租户与稳定分区，WorkClass 由数据库闭集约束；持续变化的公平记账记录不成为 Job 的外键目标。
租户与完整公平记录在同一事务中创建，领取路径拒绝缺失或无效的资格，不在扫描时补造记账。控制器只对数据库明确中止的事务
有界重试同一命令与 fence；连接中断等不确定提交不会在此边界自动重做。
编排领取先在短事务中获取有限分区提示，再在串行化事务中锁定一个确切分区并重新检查当前资格，避免分区扫描之间的
读写依赖使繁忙分区持续重试。提示不保存可执行状态，也不授予领取、配额或执行权限；过期或被占用的提示只产生空轮次。
访问位置与轮次由分区状态记录。租户公平状态完全未变化时保留该行的版本与更新时间，实际变更仍在原事务和锁序下写入，避免空访问反复干扰有工作的分区。

领取与恢复扫描在持久对象的拥有边界验证事实。可定位到单个对象的损坏只回滚该对象，保留原行、租约、额度与效果义务，
返回安全诊断并继续后部候选。分页位置可在进程重启后重新发现；诊断不构成成功、修复指令或新的业务状态。
数据库故障和权限拒绝仍使事务失败。共享配额或公平状态损坏在所属事务边界拒绝；编排可回滚受损分区后尝试其他提示，
不能把共享事实错误归为单个业务对象的诊断。日志和指标只记录有界分类，不输出诊断中的对象身份或正文。

取消、超时与失败通过有界扫描逐个收敛 Run，每个步骤使用独立事务。已准入的领域工作先记录结果或完成取消，
释放自己的执行许可；节点、内部 Scope、根 Scope 和 Run 随后按依赖顺序关闭。一个仍等待外部核对的 Run 不阻塞
其他 Run 推进。不确定的外部写入保留核对义务，不能因取消而被视为未执行或直接重发。

Outbox publisher 只读取安全事件投影，在 JetStream 确认后以自己的投递 fence 结算。投递失败保留义务；不兼容记录也
计入积压。新外部业务的准入事务读取有界积压水位，达到配置阈值时暂时拒绝新工作。这是压力保护，不是严格的全局容量
计数器；收据重放、已准入工作的完成、取消和清理仍可推进。
公开命令在读取或重放收据前检查命令时点的当前权限和适用的响应资格；收据本身不授予当前投影或正文的读取权限。

PKCE 删除由 Task 持有清理义务并通过共享 Job 执行，稳定删除身份独立于尝试与进程构建。耗尽后的受控恢复生成新的
Job，并保留前驱关系；旧 fence 不能覆盖新尝试。Sandbox 的终态清理则仍属于原 Job 的清理义务，不重新打开终态 Job。
Sandbox 控制和清理按稳定创建位置分页；损坏对象保留，已完成项或其他进程仍持有的清理租约不会遮挡后部候选。
当前进程和构建对有效清理租约的重放保留原 fence；更换构建不能夺取有效租约。控制收敛仍在同一事务结算 Invocation、Job 与配额。

公开 Run 事件使用单调序号和保留水位。历史维护只删除满足保留条件的连续前缀，未完成义务或当前冻结阻止跨越删除。
独立维护进程通过受限数据库函数读取安全元数据、锁定候选前缀并执行受约束的删除；它没有业务正文读取或任意表修改权限。
保留策略由应用判断，数据库保证锁、连续前缀和版本比较。管理 API 提供专用权限下的冻结、释放与清理恢复命令。

跨进程闭集见 [`contracts/platform-v1`](../../contracts/platform-v1/README.md)，关键物理决策见
[ADR-0009](../adr/0009-durable-kernel-and-agent-domain-boundaries.md)与
[ADR-0007](../adr/0007-opensandbox-execution-provider.md)。数据库以唯一当前 schema 在空库安装，并核验实际结构；不提供旧版本
reader 或迁移链。应用发布与回滚由部署系统管理。各项行为与恢复验证记录在所属测试和交付证据中；本地测试不替代生产拓扑、
容量、长时间运行与灾难恢复资格。
