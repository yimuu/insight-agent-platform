# ADR-0009：持久执行底座、Agent 领域与单代协议边界

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-09-07 |
| 实现入口 | [当前架构](../current/architecture.md)、[工程指南](../engineering/README.md) |
| 当前行为 authority | owning Rust type、当前 schema 定义和生成的边界合同 |

## 背景与裁决

平台继续采用 PostgreSQL、shared Job、显式 Plan 和独立 Sandbox 信任边界。跨领域事务、物理适配器和工具代码曾集中在同一层，
普通作者入口也未完整表达已有执行能力。本次决定按事实归属重组模块，并补齐调度、授权、清理、读取、编译和部署闭环。

实施过程中用户明确授权删除所有旧协议兼容、已发运与未发运的迁移链以及旧数据。此前 spec 的有限旧语义并存、历史 reader、
保留 baseline 和向前数据转换方案被本裁决替代。只保留当前版本的 Plan、Task、admission、WorkerManifest 和 compiler。
唯一当前 schema snapshot 用于空目标库建库；不保留历史升级或转换命令。删除数据只针对明确授权的工作区与部署目标。

## 职责和事务

定义与编译保持纯函数；Run/Plan、Model、Capability、Context、Task 和 Artifact 各自拥有业务事实与决定。Job、lease/fence、
调度和额度提供共享执行机制。应用层通过领域端口组合用例，PostgreSQL adapter 在调用者拥有的同一事务内完成跨 owner 原子操作。
外部 I/O 在事务外执行；结果提交重验当前 owner、版本、fence 和操作目的授权。Provider、消息、缓存或投影不裁决业务当前状态。

Plan 定义与解释器分离；共享 compiler 被 CLI、WASM 和 Registry 调用。MCP 的物理传输、应用驱动和领域决策分别归属；
执行 trace context 不依赖 RPC，HTTP metrics 不进入纯指标模型。Sandbox 领域保留合同和决定，OpenSandbox executor 实现物理生命周期。
发行、合同生成和 schema 工具拥有独立构建入口，不成为 runtime 的反向依赖。

内部数据端口使用冻结的 ClosedValueSchema 表达标量、对象和集合；公开对象边界继续使用 ClosedJsonSchema。
纯 compiler 与 WASM 共享结构预检，解释与事务提交依据冻结 schema 完整验证实际输入和结果。实例验证通过显式 feature
进入运行路径，不能把网络 schema 解析、数据库或物理适配器带入定义层。

## 执行身份与调度

Run/Job 固定 Program requirement；非 Run Job 固定 compiler 或领域 operation requirement。leaf admission 另冻结物理适配器契约，
worker 必须同时支持相关要求。当前 binary SHA256 和 manifest 是 attempt 证据，不能成为冻结业务的旧 build 依赖。
同一当前语义的修补 binary 可以执行已有工作；未知 Program、payload 或适配器能力明确拒绝。

调度使用固定虚拟分区和 PostgreSQL 租户公平状态。分区只负责轮转协调，Job 是唯一候选来源；租户 deficit 与有界扫描 continuation
持久化到独立租户行。扫描冻结 cohort 并跨事务推进，配额在领取事务中裁决可接受子集，不因队首饱和永久遮挡后续工作。
公平性限于分区，跨分区只保证近似公平。调度策略改变与 claim 共用租户公平行锁序，不额外授予 Artifact worker 业务根写权限。

Job 的物理引用只绑定租户和稳定分区，WorkClass 由数据库精确闭集约束。公平记录与租户原子创建，没有独立退休或删除
生命周期；其 deficit 与扫描游标持续变化，不应成为 Job 的外键 authority。领取与 Policy 修改仍使用公平行的既定锁序，
缺失资格不允许执行，也不在扫描中补造状态。这样持久任务的提交不会依赖调度记账的更新频率。
仅数据库明确报告的序列化中止或死锁允许有界重试原命令，所有预分配身份保持不变，不确定的提交结果不自动重做。

逻辑 effect identity 跨 attempt 保持，重领不自动授权重做外部副作用。取消先关闭新业务，合法完成、核对和清理继续按 exact fence
收敛。撤销执行或正文读取权限不阻止合法系统身份结算既有事实，也不授予该身份新的业务能力或正文披露权限。

外部叶子的成功结果与失败结果都回到原 Node 的普通控制器通路。Child、Context、Model 和 Capability 的拥有域
原子提交结果与 RunValue、结算当前额度，并把原等待 Node 唤醒到一个新的 orchestration Job；已经终止的 Job 不重开。
当前 Job payload 只携带原调度 Job、领域 owner 和 exact 输出引用，互斥区分成功结算、失败收敛和 Model tool continuation。
所有写入与读取一次替换为同一当前 payload 版本，不保留旧 reader。
仅含已提交结果或已提交失败的续行 Job 使用既有 RestrictedControl 配额通路，每租户每批仍受既有控制额度限制，
因此解除 Scheduling 业务绑定不会阻止已准入效果的结算。候选集、并发 quota、tenant active、Run 取消与期限守卫不变；
普通激活和 Model tool continuation 仍需要业务绑定，后续领域调用仍重验当前权限。SQL 只预筛当前 payload 标记，
最终由拥有类型解码并裁决通路，不能凭任意 JSON 声明获得执行资格。


PostgreSQL 在读取控制器事实和提交时独立核对领域终态、原 Job/Node/Run 关联及精确输出，不能仅信任调用者提供的成功声明。
纯控制器从冻结 RuntimeNode 推导后继，统一执行 Parallel、Map、Loop 和 quorum 的退出规则；物理结果提交者不复制结构控制逻辑，
也不直接创建嵌套作用域中的 Join。续行重验取消、截止时间、当前 fence 与操作目的权限：已经提交的效果保留为事实，
但不重复执行 provider，也不借结算权限启动新的业务。恢复验证覆盖物理结果提交后的进程重启、结构汇合和撤权后的受限结算。

## 作者、读取和清理

Registry 从实际 source bundle 独立编译并核对唯一 IR，Agent 不能绕过该流程手工记录成功。Context authoring 输入只包含逻辑 exact
引用与策略，发布时才分配最终 deployment/binding 身份。Task 使用实际冻结 schema 和资格规则，验证后的输入证据不可由客户端伪造。
发现与解析是只读产品查询；当前 Receipt 重试保持幂等。元数据与正文权限分别判断，Inline 和 Artifact 正文适用同一当前授权约束。

节点编辑描述由 owning RuntimeNode 派生，只提供草稿提示。表达式重建调用与完整编译相同的纯 owning builder 和限制，
编辑结果仍须经完整编译和 Registry 验证。SourceMap 是绑定 exact source files、Plan 和 compiler 的不可变派生产物，
源码位置来自实际解析器；它不改变源包或 IR，也不拥有发布或执行状态。

源码预检复用编译核心的解析、Schema 和 Plan 校验，只接受一次捕获的有界源文件，不接收 binding、profile 或外部事实。
CLI 和 Console 在解析依赖、上传 Artifact 或修改发布 journal 前返回可由源文件确定的错误；随后使用同一份源文件完成
依赖解析与完整编译。预检结果只有安全诊断和依赖需求，不能证明授权、候选可用性、执行能力或发布有效性。
它是原生与 WASM 共用的编辑辅助边界，不改变合法输入的 lowering、IR 身份或 Registry 独立验证要求。

源包摘要标识内容，发布命令身份标识一次意图。CLI 在外部请求前持久化新的命令身份与原并发条件，Console 在当前浏览器会话保存
相同语义的恢复记录；未知结果只允许重试原请求，不重新分配身份或读取新 ETag 覆盖原条件。已经完成的记录作为历史证据，
不能跳过下一次显式发布的当前状态核验。重复发布的判断核对当前已激活版本及完整执行闭包，保留新命令与响应丢失恢复的区别。
发布命令记录不包含源文件正文或凭证，也不能替代当前授权、服务端 Receipt 和 PostgreSQL 的并发裁决。

联合评审接受：不可变发布版本由 nominal Version ID 和资源内的种类、修订号确定。接口与 Plan 的内容摘要继续描述实际语义内容，
完整发布 payload 另行冻结作者源、文档及服务端校验证据。相同语义内容可以来自不同发布意图，不能据内容摘要复用旧版本并丢失新来源。
当前建库定义移除 ResourceVersion 内容摘要的唯一约束，保留版本主键、修订号唯一性、引用约束与不可变性；Artifact Blob 的内容去重不变。
发布仍在原 Resource 锁、当前授权、Receipt 与 CAS 下原子提交。重复版本身份或修订号返回既有冲突，失败回滚整个发布及收据，
不留下部分接口版本；其他数据库错误不被此映射掩盖。读取和运行继续使用 exact Version ID，内容摘要只用于核验。

Artifact 上传同样区分内容与命令身份。CLI 复用项目内唯一上传记录目录，并在请求前持久化独立上传意图；
相同内容不能让不同本地意图误用另一条 Receipt。完成记录的使用仍需当前授权和实际 Ready 内容核验。

上传 Receipt 保留原始 Artifact、暂存 Blob、Grant 和 Job 的命令证据，去重只改变 Artifact 的当前 Blob 绑定。
完成请求的重放按当前写权限和原请求条件识别既有提交，返回原 Artifact/Job 身份及当前安全元数据；不重新检查对象、
创建扫描、结算额度或写入对象。请求身份包含原目标、并发条件和 proof 摘要，不能用新 ETag 改写同一重试。
重放读取不制造新的业务期限，仍保留的成功 Receipt 不因原上传期限已过而被当成新命令。

取得或刷新上传 URL 属于新的物理能力授予。prepare 重放只有在原 Artifact、暂存 Blob、Grant、Job 及原期限仍允许上传时才能签发；
非上传阶段明确冲突，不为去重后的 Ready Blob 签发 PUT。查询重放不插入 Receipt，新的写事务仍重验当前授权、Receipt、CAS 与 fence。

框架导入只接受明确的静态 typed-port 导出配置，保留原始 source bundle 并经同一编译器降为普通 Plan；不导入宿主代码执行权。
评测组合普通父子 Run 和现有 Artifact、RunValue 证据，不新增执行引擎、队列或评测事实表。客户端由服务端父子关系核对结果，
失败 Run 即使没有输出值仍是失败，不能伪装成缺失或成功。作者包恢复先完整验证公开版本、Artifact 和编译闭包，再原子写入新目录。

Outbox 只投递已提交事件，使用独立 PostgreSQL 权限、JetStream ACK 和 fenced settlement；消息从不携带当前执行状态。
新业务准入在当前授权和 Receipt 重放之后观察 owning hard-limit profile 指定的积压阈值。此门槛抑制新工作压力，
不是严格计数 CAS；完成、取消、清理和幂等重试继续推进，避免积压阻止自身收敛。
PKCE 清理使用 Task 当前 Job 指针和 exact secret generation。预算耗尽后的显式恢复创建新 Job，保留新架构内的 predecessor 与 effect
证据，不重开 Task、不重置已结束 Job。这里的恢复历史是当前业务正确性的一部分，与删除旧版本兼容是不同的生命周期。

Run 根持有有界 history holds。签名维护策略、数据库时间、当前 hold 和未完成义务共同决定可删除前缀，并原子推进 replay floor。
公开续读跨越已删除历史时明确报缺口，客户端不能自动 reset 来伪装连续性。当前版本的联合恢复仍须隔离失效 writer、身份和在途副作用。
管理恢复 API 和 CLI 只组合现有 hold 与 cleanup 命令，返回有界安全投影。CLI 的私有意图记录用于重试定位，不能替代当前授权、
服务端 Receipt 或实际恢复结果；业务数据库不接受任意 purge SQL 或部署回滚命令。

## 建库、发布与证据

本地开发依赖的 Compose 命名空间使用已验证 Tenant ID 的完整 UUID。UUIDv7 的时间前缀不能用作项目身份；
相近时间创建的两个项目必须具有不同的容器和 volume 命名空间。CLI 继续拥有启动、进程身份核验与 reset，
验证工具只持有自己创建目录的文件系统身份并调用 CLI 清理，不新增数据库事实或资源登记表。
此修正直接替换截断名称，不发现、接管或清理旧名称。验收必须覆盖相同 UUIDv7 时间前缀的不同项目，
以及删除其中一套依赖后另一套仍存在的真实 Docker 证据。

Provisioner 在空目标库一次事务建立当前 schema；运行进程只读验证完整实际结构。schema snapshot、inventory 和实际建库 executable
纳入签名发行闭包。应用 promotion/rollback 由部署系统拥有，业务数据库没有第二发行 authority。

本决策替代 ADR-0001 中与旧迁移链或固定物理表数量冲突的条款；保留其单一 authority 原则。ADR-0004 的公开产品边界和
ADR-0007/0008 的 Sandbox 物理路径、两阶段激活与权限隔离继续有效。

验收依赖行为、并发、恢复、最低权限和真实依赖证据，不以目录数量或生成器输出自比评价完成。集成 target 缺少所需环境必须失败，
纯单元与真实依赖测试分别运行。生产 topology、capacity/soak、restore 和 GitOps 资格必须有相应环境证据，不能由本地 fixture 替代。
实施完成后删除临时 spec，耐久决定保留在本 ADR，可观察行为写入 current，贡献者流程写入 engineering。

## 联合恢复产物边界

恢复清单与验证报告是独立、严格有界的 deployment 合同，复用 exact Artifact/Secret 身份，不存储正文或凭证。
清单绑定 PostgreSQL 恢复点、对象和密钥版本、独立保留凭据及执行/发行/工具身份；报告引用清单摘要，签名恢复集再绑定
清单、报告和所有外部证据文件，避免摘要循环。受信签名公钥由部署策略单独提供，不能从待验产物自证。

可用恢复集要求资产、密钥、保留和隔离证据完整。未知外部效果可以继续保留为有明确隔离证据的 reconciliation 工作，
不能自动重发；未隔离的不确定效果阻止放行。离线工具只核验签名、内容身份和外部证据声明的一致性，不执行恢复或证明
声明所述外部事实。实际恢复与隔离仍由部署系统和拥有域完成，生产 PITR 资格需另有真实环境证据。

### Kind 与 Helm 的 executable identity 输入

联合评审接受：Helm 不生成 WorkerManifest 的版本、能力或执行文件身份。Sandbox Dispatcher 与其他 worker 一样，由部署输入提供完整 owning manifest；缺少输入必须拒绝渲染。Kind 本地组合先验证 exact OCI manifest，从 immutable image config 创建停止的容器读取 binary bytes，不运行镜像代码，再使用 owning catalog 校验本地 manifest。该本地组合不会修改已签名的生产 candidate，也不继承其部署配置资格。

Kind 的 Outbox 与 History 使用独立 PostgreSQL 凭证，并调用同一 closed-purpose provisioning 工具的固定 loopback Kind profile。JetStream 使用独立 provisioning 身份、持久卷和 owning certificate ACL；publisher 不取得流管理权限。Management 与 Runtime 复用既有 Gateway Artifact 身份，Registry 使用独立已受限的 Artifact 读取身份。

补充联合评审：CLI seed 的 server certificate 仅覆盖 localhost。Kind 为 Artifact、Egress、Security Authority 与 MCP 服务使用同一 seed CA 签发 exact service DNS 的 server certificate，并在 Secret 中引用；原 client URI identity 与 closed peer authorization 保持。禁止通过关闭 hostname、CA 或 mTLS 校验来跨越这两种部署地址。

补充联合评审：Kind 为已有 owning grants 的 Security Authority 和 Artifact Gateway、Data Reader、Data Worker、Maintenance 使用各自有限数据库角色。Artifact 四角色与原 owning SQL 构成同事务 cohort，四个私有凭证文件必须齐备；初始化工具拒绝不匹配的角色标记或额外 role membership。尚无独立 grants 合同的其他本地角色仍属于共享 owner 的测试范围，Kind 不据此声称整个生产部署已经具备最小权限资格。

补充联合评审：development bootstrap 在同一事务内完成租户分区登记、seed Scheduling Policy 的真实 exact 校验与 fairness 绑定，复用公开绑定命令的拥有域助手，不增加临时管理权限或 worker 领取旁路。重新启动核验当前 TenantConfig 指向的真实绑定，因此合法的后续显式改绑可继续使用；重放不重置 credit、cohort 或版本。缺失或漂移的绑定被拒绝，不能借启动流程修补。公开命令的当前权限仍在 Receipt 重放之前校验。

### 有界结构取消与终态收敛

联合评审补充：ModelTurn 的完整 canonical response 与 Plan 返回的 structured output 是同事务提交的不同 RunValue。
续行以当前 ModelTurn 结果指向的响应为证据，核验真实内容摘要、分类与 Node，再验证其中结构化值与冻结 Plan Schema、
实际输出行和 Scope exact 引用一致。不得把两个值 ID 当成同一身份，也不得只凭 continuation 自报的摘要证明结果。

取消、超时和耗尽重试的收敛按 Run 扫描，使用 owning cursor 和已有限额继续遍历；旧候选不会截断后续 Run。每个候选 Run 在独立短事务中重新观察当前控制 generation，只提交一个 owning step。暂时不可关闭是 NotReady，不能回滚同批其他 Run 已提交的步骤。Job 的实际 permit 和 quota 仅在该 Job 第一次终态结算时释放；未派发的 Node、已解决的 Task、全部直接成员结束的 Scope 分步关闭，Run 最后关闭。

结构取消不假设单个活动 Node。活跃子 Run、domain owner 和不确定外部效果仍由各自协议完成或对账；终态结果可以在父 Run 保持 Cancelling 时记录，但不能触发新业务派发。Node 所属人工作业使用既有 Task first-winner，OAuth 和审批任务及 PKCE 清理仍归各自领域，不能由通用扫描伪造完成。Scope 沿已持久化 parent-node/scope 关系自内向外关闭；pending Join 无需制造执行 Job 才能取消。最终 Run 关闭必须确认所有执行成员与实际外部义务已终态、活动 permit 已归零。明确 PostgreSQL abort 可有限重试，重试不消耗业务 Job attempt；未知提交结果不重做。

父 Run 的失败传播不复制 declared code、接口身份或 details Artifact 到子 Run。子 Run 使用 owning 的安全平台依赖失败，原始证据仍归父 Run；子自身已经观察到的取消、超时或失败保持 first-winner。超时传播重新验证 child deadline 不超过 parent deadline，并只在数据库时间已经越过 child 的真实不可变期限后观察 timeout。

联合评审补充：ChildAgent 的相对预算精确来自冻结 Plan，在 PostgreSQL 完成当前运行权限、父状态、租约与依赖闭包验证后，以同一事务观察的准入时刻转换为绝对期限，并受父 Run 和来源 Job 的不可变期限约束。不得预先用 worker 时钟计时或任意折半。子 Run、Scope、入口 Node、Job、输入值与 ChildRunLink 共用该准入时刻；事务提交期间期限经过不破坏结构约束，提交后按真实期限收敛，不能派发过期效果。收据绑定相对预算，重放返回原准入结果及期限，不按新时钟延长预算。

### PKCE 清理恢复预算与完整证据链

联合评审接受：一个终态 OAuth Task 对同一 exact 删除效果最多创建八个显式恢复 successor，加上初始 Job 共九个；每个 Job 原有 attempt 上限独立保持。恢复在 Task 锁下从实际 current cleanup pointer 核验完整 predecessor 链，逐项检查 tenant、Task generation、绑定版本、删除身份和前驱终态，拒绝缺失、环或超界链。预算由实际链计算，不能相信新请求或 payload 自报次数。当前权限校验在 Receipt 之前，合法重放返回原 Job，不消耗次数；临界并发仅一个命令能够更换当前指针。超限是已有 closed conflict，事务不得留下新 Job 或 Receipt。相同有界链核验复用于后续清理证据退休。

### 分开的历史退休义务

联合评审接受：History 维护复用现有进程和独立有限数据库角色，以有限 creation cohort 分别处理 Run 连续前缀、Receipt、Event/已发布投递和 OAuth 清理链。每个候选独立短事务；目标行和 owning root 使用 try-lock，任一锁竞争回滚候选后继续，不引入相反的阻塞锁序或持久队列。结构损坏被保留并明确记录，不能卡住以后候选。

Receipt 保证窗口同时覆盖持久 expires_at、完成时间加独立保留窗口，以及真实 callback/retry deadline；未决效果、未完成投递、hold 和当前证据引用继续阻止退休。已知 scope 使用当前 owning 事实判定，未知 scope 保留。记录存在时保留原 request digest 冲突与重放语义；物理退休后的窗口外同 key 是新命令，重新通过当前权限和业务前置条件，不承诺永久去重。审计 Event 的删除共享真实 Receipt、Artifact provenance 和 cleanup source 引用检查。已确认 Published 且完成自身窗口的投递可独立退休，不因 Event 的更长 hold 保持投递行；未决投递永不据 TTL 删除。

普通 Run、Invocation、Job、Task 和 RunValue 仍是当前详情与执行引用的 authority，不在该维护路径泛化删除。OAuth 的终态 Task 及完整专用 cleanup Job 链在各自窗口、引用、hold 和 exact 删除证明均满足后可退休：先写有限安全审计证据，再同事务先删 Task 后删完整 Job 链，不清空临时 pointer，不删除 token Secret。该证据 Event 以后按普通审计义务直接退休，不循环产生新归档。纯历史删除没有外部 I/O；Artifact/Blob 的物理删除仍由原拥有域和 existing shared Job 完成。

新 metadata 原语只提供受限字段、锁与物理操作；Rust 拥有窗口、效果和完整链语义。数据库角色保持仅 EXECUTE，无通用正文读取或删除权限。唯一当前 schema 快照与 inventory 必须同时更新；真实多轮生成、老化、退休和并发权限测试证明可回收历史增长，不把仍需保留的当前 authority 当作泄漏或无条件清除。

### 本地 runtime 配置变更

联合评审接受：CLI 在既有 lifecycle lock 下执行显式 stop 后的配置变更；有仍由本项目拥有的活动角色时拒绝改写。
版本化、有界的本地 transition journal 绑定旧 profile 和完整目标 profile，以及固定角色文件的旧、新摘要。
配置由实际安装 binary、feature 和原持久身份重新派生；独占同目录 staging 保存经完整摘要校验的目标文件，原子 profile
替换是提交点。恢复只接受旧或目标 profile 与各文件的旧或新状态，未知漂移拒绝；持锁预检全部文件及活动进程后再重放。
私有目录与文件拒绝 symlink、hardlink 或路径逃逸。完成后删除 journal 与 staging；build cache 可以重建，不成为另一份 runtime authority。

初始化 Artifact、Policy 与配额的 seed 属于持久身份，CLI 与建库工具共用纯部署合同。同一项目的配置重建只消费原摘要
校验通过的 seed 快照；当前策略与物理绑定必须一致。transition 不得替换该 seed，缺失、漂移或策略变化要求新的本地身份，
不能重新随机生成身份、从数据库反推输入或放宽建库重放检查。

源构建指纹覆盖目录迁移后的真实 CLI、服务、Rust 工具、共享 crate、合同和发行/profile 输入，排除生成物与缓存。
该指纹只决定构建缓存是否失效，不能替代实际 executable digest、安装能力和 frozen execution semantic 的校验。

补充联合评审：OAuth Task 的 owner_id 是预分配的授权结果身份，Declined/Expired 时对应 McpAuthorizationBinding 可以尚不存在。该 prospective identity 不作为 PKCE 清理链的必存父根；仅在已经验证 OAuth Task、完整链和 exact PKCE 身份后排除这一关联。不能全局忽略相同 nominal kind 的真实引用，也不能删除已有授权记录或 token Secret。

### Orchestration 领取的分区提示与事务快照

联合评审接受：在开始 Orchestration 领取的 SERIALIZABLE 事务之前，用独立短语句观察有界可用分区提示，释放其锁与连接后再建立领取快照。提示仅决定本次尝试的物理位置，不授权领取或改变公平性；事务内按精确 work class 和 partition 重新锁定当前行，验证 enrollment，并保留全部 fairness、quota、Run/Node、Job version 与 lease fence。提示为空、陈旧或已锁忙时允许无工作结果；未准备提示的通用控制事务直接拒绝领取，不能回退到事务内的全分区扫描。

该分离避免短空扫描与长业务领取通过广域谓词读反复形成 SSI 冲突；不降低业务事务隔离，不改变预算、wire 或持久 authority。单连接池必须能够先完成预读再开始事务。独立精确分区的并发提交、陈旧提示和原五十 Run 四进程场景共同验证边界；仍可能发生的明确事务 abort 使用既有有界重试，不能视为新的业务 attempt。

### 作者特性和 Run 的冻结来源

联合评审接受：作者所需运行特性从唯一 Plan 和 exact dependency Deployment 的实际不可变闭包派生。只读 resolver 提供有界、去重且绑定 exact 身份和接口的安全特性证据；source bundle 固定该输入，Registry 在原 validation 提交事务内独立重推并核对。特性证据不包含 endpoint、Secret 或 worker build，不授予安装、发布或执行资格；后端名称本身不能证明物理 adapter 已安装。Native、Remote、MCP、Sandbox、Context 和子 Agent 的真实要求分别沿已有部署合同取得，不能把所有 Capability 猜成 Remote。编译语义随该派生规则明确升级，IR 不因产品提示字段而升级。

Run 的定义读取只返回现有 RunBindings 与 exact Deployment 关联的安全引用，受当前 RuntimeRead 约束；不新增定义表、Receipt 或执行状态。用户显式检查来源后，Console 经独立的 Agent/Artifact 正文权限读取已发布源、重新编译并核对冻结 Plan，再显示不可变 source map。它展示该 Run 使用的计划位置，不从来源图推断节点已经执行；切换身份、Run 或撤权立即清空内容。

Agent 摘要提供当前 active Deployment 的 exact 引用，Console 从该已发布版本取得 Run 输入 schema、分类和默认期限，未发布 Draft 不参与运行默认值。Run admission 仍选择并裁决提交时点的 active closure；若并发变化使请求失效，界面要求明确刷新，不自动换引用或重发用户意图，也不新增全目录扫描或名称查询。

新 Run 请求可以显式带预期的 exact Agent Deployment；它是新准入的条件，而非已有 Run 的重新绑定。Console 的已发布默认值与此条件一起提交。PG 在当前授权及 Receipt 重放判定后、同一准入事务内锁定 Agent 当前 active authority 并比较；不符返回冲突且不创建新业务事实。已提交同请求的 Receipt 在 active 后续变化后仍重放原 Run；更换预期条件是不同请求，不得由客户端以同键或隐式换键自动重试。不提供此条件表示调用方明确选择提交时的 active 模式。

### Task 查询目的与服务端动作（已接受）

联合评审接受：列表快照及分页位置属于 PostgreSQL 的时间域，HTTP 进程不将数据库快照与自己的当前时间或游标到期时间比较。
数据库继续验证快照不晚于数据库当前时间；签名 codec 保留分页位置不晚于快照的同域校验，HTTP 保留续页快照完全一致和实际 DTO 边界校验。
游标签名仍绑定当前主体、租户、查询目的、过滤条件和页大小；每页重新检查数据库当前权限。游标有效期由首次 Gateway 请求确定，
续页复用原到期时间，并按 Gateway 时钟拒绝过期，不能通过数据库时钟偏差、重新签名或翻页延长。该分工不新增时钟容差参数、数据库状态或公开字段。
Run 时间过滤仍验证身份、时间格式与区间顺序；初页的时间上界由数据库的真实快照裁决，续页可以使用原签名快照校验，不能用 Gateway 当前时间代替。

Task 元数据查询显式区分 respondable 与 viewable，HTTP 默认前者，游标绑定查询目的。
Respondable 使用当前响应许可和冻结的资格规则；Viewable 按 Task 种类检查当前读取许可，
只返回安全元数据，不授予正文披露、表单读取或任何修改能力。表单仍同时要求响应资格和
ContentDisclosure。Task View/Form 当前合同升级为 schema 2，必填 allowed_actions；表单还绑定
实际 generation、version、ETag 与 safe_prompt_key。动作由 Task owner 的同一目标和时间窗口
规则结合当前权限计算，客户端不再由种类和 pending 状态推测；外部授权仅使用专用回调协议。

动作提示不是新的并发或授权 authority。修改命令仍先验证当前权限与资格，再处理 Receipt
重放，首次应用才检查 Task generation/version、状态与 deadline。终态重放不因动作列表为空
失效；交互拒绝映射 Declined，Approval 拒绝映射 Rejected，取消仍是无决策 Principal 的终态。
这些变化不新增数据表、投影或数据库 schema 版本，安全视图和动作来自同一只读事务的当前
Principal、Task 与数据库时间；失效权限、期限、查询目的游标混用及取消通过真实 PG/API 回归。

### 恢复扫描的对象损坏隔离（已接受）

Run 结构取消同样隔离叶子当前 Job 的旧对象读取与校验：在 quota 锁定前和当前对象锁定后分别验证。当前 Node 先按 exact tenant/Run/Node 定位，再验证已存在记录的种类和状态；不能用种类筛选把损坏隐藏为缺行。真正缺失的拥有关系仍使事务失败。共享账本、控制决定产生的新状态和首个 mutation 后的解码失败仍使事务失败，不能降格为对象诊断。

恢复扫描先取得有界候选的合法 nominal identity 和排序键，再进入单对象的 owning 解码、校验与事务。只有明确持久对象边界确认的数据损坏可以回滚该对象，保留原行、lease、quota 与效果义务，并返回有界安全诊断；继续处理后部对象且推进已扫描游标。诊断不含正文或自由错误字符串，不是新的状态、队列或成功结果；内部诊断保留 nominal identity，生产日志和指标仅输出有界分类，不输出 identity。重启可重新发现同一损坏对象，但不能让它永久遮挡后部正常工作。

数据库不可用、权限拒绝、共享 quota/fairness 损坏、事务级失败和不确定提交不能被当作对象损坏吞掉。明确的数据库 abort 继续使用既有重试约束。没有 cursor 的恢复批次复用 owning keyset/page 合同和进程内可重建位置；PKCE 的过期恢复复用 Recovery Job cohort，移除固定队首的前置扫描，不另建恢复队列。任何隔离均不得写伪终态、清除不确定 effect 或产生成功计费。

OAuth Pending Task 的到期驱动复用现有 PKCE Cleanup 进程：每轮先取得独立 critical-control permit，通过有界全局 keyset/page 扫描由 PostgreSQL 时间和 Task first-winner 规则决定到期及原子 cleanup obligation。进程内游标只是可重建位置；单对象损坏诊断保留并推进，数据库或权限故障仍使该轮失败。控制 permit 在领取 Recovery Jobs 前释放；不新增业务权限、队列、Job kind、可变配置或数据库结构。


### Outbox 水位读取与事务错误（已接受）

联合评审接受：准入与投递观测继续复用唯一 PostgreSQL 水位查询；其事务内结果保留原始数据库错误身份，只有实际达到水位才返回容量背压。明确的 serialization/deadlock abort 仍由原有有界事务重试处理，权限拒绝和数据库不可用不能伪装成 backlog 饱和。只有外部投递端口适配时转换为其安全闭合错误，不增加查询、计数 authority 或错误字符串披露。

### 本机浏览器对象上传的部署闭包（已接受）

开发 CLI 拥有本地 S3 上传 CORS 的安装和核验，规则由唯一 owning 模块定义。它只允许 HTTP IPv4 loopback 的临时
Console 端口提交 PUT 与声明的 Content-Type，不引入浏览器凭证、对象读取或响应头披露。预检不缓存；生产部署仍由部署者
配置 exact HTTPS Console origin，开发端口通配不进入生产配置。Gateway 不代理对象上传，数据库和业务 wire 不增加 authority。

CORS 仅是浏览器的跨来源约束。短时预签名 URL 仍是 exact 对象和上传参数的能力授权；prepare/complete 的当前权限、
期限及实际字节摘要、长度与用途验收继续归 Artifact。首次 provision 安装固定规则，每次 dev/start 在启动 Platform 角色前
对真实服务响应进行有界严格解码并核验完整唯一规则；缺失、额外规则、未知字段、类型或范围漂移均以安全依赖错误拒绝，
重启不自动修补。验证须覆盖真实浏览器的预检和 PUT、非许可来源与请求被拒绝，以及规则漂移导致重启拒绝；
合成响应和不执行浏览器 CORS 的 HTTP 客户端不能证明该边界已通过。
