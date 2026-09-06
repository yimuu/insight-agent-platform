# 安全、部署与证据

本文属于[架构重整提案](README.md)，不是当前安全或生产资格声明。
现有 [ADR-0007](../../adr/0007-opensandbox-execution-provider.md) 与 [ADR-0008](../../adr/0008-sandbox-runner-capability-boundary.md)
的 Sandbox 物理执行和权限要求继续有效；目录移动或内核抽取均不能绕开这些边界。

## 威胁模型与部署单元

目标平台区分公开客户端、平台可信服务、持有敏感凭证的 Broker、外部 Provider 和不可信 Package。
平台可信服务也遵守最小权限，不能因为使用 Rust、内部网络或 mTLS 就被赋予所有 tenant、数据和凭证的能力。

Gateway、持久编排、不可信执行和数据访问继续保持独立边界。代码复用、共享镜像、同一宿主和同一进程分别评审，不能互相推导。
同镜像内可包含多个 role binary，但运行进程只挂载当前 role 所需的凭证、配置、网络和数据库权限。
Sandbox Package 不和可信 worker 合并，也不共享它们的服务账号、数据库连接或 Broker 管理接口。

同一信任域内，只有权限集合、网络能力、伸缩方式和故障隔离要求都一致的角色，才允许通过明确部署 profile 共置。
共置不能扩大任一角色的可用凭证，不能让业务池占用恢复保留容量，也不能仅为了减少启动命令而引入万能 worker。
本提案不默认合并任何现有安全隔离角色；角色归并必须由部署合同证明上述条件。

可信 Provider adapter 只实现协议和物理 I/O，接收经领域准入后的有界请求。自定义不可信插件只能进入 Remote 或 Sandbox 路径，
不得通过动态加载库、模板表达式、用户上传 codec 或任意 command 进入可信编排进程。

当前 runc 路径仍依赖共享宿主内核、镜像闭包和实际 CNI 行为。本提案不将其升级为恶意多租户强隔离声明；也不加入未评审的
gVisor、Kata、microVM 或 Docker fallback。需要新的隔离目标时，另行更新物理 provider ADR、机器合同、镜像与动态资格证据。

## 当前授权与冻结定义

Run admission 冻结可重复解释的资源、依赖、策略要求、数据视图与预算上限。它不是长期有效的全权限授权票据。
tenant 禁用、principal 撤销、Secret binding 失效、Artifact 撤销及当前安全限制仍在实际准入和提交边界按操作目的裁决。

目标授权决定同时考虑冻结要求和当前 authority；当前限制可以拒绝继续执行，不能自动提升已经冻结的能力或扩大预算。
权限撤销不会回滚已经发生的外部副作用。对撤销与请求发出之间的窗口，合同说明实际检查点与可能结果，不承诺跨外部服务的即时撤销。
授权相关锁与版本判断必须和受保护的业务修改保持原子；外部 I/O 返回后再次校验的要求不因缓存命中或本地调用而省略。

授权 port 必须区分新业务执行、内容披露与受限系统完成/核对/清理。前两者要求当前用户和资源权限；后者要求当前有效的系统角色，
并限定到已准入的 exact effect、tenant、owner 与 fence。用户撤销或 tenant 禁用不得使已发生的外部成功无法记账，也不能使
quota 永久占用或 Secret 删除失去执行者。完成权限只记录和收敛已有义务，不授予新的用户动作、非幂等外部调用或正文读取权限。
若原 worker 身份本身被撤销，必须由合法恢复身份通过正常领取和领域命令接管，不能继续信任旧身份。

公开客户端继续使用同一 OIDC/workload credential 校验路径。本地 issuer、CA、Secret 和身份 material 永远标记为 development，
不能因为目录或 profile 改造进入 production。登录体验可以优化，但不得增加 privileged header 或跳过现有认证的 Console 路径。

## Broker 与内容路径

Secret 解析、受控 Egress、Artifact 内容读取按现有 owner 划分。共享的是基础 transport、超时、观测与错误封装，不合并全部敏感权限。
Egress 校验安装的 exact endpoint，DNS 检查与实际连接保持一致，自动 redirect/proxy 和任意 URL 注入继续禁止。
MCP discovery/OAuth、模型与普通 Capability 请求不因共享连接代码而共用可越权的 token cache。

ArtifactBlob 拥有 verified digest、长度和物理 generation；Artifact 拥有用途、授权与保留关系。
小型值与大内容仍通过现有 RunValue/Artifact 路径，不新增业务正文数据库。跨事务 I/O 使用 exact generation，内容交付之前再次验证授权。

所有可返回正文的入口共用受限内容授权 port，包括现有 Run `/result` 的 Inline 与 Artifact 分支、新 values content 入口、
Artifact content 以及 CLI result 输出。仅有运行元数据读取权限时返回 403，不通过旧路由、静默投影或预签名 URL 绕过内容策略；
授权后仍返回各自原有 success DTO。受限系统完成权限不构成这些公开读取入口的内容许可。

可缓存已校验的不可变 Plan、schema、Skill 或 Artifact 字节；缓存键必须包含足以区分内容和安全域的冻结 identity。
缓存只复用内容，不复用已过期授权，也不延长 Secret、token、hold、grant 或业务 lease 的有效期。
缓存必须遵守内容分类、内存/磁盘容量、生命周期和密钥约束；不能通过跨租户命中信息泄露内容是否存在。

## Sandbox 与外部副作用

保留 Dispatcher 作为 shared Sandbox Job 的唯一 Platform owner 路径：有界创建 inert candidate、选择、签名激活、结果提交和清理。
Job current fence 决定是否接受观察；OpenSandbox object、Pod、runner 或进程内缓存不成为执行状态 authority。

activation authorized 之后不得启动 replacement。runner boot 改变按已有协议提交 UnknownOutcome 并进入清理/absence reconciliation。
不能把旧进程消失当作“外部副作用未发生”的证明。固定 launcher/core、独立 Package identity、capability 清理、管理请求签名与
read-only rootfs 等要求必须随镜像构建和实际权限一起验证。

复用 worker lifecycle 不得统一掉 Sandbox 的特殊两阶段激活、boot rollover 和 orphan 保守处置。
cleanup 可以重试物理删除，但没有修改业务终态的权力。Provider 不可达应保留 cleanup intent 和告警，不猜测成功或删除未知对象。

## 运行角色与部署合同

角色、权限、RPC audience、网络范围、端口、配置、容量和镜像关系由一份版本化部署合同组合生成或核验；数据库角色及 SQL grants
仍由其 owning provisioning 资产定义，不在生成器中复制另一套权限语义。
ComponentRole 的运行身份与 Candidate/qualification 的发布证据区分归属；发行工具不能因读取角色信息而成为业务运行依赖。

CLI development profile、Helm、镜像检查、CI workload closure 与 release tooling 消费同一角色模型。
模板生成不能替代渲染后检查与真实集群验证，也不能生成未经评审的 privileges。二进制目录移动不改变容器 ABI、签名身份或公开入口。

NATS 在目标中承担可丢失的 wake hint 或已提交投影，不保存 Job current state。失去 NATS 时，应由投递积压和依赖指标反映降级；
依赖 PostgreSQL 扫描仍可安全执行的角色，不因通知通道不可用被全局禁止领取。生产是否满足延迟目标由容量与恢复证据裁决。
PostgreSQL、必需 Broker 或 Sandbox 信任边界无法验证时，相关角色的准入/readiness 必须按合同拒绝，不退回非受控路径。

新增 Outbox worker 作为独立 role/composition root，沿用 Outbox 投递生命周期，不创建投递 Job 或业务队列。它只拥有所需的
Outbox 领取/完成权限、immutable Event 投影读取和指定 subject 的发布权限，不获得业务状态、Secret 或 Artifact 正文权限。
JetStream stream 的创建和配置属于 deployment/provisioning；worker 验证已安装合同，不自行扩大 stream 或订阅 ACL。
发布 ACK 只证明传输接受，不能证明下游业务完成；磁盘满、stream 限制或 ACK 丢失均不能提前标记已发布。
开发、Helm、release profile、角色 grants、网络策略与镜像闭包必须同时覆盖此角色和实际 JetStream 持久配置。

配置读取、连接池、permits、timer、重连、heartbeat 与安全日志的通用机制收敛为库；不同领域仍拥有自己的错误分类和重试决定。
业务与 critical-control 使用独立预算和连接池，SQL statement/lock timeout 不能无限占住恢复容量。
外部并发、响应字节、Artifact materialization 和 pending work 都应有独立可观察的有界准入。

## 读取、排障与质量证据

运行事实、授权内容和质量证据是不同读取用途，共享 identity 和 provenance，而不共享可写 current state。

运行事实通过 owner 的公开查询及 Event 解释当前节点、等待原因、实际执行构建和预算结算。SSE 仍是有界页面，客户端持续接续游标；
浏览器状态不是 Run 状态机，重连后重新读取必要的当前快照。
metrics 只使用固定有界标签，不以 tenant、request、resource ID、endpoint 或原始错误做标签。

授权内容通过公开的受审计读取路径按分类和用途获取 RunValue/Artifact。普通日志和 tracing 不保存 prompt、tool body、凭证或正文；
产品调试界面也不能通过任意对象存储 URL 绕开 Broker。错误显示稳定 code、来源和可操作的安全说明。

质量评测复用正常 Plan/Run、预算与 Artifact 输出，可以关联 exact Agent、Model、Context 视图和策略。
评测结果只说明评测条件下的任务效果，不修改生产运行结果，不替代 infra qualification，不创建独立执行引擎。

## 保留、联合恢复与运维权限

PostgreSQL、对象存储、KMS/Secret 和仍在执行的物理工作构成联合恢复问题，不能用几个各自恢复成功的结果替代全链路保证。
恢复目标必须携带数据库恢复点、可引用对象 generation、所需密钥可用性、执行语义版本和部署身份信息。
恢复后先进入禁止新外部副作用的受限执行模式；对可能晚于数据库恢复点发生的调用、授权与激活进行核对，再由受审查流程放行。
恢复模式由 deployment/provisioning 控制：停旧 writer 和执行环境、撤销旧签名身份、终止旧数据库 session 并轮换凭证，
新进程使用新 generation 和受限配置启动，再由正常领域恢复命令核对。任何一步无法证明旧执行已隔离，都保持受限模式。
不引入未定义的数据库 restore epoch 或新的业务恢复控制表；public management API 不获得 release authority。
详细状态与合同见[执行与持久化](execution-and-persistence.md)。

运维修复通过 owning 命令、部署或 provisioning 完成，不直接改 Run、Job、lease、quota 或 Outbox 行。
恢复 scanner 可以重用这些命令，但不能成为额外的业务权限来源。数据保留必须覆盖审计、去重和历史引用的承诺；
清理 Receipt 之前先判断重试窗口，清理内容之前先判断引用、hold 和法规/租户策略等现有约束。

## 验收与文档修正

需要独立覆盖以下行为，而不只检查模板字符串或对象数量：

- 每个角色的正向权限与互斥拒绝；真实 PostgreSQL 角色执行，缺少测试连接或角色时在 CI 中失败。
- 授权撤销发生在读取、Provider I/O 与结果提交之间时的可观察结果；缓存命中不能绕过撤销。
- 外部成功后撤销用户仍能受限提交证据并结算；disabled tenant 的清理能完成，完成角色不能发起新业务调用或读取正文。
- 仅有运行读取权限时，旧 `/result` 与新内容路由均拒绝 Inline/Artifact 正文；CLI 不存在另一条可绕过入口。
- DNS/redirect、MCP OAuth token audience、对象 generation、跨租户及内容分类拒绝。
- Sandbox 管理 credential 不泄露给 Package；boot rollover 不激活 replacement，orphan owner 不明时保留并告警。
- 通知丢失时扫描继续推进；恢复资源不被业务饱和耗尽；依赖恢复后投递与 cleanup 积压收敛。
- JetStream ACK 丢失、磁盘满和 consumer 重投的行为，以及 publisher 的数据库/subject 最小权限；清理耗尽后的受控重建不能使旧 Job 生效。
- exact release 的联合 restore、执行语义兼容和隔离放行；不确定副作用未核对前不得自动重做。
- 公开 tracing、日志、SSE、报告和缓存无敏感正文/credential 泄露，同时保留可授权访问的诊断证据。

现有 `artifact_role_permissions` 测试连接变量与 CI 不一致的问题纳入真实角色验收，不以增加静态文件检查替代运行断言。
当前 Sandbox runbook 中 Controller/process-generation/replacement 的旧说明应在相应切片实现文档更新时统一为
Dispatcher/current Job fence/boot rollover/cleanup；Outbox publisher 的处置说明只有在通用投递链路实现并验证后才能称当前路径。

每项 evidence 绑定 exact source、schema、合同、配置、镜像和实际环境。仓库、进程级、单宿主 Kind 与生产多故障域证据明确区分。
未运行项保持 Not run；本提案没有新增任何已通过生产资格。
