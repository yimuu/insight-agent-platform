# ADR-0010：统一安装与多来源模型配置

Deployment lifecycle update: [ADR-0012](0012-declarative-container-installation.md) supersedes the
host-controlled Compose/Helm phases and OpenBao initialization/serving switch described below.
Other accepted ownership and security decisions remain in force.

状态：本文记录的架构子边界已联合接受（Accepted）并实现；整体验收仍有独立待验证项。
当前实现、真实部署结果及外部/人工验收范围见[统一交付索引](../specs/unified-installation/README.md)。
架构接受不等于每个拓扑、厂商能力或完整示例都已取得资格。

## 背景

提出本 ADR 时，开发 Compose 只提供依赖，CLI 同时持有本地配置生成与进程监督逻辑。模型的 Registry、Egress、
SecretBinding 和执行路径已经存在，但缺少普通用户可使用的贯通配置入口。安装目标、账户来源、
模型定义和环境变量命名属于不同事实，需要用各自已有的拥有域连接起来。

## 共享安装

Compose 为默认单机开发体验提供完整基础角色；Helm 负责 Kubernetes 部署。二者直接运行独立 role
binary，不在容器中运行 CLI supervisor，不挂载 Docker/CRI socket。Sandbox 仍由已有 Kubernetes /
OpenSandbox 边界执行，普通模型和检索无需因此启用 Sandbox。

共享配置生成器消费有类型、有界的网络拓扑、角色路径、凭据文件引用和实际 executable 身份。CLI、
Compose 和 Helm 使用同一拥有域，禁止对生成 JSON 任意替换字符串来改变地址或重新猜测摘要。
部署生成的是当前 role owning decoder 能验证的完整配置，生成成功不代表 role readiness。

部署工具分别执行 prepare 与 provision 一次性任务，再启动 serving。provision 复用现有建库、
租户/身份、Artifact、Scheduling 和消息配置拥有域。重复运行验证同一安装身份；外部 effect 使用
固定请求身份和读取核验恢复。部分失败保留可恢复状态，不清空数据、重新生成身份或扩大授权。
serving 只验证 schema，不持有初始化命令入口。每个角色只挂载自己的配置、必要 CA 和私钥。

开发环境继续使用已有 local RS256 OIDC verifier 与显式 tenant/developer bootstrap。初始短期
token 通过私有文件交付，Console 使用已有内存会话输入。过期后由持有本地 issuer 私钥的部署工具
显式续发；Console 和普通 serving role 永不持有 issuer 私钥。生产使用部署者的 OIDC 身份输入。
这不新增密码系统、首个匿名请求抢占管理员或长期凭据登录旁路。

Console 为静态 bundle；同源 transport 按公开路径转发两个 Gateway，并保留认证、Receipt、ETag、
SSE 与 bounded body 语义。transport 仅接受部署输入的目标，不持有数据库、业务缓存或特权身份。

联合评审接受：原生部署直接消费同一安装输入、一次性初始化及配置生成器。部署 owning type 定义
原生启动交接，绑定实际宿主二进制、Node 和 Console 产物的字节身份；在准备身份或启动依赖之前
核验格式、架构、版本及 compiler WASM。前台监督者仅持有本次创建的子进程句柄，任何必需进程
退出或启动失败都触发有界停止与回收；持久 PID 不授予发信号权限。独立监督锁允许运行期间短期
会话续发，每条安装命令仍独立互斥。角色仅接收生成的环境和私有临时目录，不继承宿主云凭据、
代理或运行时注入参数。退出保留依赖数据；丢失监督者后端口占用明确拒绝，不猜测或终止孤儿进程。
原生依赖使用实际非 root 宿主 UID/GID 读取自己的私有配置；这是可信单用户开发环境，不能宣称
具有不同操作系统用户之间的角色隔离。原生端到端资格需另外通过真实启动与业务流程验证。
联合评审接受启动顺序修正：原生 owning launch contract 按服务器先于其客户端的顺序输出选中角色。
每个进程启动后通过既有只读 readiness observer，再启动后继；进程选择必须属于当前安装输入。
整个 serving 启动仍共享 120 秒期限，最后核对全部角色与 Console，不因逐角色探测延长总期限。
依赖未就绪或必需进程退出时关闭当前句柄集合，不增加进程自动重启或业务重试。
联合评审接受停机修正：Gateway 的 SIGTERM 与 SIGINT 共用既有 cancellation 和有界 HTTP/数据库
观察任务 drain；信号注册失败也先关闭已启动任务再传播错误。部署的完整依赖重建先停止业务角色，
再停止物理依赖，并核验实际容器退出状态；不得将强制杀死后尚未重建的状态描述为持久恢复失败。
S3 的有界停止期限由共享部署拥有者提供给各消费者；固定 NATS 镜像的 Compose/原生部署采用其
正常 SIGINT 关闭路径。该镜像正常 SIGTERM 的退出码不同，不以此推断数据损坏，也不泛化放行
其他进程的非零退出码。

联合评审接受：公共 CLI workspace 仅保存有界私有连接引用，显式绑定管理与运行 origin、Tenant、
短期 token 文件和可选公共 CA 文件。连接不生成安装身份、不复制 token、不签发凭据；本地 JWT
tenant/expiry 检查只提前拒绝明显误连，Gateway 仍是签名与当前授权的唯一权威。首次连接不得接管
已有安装状态或业务恢复记录；同 workspace 的两个 origin 与 Tenant 不可重绑定，换目标使用新
workspace，以免把原 Receipt/CAS 重放到另一安装。token/CA 引用可以原子更新；校验拒绝保留原连接，
替换后的持久化结果若不确定，只重新读取并重试同一绑定，不删除恢复记录。
普通公共命令只消费此连接，发布使用共享 compiler 与当前 Registry 校验，不读取旧进程 profile
作为第二份能力事实。显式资格 producer 可从自己实际创建的环境导出同一连接，普通命令没有旧
LocalProjectState 回退。workspace 写操作使用独立私有锁，不要求存在本地 runtime 或 issuer。

## 来源与部署目标

ModelProvider 是账户来源，ModelProfile 是来源上的模型，ModelDeployment 仍精确绑定其 Provider
Deployment。资源别名独立于 display_name，属于原 Resource，按 tenant/resource-kind 唯一。
别名冲突、并发创建和默认设置由 PostgreSQL 原事务边界裁决，不增加 source/model 平行数据表。

Egress 的 process-installed model catalog 改为物理 destination grant：闭合匹配 endpoint digest、
wire protocol、region、credential purpose 和 network/TLS/trust/data 全部 exact policy refs。
安装文件不再重复保存业务 Provider Deployment/Revision ID。重复或歧义 grant 必须拒绝。
现有 Secret resolver 只核验 tenant 与 SecretBinding，不能独立证明一次模型请求的完整授权。因此该
替换必须先增加有类型的逐次 dispatch 授权：Security RPC 委托原 Model/PostgreSQL 拥有域，核验当前
tenant、ModelTurn、Job、worker generation、attempt、lease、admission、request digest，以及冻结
Provider exact IDs/digests、策略与 SecretBinding 属于同一闭包。授权在 DNS、密钥读取与网络发送之前
完成，development anonymous 也不跳过。只有授权 ModelWorker 可调用模型执行 RPC。变更不放宽 DNS、SSRF、TLS、
redirect、当前凭据授权或有界请求限制。API 不能写入 deployment catalog 或创建任意网络目的地。

客户端的厂商 preset 仅提供输入默认值。协议与厂商独立，同厂商可配置多个账户、地区和地址来源。
环境变量导入显式指定每个字段的变量名，只读取指定变量；显式映射优先于 preset，缺失/冲突错误不
含变量值。导入完成后由资源和 SecretBinding 持有事实，运行时不扫描厂商环境变量。

## 模型、默认选择与证据

默认模型指针放入 TenantConfig，绑定同租户的真实、active/enabled ModelDeployment；变更通过当前
权限、Receipt 和 ETag 控制。它用于后续作者依赖解析，不改变已发布 Agent 或运行中的冻结依赖。
Selection Policy 保持候选选择算法职责，不充当另一份默认模型事实。

模型配置区分管理员声明、协议实测和厂商保证。连接成功不能生成关于分词器、数据保留、训练使用、
native JSON、工具或稳定模型身份的保证。当前 UTF-8 字节估算如实标识为估算。所有 evidence
Artifact 必须包含实际源字节与声明来源，不能使用 fixture 或填充摘要来获得发布资格。
未声明厂商保证报告用量，不禁止消费实际协议返回的用量。成功响应标为 ProviderReported 时必须有
完整输入、输出计数，并继续经过原额度上限校验；缺失用量不能当作零，派发后失败沿用原保守结算。

联合评审接受：基础来源的响应预算与 Inline 输出预检共用 foundation 的容量计算，先从实际 Inline
硬上限扣除既有封装预留，再产生可用的声明预算。Egress 与 ModelWorker 的模型帧配置对齐既有 RPC
metadata 硬上限，保留逐层字节限制、流背压、并发与缓冲界限。预检不保证任意厂商响应都能存储；
规范化后的完整输出仍按实际字节和 Schema 终检，超限不截断、不隐式转存 Artifact、不自动重放厂商
请求。声明变化产生新的精确版本与部署，不改写既有 Run 的冻结输入。

联合评审接受：模型共享 SSE parser 将标准 id/retry 仅作为丢弃的传输元数据，支持单个开头 UTF-8
BOM 与 CR/LF/CRLF 的任意分块；不保存事件 ID、不重连、不发送 Last-Event-ID。所有原始字节仍受
既有容量限制，业务 JSON、事件 type 与未知字段保持严格校验。EOF 不补造事件终止空行；DONE 后仅
接受此前 CR 的配对 LF，不创造成功结果。此 framing 语义由新的 adapter mapping identity 绑定，
不得改写旧安装、已冻结 ModelTurn 或借解析修正重放已付费请求。

联合评审接受：Responses 的字符串内容与身份分别解码。空增量仅贡献零内容，不产生规范化事件；
空终态文本片段不代替完整响应、Schema 或用量校验。可选空 fingerprint 仅表示未观测，不填造身份，
原终态证据摘要仍包含实际收到的元数据。必需模型/工具身份、未知字段及隐含 reasoning 的边界不变。
此映射变化单独更新 Responses adapter 语义身份；固定错误类别不携带供应商字段值。

联合评审接受：Responses 已识别的文本计费用量明细按 codec 自有闭合集与有限条目数校验后丢弃；
标准顶层输入、输出用量仍是唯一规范化计账来源，不相加、不用明细填补缺失计数，也不推断费用。
完整终态证据摘要保留实际明细，未知或非法明细明确拒绝；此变化提升该协议的映射身份，不改变
ModelTurn、额度、租约、失败结算或已冻结安装，也不授权重放已派发请求。

安装所需模型策略由 Registry 的有类型 builder 生成一个真实声明包，内容、摘要和 exact Policy
闭包由同一拥有域产生。Artifact 物理拥有域先 stage 并核验精确对象版本与实际字节，PostgreSQL
随后在同一事务中建立 Artifact/Blob、已发布策略版本及其真实 ArtifactLink。声明包只证明所声明
的安装输入与约束，不构成厂商能力资格。Artifact metadata v2 显式区分经过上传/验证 Job 的
Upload 来源与绑定冻结 ServerRequest 的 Installation 来源，不为初始化虚构上传 Job。

作者 profile 使用安装 catalog 的真实 Execution 和 Selection 策略，并在一个当前授权快照内
解析租户默认模型。`project/default` 是可直接交给共享编译器的作者引用，不写入 Resource alias；没有默认值时模型
候选为空，已配置但失效的默认值明确报错。缺失 catalog 不回退到 Scheduling。CLI 离线编译须有
显式精确 profile 或恢复的源包，不根据目录名和安装 ID 构造策略摘要。选择在编译时冻结，发布和
执行仍由原拥有域重新验证当前授权。

既有 StructuredOutputContract 的 textual_json_fallback 通过适配器补齐：仅在该配置明确启用时
不发送 native 输出字段，响应仍经过原严格 JSON/schema 校验；不静默修复 JSON，不宣称厂商 native
structured 支持。JSON 输出指导由共享编译器放入 canonical instructions 并正常经过 assembly、
source-map 与预算核算；adapter 不增加未计入准入的隐藏提示。具体能力组合与拒绝条件由 owning type
和适配器测试控制。

## 密钥与管理入口

CLI 和 Console 共用 public management contract 与共享 Rust 编排/编译逻辑。普通 Resource、Artifact、
Validation、Deployment、Receipt 和权限生命周期继续适用，不增加绕过发布校验的直接数据库写入。
需要新公共入口的密钥导入、默认绑定和安装目标发现先联合评审对应 owning contract。

Secret Broker 持有外部 Secret/KMS 物理操作；Security 持有当前授权、SecretBinding 和审计。
联合评审接受持久 S3 与 OpenBao Transit/KV v2 的显式物理适配器。共享客户端只拥有受控 HTTPS、
证书认证与物理操作，不持有业务状态或授权；Artifact 保持精确对象 generation，Secret 以固定请求
路径、CAS0 和精确版本读取恢复，不伪造 AWS 身份、切换 latest 或重新生成未知请求。
Transit 绑定实际 key version 与原身份 AAD；精确 Secret 版本销毁保留墓碑，不删除逻辑 key。
OpenBao 首次 bootstrap 的部分失败明确保留同一卷和私有材料，不自动重试初始化、修复或重建身份；
实际认证与 cluster/mount/key 核验完成后，平台 provision 才进入既有 exact resume 流程。
serving 不持有 seal/root/初始化权限；默认安装的持久性必须经过完整依赖进程重建与原业务状态核验。
联合接受的部署子边界将首次提供者初始化异常报告为 Incomplete 或 ExternalOutcomeUnknown，
不声称该阶段所有中断均能恢复，也不增加候选目录发布流程。ProviderReady 之后必须保留原物理
身份；原卷或材料缺失时拒绝启动，不通过 self-init 重建。本地 Compose 与本地 Helm 的静态 seal
同属私有卷保管边界，不能代替生产外部 KMS 隔离；生产部署须显式提供自己的 trust/seal/auth 输入。
只读 verify 可以取得短期认证 token，并允许提供者自身审计与认证 lease；它不写 provider 配置、
业务 Secret 或对象，不修复漂移。该子边界接受不代表完整安装或生产持久性已经取得资格。
联合评审接受：S3 安装拥有者在首次签名 bucket 读取处等待实际 API 就绪，使用单一有界期限，
仅对暂时不可用重新读取。取得结果后仍由原 journal 判定缺失、身份和配置；已配置的 bucket 丢失
立即拒绝。创建、标记、版本和 CORS 写入及其后续核验不进入该等待循环，未知写入不获得第二次许可。
密钥导入须在外部操作前及持久提交时核验当前调用者权限，凭据值不得进入 Resource、日志、Event、
Receipt 投影或普通配置。重复/不确定导入以同一请求身份 prepare-or-load 和精确读取恢复；变更内容
不能复用已提交请求。轮换/撤销不把旧 Run 隐式切换到新账户。

连接诊断属于管理请求，复用安装的 Egress 和 Secret 路径，在 DNS 前及取密钥后分别校验当前授权。
它只发送固定、有界的文本请求，返回闭合的协议观察结果，不创建 Run、ModelTurn 或额度消费事实。
合法但被截断的 reasoning 响应只记录协议响应，不表示模型已完成回答。
诊断和完整 Responses 解码器共用已识别可选响应元数据的类型校验；元数据不改变规范请求、规范输出或能力声明。
Gateway 将当前公开调用的 trace 映射到内部 RPC 上下文；诊断和凭据导入不得通过缺失 trace 的私有通道发送。

凭据管理仅投影当前租户的绑定标识、状态、代次、版本与 ETag。撤销使用已有 SecretRevoke、原 CAS
和 Receipt；同一请求重放不读取新版本覆盖并发修改，也不删除外部秘密或重写默认模型。
默认选择、作者解析和连接诊断复用同一策略类型及精确闭包校验。

联合评审接受：Security Authority 的连接授权以只读当前快照核验 Provider 声明 Artifact 的可用性。
数据库角色仅获得该查询所需的 Artifact/Blob 身份、状态及内容摘要元数据列读取权限；不读取对象定位信息、
Artifact metadata 或 RunValue 正文，不获得 Artifact 写入或行锁权限。执行派发继续读取原 ModelTurn、Job
及冻结闭包的当前授权，不为连接诊断建立另一份状态或缓存。实际角色回归必须使用 owning grants，不能用 owner 连接代替。

客户端在第一次对象准备响应后持久保存有界 Artifact/Operation/Grant 身份、原 CAS 和期限。
Console 恢复先读取当前 Artifact；进入 Uploaded 或 Ready 后不重放 prepare，不轮换请求身份。
CLI 已确认对象上传后使用原 complete 的 CAS 和 Receipt 恢复，不重新准备或延长授权。
浏览器不持久保存签名 URL、上传证明或模型 API key。CLI 的附加公共 CA 是显式、单次读取的传输输入，
仍执行主机名校验，不成为业务配置或系统信任修改。

Artifact Gateway 及内部暂存 DataWorker 的窄数据库角色通过受限的 SECURITY DEFINER 函数锁定同租户的精确 PolicyRevision
及所属 Policy 行；函数仅返回存在性，固定安全搜索路径，不读取业务 payload，也不授予 Registry 写权限。
当前 gate、摘要与 ArtifactIo 类型仍由 Rust 在同一事务持锁后判定。暂存额度先锁定 QuotaAccount 并
核对版本，随后读取不可变 reserve ledger，不为只读账目授予 UPDATE。以上物理权限随当前 schema
一次部署到空库，普通启动不会修补已安装数据库。

联合评审接受：Artifact DataReader 为现有 Scheduler TypedPlan、RunValue 和 Skill 读取授权补齐
Run 冻结绑定、Registry 版本、部署及 Selection Policy 当前 gate 所需的精确列读取权限。
它保持只读，不读取 Run 当前 payload，不获得业务写入或行锁权限；原租户、租约、摘要、候选选择
和对象读取边界不变。角色授权测试须覆盖三种实际拥有域读取，而非仅 Sandbox 读取。
删除审批的创建仍归 Artifact Gateway，它仅新增 Task 的 INSERT 权限。Approved Task 是 owning
first-winner 已终止的不可变事实，删除准入读取后核对完整原请求绑定；不再要求 Task 行锁，也不授予
Task UPDATE 或 DELETE。Artifact/Blob 的锁和原 CAS/Receipt 保持不变。
DataWorker 的内部暂存负责创建 Artifact/Blob；MCP 验证完成后仅按现有精确 CAS 推进 operation 的
version 与 updated_at。因此该角色仅获对应创建权限及 Invocation 两列更新权限，不获其 state、
payload、owner 的更新或 Invocation 创建/删除权限；生产者 lease、冻结预分配和当前 wake 仍由原 owner 判定。

上传授权过期不等于 Operation 已终止。尚未提交 complete 的过期上传不能自动续期；客户端保留原
恢复记录，操作人员先核对当前 Artifact 和 Operation。只有确认原操作终态后，才显式使用新的独立
state directory 发起新配置，不能用新 Receipt 将未知结果当作失败。

模型执行额度沿用现有 QuotaAccount 与 ledger；发布权限不会隐式授予额度管理权限。TenantManage
通过原 ETag 和 Receipt 原子配置一个精确 Model Deployment 的三项有限累计上限，不刷新已使用量或
预留量。安装只创建冻结身份、初始上限为 8 的租户模型并发账户，恢复与 verify 不重置账户。客户端
必须显式给出模型额度，并持久保存原目标、限额和 CAS；响应丢失后的恢复不转向新的 active deployment。

联合评审接受：Helm 从一次性 OpenBao 初始化切到普通服务后，TCP Ready 只表示监听端口已开放。
`up/resume` 必须通过同一 owning `provider-observe` 的本次有界只读检查，核验实际集群、认证挂载、
密钥和 canary，再启动平台 provision。原初始化成功证明保持不变；单独的部署私有观察意图绑定原 Job，
不会刷新待恢复的 provision 身份。入口已有的观察意图仅解决原结果，终态确认后必须再建立本次观察，
不能将旧完成证明作为当前就绪。整个窗口使用一个期限，并核对普通 Pod 与容器身份、拥有者、
输入、镜像及 serve 命令前后未变。只有两个既有暂态只读错误允许继续观察；未知结果、消失或身份
漂移均拒绝，不重新初始化。纯 verify 不增加依赖重建或新的初始化流程。

联合评审接受：无 Skill/Capability slot 的文本 ModelLoop 可显式使用零工具调用及零并行工具预算；
两项必须同时为零或同时为正，且并行数不超过总数。有工具 slot 时仍要求正预算，轮次和 token
预算始终为正。Plan 拥有同一校验规则，公共作者 profile、共享 compiler 和 controller 复用；
不虚增 ModelProfile 的工具能力，也不跳过当前授权。IR 形状保持 v6，解释器与编译器语义身份提升，
旧 worker 不以相同兼容身份领取新计划；原已发布计划及失败 Run 保持冻结。

## 安装公共信任导出

Root 与 Operations 联合接受独立的只读 `public-trust` 安装命令。它只在已 Ready 的原安装身份通过
现有公开身份核验后，输出有界、版本化的 input/identity 摘要、唯一公共 CA PEM 与原 PEM 字节摘要。
该命令不签发会话，不访问物理提供者，不输出签发者或角色私钥，不修改安装状态。只读目录入口以
已有锁文件的只读描述符取得同一排他锁，禁止创建、写入或 fsync；写方法必须拒绝只读模式。

Native、Compose 与 Helm 消费同一 owning 输出，核对冻结身份和证书摘要后原子交付固定
`public-ca.pem`，已有不同字节、外来文件或未知结果均保留并拒绝。容器消费者只读挂载安装状态。
消费者绑定来自既有 provision/verify 的实际 Ready envelope；Native/Compose 将它保存为不可变的
本地安装绑定证据，Helm 复用已有 Ready 结果。导出不能用自身返回值建立可信身份。缺少本地绑定的
已有安装须先显式 verify；该操作可以保留本地绑定证据，但不重做 provision、不改变业务或 provider 状态。
证书文件采用原子不覆盖发布，不能替换并发出现的外来文件。返回摘要是原 PEM 文件字节摘要，
不等同于浏览器显示的 DER 证书指纹。
公共信任导出不证明端点可达、SAN 或 TLS 已通过；是否把该 CA 加入浏览器信任由操作者明确选择，
工具不修改系统信任或削弱 TLS。现有模型及 Artifact 授权不由该证书导出产生。

## Remote Context 安装与当前派发授权（联合评审接受）

Remote Context 物理目的地址许可不再包含 Registry 分配的 Context Deployment 或 Implementation 身份。
安装只冻结 HTTPS endpoint、区域、公共信任、协议映射、容量与凭证 purpose/header 注入；空目录拒绝派发。
凭证绑定及 Context、Policy 和执行身份仍来自普通 Registry 与 Query admission。安装消费者复用同一配置 owner，
按目的地址存在性启用 ContextRemote，不能通过产品管理请求修改进程配置。

每次派发前 Egress 向既有 Security Authority 请求只读当前授权。PostgreSQL 复核 Query、Job、稳定 lease/token、
当前租户与 principal、Run/Node controls、exact closure/policies 与 secrets；实际 Inline 内容摘要独立对比冻结输入。
Security 不读取 RunValue 正文、不新增表、写事务或事件。授权是有期限的当前读取结果，不是 exactly-once token；
正常 heartbeat 不因 Job version 变化而失败，结果提交继续遵循原 CAS fence。成员在 Query admission 后变化会拒绝旧 grant；
grant generation 仍对比冻结 Run binding generation，不能误当当前 membership epoch。

内部 Remote execution 升至 version 2 并改变该 adapter capability 身份，外部检索 HTTP wire 与引用映射保持 version 1。
无旧 exact catalog fallback，不修改已冻结安装。详见联合评审的临时
[派发设计](../specs/unified-installation/context-dispatch-review.md)与
[安装设计](../specs/unified-installation/remote-context-installation-review.md)。

## 评审与资格

联合评审接受：ModelLoop 的响应 schema 由该节点输出端口及冻结 Plan schema 文档唯一决定。
Plan 在发布前拒绝非对象模型输出；其他内部端口仍支持标量和数组。Agent 的最终接口身份保留在
AgentContract，模型响应 schema 与文本回退说明归属 PlanNodeInstruction，并计入同一 prompt 预算。
Scheduler 从已核验 Plan 取得 schema，准入和 PG 原 fenced 事务分别核对节点输出；后续轮次保持
原完整响应契约。Model 域同时核验 schema 与声明摘要一致，不新增表、RPC 或状态权威。
新编译器和解释器提升语义身份，已有安装、Policy 和失败 Run 保持冻结，不重放厂商调用。

Root 与 Operations 联合接受已完成安装的原模型 Policy Artifact 只读恢复观察。仅当安装处于验证模式、
对象 journal 已 Verified 时，既有完整 exact generation 读回遇到 StorageUnavailable 可在单一 30 秒截止内
再次读取；每次调用和间隔都受同一截止约束。所有身份、locator、内容及原读回摘要继续严格核验，其他错误
立即沿原分类返回，超时仍为 PrerequisiteUnavailable。新对象、Requested/Staged 及写后读回不进入该等待门，
任何写操作均不因此重试，journal 不修复。S3 监听早于卷注册或返回 500 只证明当次读取不可用，不能据此
认定数据丢失、重新上传或以旧证据放行。失败现场与既有资格保留，新字节须另行实际验证。

联合评审接受：Remote Context 的 HTTP 原始响应、完整 RPC outcome 与最终 Inline observation 各自有界。
ContextRemote 客户端采用既有 RPC metadata 上限；实际 RPC 超限保留已取得响应的摘要并返回永久失败。
Worker 在成功结算前验证整个输出，不能丢弃封装字段、自动转存或让容量失败触发新的网络请求。
RPC 调用开始后的断连、超时或非法结果仅证明尝试过调用，不能声称 HTTP 未发送；使用明确的不确定结果，
不自动重试。既有安全诊断仅保存闭合阶段词与实际证据摘要，原 Query/Job failure 及其摘要仍是持久事实。
结果提交遵循原期限及 CAS；过期或失去租约时仍由原 timeout/recovery owner 收口。内部 outcome/capability
语义提升，外部 HTTP 和引用映射不变；不新增持久字段、业务状态或额度权威。

联合评审接受：已有 durable Model 事件按唯一公共 event type/source kind 登记投影；只为 ModelTurn
补齐类型过滤及控制命令的显式 Run 关联，不增加全 aggregate 的隐式映射。Run/Node 的直接终态和
收敛终态均读取实际拥有的 `terminal_state`，不兼容另一个字段。事件、Run public sequence、outbox、
领域事实、额度和 Receipt 保留同一事务；序号递增重入既有 Run 行锁，不反向从 Job 获取 Run 锁。
公开页仅重新构造安全身份与版本投影，不披露 Model payload 或正文；当前权限、容量和历史截断规则不变。
CLI watch 先读 Run 再读事件页，已观察终态后仍排空后续有界页；Run 终态事件不必是该事务的最后一个序号。
本次拥有路径禁止在已终态 Run 下再产生新的模型执行事件，因此不需要新的终态 watermark 或并行状态。
修复只影响以后正常提交及读取，不回填、重排或重放旧 N 事件，也不重做已成功的厂商请求。
范围与待补证据见[联合评审](../specs/unified-installation/model-public-event-review.md)。

完整目标及证据矩阵见临时 spec。各子边界在 owning type、process/public wire、当前 schema 与
安全/恢复证据要求一起评审后实施；实现后更新 current docs。单机与本地 Kubernetes 的通过证据
不替代生产隔离、恢复、容量或多架构资格。schema 使用当前快照及 fresh provisioning，删除仅限
明确拥有的本轮测试资源。
