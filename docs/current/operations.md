# 部署与运维

## 单节点开发环境

本地环境使用 `insight init/dev/start/status/logs/stop/reset` 管理 project-local authority。默认 `starter` 与每个显式
feature 都运行相同 public `/v1` 合同和独立 role；loopback、local OIDC/CA 与单节点部署始终标记 non-production。

预构建 runtime 由 Linux image 提取并在宿主执行，只支持 Linux x86_64/ARM64。macOS 使用仓库 checkout 的显式
`--from-source` 路径；四平台 CLI archive 不等同于四平台预构建 runtime。启动与恢复在写配置前验证目标运行模式，
不支持时仍保留状态查看和清理入口。

- `init` 只创建 local identity/config，不启动服务；
- local identity 与 development bootstrap 只接受当前闭合格式；旧 project state 不自动迁移或启动，备份并移除其 `.insight` 后重新 `init`；
- 首次 `dev` 通过独立建库工具建立当前 schema，服务启动只读核验结构。本机开发环境的部分角色仍共用测试 owner
  凭证，不能据此声明全部运行角色已隔离 DDL 权限；
- 后续 `dev`/`start` 重验 release、schema、tenant/local-identity digest、profile closure digest、全部非零唯一端口、持久 TLS
  identity closure，并从 bounded strict-JSON config 文件重新计算 canonical digest；未知 config 文件、文件漂移、TLS key/certificate
  与 CA/SAN/EKU 不一致或跨 project 复制均 fail closed。`stop/reset` 使用同一内部 closure 与 process binding，但不依赖可能已丢失或损坏的
  runtime config/TLS bytes，因此仍可安全收口已证明 generation 的 Platform roles；
- 本地 process journal 只有 `starting`、`running`、`stopped` 三种状态。启动先持久化空 `starting`，每个 child 在
  PID/generation/精确 readiness endpoint 原子落盘并同步目录前不会 exec；`running` 必须精确包含 selected profile 的完整 role
  closure，`stopped` 必须为空。失败会停止所有已记录进程，只有完整清理后才写 `stopped`；
- process state 绑定 tenant、profile、release 与 source；`dev`/`status`/`stop`/`reset` 在使用已记录 PID 前核对每个进程的唯一
  generation。mismatch PID 永不接收信号，只在同一 exact journal 的其余 owned role 已收口后作为 stale record 丢弃。generation 是误杀防护而不是同 UID 安全边界；macOS 的 POSIX PID
  signal 在检查与发送之间仍有极窄的同 UID PID-reuse residual；
- 同一 project 的 `dev/start/stop/reset --confirm` 全流程由私有、single-link lock file 上的内核 exclusive lock 串行化；进程崩溃会由内核
  释放锁，不依据锁文件中的 PID 猜测或删除 stale owner。`status/logs` 可继续读取原子 journal 并显示 `starting`；
- 同一 release/source 下增加 feature 保留本地身份，在停机后通过有界 journal 重建所选角色的完整配置。
  identity 切换与 feature 增加分开执行，恢复和摘要核验规则见 [CLI 本地 profile](cli.md#本地-profile)；
- `start` 的 feature 与 exact release/source 选择只来自已完整验证的 runtime profile，而不是 project summary；它不会隐式切换 identity，并在
  已证明完整 running closure 时原子修复中断留下的 project summary 漂移；
- `stop` 只停止 Platform role；PostgreSQL、NATS 与 LocalStack dependency 保持 ready，以保留同一份
  database/object/KMS/Secret authority；
- `reset` 先打印 project path 与 Compose volume 范围，再要求 exact project name，删除后不可恢复。

Console 直接使用 Artifact 授予的短时 HTTPS URL 上传对象。本地 bucket 的浏览器跨来源规则由
[CLI 开发上传策略](../../apps/insight-cli/src/local_artifact_cors.rs) 统一管理：首次 provision 安装，后续 `dev/start`
按有界合同严格解码并核验，缺失或漂移时拒绝启动，不自动修补。许可仅覆盖本机临时 Console 端口的上传；对象下载仍经 Gateway，
预签名 URL 和 Artifact 的当前授权、期限、摘要与长度验收继续生效。生产 bucket 应由部署者配置 exact HTTPS Console origin。
实际浏览器的预检、上传和拒绝场景是该路径的验证要求，普通 HTTP 客户端成功不能替代浏览器证据。

默认发行路径从签名 ReleaseBundle 解析 exact runtime image tag@index digest，并把所选 binary closure 提取到
`.insight/runtime/releases/<bundle-digest>/bin`。缓存不完整、image/profile/schema drift 或签名失败均 fail closed；不会回退
到 Cargo。`--offline` 需要 bundle、signature、image 与 binary cache 全部已存在。
`insight update apply` 只原子安装已签名的 exact CLI；随后显式运行同 feature 的 `insight stop && insight dev` 才完成 project-local
release transition，之后另一次 `dev` 才可增加 feature。

`doctor`检查Docker Engine/Compose、固定dependency端口、Docker CPU/memory和本地free disk，不会自动删除用户容器或
volume。`dev/start`启用`sandbox` feature时会另外检查kubectl、Established BatchSandbox CRD、internal-only OpenSandbox服务、固定官方
image digest、containerd节点、Direct/Disabled NetworkPolicy和不存在public Ingress；检查失败不会回退到host execution或其他backend。

指定 release runner 生成 closed `insight.dev.performance-report/v1`，分别记录 cold pull-to-ready、warm start、download
content bytes/time、稳定 5 分钟后的 RSS/CPU、project/volume disk 与 source compilation count。预算为 cold ≤300 秒、warm
≤60 秒、RSS ≤6 GiB、CPU ≤10% 单核等效、disk ≤8 GiB、source compilation=0；只有真实测量通过才能标 Passed。

Compose 名称使用完整租户 UUID，避免相近时间创建的项目共用容器或 volume 名称；固定宿主端口仍限制完整 profile 的并行启动。
验证脚本创建的临时项目属于单次运行，结束时使用 CLI 的 `stop/reset` 清理进程、Compose 容器与 volume，
随后删除该次项目目录。`run-productization-journey.sh --logs-directory <new-path>` 在删除前导出有界日志；CI 自动上传日志及独立的验证报告。
只有显式传入 `--keep-failed-resources` 才保留失败旅程，成功运行始终清理。PostgreSQL 物理测试使用独立临时目录与进程守卫；
排查断言失败时可设置 `INSIGHT_TEST_KEEP_FAILED_RESOURCES=1` 保留该测试目录。未完成的清理会保留所属目录供重试；
日志导出失败即使发生在目录删除之后，也会使验证失败。
Kind 产品旅程在 bootstrap 和 Sandbox 验证成功后清理已消费的 seed 项目，释放依赖端口后才启动新的公开验证项目。
seed 清理失败会阻止后续启动；结束阶段的清理仍校验原目录身份，不会删除替换进来的其他目录。

## 发行与生产

0.x 版本由 GitHub Release 标记为开发预览，不替换稳定版 latest；签名、内容身份与真实资格门槛仍完整执行。
受保护 tag workflow 构建四个平台 CLI archive、runtime/fixed Sandbox runner/Console与official OpenSandbox image闭包、checksum、SPDX SBOM、
SLSA provenance、签名与 canonical ReleaseBundle。资产不可覆盖，修复必须发布新版本。未实际执行的跨架构、push/sign 或
performance 项必须保留 Not run，不能由本机结果推断。
写入公开发行 tag 和 Release 之前，[匿名索引检查](../../tools/release/verify-public-release-images.py) 使用已验证候选的精确镜像身份，
从固定 GHCR 地址取得匿名 token 并核对原始 index 摘要。它不读取账户凭证或 Docker 登录配置，也不更改包权限。
检查证明 exact index 匿名可读，不能替代已有运行资格或宣称完整跨架构 layer 下载；网络失败按安全类别报告。
runtime image 的角色与工具 binary 闭包由发行配置固定。PostgreSQL 的唯一结构来源是
[`schema.sql`](../../crates/adapters/platform-postgres/schema.sql)，`platform-schema provision` 只接受空库并在单事务内建成、核验完整结构；
运行角色通过 `verify_schema` 只读核验结构清单。项目不保留迁移链或旧结构读取器；已有非当前结构必须由部署侧按明确的数据清理范围重新建立。

发行[基础镜像](../../deploy/images/platform.Dockerfile)的构建与运行阶段使用同一受支持的 Debian 代际，并固定多架构摘要。
CLI 会在宿主运行从镜像提取的角色程序，因此更换基础镜像也需要重新验证宿主 ABI、Sandbox 静态入口、文件权限与完整候选；旧镜像资格不自动继承。

候选发布使用实际部署的每份 WorkerManifest 与 process config，并从已验证的 runtime image 提取可执行文件。工具核对真实 binary
字节摘要、固定角色和当前编译的能力目录，再生成签名包内的执行证据；启动时重复核对同一目录和当前 executable。
配置不能自行宣告未安装的解释器能力。构建身份用于每次尝试的来源记录，Program/领域操作的语义身份决定能否执行冻结的工作。

签名发布包同时包含当前 schema 快照、结构清单、schema 合同、实际 `platform-schema` binary 和
[`SchemaExecutableEvidenceV1`](../../crates/deployment/platform-deployment-contracts/src/schema.rs)。这些文件以原始字节 SHA-256 绑定；缺文件、
多文件或字节改变会拒绝校验。发布证据不授予业务进程 DDL 权限。

生产promotion仍由Kubernetes/GitOps持有。Helm role chart位于[`deploy/helm`](../../deploy/helm)；Sandbox chart部署internal
ClusterIP OpenSandbox Server、BatchSandbox Controller和Dispatcher，并固定command、ServiceAccount、NetworkPolicy、workload identity
与image/config digest。开发Profile网络默认Direct，也支持显式Disabled；二者均禁止public ingress、host network/runtime socket和
Platform credential。项目当前处于开发阶段，本机验证与延期的生产资格边界见
[开发阶段验证状态](../qualifications/README.md)。

## 已提交事件投递

独立 Outbox Worker 从 PostgreSQL 领取已提交事件的有界元数据投影，持久 JetStream ACK 后才以当前 lease fence 标记已投递。
Event 正文、Secret 和 Artifact 内容不进入消息。ACK 后进程崩溃会重投，稳定 Event ID 支持传输去重；消费者仍须按 Event ID 幂等处理，
不能从传输 ACK 推断业务完成。lease 过期的 Worker 不能完成新 generation 的投递。

Stream 由部署工具 provision；Worker 只验证已安装的 stream 和自己的窄发布权限。开发环境和 Helm 使用独立数据库角色、mTLS
身份与网络闭包。Stream 满时拒绝新消息，PostgreSQL 中未投递义务继续保留；不兼容记录也计入准入积压，不能自动清理。
发布依赖不可用时 Worker readiness 降级，仍可依赖 PostgreSQL 扫描执行的业务角色独立判断 readiness。
新外部业务入口在同一 PostgreSQL 事务内按签名 HardLimitProfile 的 Outbox 水位检查准入，所有未投递状态均计入压力。
该检查是有界压力门，不提供跨并发事务的严格容量预留。已接受请求的 receipt 重放先返回；完成、控制与清理路径继续工作，帮助解除积压。
运维通过 [Outbox 告警说明](observability.md#insightplatformoutboxincompatibleeventspresent) 定位依赖与合同问题，不直接修改投递状态。

OAuth PKCE 清理进程同时驱动 Pending Task 到期：先使用独立控制配额扫描一个有界全局页，
由数据库时间和 Task 当前状态决定胜者并原子创建 cleanup Job，再释放控制配额领取待清理工作。
坏对象被保留并输出安全诊断，后续页继续；数据库或权限故障使本轮失败。进程重启可重建扫描位置，
不会重新授权过期回调或把未确认的 Secret 删除视为完成。

## 历史保留与公开事件清理

`platform-history-maintenance` 使用独立进程、数据库凭证和 Helm role。部署签名包绑定
[`HistoryMaintenanceConfigV1`](../../crates/deployment/platform-deployment-contracts/src/history.rs) 中的保留策略、批量预算与实际 executable
字节摘要；启动时核对完整配置摘要、固定角色和当前 binary。签名验证由部署系统负责，进程信任这一已验证的配置交付链。
它不领取 Job，因此不构造 WorkerManifest，也不装配 Artifact provider 或持有对象存储凭证。

维护按有限创建区间扫描四类历史：公开 Run Event 的连续前缀、到期 Receipt、非 Run 审计 Event 与已完成投递行，
以及终态 OAuth Task 的完整 PKCE cleanup Job 链。当前
[`HistoryRetentionPolicy`](../../crates/execution/platform-orchestrator/src/history.rs) 同时约束这些窗口；
运行中的工作、未知外部效果、当前 hold、Artifact provenance/source 引用和未完成投递会阻止相关记录退休。
已 Published 且达到独立窗口的 Outbox 行可以先退休，Event 自身被保留不要求永久保留已完成投递行。

Receipt 的承诺窗口取持久化到期时间、完成后的最低保留期，以及实际 owner 的 callback/retry deadline 与 claim 到期时间的最大值。
窗口内重试仍复用原结果，并重新验证当前命令权限。Receipt 实际退休后，同一幂等 key 被当作新命令：重新执行当前授权和业务前置条件，
不会无限保留一个旧结果墓碑。Processing Receipt 即使时间已经过去也不能退休。

公开 Event 只删除符合条件的连续前缀，replay floor 单调前进。OAuth Task 仅在当前 pointer 指向已证明完成的 cleanup、
完整有界 predecessor 链通过身份校验且不存在引用时，与整条 Job 链在同一事务退休；实际授权 binding 和 token Secret 保留。
安全 cleanup 证据写入既有审计 Event，之后同样按审计规则到期，不形成无限归档链。普通 Run、Job、Invocation 和 RunValue 仍是当前详情的 authority，
这项维护不承诺全库行数恒定。

每个候选使用有界事务和数据库超时。锁忙会回滚该候选并推进本轮有限扫描，下一轮重新评估；损坏或被保留的记录不会阻塞后续候选。
依赖失败按轮询间隔重试，重启可以重新扫描。真实增长与清理证据位于
[`history_retention`](../../crates/adapters/platform-postgres/tests/history_retention.rs) 和
[`phase4_mcp_oauth`](../../crates/adapters/platform-postgres/tests/phase4_mcp_oauth.rs)：分别覆盖多轮历史回收与当前 authority 保留，
以及完整 cleanup 链、恢复次数上限、引用和凭证保护。

该角色仅获 [`history-role-grants.sql`](../../crates/adapters/platform-postgres/history-role-grants.sql) 中受限函数的执行权限，
没有业务表的正文读取、直接删除或任意 Run 更新权限。开发 profile 通过独立 `platform-database-role --purpose history` 工具 provision
凭证；生产部署由数据库管理员对预先建立的角色应用同一 owner grants，运行进程不持有 DDL 权限。

## 恢复验证

普通 PostgreSQL 集成测试由 [CI runner](../../tools/ci/run-rust-integration-tests.py) 逐目标运行。依赖实际工作进程、HTTPS
对象存储或 Kubernetes 的测试使用显式资格入口；缺失环境或实际 executable 会失败，不能记为通过。
安装初始化测试使用 `PLATFORM_TEST_INSTALLATION_DATABASE_URL` 指定的独立 current-schema 库，执行前不得有 Principal
或 tenant binding；它不能复用普通业务测试库，也不能依靠测试排序维持初始化前提。
Run kernel 的同事务领取与回滚验证使用 `PLATFORM_TEST_RUN_KERNEL_DATABASE_URL` 指定的独立 current-schema 库，
避免其他测试登记的调度分区改变该夹具的事务快照。
Coordinator 的单 Job 恢复与 Q1 多进程领取分别使用独立测试库，并在 admission 前拒绝已有 Tenant；子进程只连接对应父测试的库。
Q1 刻意保留 leased Job 来验证领取与配额，串行测试或等待租约不能隔离这些事实。夹具不删除旧行，也不缩小生产 scheduler 的领取范围。
[Worker 恢复](../../tools/qualification/qualify-platform-worker-recovery.sh)、
[MCP 恢复](../../tools/qualification/qualify-platform-mcp-recovery.sh) 与
[Artifact S3/KMS](../../tools/qualification/qualify-platform-artifact-provider.sh) 入口只选择各自的精确场景。
进程恢复验证期间必须保持 executable 字节不变，重建后重新生成部署身份并完整重跑对应场景。

## 联合恢复的部署流程

恢复集由 [`RecoveryManifestV1`、`RecoveryVerificationReportV1` 和 `RecoverySetV1`](../../crates/deployment/platform-deployment-contracts/src/recovery.rs)
定义，属于运维产物。它绑定 PostgreSQL 恢复点、当前 schema、exact Artifact 对象 generation、仍需保留的 Secret/KMS 版本、
执行与发行身份，以及实际验证工具的 executable 摘要。文件只能包含安全身份与证据引用，不能包含 Secret value、正文或凭证 URL。

部署负责人按以下顺序收集独立证据；离线工具不代替这些外部操作：

1. 记录数据库实例、timeline、WAL 恢复点和备份摘要，导出该恢复点的对象、密钥与 Package 引用清单。
   对恢复窗口需要的每个对象和密钥建立独立 backup hold，保护期覆盖备份时点至恢复集有效期；不能只完成数据库备份。
2. 关闭新业务准入、领取与外部 effect，隔离旧 writer 和执行环境，撤销旧身份、终止旧数据库 session 并轮换凭证。
   无法证明旧环境已隔离时，不启动可发起外部动作的新进程。
3. 在受限环境恢复匹配的当前 schema 和数据，核验结构、tenant/owner/current pointer、额度与未完成 cleanup。
   按 exact generation 核对对象和密钥可用性；过期、吊销或已删除的 PKCE/token 不因恢复数据库而重新获得授权。
4. 通过原 owning 命令核对恢复点前后可能已发生的外部效果、Outbox 与消费者水位。未知效果保持
   `quarantined`，附上实际隔离证据并列入未恢复范围；不得自动重发或根据数据库缺行删除未知对象。
5. 生成验证报告，核对阶段证据、完整的资产/密钥/hold 映射，再由独立受信的部署签名身份签署恢复集。
   放行前重新验证签名、文件字节和有效期；仅恢复已经安全准入的工作，隔离项继续由正常 reconciliation 处理。

[`platform-recovery.py`](../../tools/release/platform-recovery.py) 使用现有离线 Ed25519 签名机制。受信公钥必须来自独立部署策略，
不能读取待验清单中自带的公钥。所有证据文件以原始字节 SHA-256 命名，必须恰好覆盖报告引用；不接受缺失、多余、篡改或符号链接。
验证器必须是清单指定的实际 `platform-qualification` executable，部署侧同时核对其已批准发行来源。

```bash
cargo build --locked -p insight-platform-contract-tooling --bin platform-qualification
python3 tools/release/platform-recovery.py create \
  --manifest recovery-manifest.json --report recovery-verification-report.json \
  --evidence-directory recovery-evidence --output verified-recovery-set \
  --validator target/debug/platform-qualification --private-key deployment-recovery-key.pem \
  --trusted-public-key-base64="$RECOVERY_TRUSTED_PUBLIC_KEY"
python3 tools/release/platform-recovery.py verify --set verified-recovery-set \
  --validator target/debug/platform-qualification \
  --trusted-public-key-base64="$RECOVERY_TRUSTED_PUBLIC_KEY"
```

成功输出表明签名来源、文件完整性和证据声明一致，明确保留 `external_state_verified_by_tool=false`。
它不执行 PITR、不访问云服务，也不授予管理 API 发行权。签名不能把自填声明变成已观察的外部事实；完整生产恢复仍须
绑定 exact release 和真实环境执行，其资格状态见[开发阶段验证状态](../qualifications/README.md)。

## Kind 本地部署验证

`tools/qualification/bootstrap-platform-kind-local.sh` 创建新的隔离 Kind 环境。输入包括 exact OCI archives、CLI seed 配置以及预构建的 `platform-schema`、`platform-database-role`、`platform-jetstream-provision` 和 `platform-qualification`。生成器读取已验证镜像内的实际 worker bytes，通过 owning catalog 验证本地配置；Sandbox chart 必须收到完整 WorkerManifest，不能依赖默认身份。Management 与 Runtime 共用 Gateway 的 Artifact mTLS Secret，Registry 使用独立受限客户端 Secret。

Artifact maintenance 由 Kind 单独组合配置，复用该环境的 Artifact provider catalog；CLI seed 不包含这个未选择的本地角色。
替换镜像内的 Worker bytes 时，生成器同时重绑定 Model adapter 和 Dataset source 的 manifest 引用，
并校验原 seed 的引用一致性；内容索引与 adapter 语义不会因重新编译而改变。
生成阶段验证其 owning 执行能力与实际 binary 身份，完整字段、限额和 provider readiness 继续由服务启动检查；仅通过 manifest 校验不能声明角色已可运行。

Kind 验证要求 Docker 启用 containerd image store，并支持按平台保存镜像（API 1.48 或以上）。CI 在构建前配置、检查该存储模式；本地 bootstrap 在创建资源前检查。镜像仍按原始 OCI manifest 与 config 摘要核验，再从停止的容器读取字节。

[开发资源输入](../../deploy/kind/workload-resources.json)单独降低 Rust 服务的 CPU 预留，并进入本地部署身份。内存、limits、双副本与安全配置仍来自各 owning chart。完整渲染检查开发调度预算，实际运行是否存在 CPU 饱和、内存压力或恢复问题仍以动态验收为准。

开发初始化在同一事务内建立租户与真实 Scheduling Policy 绑定，初始任务可以直接进入正常领取流程。重复启动只核验当前绑定，保留调度额度与进度；合法的后续策略改绑可继续使用。绑定缺失或漂移会拒绝启动，需要通过拥有域诊断，不能依赖 worker 或启动工具自动修补。

Outbox、History、Security Authority 与 Artifact 的四个 pool 各用已有 owning grants 的独立数据库角色。Artifact 四角色在同一事务中初始化；其他尚无独立 grants 的本地角色仍使用测试 owner，不能据此声明全生产最小权限资格。初始化工具只允许固定本地或固定 Kind loopback 数据库配置，权限来自 PostgreSQL owner 的 grants。JetStream 使用独立初始化证书创建 owning stream，publisher 仅发布安全通知；本地持久卷保留 Pod 重建前的流数据。Kind 的 PVC 不构成生产备份或跨集群恢复承诺。

生成配置、Helm 渲染和权限边界检查可以离线验证；只有真实启动并完成 owning qualification harness 才能记录该 exact revision 的 Kubernetes 动态证据。当前本轮新增 Kind 闭环的动态资格尚未运行，历史 Kind 记录不会自动继承。
