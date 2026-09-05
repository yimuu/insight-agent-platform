# 部署与运维

## 单节点开发环境

本地环境使用 `insight init/dev/start/status/logs/stop/reset` 管理 project-local authority。默认 `starter` 与每个显式
feature 都运行相同 public `/v1` 合同和独立 role；loopback、local OIDC/CA 与单节点部署始终标记 non-production。

- `init` 只创建 local identity/config，不启动服务；
- local identity 与 development bootstrap 只接受当前闭合格式；旧 project state 不自动迁移或启动，备份并移除其 `.insight` 后重新 `init`；
- 首次 `dev` provision fresh schema，运行角色无 DDL 权限；
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
- 同一 release/source 下增加 feature 只追加 exact config/identity/role。切换到新的已验证 release/source 时第一次 `dev` 必须保持
  persisted feature 集合完全相同；identity 切换和 feature 增加不能在同一次操作中完成；
- `start` 的 feature 与 exact release/source 选择只来自已完整验证的 runtime profile，而不是 project summary；它不会隐式切换 identity，并在
  已证明完整 running closure 时原子修复中断留下的 project summary 漂移；
- `stop` 只停止 Platform role；PostgreSQL、NATS 与 LocalStack dependency 保持 ready，以保留同一份
  database/object/KMS/Secret authority；
- `reset` 先打印 project path 与 Compose volume 范围，再要求 exact project name，删除后不可恢复。

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

## 发行与生产

受保护 tag workflow 构建四个平台 CLI archive、runtime/fixed Sandbox runner/Console与official OpenSandbox image闭包、checksum、SPDX SBOM、
SLSA provenance、签名与 canonical ReleaseBundle。资产不可覆盖，修复必须发布新版本。未实际执行的跨架构、push/sign 或
performance 项必须保留 Not run，不能由本机结果推断。
runtime image 只包含当前 Platform role binary；PostgreSQL 基线由 `platform-postgres` migration authority 提供，镜像不携带第二套
旧 schema、SQLite 业务后端或单进程 runtime。

生产promotion仍由Kubernetes/GitOps持有。Helm role chart位于[`deploy/helm`](../../deploy/helm/)；Sandbox chart部署internal
ClusterIP OpenSandbox Server、BatchSandbox Controller和Dispatcher，并固定command、ServiceAccount、NetworkPolicy、workload identity
与image/config digest。开发Profile网络默认Direct，也支持显式Disabled；二者均禁止public ingress、host network/runtime socket和
Platform credential。项目当前处于开发阶段，本机验证与延期的生产资格边界见
[开发阶段验证状态](../qualifications/README.md)。
