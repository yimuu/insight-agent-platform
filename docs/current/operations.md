# 部署与运维

## 单节点开发环境

本地环境使用 `insight init/dev/start/status/logs/stop/reset` 管理 project-local authority。默认 `starter` 与每个显式
feature 都运行相同 public `/v1` 合同和独立 role；loopback、local OIDC/CA 与单节点部署始终标记 non-production。

- `init` 只创建 local identity/config，不启动服务；
- 首次 `dev` provision fresh schema，运行角色无 DDL 权限；
- 后续 `dev`/`start` 重验 release、schema、profile digest 与现有 dependency；
- 增加 feature 只追加 exact config/identity/role；
- `stop` 停止 Platform 与 dependency，保留 PostgreSQL/object/project data；
- `reset` 先打印 project path 与 Compose volume 范围，再要求 exact project name，删除后不可恢复。

默认发行路径从签名 ReleaseBundle 解析 exact runtime image tag@index digest，并把所选 binary closure 提取到
`.insight/runtime/releases/<bundle-digest>/bin`。缓存不完整、image/profile/schema drift 或签名失败均 fail closed；不会回退
到 Cargo。`--offline` 需要 bundle、signature、image 与 binary cache 全部已存在。

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

生产promotion仍由Kubernetes/GitOps持有。Helm role chart位于[`deploy/helm`](../../deploy/helm/)；Sandbox chart部署internal
ClusterIP OpenSandbox Server、BatchSandbox Controller和Dispatcher，并固定command、ServiceAccount、NetworkPolicy、workload identity
与image/config digest。开发Profile网络默认Direct，也支持显式Disabled；二者均禁止public ingress、host network/runtime socket和
Platform credential。项目当前处于开发阶段，本机验证与延期的生产资格边界见
[开发阶段验证状态](../qualifications/README.md)。
