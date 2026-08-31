# ADR-0003：产品化 CLI 与本地 profile

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-207 |
| 日期 | 2026-08-31 |
| 影响阶段 | Productization、Agent Product Experience |

## 决策

新增 workspace crate `crates/insight-cli/`，发布单一 `insight` binary。它是用户操作 target Platform 的
客户端和本地进程 supervisor，不是新的 Gateway、Scheduler、repository 或 Sandbox。

首批命令固定为 `doctor`、`init`、`dev`、`status`、`logs`、`stop`；M2 增加 `apply`、`run`、`watch`、
`task`、`artifact` 和 `operation`。所有业务 mutation 通过 public `/v1`，不允许 CLI 直连数据库、内部
gRPC 或生成特权身份 header。

`dev` 采用 Docker Compose v2 作为 macOS/Linux 的首批受支持依赖，并只编排现有独立 role。Agent
产品体验阶段对未发布的`base/full`候选做clean cut，默认profile改为`starter`，可追加closed feature：

- `starter` 为 deterministic first Run 所需的最小真实多进程 closure；
- `starter` 也包括 Runtime Gateway 和 Orchestration 启动时强制连接的 Artifact Gateway、Artifact Data Worker 与
  HTTPS S3/KMS-compatible local dependency；这是实际 process closure，不是把 Artifact 语义塞进 Gateway 的例外；
- `model | remote-capability | context | mcp | wasi`分别追加已存在的exact role、identity、config、dependency与image；
- `all`是上述feature的规范排序并集，不是单独的宽松profile；
- `qualification` 只运行 runsc/Kubernetes preflight，不能声明 gVisor 实测通过。

不保留`base/full`别名。unknown、duplicate或不满足依赖的feature必须在pull、build、provision和进程启动前失败。
`insight dev`默认消费同一release的签名`ReleaseBundle`和exact image digest；贡献者必须显式使用`--from-source`才从
checkout构建。离线模式只复用已验证的exact artifact，不回退mutable tag或隐式源码build。

`init` 生成 project-local、gitignored 的 non-production state 和 digest 固定配置。schema provision 是可见的
one-shot step：既有 `platform-schema provision` 只接受 fresh PostgreSQL target，在一个 transaction 内安装唯一
checked-in baseline，重复调用会 fail closed。运行时进程继续只验证 schema，绝不持有 DDL 权限。源码、lockfile 与
profile digest 未变化时，`dev` 复用已验证的本地artifact，不能无条件重新build release workspace。

## 后果

用户获得单一入口，而 durable authority 仍在 PostgreSQL 和现有 workers。Docker Compose 仅用于开发；生产
部署仍由 Kubernetes/GitOps 管理。缺少 Docker Compose、端口、磁盘、schema、OIDC 或 role readiness 必须由
`doctor/status` 指出具体原因。单节点profile始终报告non-production及L4～L6 Not run；`reset`是单独、显式确认的
破坏性命令，任何启动失败都不得偷偷删除project-local authority。
