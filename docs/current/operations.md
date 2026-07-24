# 部署与运维

状态：Current

正式二进制通过 `PLATFORM_CONFIG` 读取平台配置；未设置时默认使用 `config/platform.yaml`。配置中的
相对路径以配置文件所在目录为基准。

## 部署模式

| 模式 | Repository | Artifact store | 适用范围 |
|---|---|---|---|
| `single_process_development` | SQLite | `local_filesystem` | Quickstart、单进程开发 |
| `production` | PostgreSQL 16 | `shared_filesystem` | 多 runtime、恢复与生产部署 |

Quickstart 使用 [`config/platform.quickstart.yaml`](../../config/platform.quickstart.yaml)，只启用
`action_demo`。生产样例位于 [`config/platform.yaml`](../../config/platform.yaml)。对外暴露服务前，
必须按部署要求配置认证、数据库凭据、模型凭据和共享 Artifact 挂载。

生产模式强制使用 PostgreSQL 和显式的 `artifacts.provider: shared_filesystem`。共享存储必须声明
`namespace`，连接同一数据库的所有 runtime 必须挂载同一个物理目录。首次启动生成的 store marker
会与数据库中的 Artifact-store authority 绑定；identity 不一致时 runtime fail-closed。

`local_filesystem` 不接受 namespace，只允许单进程开发。SQLite 不承诺多进程所有权、HA、lease
fencing 或生产恢复。

## PostgreSQL 迁移

进程连接 PostgreSQL 后会在创建 RunService 或读取运行表之前自动执行
[`migrations/durable/postgres/`](../../migrations/durable/postgres) 中的前向 migration manifest。
并发实例通过固定的事务级 advisory lock 串行化。

`schema_migrations` 记录 version、文件名、SQL SHA-256 和应用时间。已应用记录必须是当前
manifest 的精确前缀；版本空洞、未知或更高版本、文件名/checksum 漂移，以及已有受管表但没有
ledger，都会使进程 fail-closed。每个 migration SQL 与 ledger 写入处于同一事务。

数据库角色需要目标 database/schema 的连接和使用权限、创建对象权限、后续 migration 对既有对象的
owner/ALTER 权限，以及 migration ledger 的读写权限。

## Artifact 生命周期

小值按 `inline_threshold_bytes` 内联保存；大值写入内容寻址 Artifact store，并在结果事务中提交引用。
读取接口还会检查 Run 归属、公开终包引用、`max_read_bytes` 和 retention。`orphan_retention`、
`reference_retention`、`gc_interval` 与 `deletion_claim_seconds` 控制回收，不应在多个 runtime 间配置
不一致的物理 store。

## 认证

`auth.mode: bearer_env` 从环境变量加载平台管理凭据和独立的人工任务身份，配置文件只保存环境变量名：

```yaml
auth:
  mode: bearer_env
  token_env: PLATFORM_ADMIN_TOKEN
  human_task_credentials:
    - identity: alice
      groups: [medical-reviewers, triage]
      token_env: HUMAN_ALICE_TOKEN
```

缺失或空环境变量、重复 identity、重复 token（包括与平台 token 重复）会阻止启动。人工身份不能管理
Run，平台管理身份也不会自动获得人工任务权限。未配置人工凭据时，HumanTask 路由保持 fail-closed。
token 不应进入配置明文、Debug、错误或日志。

## 就绪、保留与关闭

- `/health/live` 只表示进程存活；
- `/health/ready` 检查 repository 与 runtime admission readiness；
- `runtime.public_event_retention` 只清理已发布的非终态 Public Event；
- terminal Public Event 和 durable response snapshot 不受该策略影响；
- `shutdown_grace_period` 与 `shutdown_hard_deadline` 控制停止 admission、drain 与最终退出边界。

生产语义与故障边界见[架构概览](architecture.md)，具体配置以严格配置 schema 和启动校验为准。
