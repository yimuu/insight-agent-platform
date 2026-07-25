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

## 启动前 Schema provisioning

Durable Schema 的唯一权威资产是：

- [`database/durable/postgres/schema.sql`](../../database/durable/postgres/schema.sql)；
- [`database/durable/sqlite/schema.sql`](../../database/durable/sqlite/schema.sql)。

两份文件都只面向新的空目标，并在一个显式事务中创建完整结构；contract metadata 是最后一个安装
步骤。业务服务不会创建数据库文件、表、索引、函数或触发器，也不会升级或修复部分 Schema。

SQLite Quickstart 的新目标必须先执行：

```bash
bash scripts/provision-sqlite-schema.sh
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

指定其他文件时，把路径作为第一个参数。脚本拒绝覆盖已存在的目标：

```bash
bash scripts/provision-sqlite-schema.sh /absolute/path/runtime.sqlite3
```

Provisioning 对每个新目标只执行一次；普通进程重启直接使用已通过当前 contract 校验的数据库，
不得重复执行 Schema 文件。

PostgreSQL 必须由 DDL-capable provisioner 角色在服务启动前执行：

```bash
SCHEMA_PROVISIONER_POSTGRES_URL='postgres://schema_owner:...@host/database' \
  bash scripts/provision-postgres-schema.sh

RUN_HISTORY_POSTGRES_URL='postgres://runtime:...@host/database' \
  PLATFORM_CONFIG=config/platform.yaml cargo run
```

URL 可以通过 PostgreSQL `options` 指定一个新的空 `search_path`。本仓库的
`docker-compose.postgres.yml` 将 Schema 挂载到官方镜像的初始化目录，因此新 volume 会在
PostgreSQL healthcheck 通过前完成 provisioning；它只是本地开发样例，不替代生产权限设计。
镜像初始化目录只对新 volume 生效；本次 pre-1.0 clean cutover 不接纳旧 volume，已有开发 volume
必须先明确丢弃并重新创建，不能直接交给新服务启动。

服务启动会在 HTTP bind、scheduler 和 worker 启动前只读校验
`durable_schema_contract` 的 contract ID 与 backend。metadata 缺失、contract 不匹配或 backend
错误都会使启动 fail-closed；服务不会扫描全部数据库对象，也不会自动修复。

启动错误码分别为 `DATABASE_SCHEMA_NOT_INITIALIZED`、
`DATABASE_SCHEMA_CONTRACT_MISMATCH` 和 `DATABASE_SCHEMA_BACKEND_MISMATCH`。它们只表达部署合同
不成立，不包含连接凭据或数据库路径。

生产模式强制使用 PostgreSQL 和显式的 `artifacts.provider: shared_filesystem`。共享存储必须声明
`namespace`，连接同一数据库的所有 runtime 必须挂载同一个物理目录。首次启动生成的 store marker
会与数据库中的 Artifact-store authority 绑定；identity 不一致时 runtime fail-closed。

`local_filesystem` 不接受 namespace，只允许单进程开发。SQLite 不承诺多进程所有权、HA、lease
fencing 或生产恢复。

## PostgreSQL 权限边界

Schema provisioner 角色需要在目标 database/schema 中创建和拥有表、索引、函数、触发器与约束。
业务服务使用独立 runtime 角色，只授予 durable repository 所需的 `SELECT`、`INSERT`、`UPDATE`、
受控 `DELETE` 和必要函数执行权限；不得授予创建、修改、删除或替换受管数据库对象的权限。

部署顺序必须是“创建空目标 → provisioner 成功 → 授予/确认 runtime 最小权限 → 启动服务 → 只读
contract 校验 → Ready”。不得先启动服务再等待它补齐 Schema。

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
