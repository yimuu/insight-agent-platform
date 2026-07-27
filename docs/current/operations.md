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

## Helm 与 Kubernetes

仓库提供 [`deploy/helm/insight-agent-platform`](../../deploy/helm/insight-agent-platform) chart。
默认部署一个 runtime、一个供评估使用的 PostgreSQL 16、ClusterIP Service、持久 Artifact PVC，
并为内置 PostgreSQL 生成内部 CA 和服务证书。runtime 使用 `sslmode=verify-full` 校验数据库 Service
DNS，不会绕过远程 PostgreSQL TLS 合同。

默认 Artifact PVC 是 runtime 重启安全性的必要条件：共享文件系统首次生成的 store marker 会绑定到
PostgreSQL authority；不能用随 Pod 删除的 `emptyDir` 替代。默认 Deployment 使用 `Recreate`，适合
单副本和 `ReadWriteOnce` Artifact PVC。需要多 runtime 时，应改用支持 `ReadWriteMany` 的真实共享
存储、外部 PostgreSQL，并单独验证发布策略和容量。

内置 PostgreSQL 默认面向本地评估，不替代生产数据库的持久化、备份、HA、证书和最小权限设计。
有限资源部署和 k6 生命周期压测见
[`bench/k8s/README.md`](../../bench/k8s/README.md)。

容量资格使用三个显式 overlay，均应叠加在 `values-benchmark.yaml` 之后：

- `values-benchmark-limited.yaml`：runtime/PostgreSQL 各 `500m / 256Mi`，50 active Run、4 permits；
- `values-benchmark-c1.yaml`：runtime `2 CPU / 1Gi`，PostgreSQL `4 CPU / 8Gi`，12 permits；
- `values-benchmark-c2.yaml`：runtime `4 CPU / 2Gi`，PostgreSQL `8 CPU / 16Gi`，16 permits。

C1/C2 是短 Run burst 的资格档位，不是所有 LLM、retrieval 或第三方 API workload 的资源保证。
默认值只有在容量矩阵通过后才能据此调整，不能用 values 中的数字替代压测结论。

`history.max_connections` 是单个 runtime 进程共享的 PostgreSQL pool 上限；durable transition、
readiness 和 LISTEN consumer 都从该有界 pool 获取连接。它必须至少为 4，且应高于 operation
permit 数并为监听与控制查询留出余量。Helm 用
`runtime.databasePoolMaxConnections` 生成该配置：limited/C1/C2 分别使用 6/24/32。盲目增大
pool 会增加 PostgreSQL backend 私有内存和瞬时竞争，不等价于提高吞吐。

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
- `/metrics` 以 Prometheus text format 暴露 bounded-label 的 active、executing、admission、
  coordinator wakeup/poll、claim latency、notification listener，以及跨进程 hint
  `requested/published/error` 指标；`published/requested` 的差值体现进程级合并效果；
- work notification listener 断开不会单独使 readiness 失败，安全轮询继续保证最终发现；listener
  状态会在 metrics 中降级；
- Kubernetes 资格脚本另外保存 PostgreSQL queue oldest age、进程 RSS/PSS、cgroup memory、
  lock waiter、top SQL 的 temp/WAL blocks 和一致性抽样；cgroup page cache 上升不能单独等价为
  进程 RSS 泄漏；
- `runtime.public_event_retention` 只清理已发布的非终态 Public Event；
- terminal Public Event 和 durable response snapshot 不受该策略影响；
- `shutdown_grace_period` 与 `shutdown_hard_deadline` 控制停止 admission、drain 与最终退出边界。

生产语义与故障边界见[架构概览](architecture.md)，具体配置以严格配置 schema 和启动校验为准。
