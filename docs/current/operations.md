# 部署与运维

状态：Current

正式二进制通过 `PLATFORM_CONFIG` 读取平台配置；未设置时默认使用 `config/platform.yaml`。配置中的
相对路径以配置文件所在目录为基准。

## 部署模式

| 模式 | Repository | Artifact store | 适用范围 |
|---|---|---|---|
| `single_process_development` | SQLite | `local_filesystem` | Quickstart、单进程开发 |
| `production` | PostgreSQL 16 | `shared_filesystem` | `full` 多 runtime/恢复，或 terminal-only 单 owner 生产部署 |

Quickstart 使用 [`config/platform.quickstart.yaml`](../../config/platform.quickstart.yaml)，只启用
`action_demo`。生产样例位于 [`config/platform.yaml`](../../config/platform.yaml)。对外暴露服务前，
必须按部署要求配置认证、数据库凭据、模型凭据和共享 Artifact 挂载。

两份样例都显式关闭 MCP。启用 MCP Client 管理面、`/mcp` Server、OAuth/PKCE keyring、stdio
isolation、Tasks maintenance 和 readiness 的配置及 rollout 顺序见
[MCP 使用、运行与安全合同](mcp.md)。Helm chart 只生成 `mcp.version: 2` 全局策略和 MCP Server
exports；Client Server 实例通过 durable `/v1/admin/mcp/**` 管理，不进入 ConfigMap。Operator token、
远端 credential reference 对应值和 stdio secret env 通过 `mcp.secretEnv[]` 从现有 Kubernetes Secret
注入，MCP ciphertext keyring 通过 `mcp.secretEncryption.existingSecret` 注入。

MCP discovery worker 使用 durable claim/lease，running operation 的取消请求会在 100ms 轮询边界内
传播到 transport cancellation。失败和取消且超过 30 天的未引用 operation 每 60 秒按最多 1000 条
清理；成功 snapshot、Validation、Revision 以及被发布对象引用的数据不进入该清理。每个有效批次在
同一事务写 body-free audit 和 bounded outbox evidence。`list_changed` 只把成功候选快照标记为 stale，
不会改写 Draft、Revision、Deployment 或 Run。

## Provider Catalog 与模型配置

平台随版本发布只读的
[`catalog/provider-catalog.yaml`](../../catalog/provider-catalog.yaml)，集中维护 Provider route、
endpoint、adapter、默认凭据环境变量名和最小模型事实。Agent 直接使用
`{provider, id}` selector；没有独立的模型别名列表、`models.yaml` 或模型级 `enabled`。
Catalog 不保存 thinking、stream、tools、temperature 等调用选择。

当前内置路由如下：

| Provider route | Endpoint | 默认凭据 |
|---|---|---|
| `dashscope-cn` | `https://dashscope.aliyuncs.com/compatible-mode/v1` | `DASHSCOPE_API_KEY` |
| `dashscope-intl` | `https://dashscope-intl.aliyuncs.com/compatible-mode/v1` | `DASHSCOPE_API_KEY` |

区域是 Provider 身份的一部分。Agent 必须显式选择 route；请求失败时不会从 `cn` 自动切换到
`intl`。单 endpoint Provider 不需要额外 endpoint selector。只在发布已启用 Agent 且实际解析其
模型引用时检查对应 secret，所以 Action-only 部署无需模型凭据。缺失和空 secret 分别 fail-closed，
日志与 Deployment Revision identity 只包含环境变量名，不包含值。

不同账户或 Catalog 尚未收录的新模型可以继承内置 route：

```yaml
providers:
  dashscope-cn-team-a:
    extends: dashscope-cn
    credential:
      env: TEAM_A_DASHSCOPE_API_KEY
    models:
      qwen-new-model:
        input: [text]
```

私有 OpenAI-compatible endpoint 使用显式自定义 route：

```yaml
providers:
  company-llm:
    type: open_ai_compatible
    endpoint: https://llm.company.internal/v1
    credential:
      type: bearer
      env: COMPANY_LLM_API_KEY
    models:
      vendor/internal-chat/v1:
        input: [text, image]
```

自定义 route 必须声明 endpoint、bearer secret reference 和至少一个模型；模型能力是 operator
assertion，并与扩展配置 digest 一起进入 Deployment Revision。扩展不能覆盖内置 route。需要组织
级收窄时，另用可选治理策略：

```yaml
model_policy:
  allow:
    - provider: company-llm
      id: vendor/internal-chat/v1
```

`model_policy` 只限制可用集合，不注册模型，也没有隐式默认列表。Provider/model、endpoint identity、
adapter/version、Catalog/extension digest、非秘密 transport 与能力事实都会冻结到 Deployment
Revision；secret 值不会进入 Plan、revision 或日志。

## Run persistence policy

每个 Deployment Revision 冻结一个 `execution.persistence_mode`：

```yaml
execution:
  persistence_mode: terminal_only # full | terminal_only
```

Run 创建请求不能覆盖该值。作者未声明时使用 `runtime.default_persistence_mode`；当前 rollout
默认值明确保持 `full`，`terminal_only` 只允许兼容的 immutable Deployment Revision 显式
opt-in。容量资格只是修改默认值的前置证据，不会自动授权切换；2026-07-28 的 Phase 0、
Gate A～D 与完成定义 1～12 已全部通过，整体状态为 Qualified，正式结果见
[Terminal-only 验收与 WAL 资格归档](../archive/qualifications/2026-07-28-terminal-only-qualification.md)。
独立 rollout 决议仍保留
`full` 默认值，未来切换需要新的显式评审。`full` 使用既有 event、
checkpoint、lease/fence 和恢复路径。`terminal_only` 只持久化 admission 与 terminal result，
执行中间状态只在 owner 进程内存在；进程或数据库重启会令未完成 Run 变成 `interrupted`，不会自动
恢复、接管或重试。

平台配置的默认值与边界如下：

```yaml
runtime:
  default_persistence_mode: full
  terminal_only:
    enabled: true
    owner_lease_seconds: 30
    owner_heartbeat_seconds: 10
    terminal_commit_retry_seconds: 10
    run_retention_days: 30
    allow_volatile_waits: false
    max_concurrent_runs: 50

conversations:
  enabled: true
  inline_content_max_bytes: 8192
  message_page_size_default: 50
  message_page_size_max: 200
  summary_trigger_messages: 30
  summary_trigger_tokens: 24000
  recent_context_messages: 20
  retention_days: 90
```

配置反序列化严格拒绝未知字段和未知 persistence mode。owner heartbeat 必须不大于 lease 的三分之一；
Conversation inline/page/summary/context/retention 数值均有上下界，默认 page size 不能超过最大值。
关闭 `runtime.terminal_only.enabled` 会使 terminal-only Deployment publication fail-closed，并保留
既有 full 行为。

运行时采用以下闭区间：owner lease `3..=300s`、heartbeat `1..=100s`、terminal commit retry
`1..=300s`、terminal-only concurrency `1..=10000`；Conversation inline content
`256..=1048576 bytes`、page maximum `1..=200`、summary messages `2..=10000`、summary tokens
`256..=1000000`、recent context `1..=200`、retention `1..=3650 days`。recent context 不能超过
summary message threshold，page default 不能超过 page maximum。

新增字段也可用严格的双下划线环境层级覆盖，优先级固定为 `env > YAML > built-in default`：

| YAML 字段 | 环境变量 |
|---|---|
| `runtime.default_persistence_mode` | `INSIGHT_RUNTIME__DEFAULT_PERSISTENCE_MODE` |
| `runtime.terminal_only.enabled` | `INSIGHT_RUNTIME__TERMINAL_ONLY__ENABLED` |
| `runtime.terminal_only.owner_lease_seconds` | `INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_LEASE_SECONDS` |
| `runtime.terminal_only.owner_heartbeat_seconds` | `INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_HEARTBEAT_SECONDS` |
| `runtime.terminal_only.terminal_commit_retry_seconds` | `INSIGHT_RUNTIME__TERMINAL_ONLY__TERMINAL_COMMIT_RETRY_SECONDS` |
| `runtime.terminal_only.run_retention_days` | `INSIGHT_RUNTIME__TERMINAL_ONLY__RUN_RETENTION_DAYS` |
| `runtime.terminal_only.allow_volatile_waits` | `INSIGHT_RUNTIME__TERMINAL_ONLY__ALLOW_VOLATILE_WAITS` |
| `runtime.terminal_only.max_concurrent_runs` | `INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS` |
| `conversations.enabled` | `INSIGHT_CONVERSATIONS__ENABLED` |
| `conversations.inline_content_max_bytes` | `INSIGHT_CONVERSATIONS__INLINE_CONTENT_MAX_BYTES` |
| `conversations.message_page_size_default` | `INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_DEFAULT` |
| `conversations.message_page_size_max` | `INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_MAX` |
| `conversations.summary_trigger_messages` | `INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_MESSAGES` |
| `conversations.summary_trigger_tokens` | `INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_TOKENS` |
| `conversations.recent_context_messages` | `INSIGHT_CONVERSATIONS__RECENT_CONTEXT_MESSAGES` |
| `conversations.retention_days` | `INSIGHT_CONVERSATIONS__RETENTION_DAYS` |

布尔环境值只接受小写 `true|false`，persistence mode 只接受 `full|terminal_only`，数值只接受无符号
十进制整数；环境覆盖后仍执行与 YAML 相同的关系和范围校验。错误信息不会回显环境值。

terminal-only v1 只允许一个 runtime owner replica。API 副本可以读取最终结果和 Conversation，但
active Run 的创建、取消和查询必须路由到唯一 owner。owner lease 是实例级提示，不是逐 Run heartbeat，
也不是恢复租约。lease 续约失败时进程必须停止 admission；过期 owner 不能再提交 terminal result。

发布时的静态兼容性校验拒绝 human task、external signal wait、durable child subflow 和要求 durable
effect fence 的 provider。`allow_volatile_waits: true` 在当前版本只放行静态且不超过
owner/shutdown budget 的短 timer；external signal 与 human task 仍不兼容。fork/join 等纯内存控制流
可以使用 terminal-only。pause/resume、redrive、recovery fork、migration 和 continue-as-new 仍只属于
`full`。

## Helm 与 Kubernetes

仓库提供 [`deploy/helm/insight-agent-platform`](../../deploy/helm/insight-agent-platform) chart。
默认部署一个 runtime、一个供评估使用的 PostgreSQL 16、ClusterIP Service、持久 Artifact PVC，
并为内置 PostgreSQL 生成内部 CA 和服务证书。runtime 使用 `sslmode=verify-full` 校验数据库 Service
DNS，不会绕过远程 PostgreSQL TLS 合同。

默认 Artifact PVC 是 runtime 重启安全性的必要条件：共享文件系统首次生成的 store marker 会绑定到
PostgreSQL authority；不能用随 Pod 删除的 `emptyDir` 替代。默认 Deployment 使用 `Recreate`，适合
单副本和 `ReadWriteOnce` Artifact PVC。需要 `full` 多 runtime 时，必须关闭 terminal-only feature，
改用支持 `ReadWriteMany` 的真实共享存储、外部 PostgreSQL，并单独验证发布策略和容量；
terminal-only v1 仍限定单 owner replica。

内置 PostgreSQL 默认面向本地评估，不替代生产数据库的持久化、备份、HA、证书和最小权限设计。
有限资源部署和 k6 生命周期压测见
[`bench/k8s/README.md`](../../bench/k8s/README.md)。

容量资格使用三个显式 overlay，均应叠加在 `values-benchmark.yaml` 之后：

- `values-benchmark-limited.yaml`：runtime/PostgreSQL 各 `500m / 256Mi`，50 active Run、4 permits；
- `values-benchmark-c1.yaml`：runtime `2 CPU / 1Gi`，PostgreSQL `4 CPU / 8Gi`，12 permits；
- `values-benchmark-c2.yaml`：runtime `4 CPU / 2Gi`，PostgreSQL `8 CPU / 16Gi`，16 permits。

Terminal-only Gate A～D 还必须最后叠加
`values-terminal-only-qualification.yaml`。该 overlay 显式选择 terminal-only、允许受限的短
volatile timer，并启用 effect ledger、故障与流式 fixture；普通 chart 默认禁用这些资格能力。
普通 chart 不生成占位模型配置；qualification overlay 只为两个 LLM fixture 显式生成
`providers.fixture` 自定义 route。
非零 `runtime.qualificationFaults.*` 在 `qualification.enabled=false` 时会被 Helm 拒绝。
可用的限定故障包括 admission、terminal post-commit 和 Conversation summary 三个最多 30 秒的
延迟；summary 延迟用于在 Gate C 中于 terminal commit 后、summary 发布前终止进程，验证重启后
下一轮 admission 会非阻塞地补偿生成 summary。所有延迟默认均为 `0`，且运行时还要求
`INSIGHT_QUALIFICATION_ENABLED=true`，避免绕过 Helm 直接启用资格故障。
同一开关还会注册仅容器内可触发的 SIGUSR2 self-abort handler，供故障 Gate 让 runtime PID 1
自行 hard-abort；它没有 HTTP 端点，普通生产配置不会注册。harness 必须先以 container ID、
restart count、预绑定的 PID/start-time incarnation、预挂 Pod status watch、非 OOM terminated
cause 和 SIGABRT/容器 PID 1 abort-trap exit 证据证明原进程死亡。`QUALIFICATION_SELF_ABORT`
marker 可以来自预挂的 exact-container live log 或重启后的 exact previous-container log，但两者
聚合后必须恰好出现一次，再删除 Pod 获取新 UID；正式 pass5 为 live `1`、previous `0`。
PostgreSQL SIGQUIT 场景还必须聚合 live/previous immediate-shutdown 线索并证明 postmaster
start time 已变化。组合 Gate 和 summary crash 在退出时会复原三个 delay，并同时核对 Helm values
与唯一 Ready runtime Pod 的对应环境变量全部为 `0`；reset 或该核对失败都会令 harness 最终失败。

C1/C2 是短 Run burst 的资格档位，不是所有 LLM、retrieval 或第三方 API workload 的资源保证。
容量矩阵通过也不会自动修改默认 persistence mode；当前默认仍为 `full`，不能用 values 中的数字
替代压测结论或独立 rollout 决策。

Helm 的 `runtime.defaultPersistenceMode`、`runtime.terminalOnly.*` 与 `conversations.*` 会生成上述
严格平台字段；chart 默认仍为 `full`。只要 terminal-only feature enabled，chart 就拒绝
`replicaCount != 1`；渲染时也会执行与平台相同的数值边界和 heartbeat/lease 关系校验。因此 Agent
自身显式选择 terminal-only 时仍保持单 owner replica。chart 选择只渲染一份权威
`platform.yaml`，不会同时注入同义环境覆盖；需要环境覆盖的外部部署必须使用上表中的精确名称。

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

Conversation、summary 与 terminal input/output 的大对象使用带 `tenant:<id>:<scope>` 的封闭
envelope。可选的 tenant encryption 在写入文件系统前，以 Secret keyring 中的 active master key
和不可逆 tenant-prefixed scope digest 通过 HKDF-SHA256 派生独立 AES-256-GCM subkey；数据库中的 content hash/size
仍对应解密后的 canonical envelope。密文头只包含格式、key version、scope digest 和随机 nonce，
不保存明文 tenant ID。AEAD 同时绑定 content hash、size 和 media type，头或密文被修改时读取
fail-closed。

配置文件只保存 Secret 环境变量名：

```yaml
artifacts:
  tenant_encryption:
    active_key_version: "2026-07"
    keyring_env: INSIGHT_ARTIFACT_TENANT_KEYRING
```

环境变量是严格 JSON object；value 为 32-byte master key 的 64 位小写十六进制：

```text
{"2026-06":"<64 lowercase hex>","2026-07":"<64 lowercase hex>"}
```

轮换时先把旧、新 key 同时部署到所有 runtime 并把 active version 切到新 key；旧密文按头中的
version 继续读取，新写入及被再次写入的旧对象使用 active key。确认旧对象已迁移/到期删除后才能
移除旧 key。已加密对象缺少对应历史 key 时拒绝读取，不能退回明文。启用前已存在的明文对象可读，
在下一次幂等写入时原子迁移；要求立即全量加密的部署应在启用后执行离线迁移审计。privacy/retention
删除的是同一受 repository fence 管理的密文对象。

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
- `/health/ready` 检查 repository、runtime admission，以及被已启用 Agent 实际引用的 MCP binding。
  Service-account/legacy binding 运行真实协议探针；`oauth_user` 验证 protected-resource、issuer、
  PKCE 与 scope metadata。未被引用的 optional MCP Server 不阻断 readiness；
- `/metrics` 以 Prometheus text format 暴露 bounded-label 的 active、executing、admission、
  coordinator wakeup/poll、claim latency、notification listener，以及跨进程 hint
  `requested/published/error` 指标；`published/requested` 的差值体现进程级合并效果。Conversation
  指标固定使用 `persistence_mode="full|terminal_only"` 区分路径，包括
  `conversation_messages_total{role,persistence_mode}`、
  `conversation_context_messages{persistence_mode}`、
  `conversation_context_tokens{persistence_mode}` 和
  `conversation_summary_jobs_{active,total}`；功能关闭时 `full` 样本仍以零值存在，且所有 label
  都不得包含 Run、Conversation、tenant 或 user 标识；
- MCP 指标覆盖 discovery/list/call/read/get/completion 的计数与 duration、transport event、
  subscription、interaction、remote task、OAuth、stdio restart、cache、body/frame rejection 与
  stale publication candidate；管理面额外暴露 bounded route/result 请求计数与延迟、
  validation/discovery/publish/activate/disable/retire lifecycle、候选/导入/拒绝 catalog histogram，
  以及 pending/running/oldest discovery 和 active/disabled/stale Server gauge；label 只允许闭合 route、
  operation、kind、state、primitive、transport 和结果类，不含 tool、URI、Prompt、task、Run、tenant
  或 user 标识；
- work notification listener 断开不会单独使 readiness 失败，安全轮询继续保证最终发现；listener
  状态会在 metrics 中降级；
- Kubernetes 资格脚本另外保存 PostgreSQL queue oldest age、进程 RSS/PSS、cgroup memory、
  lock waiter、top SQL 的 temp/WAL blocks 和一致性抽样；cgroup page cache 上升不能单独等价为
  进程 RSS 泄漏；
- `runtime.public_event_retention` 只清理已发布的非终态 Public Event；
- terminal Public Event 和 durable Run snapshot 不受该策略影响；
- `shutdown_grace_period` 与 `shutdown_hard_deadline` 控制停止 admission、drain 与最终退出边界。
  管理面关闭顺序是：先拒绝新 mutation 和 discovery claim，再取消 worker 并归还/等待 lease，随后
  停止 revision projection/subscription，最后关闭 HTTP、stdio 与 RunService；未完成 discovery 可在
  lease 到期后由其他实例安全接管。

Conversation 只保存不可变 user/assistant 最终消息与低频 summary；SSE delta、token 和 provider chunk
不得落入 message 表。`retention_days` 独立于 Run retention。分页必须使用
`(message_order,message_id)` cursor，不得使用 offset；删除必须同时清理 message、summary 和关联
object content。Conversation-bound `full` Run 的 workflow event/checkpoint/output 仍由独立的 Run
audit/retention authority 管理，privacy delete 不直接篡改该账本；Conversation tombstone 会在所有
公共读取边界 fail closed：Run GET/取消响应只返回已清除 input/output/error 的终态，artifact、
trace 和 recovery/control 派生面返回 not found，execution graph 只保留不含 Run payload 的冻结
Plan。

`full` Conversation summary 只做后台 enqueue：新的 turn admission 使用“最新已提交摘要 + 最近消息”
组装 context，不等待摘要 I/O、资格延迟或失败。进程内按 Conversation 合并重复触发，并用 dirty bit
保证 worker 退出前再做至多一个必要 pass；跨 service 由数据库
`conversation_summary_jobs` 的 Conversation 唯一 claim 保证同一时刻只有一个 worker。worker 在
claim 前先以只读分页和 object read 检查“摘要边界后消息数或 token 是否达到阈值”；低于阈值的 turn
不会 INSERT/UPDATE/DELETE claim row。claim 在资格延迟和 eligibility preflight 之后创建，
generation 有 30 秒 hard timeout，lease 为 35 秒；正常、失败和 timeout 都按
`conversation_id + claim_token + claimed_by` 精确释放，进程崩溃则由 lease 到期后接管。失败只增加固定
result 指标并回退最近消息，不进入 turn mutation/admission 临界路径。

terminal-only admission/result retention 使用时间分区或带索引的有界批处理，不能通过全表扫描或
逐 Run heartbeat 维持状态。关闭 terminal-only execution engine 但保留 full Conversation 时，
RunService 每 60 秒运行一次 full-only retention drain。每条删除 SQL 仍严格限制 100 行，但单周期会
在共享的 64-batch、45 秒 wall-time budget 内连续执行；retention 固定保留最多 22 个 batch pair，
因此每类容量为 2,200 行/分钟（约 36.7/s），覆盖 10/s 的稳定到期流入。
`conversation_retention_deleted_total`、
`conversation_retention_batches_total` 和 `conversation_retention_backlog_pending` 提供固定 label 的
删除量、批次与未清空证据；最后一个 batch 未饱和时 backlog 清零，达到 batch/time budget 时置一。
object deletion jobs 与 terminal artifact staging 共享同一 64-batch/45 秒周期预算，也以每次 claim
100 条连续 drain 到未饱和。retention 周期为这两个后置队列各保留 21 个 batch；其余 5 秒维护周期
则各保留 32 个 batch，前置队列持续饱和也不会使后置队列永久饥饿。对象失败会立即退出该队列的本轮
drain，并保留 60 秒 claim lease，避免 tight-loop。
`conversation_maintenance_{batches_total,backlog_pending}{queue}` 分别报告
`content_deletion` 与 `artifact_staging` 的追赶和积压状态。安装 terminal engine 后 full-only pump
自动让出所有这些队列，不能形成双 owner。

生产语义与故障边界见[架构概览](architecture.md)，具体配置以严格配置 schema 和启动校验为准。
