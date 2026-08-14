# ADR-0001: Platform v2 PostgreSQL Baseline

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-10 |
| 决策范围 | clean-cut `/v1` 的 PostgreSQL 物理模型 |
| 规范输入 | `docs/specs/platform-v2/00-overview.md`～`18-deployment-observability-and-qualification.md` |
| 被替代设计 | migration 1～35、177 表 catalog/schema contract |

## 1. 决策

Platform v2 从一个全新 `0001` 建立 23 张表，其中包括 migration ledger。业务 schema 只有 22 张表。

`0001`只能由部署期独立provisioning Job/运维流程对fresh target执行；API、Scheduler、Worker及其他运行时
进程不持有DDL权限，也不提供apply/migrate入口。运行时只做read-only schema verification。重复provision不是
幂等启动步骤，而是对非fresh target稳定失败。

不兼容旧 schema，不 dual-write，不做数据搬迁，不保留旧 repository API。领域新增状态、证据、拒绝结果、callback、
remote task、retry generation 或 ResourceKind 时，默认扩展 closed Rust type 与 bounded payload，不新增表。

## 2. 表清单

| # | 表 | 唯一事实 |
|---:|---|---|
| 1 | `schema_migrations` | 已应用 baseline/version/checksum |
| 2 | `tenants` | tenant 当前 gate、配置 generation 与 version |
| 3 | `principals` | 外部 principal identity 与少量 installation binding 当前事实 |
| 4 | `tenant_principals` | principal 在 tenant 中按 kind 隔离的 binding、permission snapshot source 与 version |
| 5 | `secret_bindings` | opaque Secret reference、purpose、generation 与 revoke state |
| 6 | `resources` | 所有 registry object 的 Draft/lifecycle/gate/active target 当前事实 |
| 7 | `resource_versions` | 所有 immutable revision、validation snapshot 与 ArtifactRefs |
| 8 | `deployments` | environment-bound immutable exact binding closure |
| 9 | `runs` | Run 当前状态、bindings snapshot、control、budget、public sequence |
| 10 | `run_nodes` | Scope/Node/current control relation 与执行状态 |
| 11 | `run_values` | bounded typed Plan value 或 ArtifactRef |
| 12 | `invocations` | Capability/Model/Context/MCP/Sandbox/Management logical call 当前状态 |
| 13 | `jobs` | 所有物理工作、attempt generation、lease、wake、retry 与 terminal winner |
| 14 | `tasks` | Approval/HumanInput/Elicitation 等人机 first-winner |
| 15 | `events` | transition、outcome、audit、安全 projection 的 append-only 历史 |
| 16 | `receipts` | Command/Callback/JobCommit 幂等和稳定 disposition |
| 17 | `outbox_events` | Event 的可靠外发状态，不复制 event payload |
| 18 | `artifacts` | Artifact 当前 lifecycle、prepare admission、verified media、retention 与 primary Blob reference |
| 19 | `artifact_blobs` | object generation、encryption/storage binding 与 verified digest/size 的唯一物理事实 |
| 20 | `artifact_links` | reference/grant/hold/provenance/operation-target closed relation |
| 21 | `quota_accounts` | scope/work-class 当前额度、reserved/used 与 CAS version |
| 22 | `quota_ledger` | reserve/settle/release/adjust 的 append-only accounting entries |
| 23 | `scheduler_state` | 每 WorkClass 公平 round/cursor 与 bounded tenant deficit state |

这个数量是设计约束，不是 KPI。新增第 24 张业务表需要证明它拥有无法归入现有聚合的独立生命周期；超过 24 张必须
通过新的 ADR，并同时说明为何不能合并、为何 JSONB 不合适、读写路径和删除候选。

## 3. 共享列规则

除 `schema_migrations` 外，tenant-owned 表都包含 `tenant_id`。当前状态聚合统一包含：

```text
<nominal_id> text primary/composite key
tenant_id text
state/kind text closed by Rust enum
version bigint >= 1
schema_version integer >= 1       # 仅有 typed payload 时
payload jsonb                      # bounded closed payload
payload_digest sha256              # 有 payload 时必须匹配 canonical bytes
created_at timestamptz
updated_at timestamptz
```

ID 使用 `<prefix>_<canonical UUIDv7>`。数据库验证通用 canonical shape；Rust `ResourceId` 验证 prefix 与 nominal kind。
热查询、锁、排序、唯一性和 first-winner 所需字段必须使用普通列，不能藏在 JSONB。

## 4. Bounded JSONB 合同

允许 payload 的表只有 `tenants`、`principals`、`tenant_principals`、`secret_bindings`、`resources`、`resource_versions`、`deployments`、`runs`、
`run_nodes`、`run_values`、`invocations`、`jobs`、`tasks`、`events`、`receipts`、`artifacts`、`artifact_links`、
`quota_accounts` 与 `scheduler_state`。

每个 payload 必须：

1. 对应一个 Rust nominal struct/enum；
2. 有顶层 `schema_version`，unknown field/value fail closed；
3. 使用 RFC 8785 canonicalization 或平台等价 canonical encoder计算 digest；
4. 通过平台 hard limit：总字节、depth、array items、object properties、string bytes；
5. 不含 Secret value/token、raw prompt/model/file body、unbounded diagnostic、signed URL 或 object credential；
6. 大值使用 `run_values` + ArtifactRef 或直接 Artifact；
7. 升级时显式支持旧版本 reader 或在发布前完成 bounded backfill，不允许运行时猜测 shape。

数据库只做 `jsonb_typeof(payload) = 'object'` 与总字节硬限制；完整 closed-schema validation 属于 Rust 和共享 fixture。

## 5. 关键表结构

### 5.1 Resource

`resources` 热字段：`resource_id`、`resource_kind`、`lifecycle_state`、`gate_state`、`draft_generation`、
`active_version_id`、`active_deployment_id`、`version`。两种 active target 最多存在一个。

`resource_versions` 热字段：`resource_version_id`、`resource_id`、`revision_no`、`content_digest`、`artifact_id`。
同 Resource 的 revision number 与 content digest 唯一。行创建后 immutable。

`deployments` 热字段：`deployment_id`、`resource_id`、`resource_version_id`、`environment`、`bindings_digest`。
bindings payload 是 exact ID/digest closure；行创建后 immutable。

### 5.2 Run kernel

`runs` 热字段至少包含 `run_id`、`agent_deployment_id`、`state`、`version`、`deadline`、`retry_at`、
`active_work_count`、cancel/timeout/pause generation、`public_sequence`。RunBindingsSnapshot 是 bounded payload。

`run_nodes` 统一 Scope/Node/ChildRunLink/control relation，以 `node_kind` 区分；热字段包含 parent、plan node key、state、
generation、version、enqueue round、deadline。一个 Run 的 logical node key 唯一。

`run_values` 保存 nominal schema/classification/digest 与 `inline_value XOR artifact_id`。inline value 受 hard limit。

### 5.3 Invocation、Job 与 Task

`invocations` 以 `invocation_kind` 区分 Capability/Model/Context/MCP/Sandbox/Management。热字段包含 owner Run/Node、
exact resource/deployment、state、version、deadline、input/output value/artifact reference 与 effect key；current Job 由
`jobs` 的 live-owner unique key 查询，不在 Invocation 复制。

`jobs` 是唯一物理执行权威。热字段包含：

```text
job_id / work_class / owner_kind / owner_id
state / version / attempt_no / attempt_limit / lease_epoch
worker_id / lease_expires_at / heartbeat_at
scheduled_at / retry_at / deadline / priority
wake_kind / wake_state / wake_generation
request_digest / result_digest / effect_key_digest
quota_reservation_id / started_at / terminal_at
```

一个 logical owner 最多一个 nonterminal Job；一个 Job 同时最多一个 live lease。Job retry 推进同一行的 attempt generation，
历史 attempt 写 Event/Receipt。等待时清空 worker/lease 并保存 WakeContract，不占 worker slot。

`tasks` 以 `task_kind` 区分 approval/human_input/elicitation/url_consent。owner、generation、state、deadline、schema digest、
principal snapshot 与 safe prompt 是 first-winner 所需事实；response 大值进入 RunValue/Artifact。

### 5.4 Event、Receipt 与 Outbox

`events` 保存 `aggregate_kind/id/version`、可选 `run_id/public_sequence`、`event_type`、visibility、payload/digest 与时间。
同 Run 的 non-null public sequence 唯一。

`receipts` 保存 `receipt_kind`、scope kind/id、idempotency key digest、request digest、state、disposition、response reference/
payload digest 与时间；`tenant + kind + scope + key_digest` 唯一。相同 key 不同 request digest 是稳定 conflict。

`outbox_events` 只保存 `outbox_id`、unique `event_id`、publish state/attempt/next time/claim lease 与 failure code。
Event 在业务事务写入，Outbox 在同一事务创建；publisher 不重新生成 payload。

### 5.5 Artifact

`artifacts` 保存 lifecycle、purpose、classification、expected size/optional digest、optional declared media、verified media、primary
Blob、exact retention Policy Revision、retain-until、creator 与 version。Prepare 阶段不得伪造 verified digest/media；只有进入
`Verified`/`Ready` 时才要求 primary Blob 与 verified media。

`artifact_blobs` 是 verified content digest 与 byte length 的唯一物理 authority，同时保存 backend、storage binding digest、
由classification、exact retention revision与encryption domain组成的closed security-domain digest、encrypted object reference、
object generation、key ID、encryption domain、integrity state 与 version。Staging Blob 的 object
generation/digest/size 可以为空；进入 `Verified` 后三者与 `verified_at` 必须完整。`artifacts` 不复制 Blob digest/size，构造
ArtifactRef 时在同一 tenant 下 join exact Blob。verified content dedupe 只在 tenant + backend + storage binding + encryption
domain + security-domain digest 内生效，不能跨 tenant、classification、retention 或 encryption 安全域。

`artifact_links` 以 closed `link_kind` 表达 owner reference、grant、hold、provenance、derived-from 与 operation target；
unique key 由 link kind 的 typed payload 派生。专用 upload session 只有在 multipart 真正进入范围且不能由 Link/Job 表达时再审查。

Artifact 必须引用 exact Retention Policy Revision。该 Revision 的 `PolicyResourceSpec` 在 `PolicyKind::Retention` 时内嵌 closed
`ArtifactRetentionPolicy`，且 `rules_digest` 必须等于 policy document 的 canonical digest。首个 tenant retention revision 与其
自持有的 authoring Artifact 构成有意的 bootstrap 闭环，因此只有 `artifacts_retention_policy_fk` 是
`DEFERRABLE INITIALLY DEFERRED`；onboarding 必须在同一事务内同时建立并在 commit 时满足两端 FK，不允许 nullable policy、
sentinel revision 或提交后补边。Blob安全域修正同样不增加表，schema contract version 为 6。

### 5.6 Quota 与 Scheduler

`quota_accounts` 的唯一 key 是 `tenant + scope_kind + scope_id + work_class + metric`，保存 limit/reserved/used/version。
每次 reserve/settle/release 原子 CAS account 并向 `quota_ledger` 插入 entry；ledger 不更新、不删除。

`scheduler_state` 每 WorkClass 一行，保存 round、cursor、version 与 bounded tenant deficit map；每个 tenant entry
内冻结自身 exact Scheduling Policy version/digest，不伪造跨 tenant 的单一 policy version。
Ready truth 始终在 owner aggregate/Job，scheduler state 不保存 Ready boolean、lease 或 permit。

## 6. 数据库职责

PostgreSQL 必须强制：

- primary key、同 tenant composite foreign key、unique key 与 non-null；
- version/generation/ordinal 非负及时间列基本 shape；
- receipt/idempotency、public sequence、logical owner Job 唯一；
- lease claim/heartbeat/terminal 的 compare-and-swap；
- quota account update + ledger、aggregate + Event + Outbox 的事务原子性；
- Ready Artifact 才能由新业务引用的 repository transaction check；
- 所有 scan/claim 使用 bounded indexable predicate。

PostgreSQL 不使用业务状态机 trigger、跨十几张表的 deferred verifier、每边 transition table 或 catalog checksum 来表达
Rust 语义。绕过 repository 的直接业务写入不属于支持的接口；生产 role 只授予 repository 所需 DML。

## 7. 索引预算

每张当前状态表默认只有 PK、必要 unique/FK index 和 1～3 个热路径 partial/composite index：

- Resource：tenant/kind/lifecycle、active target；
- Run/Node：tenant/state/retry/deadline、Run logical node；
- Invocation/Job/Task：tenant/state/due/lease/owner；
- Event/Outbox：aggregate order、Run public sequence、unpublished due；
- Artifact/Link：tenant/state/content、owner/target/link kind；
- Quota/Scheduler：account key、reservation correlation、work class。

禁止为“未来可能查询”预建索引。真实 query plan/qualification 证明需要后再增加。

## 8. 删除与保留

- current aggregate 默认 soft terminal，不级联删除历史；
- Event、Receipt、QuotaLedger 根据 tenant policy 分区/归档是后续物理优化，不改变逻辑表；
- Artifact Blob 删除必须先验证所有 ArtifactLink、hold、grant 与 alias；
- ResourceVersion/Deployment/Run terminal 在仍被引用时不可删除；
- Outbox 发布完成后可按 retention 清理，但 Event retention 独立；
- migration baseline 不包含 partition、materialized view、business trigger 或 compatibility view。

## 9. 验证门禁

新的 `0001` 必须通过：

1. 静态解析与 `git diff --check`；
2. fresh PostgreSQL 16 apply，表数量精确为 23；
3. 所有表/列/index 与本 ADR 的 machine schema contract 一致；
4. tenant/FK/unique/payload-size negative fixture；
5. Job claim/heartbeat/stale fence/retry/wake/terminal concurrency fixture；
6. Receipt exact replay/conflict、Event/public sequence/Outbox 原子 fixture；
7. quota reserve/settle/release 与 concurrent oversubscription fixture；
8. Artifact prepare允许optional expected digest且不伪造verified content，并覆盖Ready/link/delete closure；
9. repository all-target test、strict Clippy 与 workspace consumer compile。

完成前不得把 00～18 推进为 Implemented 或把新 schema 声明为当前生产行为。
