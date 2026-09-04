# ADR-0001: Platform v2 PostgreSQL Baseline

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-20 |
| 决策范围 | clean-cut `/v1` 的 PostgreSQL 物理模型与 authority 边界 |
| 被替代设计 | migration 1～35、177 表 catalog/schema contract |

## 背景

旧设计把执行细节、发布流程和公共投影拆成大量持久化对象，形成重复状态机、跨表校验和迁移负担。
Platform v2 尚未发布，因此选择 clean cut，而不是为旧 schema 建立长期兼容路径。

目标是让一个业务事实只有一个 current-state authority：PostgreSQL 负责持久状态、事务、并发与 fence，
应用代码负责业务语义；消息只传 wake hint 或已提交投影。

## 决策

平台只向 fresh PostgreSQL 16 target 安装单一 baseline。baseline 固定为 23 张表，其中一张是 migration
ledger、22 张是业务表。精确表、列、约束、索引、checksum 和 schema contract version 由
[`0001_platform_baseline.sql`](../../crates/platform-postgres/migrations/0001_platform_baseline.sql)与
[`schema-contract.json`](../../crates/platform-postgres/schema-contract.json)共同定义，不在 ADR 重抄。

23/22 是架构上限而非 KPI。新增表必须由新 ADR 证明它拥有独立生命周期、并发边界或核心查询，且说明
为何不能复用现有 aggregate、为何 bounded JSONB 不合适、读写路径是什么，以及可删除或合并的既有对象。

Release、promotion 和 rollback 由 Kubernetes/GitOps 持有。Candidate 与 qualification report 是 CI/CD
中的内容寻址签名产物，不是业务 aggregate、Resource 或 API current state。Run admission 在 tenant 事务中
冻结 exact ResourceVersion、Deployment 与 digest；之后的 active-head 或部署切换不得改写既有 Run。

公共 Operation 不拥有第二状态机。异步管理命令创建 shared Job，Operation API 只返回该 Job 的安全投影；
Event 与 Receipt 保存历史和幂等。只有 Operation 必须跨多个独立 Job 生存时，才可由新 ADR 引入 aggregate。

`0001` 只能由独立 provisioning Job 或运维流程对 fresh target 执行。API、Scheduler、Worker 与其他运行时
角色不持有 DDL 权限，也不暴露 migrate/apply 入口；运行时只做只读 schema verification。对非 fresh target
重复 provision 必须稳定失败，不能把 migration 当作幂等启动步骤。

## 持久化边界

除 migration ledger 外，tenant-owned 数据必须显式携带 `tenant_id`；发布与 qualification 不得通过 fake
tenant 写入业务表。ID 使用 nominal prefix 加 canonical UUIDv7，prefix 与 kind 由 owning Rust type 校验。

锁、排序、唯一性、first-winner 与热查询所需事实必须是普通列。JSONB 只保存 bounded、versioned、closed
Rust payload，并以 canonical bytes 计算 digest；unknown field/value fail closed。payload 不得保存 Secret、token、
raw prompt/file body、signed URL、credential 或无界诊断，大值进入 RunValue 或 Artifact。

PostgreSQL 强制结构、同 tenant 引用、唯一性、CAS/fence、lease、quota accounting 和事务原子性；完整状态机与
closed-payload 语义由 owning Rust type 和 domain test 强制。不得用业务 trigger、事件重放、跨大量表的 deferred
verifier 或 catalog checksum 建立第二套业务语义。绕过 repository 的直接业务写入不是受支持接口。

Job 是物理执行的唯一 authority；owner row 的 current pointer 才决定当前关系，Job owner 只是 immutable
back-reference。current state、Event、Outbox、Receipt completion 与 quota settlement 必须在同一事务中提交；
Outbox 引用 Event，不复制 event payload。

QuotaLedger 是 append-only accounting evidence。首版只使用 `reserve` 与 `settle` 两种 entry；未使用的 reservation
随 settlement 释放，不另设 `release` 或 `adjust` 状态机。account CAS 与 ledger insert 必须原子完成。

ArtifactBlob 是 verified digest 与 byte length 的物理 authority；Artifact 不复制这些事实。Artifact 必须引用 exact
Retention Policy Revision。首个 retention revision 与其 authoring Artifact 的 bootstrap 闭环只允许由 migration 中
明确的 deferred FK 在同一事务提交，不使用 nullable policy、sentinel revision 或提交后补边。

索引只服务已证明的锁、唯一性和热查询路径，不为假设需求预建。current aggregate 默认 soft terminal；历史引用、
ArtifactLink/hold/grant、Event retention 与 Outbox retention 必须分别裁决，不能依赖级联删除。

## 负向边界与后果

不提供旧 schema compatibility、dual write、数据搬迁、compatibility view 或旧 repository API。baseline 不引入
partition、materialized view、业务 trigger、Installation Release、ManagementOperation 或独立 SandboxJob 表。

新增状态、证据、拒绝结果、callback、remote task、retry generation 或 ResourceKind 时，默认扩展 closed Rust
type 与 bounded payload，而不是新增 aggregate。代价是 payload reader 必须显式支持旧版本或在发布前完成 bounded
backfill，且 fresh-only cutover 需要部署系统明确管理数据生命周期。

该模型减少了重复 authority 和跨表状态同步，但不会自动证明生产可用性。当前可观察行为、部署边界与资格状态分别见
[`docs/current`](../current/README.md)、[部署与运维](../current/operations.md)和
[`docs/qualifications`](../qualifications/README.md)。Rust producer、repository 与测试是进程内语义及行为证据，不能由
本 ADR 的 prose 替代。
