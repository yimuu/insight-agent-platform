# Durable Runtime 50 活跃 Run 并发优化规范

日期：2026-07-26

状态：Implemented / capacity-qualified（最终 v3 Gate A/B/C 与 Gate D 2 小时均通过；
24 小时 RC qualification 延后执行）

目标版本：pre-1.0 performance cutover

影响范围：`insight-runtime`、`insight-durable`、`insight-storage`、`insight-api`、平台配置、
Helm chart、benchmark

基线证据：[Kubernetes 有限资源压测报告](../../bench/reports/2026-07-25-k8s-limited-resource.md)

## 1. 决策摘要

本规范保留 PostgreSQL 作为生产 durable authority，不用进程内队列替换持久化事实。优化的核心是把
“允许存在多少个 Run”和“同一时刻允许多少个数据库状态转移”分开：

```text
最多 50 个活跃 Run
        │
        ├── waiting / timer / signal / human / external I/O
        │      不占 operation permit
        │
        └── runnable
               ↓ 公平调度
           4～12 个 execution permit
               ↓
        有界 PostgreSQL 事务与行锁
```

必须实施以下四项 P0 改造：

1. 用一个进程级 `WorkCoordinator` 代替每个 worker 独立以 25ms 轮询所有 durable queue；
2. PostgreSQL 使用进程内立即唤醒与有界聚合的 `pg_notify` 作为低延迟提示，同时保留安全轮询，
   通知不承担正确性；
3. 将 pending、lease reclaim、timer due 等候选查询拆开，使 partial index 真正约束扫描范围；
4. 用 `wait_late_audit_outbox` 替代每轮全量解码 `execution_events` 中所有 late event。

容量耗尽必须与业务冲突分离：`RUN_CAPACITY_EXCEEDED` 返回 HTTP 429 和 `Retry-After`，真实
`RUN_CONFLICT` 继续返回 409。

“支持 50 并发”的正式口径是：

- 在 limited profile 上能够稳定维持 50 个 durable active/waiting Run；
- 在 capacity profile 上能够承受 50 个同时到达的短 Run，且全部被接收并在延迟门槛内闭合；
- 它不表示 PostgreSQL 同时执行 50 个写事务，也不表示能够同时调用 50 个外部模型。

本轮资格测试使用一个 runtime 实例。`max_concurrent_runs` 仍是进程级 admission 上限；多副本下的
集群级全局 admission 配额不是本规范隐含承诺。

## 2. 背景与问题定义

### 2.1 已有证据

有限资源基线中，runtime 与 PostgreSQL 均限制为 `500m CPU / 256Mi`：

| 场景 | operation workers | 完成吞吐 | lifecycle p95 | PostgreSQL |
|---|---:|---:|---:|---|
| fresh 1 VU | 16 | 2.88 run/s | 405ms | 475m CPU |
| 4 VU | 16 | 1.50 run/s | 5.386s | 501m CPU |
| 16 VU | 16 | 1.19 run/s | 14.845s | 501m / 254Mi |
| fresh 1 VU | 2 | 3.93 run/s | 301ms | 244m CPU |
| 4 VU | 2 | 3.67 run/s | 1.315s | 501m / 244Mi |

把 worker 从 16 降到 2 后，4 VU 吞吐提升约 145%，p95 降低约 76%，证明增加轮询者会降低而不是
提高该资源预算下的吞吐。

累积 415 个已闭合 Run 后，约 25 秒采样窗口内观察到：

- `task_outbox` 13,151 次顺序扫描，读取约 543 万 tuples；
- `execution_events` 607 次顺序扫描，读取约 576 万 tuples；
- `workflow_runs` 1,947 次顺序扫描，读取约 77 万 tuples；
- `workflow_runs ... FOR UPDATE` 平均约 45.26ms；
- 空 `model_tool_call_batches` 轮询 9,192 次。

`public_event_delivery_heads` 已不在主要耗时 SQL 中，说明此前 head 快路径优化有效；当前主要矛盾已经
转为固定轮询、历史扫描和 Run 行锁持有时间。

### 2.2 当前实现为何退化

当前 `RunService` 使用同一个 25ms `pump_interval` 驱动 recovery、runtime ingress、public event，
并为每个 operation worker 单独启动循环。每个空闲 worker 都会依次尝试 model-tool queue 和普通
task queue。worker 数越大，空队列查询、连接池竞争和 recovery 调用也同步放大。

`task_outbox` 已有 pending/reclaim partial index，但候选 SQL 使用
`pending OR expired claimed`，优化器无法稳定用一个有界索引完成两种不同状态的排序发现。

`reconcile_wait_late_audits()` 每轮先读取并解码历史中全部 `timer.late`、`signal.late` 事件。该成本
随历史单调增加，即使当前没有任何待处理 late audit 也不会消失。

同一 Run 的 authoritative transition 必须通过 `workflow_runs` 行锁串行化。该不变量不能删除，
但必须避免在没有工作时获取它，也不能让一个跨 Run 的大 claim 事务同时持有多行 Run 锁。

### 2.3 实施后资格结论

本规范的 P0 实现已落地，最终资格镜像为
`insight-agent-platform:qualification-v3@sha256:7801bd7c572fc850bd63af23e64d22ef64bca94b301da6faa990e0724833be64`，
schema contract 为 `durable-schema-cd9a5c3f-5f12-46d2-ab96-78820a13186f`。正式结果见
[容量资格报告](../../bench/reports/2026-07-26-durable-runtime-50-active-runs-optimized.md)。

- Gate A：limited `500m/256Mi` 下 50 个 waiting Run 保持 30 分钟，200/200 收敛，429、slot
  恢复和 listener 重连均通过；
- Gate B：50 × 20 burst 为 1,000/1,000，lifecycle p95 2.393s、p99 2.581s；10 arrival/s
  持续 10 分钟为 6,000/6,000，0 dropped；
- Gate C：180,907 个闭合 Run、3,075,983 events 起点的 aged dataset 相对 fresh 吞吐为
  91.18%，p95 为 110.58%，idle 未扫描闭合 Run/event/outbox 全表；p99 为 fresh 的
  203.47%，保留为 P1 历史/WAL 尾延迟优化信号；
- P1 notification 与 retention 修复：4 VU/10m 只有 0.238 database notifications/Run；
  aged retention GC 从 66.1ms/102,200 buffers 降为 0.025ms/3 buffers，temp spill 为 0；
- Gate D：v3 以 10 arrival/s 运行 2 小时，71,801/71,801 个 accepted Run 全部终态闭合，
  scheduled 成功率 99.9735%；listener 断线和 claimed-task runtime 重启均恢复，deadlock、
  temp spill、OOM 与六类一致性违例均为 0。

资格过程中发现并修复了 recovery 谓词的两个边界：最新 `task_completed` checkpoint，以及
“父 Run 存在未结算 subflow、子 Run 已终态”。v2 两小时测试又发现 terminal commit 与 terminal
Public Event publication 的锁顺序反转；v3 publisher 先锁 `workflow_runs`，与 terminal
transition 的 `Run → outbox` 顺序一致，并用真实 PostgreSQL 并发回归测试覆盖。

## 3. 目标、非目标与术语

### 3.1 目标

1. 在不削弱 durable、fence、replay 和 per-Run 顺序不变量的前提下支持 50 个活跃 Run；
2. 空闲和等待型负载的数据库成本不随 operation worker 数线性增长；
3. 所有全局发现查询只访问可执行、到期或待恢复的热集合，成本不随闭合历史线性增长；
4. 过载时快速、明确地背压，不形成 409 快速重试风暴；
5. 通过 fresh、aged、burst、soak 四类压测证明容量，而不是用配置值推断容量；
6. PostgreSQL 与 SQLite 保持相同的 durable 状态机语义；SQLite 仍只承诺单进程开发模式。

### 3.2 非目标

- 不删除 PostgreSQL，也不把内存队列升级为生产权威；
- 不改变 execution event、projection、public event 的顺序与幂等合同；
- 不把外部副作用从 at-least-once 宣称为 exactly-once；
- 不在本规范中承诺 50 路 LLM、retrieval 或第三方 API 并发；
- 不把多 runtime 水平扩展作为本轮容量达标的必要条件；
- 不新增跨 runtime 的全局 active-Run 配额或租户配额；
- 不在 P0 中制定 execution event 的删除、归档或长期审计保留政策；
- 不通过提高 `max_concurrent_operations` 掩盖查询和轮询问题。

### 3.3 规范术语

| 术语 | 精确定义 |
|---|---|
| admitted Run | 已成功创建且占用 admission slot 的非终态 Run |
| active Run | lifecycle 尚未闭合的 admitted Run，包括 waiting |
| runnable operation | durable state 表明当前可被 claim 并执行的 operation |
| executing operation | 已持有 process-global execution permit 的 operation |
| waiting Run | 等待 timer、signal、human task、model/tool continuation 或外部结果，不执行用户代码 |
| wakeup hint | 只提示“可能有工作”的可丢失信号，不包含 authoritative payload |
| safety poll | 通知丢失、连接重建或写入方异常时保证最终发现工作的低频查询 |
| aged dataset | 至少 10,000 个闭合 Run、250,000 条 execution event 的预置数据库 |

`runtime.max_concurrent_runs` 表示单个 runtime 进程的 active Run admission 上限，不是执行线程数；
`runtime.max_concurrent_operations` 才是进程级 operation execution 上限。

## 4. 不可破坏的正确性不变量

以下条件优先于所有性能指标：

1. 数据库仍是 Run、claim、lease、timer、signal、outbox 和 terminal state 的唯一 durable authority；
2. 通知可重复、合并、乱序或丢失；只要数据库提交成功，safety poll 最终必须发现工作；
3. 进程在数据库 commit 后、处理通知前崩溃，不得丢失已提交工作；
4. per-Run execution sequence、transition replay、projection checkpoint 和 public event order 保持不变；
5. 过期 fence 的 Worker 仍不能提交结果；
6. 同一 task 可以因 lease 超时被重新执行，但只有一个 fenced commit 成为权威；
7. terminal、cancel、signal/timeout 竞态仍由数据库事务选择唯一赢家；
8. dispatcher 公平性不能改变任务的 durable eligibility，只能决定候选尝试顺序；
9. 任一进程内 ready 标记丢失后，数据库中的 pending 工作仍可恢复；
10. 日志、通知 payload 和 metrics 不得包含用户输入、模型正文、Artifact 内容或凭据。

任何优化若不能通过现有 PostgreSQL/SQLite repository contract、recovery 和 real-process tests，不得
以性能收益为理由合入。

## 5. 目标架构

### 5.1 单一 WorkCoordinator

`RunService` 每个进程只创建一个 `WorkCoordinator`。它拥有：

- 一个按 work class 合并的进程内 pending bitset；
- operation permit semaphore；
- per-Run permit 计数；
- active/idle backoff 状态；
- 下一次 timer、lease、retention 或 safety scan 的数据库时间；
- shutdown token 和 listener health。

work class 至少包括：

```text
scheduler_task
model_tool_task
runtime_ingress
public_event
recovery
maintenance
```

worker 不再自行查询 durable queue。执行流程必须变为：

```text
database commit / local writer
          │
          ├── wakeup hint ───────────────┐
          │                              │
          └── durable pending row        │
                                         ↓
timer deadline ───────────────→ WorkCoordinator
safety poll ──────────────────→    │
                                   ├── coalesce
                                   ├── inspect available permits
                                   ├── bounded claim
                                   └── dispatch claim to worker task
```

一个 worker 完成后只归还 permit 并重新唤醒 coordinator，不直接启动下一轮全局扫描。无工作时不得
按 worker 数执行重复 query。

### 5.2 Active Run 与 execution permit 分离

默认 capacity 目标配置：

```yaml
runtime:
  max_concurrent_runs: 50
  max_concurrent_operations: 12
  max_concurrent_operations_per_run: 2
```

limited profile 仍使用 4 个 execution permit；最终 Helm 默认值由容量矩阵选择。所有 scheduler
recovery drive 也必须占用同一个 process-global permit，不能在 coordinator 内串行执行，也不能另开
一个不受限的数据库并发池。

以下状态必须释放 operation permit：

- timer/signal/human wait；
- model 请求或 tool continuation 等待；
- retry deadline 尚未到期；
- public event 等待下游 publication；
- paused Run。

每个 Run 同时最多获得 `max_concurrent_operations_per_run` 个 permit。候选选择采用轮转公平性：

1. 同一轮每个 Run 先获得一个候选；
2. 有剩余全局 permit 时才分配同一 Run 的第二个候选；
3. 已连续跳过三轮的 eligible Run 必须优先于新出现的同优先级 Run；
4. fairness 只在 durable eligibility 相同时生效，不越过 deadline、termination finalizer 或 lease reclaim。

### 5.3 Wakeup 合同

在 `insight-durable` 定义后端中立的 `WorkWakeup`/`WorkNotificationStream` 合同；它传递 work class，
不传 Run、task 或 payload 内容。

PostgreSQL 后端：

- runtime writer 的 commit 先立即设置本进程 pending bit，不在权威事务中执行 `pg_notify`；
- process-global publisher 把 commit/completion 提示在
  `notification_reconnect_interval` 窗口内合并为一个 `maintenance`/all-class 提示，最大频率由
  该窗口约束；
- runtime PostgreSQL 连接使用固定低基数 `application_name=insight-agent-platform-runtime`，
  schema trigger 据此跳过逐行通知；不属于 runtime 的 repository writer 仍保留 commit-scoped
  trigger 提示；
- publisher 在权威 commit 之后发送，commit 与 hint 之间崩溃时允许丢提示，safety scan 必须恢复；
- claimed/running 等尚未 eligible 的状态不发送即时提示；
- channel 名由数据库 schema OID 派生，例如 `iap_work_<schema_oid>`；
- PostgreSQL 只在 publisher 自身事务 commit 后交付通知，rollback 不产生有效唤醒；
- listener 断线后指数退避重连，重连成功立即触发一次全 work-class safety scan；
- 多条相同通知可以合并为一个 pending bit；
- 多 runtime 会同时收到提示，但 durable claim、`SKIP LOCKED`、lease 和 fence 继续决定权威。

SQLite 后端：

- 同一进程的 repository writer 在 commit 成功后触发 `tokio::sync::Notify`；
- 没有跨进程通知保证；
- safety poll 仍存在，因此测试可主动丢弃内存通知验证恢复。

定时工作不能只依赖通知。插入或修改 timer/lease 时必须唤醒 coordinator 重新计算最近 deadline；
coordinator 睡眠到数据库返回的最近 due time，或被更早通知打断。

### 5.4 Safety poll 与退避

新增严格配置：

```yaml
runtime:
  scheduler:
    active_poll_interval: 25ms
    idle_poll_min_interval: 100ms
    idle_poll_max_interval: 2s
    safety_poll_interval: 5s
    claim_batch_size: 8
    notification_reconnect_interval: 250ms
```

约束：

- 所有 duration 必须大于零；
- `active_poll_interval <= idle_poll_min_interval <= idle_poll_max_interval`;
- `idle_poll_max_interval <= safety_poll_interval`;
- `claim_batch_size` 范围为 1～256，且实际 claim 数不得超过空闲 execution permits；
- strict YAML 继续拒绝未知字段；
- 生产 PostgreSQL 默认启用通知；SQLite 自动使用 local notify，不暴露虚假的跨进程能力。

退避规则：

1. claim 到工作时重置为空闲最小间隔；
2. 连续空结果使用带 ±20% jitter 的指数退避，直到 `idle_poll_max_interval`；
3. notification 到达时立即清零对应 work class 的退避；
4. 无论通知是否健康，每个 work class 至少每 `safety_poll_interval` 检查一次；
5. safety poll 不得由每个 worker 分别执行。

原有单一 `pump_interval` 不再作为生产可调度循环的事实来源；内部测试若仍需要手动长间隔，应改为
注入 coordinator clock/wakeup，而不是设置 3,600 秒 pump。

### 5.5 Redis 与外部消息队列边界

P0 不引入 Redis，也不把 Kafka、RabbitMQ、NATS 等外部消息队列放进核心 scheduler 路径。原因不是
这些组件没有价值，而是它们不能消除 per-Run 数据库权威事务，反而会增加“双写成功了一半”的一致性
边界。

允许后续单独引入的范围：

- Redis 只能缓存 immutable agent/deployment metadata、编译结果或允许短暂过期的只读聚合；
- 所有缓存必须 cache-aside、带 TTL/版本 key，并在缓存不可用时回退 PostgreSQL/Artifact authority；
- Run lifecycle、lease、fence、timer、signal、outbox、admission slot 不得以 Redis 为权威；
- 外部消息队列只用于 `public_event_outbox` 之后的跨服务 fan-out 或第三方副作用投递；
- 数据库 outbox receipt 继续保存发布权威，消息投递按 at-least-once，消费者按 public event ID 幂等；
- PostgreSQL `NOTIFY` 仍只是可丢失 wakeup hint，不升级为消息总线或审计日志。

只有 aged/soak 证据显示明确的可缓存读热点，或者出现需要独立扩缩容的跨服务消费者时，才启动对应
Redis/MQ 规范；不能用新增中间件替代本规范中的查询边界和 recovery 并发修复。

### 5.6 实施后 P1 边界

旧镜像 2 小时 soak 证明当前 50 并发和 10 run/s 不需要 Redis 或外部 MQ，同时暴露了两个应直接
在 PostgreSQL 内修复的热点；二者现已包含在 v3：

1. runtime writer commit 后先触发 local wakeup，同类 scheduler work 的跨进程 hint 以 250ms
   窗口合并；Public Event publication 使用 commit 后本地 durable-by-ID 投递，远端订阅保留
   100ms bounded durable poll，5s scheduler safety scan 继续兜底。v3 两小时故障 soak
   为 0.380 database notifications/Run，替代旧镜像每 Run 5 次逐事件通知；
2. public-event retention GC 使用
   `idx_public_outbox_retention(retain_until,run_id,public_event_id)` partial index，并以
   `statement_timestamp()` 生成可进入 B-tree range condition 的 statement-stable deadline。
   在 26k+ Runs 上，aged 计划从 66.1ms/102,200 shared buffers 降为
   0.025ms/3 buffers；Gate C temp spill 为 0。

这两项都不授权把 durable state 双写到 Redis/MQ。v3 两小时中没有 notification ordering lock、
deadlock 或 temp spill；当前剩余热点是约 66.16GiB WAL、129 次 requested checkpoint，以及历史
关系增长。引入新中间件的触发条件仍以 5.5 节为准。

## 6. 存储与查询改造

### 6.1 `task_outbox` 候选发现

必须把以下逻辑拆成两个 index-bounded 查询：

```sql
-- 新工作
WHERE task_state = 'pending' AND available_at <= database_now
ORDER BY available_at, run_id, task_id

-- lease 回收
WHERE task_state = 'claimed' AND claim_expires_at <= database_now
ORDER BY claim_expires_at, run_id, task_id
```

两组候选在 Rust 中按 due time 合并，再应用 per-Run fairness。不得继续用一个 `OR` 查询同时承担两种
状态。现有 `idx_task_outbox_dispatch` 与 `idx_task_outbox_reclaim` 应先用
`EXPLAIN (ANALYZE, BUFFERS)` 验证；只有真实计划仍不满足门槛时才新增索引。

claim 阶段必须重新检查 eligibility 并使用 `FOR UPDATE SKIP LOCKED`。一个事务不得为了一个跨 Run
候选批次长期同时持有大量 `workflow_runs` 行锁。repository contract 应把“发现候选 Run”和
“原子 claim 单个 Run 的候选”分开；coordinator 按 Run 调用 claim，因此每次成功提交都已经返回
可派发 claim。实现不得先提交多个 Run 的部分 claim，随后因另一个 Run 失败而只返回 `Err`，导致
已提交 claim 无人执行。

相同拆分原则适用于：

- `model_tool_calls` pending/reclaim；
- `public_event_outbox` pending/reclaim；
- due timer 与 lease expiry；
- recovery 中只需处理非终态 lifecycle 的发现查询。

### 6.2 `wait_late_audit_outbox`

新增后端一致的 durable table：

```text
wait_late_audit_outbox
  run_id
  activation_id
  loser_kind          timer | signal
  loser_id
  due_at
  audit_state         pending | claimed | completed
  claimed_by?
  claim_token?
  claim_expires_at?
  created_at
  completed_event_id?
  PRIMARY KEY (run_id, loser_kind, loser_id)
```

要求：

- wait winner 被提交时，在同一事务中为可能需要 late audit 的 loser UPSERT 一行；
- timer loser 的 `due_at` 为原 timer deadline，signal loser 的 `due_at` 为立即可审计时间；
- due/reclaim 使用各自 partial index；
- claim 后通过现有 transition/replay 逻辑追加 `timer.late` 或 `signal.late`；
- event 已存在时按 exact replay 收敛，并将 outbox 标记 completed；
- claim token、expiry 与状态组合由 CHECK 约束封闭；
- FK 必须绑定 Run、activation，并在 completed 时绑定 execution event；
- 通知丢失由 safety poll 发现 pending/reclaim 行。

完成后，`reconcile_wait_late_audits()` 不得再无界扫描并解码所有历史 late event。历史完整性校验属于
显式 verify/rebuild 命令或测试，不属于 25ms 在线调度循环。

### 6.3 历史规模与 projection

P0 不删除 `execution_events` 或 `projection_checkpoints`。所有在线 global discovery query 必须在
aged dataset 上证明与历史总量近似无关：

- 查询入口是 partial index、pending outbox 或非终态 Run；
- 不允许用 retention 掩盖未限定的全表扫描；
- per-Run audit/replay 查询可以访问该 Run 历史，但不能在每轮全局 pump 中遍历所有闭合 Run；
- 归档、分区和 checkpoint 压缩另立规范，不阻塞本轮 50-active 验收。

### 6.4 Schema 生命周期

该改造需要同步更新：

- `database/durable/postgres/schema.sql`;
- `database/durable/sqlite/schema.sql`;
- `durable_schema_contract.contract_id`;
- PostgreSQL/SQLite schema parity 和 repository contract tests；
- Helm 初始化与 schema provisioning evidence。

runtime 不得自行执行 DDL。pre-1.0 按现有 clean cutover 规则重新 provision 新 schema；本规范不引入
隐式 startup migration 或双写兼容路径。生产数据迁移若成为要求，必须另立 migration spec。

## 7. API 背压合同

当前 `RUN_CAPACITY_EXCEEDED` 被折叠成 `RUN_CONFLICT`/HTTP 409。实现后必须改为：

| 内部错误 | HTTP | 公共 code | Header |
|---|---:|---|---|
| `RUN_CAPACITY_EXCEEDED` | 429 | `RUN_CAPACITY_EXCEEDED` | `Retry-After: 1` |
| `RUN_CONFLICT` | 409 | `RUN_CONFLICT` | 无 |

约束：

- 只有 active Run admission slot 用尽才返回 capacity exceeded；
- 请求格式、idempotency key 或当前 Run 状态冲突仍返回 409；
- `Retry-After` 使用整数秒，不返回队列深度或租户信息；
- 相同 idempotency request 已成功创建 Run 时，重放必须返回原权威结果，不能因当前容量满而变成 429；
- benchmark 客户端遇到 429 时记录 rejection，不把快速失败计入成功 create latency；
- SDK/示例客户端采用 exponential backoff + jitter，不进行无间隔重试。

容量上限保护的是内存、数据库连接与可接受排队时间；不得通过无界 admission queue 模拟“接收成功”。

## 8. 可观测性

实现必须提供以下不含业务 payload 的 metrics。命名可按项目统一前缀调整，但语义与 label 集合必须
固定，禁止使用 `run_id`、`task_id` 等高基数 label。

| Metric | 类型 | 必需 labels |
|---|---|---|
| active runs | gauge | lifecycle class |
| runnable operations | gauge | work class |
| executing operations | gauge | work class |
| admission accepted/rejected | counter | outcome |
| coordinator wakeups | counter | source=`notify|deadline|safety|completion` |
| coordinator poll cycles | counter | work class、outcome |
| claim latency | histogram | work class、outcome |
| queue oldest age | gauge | work class |
| DB pool acquire latency | histogram | outcome |
| Run row lock latency | histogram | transition class |
| notification listener state/reconnects | gauge/counter | backend |
| capacity rejection | counter | endpoint |
| public event publication latency | histogram | terminal flag |

每 60 秒输出一次聚合运行日志仅作为 metrics exporter 不可用时的诊断补充；不能逐 poll 输出 info 日志。

readiness 必须继续验证 durable repository 可用性。notification listener 断开时 readiness 可以保持
Ready，因为 safety poll 保证正确性，但必须暴露 degraded metric；durable repository 或 safety poll
连续失败时 readiness 才失败。

## 9. 性能验收

### 9.1 通用方法

所有容量结论必须使用真实 HTTP lifecycle 和真实 PostgreSQL 16：

- k6 使用明确的 arrival-rate 或一次性 burst，不使用无退避的 capacity rejection 热循环；
- 成功率只统计成功创建且达到预期 terminal state 的 Run；
- 429、409、5xx、timeout 分开报告；
- create latency 只统计 202 或 idempotent success，不能被快速拒绝美化；
- 同时采集 k6、runtime metrics、`pg_stat_statements`、locks、cgroup throttle、memory events；
- fresh 与 aged 使用相同镜像、配置、资源和负载；
- 每次报告保存 Helm values、manifest、schema contract、commit SHA 和原始结果。

### 9.2 Gate A：50 个 durable active Run

limited profile：

| 组件 | CPU limit | Memory limit |
|---|---:|---:|
| runtime | 500m | 256Mi |
| PostgreSQL | 500m | 256Mi |

部署固定为一个 runtime 实例和一个 PostgreSQL 实例，避免把多个进程各自的 admission slot 混入
单实例资格结论。

负载：

1. 创建 50 个进入 durable wait 的 Run；
2. 保持 30 分钟，其中至少 45 个 Run 始终处于 waiting；
3. 每分钟随机唤醒并重新进入等待 5 个 Run；
4. 第 51 个非幂等新建请求必须稳定返回 429；
5. 释放一个 slot 后，新请求应在 1 秒内恢复可接收。

通过标准：

- 前 50 个 Run 创建成功率 100%，没有 5xx、deadlock、OOM 或 Pod restart；
- waiting 期间 operation permit 占用不超过实际被唤醒数量；
- 30 分钟内 safety/discovery query 总率不超过每进程每 work class 2 qps；
- PostgreSQL 平均 CPU 不超过 250m，CPU throttled periods 比例低于 10%；
- runtime 与 PostgreSQL RSS 在最后 20 分钟无持续上升趋势，峰值低于 limit 的 90%；
- signal/timer wake-to-claim p95 ≤ 500ms、p99 ≤ 1s；
- `GET /v1/runs/{run_id}` p95 ≤ 250ms；
- 主动丢弃通知或重启 listener 后，pending work 在 `safety_poll_interval + 1s` 内被发现。

Gate A 证明“50 个活跃 Run”，不证明 50 个短 Run 同时执行。

### 9.3 Gate B：50 个短 Run 同时到达

capacity profile 采用资源矩阵，选择满足标准的最小档位：

| 档位 | runtime | PostgreSQL |
|---|---|---|
| C1 | 2 CPU / 1Gi | 4 CPU / 8Gi |
| C2（资格上限） | 4 CPU / 2Gi | 8 CPU / 16Gi |

如果 C2 仍不通过，不得宣称支持 50 short-run concurrency，必须回到 SQL/事务设计继续优化。

负载：

- 预热后一次性同时创建 50 个 `action_demo` Run；
- 每个 Run 只创建一次，不在失败后立即重试；
- 连续执行 20 轮，轮间恢复到零 active Run；
- 另以 10 arrival/s、最多 50 in-flight 运行 10 分钟。

通过标准：

- burst 中 1,000 个 Run 的 create accepted 与 terminal success 均 ≥ 99.9%；
- 不允许出现 capacity rejection、5xx、deadlock、OOM 或 workload-induced restart；
- burst lifecycle p95 ≤ 5s、p99 ≤ 8s；
- sustained completed throughput ≥ 9 run/s，lifecycle p95 ≤ 5s；
- DB pool acquire p95 ≤ 100ms；
- `workflow_runs` row-lock wait p95 ≤ 100ms、p99 ≤ 250ms；
- PostgreSQL CPU p95 不超过 limit 的 85%，memory peak 不超过 limit 的 85%；
- operation execution 数不超过配置值，per-Run 数不超过 per-Run 配置值。

该档位是资格测试环境，不是所有生产 workload 的资源保证。真实 LLM、retrieval、Artifact 和存储 IOPS
必须用各自 workload profile 重新定容。

### 9.4 Gate C：历史规模不退化

在 fresh 与 aged dataset 上分别运行同一 4 VU/10 分钟短 Run profile。aged dataset 至少包含：

- 10,000 个闭合 Run；
- 250,000 条 `execution_events`；
- 对应完整 projection、scheduler checkpoint 和 public event 数据。

通过标准：

- aged completed throughput 不低于 fresh 的 90%；
- aged lifecycle p95 不高于 fresh 的 120%；
- 空闲 5 分钟内不得出现 `execution_events`、闭合 `workflow_runs` 的全表扫描；
- `task_outbox`、model tool、timer、public event 候选计划必须命中预期 partial index；
- 每种 discovery SQL 的 buffers read 与 eligible row 数量相关，而不是与历史总行数相关。

### 9.5 Gate D：稳定性

合入和本轮容量资格 gate 为 2 小时 soak；发布候选需要保存一份 24 小时 soak evidence。

要求：

- 负载保持 10 arrival/s、in-flight 上限 50；
- terminal success ≥ 99.9%，其余失败必须全部可解释且不能是数据丢失；
- deadlock、PostgreSQL conflict、OOM kill、Pod restart 为零；
- listener 至少被强制断开并重连一次；
- runtime 至少在有 pending claim 时重启一次，lease/fence 最终正确收敛；
- 最后 30 分钟的 RSS、DB connections、queue oldest age 无持续上升；
- 抽样 Run 的 event、projection、terminal snapshot 和 public event 顺序通过一致性校验。

v3 实施结果：71,820 次 scheduled iteration 中 71,801 次被受理且全部终态闭合，scheduled
成功率 99.9735%；lifecycle p95/p99 为 938ms/2.971s。listener 断线与 claimed-task runtime
重启均正确恢复；deadlock、conflict、temp spill、OOM、非预期 Pod restart 和一致性违例均为 0。
最后约 30 分钟 runtime RSS/PSS 与 cgroup memory 均为负斜率，PostgreSQL RSS 为 0、PSS
`+5.85KiB/min`、cgroup memory `-17.84MiB/min`，旧的内存条件项已关闭。2 小时合入 Gate D
通过。根据 2026-07-27 的交付决定，本轮实现与 capacity qualification 在此关闭；发布候选仍必须
在 always-on runner 上补齐 24 小时证据，且不得把本次 2 小时结果改称 24 小时结论。

## 10. 测试要求

### 10.1 单元与配置测试

- scheduler 配置默认值、上下界、strict unknown-field rejection；
- exponential backoff、jitter 范围和 deadline 抢占；
- work-class coalescing，重复通知只产生一个待处理 bit；
- fairness：单个高 fan-out Run 不饿死其他 Run；
- capacity 429、`Retry-After`、idempotent replay 和 409 区分。

### 10.2 Repository contract

PostgreSQL 与 SQLite 必须覆盖：

- commit 后通知、rollback 不产生 authoritative work；
- 丢通知后 safety poll 能 claim；
- pending/reclaim 查询顺序和 eligibility 二次确认；
- 两个 claimant 并发时只有一个有效 claim；
- `wait_late_audit_outbox` pending、reclaim、exact replay、completed；
- timer/signal loser 乱序、重复与进程崩溃；
- schema 表、索引、CHECK、FK、contract ID parity；
- aged fixture 的 `EXPLAIN` 计划断言。

### 10.3 Runtime 与 real-process

- coordinator listener 断线、数据库短暂不可用和恢复；
- commit 后通知前进程退出；
- claim 后 worker 退出、lease 到期后重新领取；
- 50 waiting Run 随机 signal/timer 唤醒；
- 两个 runtime 同时监听时无重复 authoritative commit；
- shutdown 不接受新 Run，但仍在 grace period 内归还/完成已有 claim；
- public event、recovery 和 runtime ingress 不因 worker queue 繁忙永久饥饿。

## 11. 实施阶段

当前进度：

| 阶段 | 状态 |
|---|---|
| Phase 0～4 | 已实施并通过代码、contract、Helm 和 Gate A/B/C 验证 |
| Phase 5 / Gate A～C | 已通过 |
| Phase 5 / Gate D 2h | v3 已通过；故障恢复、内存趋势和一致性门槛均关闭 |
| Phase 5 / Gate D 24h | 延后到 release-candidate qualification；两次本机运行均因 OrbStack host sleep 失效，第二次已由 macOS 日志确认为 `Clamshell Sleep`。下一次要求 always-on runner，且不阻塞本轮 capacity qualification 收尾 |

### Phase 0：固定证据与观测口径

- 将现有 k6 profile 改为区分 accepted、capacity rejected、terminal success；
- 增加 active/runnable/executing、poll、claim、pool wait、row-lock metrics；
- 保存 fresh 与 aged 基线；
- 在任何 SQL 改动前记录 `EXPLAIN (ANALYZE, BUFFERS)`。

完成标准：报告能区分真实完成、排队和快速拒绝，不能再用 409 的低延迟推导容量。

### Phase 1：查询边界收敛

- 拆分 task/model/public pending 与 reclaim query；
- recovery、timer、signal 查询限制到非终态/到期集合；
- claim 按 Run 分组，缩短跨 Run 锁持有；
- 基于计划证据决定是否补充 partial index。

完成标准：aged dataset 上 global discovery 不再扫描闭合历史，现有 contract tests 全绿。

### Phase 2：WorkCoordinator 与唤醒

- 引入后端中立 wakeup port；
- 将 worker 独立 pump 合并为 coordinator；
- PostgreSQL 接入 local wake + 进程级聚合 `pg_notify`，SQLite 接入 local notify；
- timer deadline、listener reconnect 和 safety poll 共用同一调度循环；
- 保留 durable claim/fence，不建立权威内存任务队列。

完成标准：通知全丢场景仍收敛，空闲 query rate 达到 Gate A。

### Phase 3：Late-audit 有界化

- 新增 `wait_late_audit_outbox`、索引、约束和 contract ID；
- winner transaction 原子写入 audit work；
- reconciler 只 claim due/reclaim work；
- 删除在线全量 late-event decode。

完成标准：aged dataset 上 `execution_events` 不再参与空闲全局扫描，竞态测试全绿。

### Phase 4：背压与 Helm

- capacity exhausted 改为 429；
- 暴露 scheduler 配置；
- Helm 提供 limited、C1、C2 benchmark values；
- 默认 execution workers/permits 由通过容量矩阵的最小配置决定；
- 更新 current API、operations、architecture 文档。

完成标准：API conformance 和 `helm lint/template` 通过，未知配置仍 fail closed。

### Phase 5：容量资格

- 依次执行 Gate A、B、C、D；
- 对失败档位保留原始结果，不选择性删除；
- 生成新的容量报告，并明确硬件、存储和 workload 限制；
- 全部实现完成后将本规范移入 `docs/archive/specs`。

## 12. 交付清单

预计代码落点：

| 边界 | 交付物 |
|---|---|
| `crates/durable` | wakeup port、work class、late-audit claim/receipt contract |
| `crates/storage` | PostgreSQL/SQLite wakeup adapter、候选查询、late-audit outbox |
| `crates/runtime` | `WorkCoordinator`、fair permits、deadline/backoff/safety poll |
| `crates/api` | 429/409 错误映射与 `Retry-After` |
| `src/config.rs` | strict `runtime.scheduler` 配置解析与校验 |
| `database/durable` | 双后端 schema、索引、约束、contract ID |
| `deploy/helm` | scheduler values、limited/C1/C2 profile |
| `bench` | active-50、burst-50、aged、2h/24h soak profile 与汇总 |
| `docs/current` | 实现完成后的 architecture、API、operations 更新 |

## 13. 风险与缓解

| 风险 | 后果 | 缓解 |
|---|---|---|
| 把 NOTIFY 当消息队列 | 断线或合并时丢任务 | payload 只作 hint，5s safety poll，断线恢复后全类扫描 |
| 多 runtime 被同时唤醒 | thundering herd | 进程内合并、jitter、有限 claim、`SKIP LOCKED`；本轮不以副本数换容量 |
| fairness 改变优先级 | termination/reclaim 延迟 | fairness 只作用于同 eligibility/priority 候选 |
| claim 按 Run 拆事务 | discovery 与 claim 间候选变旧 | 单 Run claim 原子重检 eligibility；失败不产生 claim，其他 Run 独立重试 |
| 新 outbox 与事实漂移 | late audit 永久遗漏 | winner 同事务写入、FK/CHECK、rebuild verifier、safety poll |
| 新索引放大写成本 | transition 延迟上升 | 先验证现有索引，只用 partial index，A/B 记录写放大 |
| 429 行为变化 | 客户端兼容问题 | API 文档、contract tests、SDK backoff、pre-1.0 release note |
| 指标高基数或泄露 | 内存/隐私风险 | 固定低基数 labels，禁止任何业务 ID/payload |
| 只在 fresh DB 达标 | 线上随历史退化 | aged Gate C 为合入硬门槛 |
| 提高资源掩盖 SQL 问题 | 成本上升但容量不稳定 | Gate A 固定有限资源，C1/C2 选择最小通过档 |
| 高频事务 `NOTIFY` 竞争提交排序锁 | checkpoint 时延迟和 active Run 堆积 | scheduler local wakeup 与 hint 合并；runtime public-event commit 后本地投递；保留订阅 poll/safety scan |
| retention GC 排序落盘 | temp I/O 放大并干扰前台事务 | 有界 key 选择、匹配 partial index、写放大与 temp spill 双门槛 |

## 14. 完成定义

本轮实现与 capacity qualification 的完成条件：

1. Phase 0～4 和 Phase 5 的 Gate A、B、C、2 小时 Gate D 全部完成；
2. PostgreSQL、SQLite、recovery、real-process 和 API conformance tests 全绿；
3. Gate A、B、C 与 2 小时 Gate D 的原始证据保存在 benchmark evidence 目录，容量报告记录
   精确结果；
4. `docs/current`、Helm values 和容量报告与实际通过档位一致；
5. 未把 429、timeout 或未闭合 Run 计入成功。

以上条件均已满足，本规范的实现与 capacity qualification 于 2026-07-27 结束。

把“50 并发”升级为 release-candidate 级承诺仍需满足独立的后续门槛：

1. 在 always-on runner 上完成不中断的 24 小时 soak；
2. 终态成功率、deadlock、OOM、Pod restart、内存趋势和一致性满足 Gate D 门槛；
3. 保存完整证据并更新容量报告；
4. 将规范改为无条件 release-candidate-qualified 后移入历史档案。

当前可靠表述是：单实例已验证 50 个 durable active/waiting Run；C1 已验证 50 个短本地 action
同时到达，以及 10 arrival/s 的 2 小时负载。该结论不覆盖 50 路真实 LLM/retrieval、跨 AZ、
多 runtime 全局配额或 24 小时发布资格。本轮目标已按 capacity-qualified 结束；24 小时 soak
作为独立 RC follow-up 保留在 `docs/specs`，完成前不移入历史档案。
