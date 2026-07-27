# Durable Runtime 50 并发优化与容量资格报告

日期：2026-07-26

状态：本轮实现与容量资格已完成；最终 v3 Gate A/B/C 与 Gate D 2 小时均已通过，24 小时 RC soak 延后执行

对象：Insight Agent Platform durable runtime，`action_demo` 与 `benchmark_wait`

最终资格镜像：`insight-agent-platform:qualification-v3@sha256:7801bd7c572fc850bd63af23e64d22ef64bca94b301da6faa990e0724833be64`

Schema contract：`durable-schema-cd9a5c3f-5f12-46d2-ab96-78820a13186f`

## 1. 当前结论

本轮已经证明“数据库设计重”不等于“必须加 Redis 或消息队列才能支持 50 并发”。真正的主要瓶颈
是无界/重复发现和 scheduler recovery 串行化。修复后：

- limited `500m/256Mi` 档能够保持 50 个 durable waiting Run 30 分钟，200/200 闭合，正确执行
  429 背压，并在 LISTEN backend 被终止后自动重连；
- C1 档能够连续 20 轮承受 50 个同时到达的短 Run，1,000/1,000 成功，
  lifecycle p95 2.313s、p99 2.388s；
- C1 档能够持续接收 10 arrival/s，10 分钟 6,001/6,001 成功、0 dropped，
  lifecycle p95 213ms；
- v3 aged profile 在 180,907 Runs / 3,075,983 events 起点上相对 fresh 吞吐为 91.18%，
  lifecycle p95 为 110.58%，通过 Gate C，但 p99 为 fresh 的 203.47%，历史尾延迟仍需优化；
- 核心调度不需要 Redis/MQ：PostgreSQL durable rows 继续承担权威，`NOTIFY` 只承担可丢失提示；
- runtime 写路径把 4,000～5,000 次/分钟的进程内 wake 请求合并为约 240 次/分钟跨进程 hint；
  Public Event publication 不再为每个短 Run 产生 5 次事务 `NOTIFY`；
- retention GC 使用 statement-stable deadline 与 partial-index range condition；在 26k+ Runs
  的 aged DB 上从 66.1ms / 102,200 buffers 降到 0.025ms / 3 buffers，temp spill 为 0；
- 旧镜像 Gate D 暴露的 notification ordering lock、retention temp spill 和 PostgreSQL cgroup
  memory 趋势已经关闭；
- v2 2 小时重跑进一步暴露了 terminal Public Event publication 与 Run terminal commit 的锁顺序
  反转；v3 统一为 `workflow_runs → delivery head/outbox` 后，2 小时 71,801/71,801 个已受理
  Run 全部闭合，deadlock、temp spill 和一致性违例均为 0；
- 本轮按 capacity-qualified 结论结束；24 小时证据延后补齐，在完成前不升级为
  release-candidate-qualified。

这里的“50 并发”有两个精确定义：

1. 单实例可以稳定持有 50 个 active/waiting Run；
2. C1 资格资源下，50 个短 Run 同时到达可以在 p95 5s、p99 8s 门槛内全部闭合。

它不承诺 50 路真实 LLM、retrieval 或第三方 API 同时执行，也不承诺多 runtime 副本的集群级全局
admission 配额。

## 2. 测试环境与口径

| 项目 | 配置 |
|---|---|
| Kubernetes | OrbStack 单节点，Kubernetes `v1.34.8+orb1` |
| 节点 | ARM64，约 18 CPU / 16GiB 可分配 |
| runtime C1 | request `1 CPU / 512Mi`，limit `2 CPU / 1Gi` |
| PostgreSQL C1 | PostgreSQL 16，request `2 CPU / 4Gi`，limit `4 CPU / 8Gi` |
| C1 scheduler | 50 active Runs、12 global permits、每 Run 2 permits、pool 24 |
| limited | runtime/PostgreSQL 各 `500m / 256Mi`，4 permits、pool 6 |
| k6 | `grafana/k6:1.7.1`；短测 1 CPU / 256Mi，Gate D 2 CPU / 2Gi；真实 HTTP create + durable GET |
| 数据权威 | PostgreSQL TLS + Artifact PVC |
| 采样 | k6 summary、5s cgroup/resource、2s DB locks/active、runtime metrics、`pg_stat_statements` |

短 Run workload 执行本地 `example.text_metrics` action，不包含模型或外部网络延迟。所有成功数必须
同时满足 create accepted 和 durable terminal success；429、409、5xx、timeout、dropped iteration
分别统计。

## 3. 落地优化

### 3.1 WorkCoordinator 与有界执行

- 每进程只保留一个 `WorkCoordinator`，合并 notify、deadline、completion 和 safety poll；
- task/model worker 不再分别以 25ms 扫描全部 durable queue；
- scheduler recovery drive 与叶节点 worker 共用同一个 global permit 池；
- recovery 可跨 Run 有界并发，同一 Run 仍由数据库 lease/fence 决定唯一权威；
- 进程内维护有界的 recovery in-flight RunId 集合，避免通知期间对同一 Run 重复发起 recovery；
- in-flight 集合只抑制重复尝试，不保存 payload 或权威状态，进程退出后 safety scan 可完整恢复。

最后一项把 4 active Run 时 observed execution peak 从 12 降到 4；4 VU/1m 诊断为
899/899 成功，p95 314ms。

### 3.2 PostgreSQL/SQLite wakeup

- PostgreSQL 的非 runtime writer 仍在使工作 eligible 的事务中提交低基数 work-class hint；
- runtime writer 在 commit 后请求一个进程级合并 publisher；250ms debounce 内的任意完成信号只
  产生一次跨进程 `pg_notify`；
- runtime Public Event publication 在 commit 后直接本地按 durable ID 投递，远端订阅每 100ms
  轮询 durable order，不再为每个 Public Event 事务逐条 `NOTIFY`；
- rollback 不产生 authoritative work；listener 断线会暴露 degraded metric 并重连；
- 通知不包含 RunId 或业务 payload；
- SQLite 使用进程内 notify，同时保留 safety poll；
- 5 秒安全扫描是最终进展保证，通知不承担正确性。

v2 4 VU/10m fresh profile 中，10,060 个 Run 只产生 2,391 次数据库 notification statement，
约 0.238 次/Run；修复前 Public Event publication 固定约 5 次/Run。

### 3.3 查询与历史边界

- task/model/public pending 与 expired-claim reclaim 查询拆分；
- recovery 只发现非终态且确实需要 scheduler drive 的 Run；
- quiescent wait 在 checkpoint 追平后不会被反复 recovery；
- `task_completed` checkpoint 会继续触发下一次 scheduler drive，避免 task 成功但 Run 不终结；
- 新增 `wait_late_audit_outbox`，在线循环不再全量解码历史 late events；
- 所有 due/pending/reclaim discovery 使用 statement-stable clock，使 deadline 能进入 B-tree
  `Index Cond`，而不是 volatile `clock_timestamp()` 的 post-index filter；
- 新增 `idx_public_outbox_retention(retain_until,run_id,public_event_id)` partial index；
- PostgreSQL/SQLite schema、约束、索引与 contract ID 同步。

最终 aged 计划证据显示：

- task pending：`idx_task_outbox_dispatch` index-only scan；
- task reclaim：`idx_task_outbox_reclaim` index-only scan；
- model pending/reclaim：对应 partial index；
- recovery：`idx_runs_recovery` + `idx_scheduler_checkpoints_recovery`；
- retention GC：`idx_public_outbox_retention` range scan，3 shared buffers；
- 无工作时单次计划实测约 0.01–0.10ms。

诊断阶段曾只增加 retention partial index，但生产查询仍使用 volatile `clock_timestamp()`，导致
PostgreSQL 在 26k+ Runs 上从索引起点扫描 104,516 个 future-retention entries：
66.1ms、102,200 shared buffers。改为 `statement_timestamp()` 后形成
`Index Cond: retain_until <= statement_timestamp()`，同一 aged DB 为 0.025ms、3 buffers。

### 3.4 背压与配置

- `RUN_CAPACITY_EXCEEDED` 映射 HTTP 429，返回 `Retry-After: 1`；
- `RUN_CONFLICT` 保持 HTTP 409；
- idempotent replay 不会因当前容量满而变成 429；
- Helm 提供 limited/C1/C2 overlays；
- scheduler interval、safety poll、claim batch、notification reconnect 和 pool 上限 strict parse；
- `/metrics` 使用固定低基数 labels，并新增 recovery executing gauge。

## 4. 失败证据与修正过程

失败结果没有删除：

| 结果 | 观察 | 根因/修正 |
|---|---|---|
| `limited-gatea-30m-final2` | 20 分钟时 46,924 events、45,424 control receipts，PG 触及 256Mi | recovery 把已追平的 quiescent wait 反复驱动；收紧恢复谓词和触发器 |
| `c1-fresh-4vu-10m` | action task 已成功，但 Run 长期不终结 | recovery 谓词过严，漏掉最新 `task_completed` checkpoint；增加显式分支 |
| `c1-burst-50x20` | 1,000/1,000，但 p95 6.201s | recovery 在 coordinator 中逐 Run 串行 |
| `c1-burst-50x20-v2` | permits 8→12 后 p95 6.241s | 证明单纯增加 worker/连接无效 |
| `c1-burst-50x20-v3-recovery-concurrent` | p95 3.372s，但采集脚本运行中被修改 | 只作为诊断，不作为正式资格证据 |
| `c1-fresh-4vu-10m-qualified` | 4 active 时 execution peak 12 | 同一 Run recovery 重复入 flight；增加进程内 in-flight 去重 |
| `c1-soak-10rps-2h-invalid-k6-limited` | 约 22 分钟时 k6 触及 1 CPU/256Mi；`memory.events.max=118,719`，VU 1→14 | 负载发生器资源失真；服务端当时仅 runtime 37m、PG 137m。保留失败证据，将 k6 提升到 2 CPU/2Gi 后从头重跑 |
| `qualification-v2/gate-d-soak-10rps-2h` | 71,883 个已受理 Run 全部闭合，但 `pg_stat_database.deadlocks=1` | terminal commit 的 deferred FK 持有 Run 行并等待 outbox；publisher 持有 outbox/head 后等待 Run 行。v3 terminal publish 先锁 `workflow_runs`，并新增真实 PostgreSQL 并发回归测试 |
| `qualification-v3/gate-d-rc-soak-10rps-24h` | 主机 sleep 后 OrbStack Docker/Kubernetes control plane 无法恢复，fault helper 未执行，wrapper status 143 | benchmark infrastructure failure，不计资格；全局 OrbStack 重启后 PVC/schema 完整、无非终态残留。保留 `failure.env`，完整 24h 使用 macOS `caffeinate` 从零重跑 |
| `qualification-v3/gate-d-rc-soak-10rps-24h-rerun1` | `caffeinate` assertion 仍有效时，macOS 于 08:37:51 明确因 `Clamshell Sleep` 进入休眠；k6 exit 255、wrapper status 143，故障注入未完成 | benchmark infrastructure failure，不计资格。合盖休眠会覆盖 `caffeinate`；OrbStack 恢复后为 202,408 succeeded、3 timed_out、0 非终态，DB deadlock/temp bytes 均为 0。下一次必须保持开盖并接电，或迁移到 always-on runner |

这些失败说明本轮收益来自缩小数据库工作集和消除无效并发，而不是增加中间件或无限扩大连接池。

## 5. Gate A：50 个 durable active/waiting Run

最终镜像 30 分钟 qualified 结果目录：
`bench/results/2026-07-26-qualification-v3/gate-a-limited-50-waits-30m-listener-fault/`。

| 指标 | 结果 | 门槛 |
|---|---:|---:|
| accepted / terminal | 200 / 200 | 100% |
| 第 51 个请求 | 1 次 HTTP 429 | 必须 429 |
| GET p95 / p99 | 1.298ms / 4.667ms | p95 ≤250ms |
| wake-to-terminal p95 / p99 | 148ms / 167.59ms | ≤500ms / ≤1s |
| slot recovery p95 / p99 / max | 58.82ms / 73.83ms / 87.01ms | ≤1s |
| runtime avg / peak | 5.87m / 32m，44Mi top-sample peak | 500m / 256Mi |
| PostgreSQL avg / peak | 27.33m / 99m，144Mi top-sample peak | 500m / 256Mi |
| DB lock waiter / ungranted lock peak | 0 / 0 | 必须 0 |
| Pod restart / deadlock / OOM | 0 / 0 / 0 | 必须 0 |
| idle poll rate | 约 0.89 qps/class | ≤2 qps/class |
| PostgreSQL throttled-period ratio | 2.185% | <10% |
| 最后约 20 分钟 RSS/PSS 趋势 | runtime +21.7/+22.8 KiB/min；PG -0.3/-5.2 KiB/min | 无显著常驻集增长 |
| listener fault | backend PID 106 被终止，reconnect 0→1，state=1 | 必须恢复 |

resident window 中 active Run 始终为 50；listener 被终止后 churn、slot replacement 和 cleanup
继续收敛。最终所有 200 个 Run 均为 `succeeded`，100 个终态抽样无 event/projection/public
event/delivery head/snapshot 不一致。

## 6. Gate B：50 burst 与 10 rps sustained

### 6.1 50 × 20 burst

最终结果目录：
`bench/results/2026-07-26-qualification-v3/gate-b-burst-50x20/`。

| 指标 | 结果 | 门槛 |
|---|---:|---:|
| accepted / terminal | 1,000 / 1,000 | ≥99.9% |
| 429 / 409 / 5xx / timeout | 0 / 0 / 0 / 0 | 必须 0 |
| lifecycle p95 / p99 / max | 2.393s / 2.581s / 2.867s | ≤5s / ≤8s |
| create p95 / p99 | 566.13ms / 694.42ms | 诊断值 |
| active Run peak | 50 | 50 |
| global execution peak | 12 | ≤12 |
| DB active connection peak | 10 | pool 24 |
| DB lock waiter peak | 1 | 无 deadlock |
| runtime avg / peak | 221.17m / 331m，52Mi peak | 2 CPU / 1Gi |
| PostgreSQL avg / peak | 595.49m / 883m，4.43Gi peak | 4 CPU / 8Gi |
| restart / deadlock / temp spill / OOM | 0 / 0 / 0 / 0 | 必须 0 |

### 6.2 10 arrival/s × 10m

最终结果目录：
`bench/results/2026-07-26-qualification-v3/gate-b-sustained-10rps-10m/`。

| 指标 | 结果 | 门槛 |
|---|---:|---:|
| scheduled / accepted / terminal | 6,000 / 6,000 / 6,000 | ≥9 run/s |
| dropped / 429 / 5xx / timeout | 0 / 0 / 0 / 0 | 必须 0 |
| 实际 completed throughput | 10.000 run/s（600s load window） | ≥9 run/s |
| lifecycle p95 / p99 | 213ms / 237ms | p95 ≤5s |
| active / execution peak | 3 / 3 | execution ≤12 |
| runtime avg / peak | 452.44m / 489m，50Mi peak | 2 CPU / 1Gi |
| PostgreSQL avg / peak | 1,076.35m / 1,171m，4.51Gi peak | 4 CPU / 8Gi |
| deadlock / conflict / temp spill | 0 / 0 / 0 | 必须 0 |

## 7. Gate C：aged dataset

最终镜像 fresh 基线目录：
`bench/results/2026-07-26-qualification-v3/gate-c-fresh-4vu-10m/`。

aged profile 开始前数据库为 180,907 closed Runs、3,075,983 execution events。

| 指标 | fresh | aged | aged/fresh | 门槛 |
|---|---:|---:|---:|---:|
| accepted / terminal | 10,161 / 10,161 | 9,265 / 9,265 | 100% success | 100% |
| completed throughput | 16.935 run/s | 15.442 run/s | 91.18% | ≥90% |
| lifecycle p95 | 274ms | 303ms | 110.58% | ≤120% |
| lifecycle p99 | 301ms | 612.44ms | 203.47% | 诊断值，已列入优化项 |
| 429 / 5xx / timeout | 0 / 0 / 0 | 0 / 0 / 0 | — | 必须 0 |
| runtime avg / peak CPU | 754.13m / 791m | 677.21m / 768m | — | 2 CPU limit |
| PostgreSQL avg / peak CPU | 1,747.25m / 1,897m | 1,732.09m / 1,967m | — | 4 CPU limit |
| global execution peak | 5 | 5 | — | ≤12 |

aged 运行完成后数据库为 190,172 Runs、3,233,518 execution events。aged 结果目录：
`bench/results/2026-07-26-qualification-v3/gate-c-aged-4vu-10m/`。

aged 吞吐只比 90% 硬门槛高 1.18 个百分点，且 p99 比 fresh 高约一倍，因此 Gate C 判定为通过，
但不是“历史成本可以忽略”。结合 Gate D 的 WAL/checkpoint 数据，历史生命周期与尾延迟优化继续
保持 P1；这不是引入 Redis 的依据，因为 Run transition 和 checkpoint 写入不能安全地从数据库旁路。

首轮 idle 观测发现 legacy Artifact retention repair 每分钟对全部闭合 Run 做一次反连接扫描：
5 分钟 `workflow_runs seq_scan=5`、`seq_tup_read=130,835`。修正为“terminal transaction 原子注册；
legacy repair 只由显式 verifier 调用”后，最终镜像 5 分钟结果为：

- `workflow_runs seq_scan=0`、`seq_tup_read=0`，190,172 条历史 Run 未被全扫；
- `execution_events seq_scan=0`、`seq_tup_read=0`，3,233,518 条历史 event 未被全扫；
- task/model/timer/recovery discovery 均为 index scan；
- recovery 空查询平均约 0.032ms；
- task candidate 180 次/300 秒，平均约 0.191ms；model discovery 仍为 index scan；
- retention 本轮存在真实过期工作：5 次处理 1,280 行，平均 13.52ms；单次 256 eligible row 的
  `EXPLAIN` 命中 `idx_public_outbox_retention`，0.695ms、944 hit + 44 read buffers，成本与
  eligible row 数相关；
- temp spill、deadlock 和全表扫描均为 0。

最终 idle 目录：
`bench/results/2026-07-26-qualification-v3/gate-c-aged-idle-5m/`。

## 8. Gate D：稳定性

最终 v3 结果目录：
`bench/results/2026-07-26-qualification-v3/gate-d-soak-10rps-2h/`。

| 指标 | 结果 | 判定 |
|---|---:|---|
| scheduled / dropped | 71,820 / 181 | dropped 集中于故障/存储抖动窗口 |
| accepted / terminal success | 71,801 / 71,801 | accepted 收敛率 100% |
| 按 scheduled 口径成功率 | 99.9735% | ≥99.9%，通过 |
| completed throughput | 9.961 run/s | ≥9 run/s，通过 |
| lifecycle p95 / p99 / max | 938ms / 2.971s / 11.102s | ≤5s / ≤8s，通过 |
| create p95 / p99 / max | 341.61ms / 1.806s / 9.039s | 诊断值 |
| create timeout / capacity / conflict / 5xx | 19 / 0 / 0 / 0 | 19 次未受理，不影响 accepted closure |
| 最终 lifecycle | 173,907 succeeded | 无残留 active Run |
| deadlock / conflict / OOM / spontaneous restart | 0 / 0 / 0 / 0 | 通过 |
| runtime avg / peak | 463.25m / 588m，41.19 / 52Mi | 2 CPU / 1Gi 内 |
| PostgreSQL avg / peak | 1,153.33m / 2,097m，3,203 / 5,637Mi | 4 CPU / 8Gi 内 |
| DB active / waiter / ungranted peak | 25 / 6 / 6 | pool 24 + sampler；最终排空 |
| runtime active Run peak | 50 | ≤50 |
| scheduler / recovery / all-operation peak | 3 / 11 / 12 | global permit ≤12 |
| queue oldest age peak / final | 8.620s / 0 | 峰值后完全排空 |
| PostgreSQL temp spill | 0 files / 0 bytes | 通过 |
| PostgreSQL throttled periods | 770 / 75,185（1.024%） | <10%，通过 |
| consistency sample | 100 Runs，六类 violation 均为 0 | 通过 |

故障与正确性验证：

- 20 分钟时主动终止 listener backend PID 29，reconnect counter `0→1`，listener state
  恢复为 1；
- 45 分钟时在 `claimed_tasks_before=1` 的窗口删除 runtime Pod，替代 Pod Ready；故障瞬间采样有
  2 claimed / 1 pending task，最终全部闭合且没有 active Run 残留；
- 替代 Pod `restartCount=0`，没有 workload-induced restart；
- 从最新 Run 中抽样 100 个，event sequence、projection、public event、delivery head、terminal
  snapshot 的不一致数均为 0；
- 最后约 30 分钟 runtime RSS/PSS 斜率为 `-52.2/-52.1 KiB/min`，cgroup memory
  `-0.042MiB/min`；PostgreSQL RSS 为 0、PSS `+5.85KiB/min`、cgroup memory
  `-17.84MiB/min`，关闭了旧结果的持续内存上升条件项；
- 最后约 30 分钟 DB active connection、lock waiter、active Run 的斜率分别为
  `-0.0229/-0.00034/-0.00867 每分钟`，没有持续积压。

### 8.1 PostgreSQL 锁与写入压力

2 小时内数据库 notification statement 为 27,256 次，即约 3.78 次/秒、0.380 次/accepted Run；
相对旧镜像 49.9 次/秒降低约 92.4%。PostgreSQL 日志没有 notification ordering lock
（`class 1262 / object 0`）记录。

仍有 790 次普通 transaction/tuple lock 等待超过 100ms，31 次超过 1 秒，2 次超过 5 秒，最大
8.557 秒；这些尖峰与 requested checkpoint/WAL 写压重合，但最终没有 deadlock、事务冲突或
queue residue。两小时产生约 66.16GiB WAL（约 9.40MiB/s），requested checkpoint 129 次，
checkpoint write time 累计 6,472.5 秒。这个结果表明下一项优化应是 checkpoint/WAL 平滑和历史
写放大，而不是把权威状态旁路到 Redis/MQ。

最终数据库物理大小为 35,883,154,455 bytes（约 33.4GiB）。主要关系：

| 关系 | total size |
|---|---:|
| `projection_checkpoints` | 13,294MiB |
| `execution_events` | 9,449MiB |
| `scheduler_checkpoints` | 3,274MiB |
| `public_event_projection_decisions` | 1,347MiB |
| `public_event_outbox` | 1,278MiB |
| projection batches | 965MiB |
| `public_event_receipts` | 713MiB |
| `task_outbox` | 556MiB |

24 小时 soak 属于 release-candidate gate，本报告不把 2 小时结果替代为 24 小时结论。根据
2026-07-27 的交付决定，它不再阻塞本轮实现与容量资格收尾，后续应在 always-on runner 上从零执行
完整 24 小时，并以独立报告补充 RC 结论。

## 9. Redis 与消息队列决策

### 9.1 当前不需要 Redis

不能缓存为权威的数据：

- Run lifecycle、projection version、admission slot；
- scheduler/task/model claim、lease 和 fence；
- timer、signal、human wait winner；
- public outbox、receipt、delivery head；
- execution event 和 checkpoint。

这些状态都参与事务、竞态或 replay。把它们复制到 Redis 只会引入失效、双写和故障切换问题，不能
删除 PostgreSQL 中必要的 per-Run 串行化。

Redis 的可选未来/P2 范围仅包括 immutable agent/deployment metadata、编译结果或允许短暂过期的只读
聚合；必须使用版本 key + TTL + cache-aside，并且 Redis 故障时可以安全回源。

### 9.2 当前不需要外部 MQ

核心 scheduler 不应把 Kafka/RabbitMQ/NATS 当权威。当前 PostgreSQL outbox + `NOTIFY` hint +
safety scan 已通过 50 burst、10 rps sustained、fresh/aged 和 v3 两小时故障 soak。v3 已把 runtime 写入产生的
wake request 合并为进程级跨进程 hint，并将 Public Event 本地投递改为 commit 后 durable-ID
delivery；远端订阅继续使用 100ms durable poll。因此不需要为了规避事务性 notification ordering
lock 引入外部 MQ。增加 MQ 仍会产生 DB commit 与 broker publish 的第二个一致性边界，也不能直接
减少 durable state 本身的写放大。

只有出现独立扩缩容的跨服务消费者时，才在 `public_event_outbox` 之后增加外部 MQ。数据库 receipt
继续记录发布权威，broker 使用 at-least-once，消费者按 public event ID 幂等。MQ 不进入 Run、
lease、timer、signal 或 admission 路径。

## 10. 后续优化优先级

已完成并保留回归门槛的 P1：

- runtime write wake request 进程级合并、Public Event commit 后本地 durable-ID 投递、远端
  100ms durable poll；v2 fresh/aged 仅约 0.238 database notifications/Run；
- `public_event_outbox` retention partial index 与 statement-stable deadline；aged 计划从
  66.1ms/102,200 buffers 降到 0.025ms/3 buffers，temp spill 为 0。

剩余优先级：

1. **P1 checkpoint/WAL 平滑化。** v3 两小时产生约 66.16GiB WAL、129 次 requested checkpoint，
   并在该窗口出现短暂 Run 行锁与 queue age 尖峰；应在真实生产存储上
   验证更大的 `max_wal_size`、`checkpoint_timeout`/`checkpoint_completion_target` 与恢复时间
   预算，不能仅在本地盘上提高参数后宣称延迟改善。
2. **P1 历史存储生命周期。** 应基于最终 soak 关系大小制定在线保留、归档、分区和
   checkpoint 压缩规范，首先控制 `projection_checkpoints` 与 `execution_events`。
3. **P1 数据库延迟直方图。** 当前用 SQLx 100ms slow-acquire warning、PostgreSQL lock log 和
   activity sampler 验证门槛；应把 pool acquire/Run row-lock p50/p95/p99 作为原生 exporter 指标。
4. **P1 多副本配额。** 当前 `max_concurrent_runs=50` 是单 runtime 进程上限；水平扩展前需要
   cluster/tenant admission 规范。
5. **P1 真实 workload 定容。** 分别压测 LLM、retrieval、大 Artifact、慢第三方 API 和云盘 IOPS；
   不能把本地 action 的 10 rps 直接外推。
6. **P2 只读缓存。** 只有 metrics/trace 证明 immutable metadata 读取成为热点后，再评估 Redis；
   不提前增加运维组件。
7. **P2 跨服务 fan-out。** 只有公共事件消费者需要独立扩缩容时，再评估外部 MQ。

## 11. 限制

- 本地单节点 Kubernetes 不覆盖跨 AZ 网络、云盘 fsync、managed PostgreSQL HA 或真实备份恢复；
- C1 的 local-path PVC 申请值不会像生产 CSI quota 一样限制实际节点磁盘，soak 必须同时报告真实
  database size；
- workload 不含真实 LLM、retrieval、human interaction 和大 Artifact；
- 单 runtime 通过不等于多副本全局 50 配额；
- v3 镜像已包含跨 runtime subflow 父子恢复条件、notification 合并、retention query 修复，以及
  terminal Public Event 的统一锁顺序；
- k6 在长测中因动态 URL tag 生成约 40 万 time series，内存峰值约 1.28GiB；压测脚本已把动态
  GET/signal URL 固定为低基数 `name` tag，服务端结论不受影响；
- v3 Gate D 最后 30 分钟未见 runtime 或 PostgreSQL 持续内存上升；
- 24 小时 release-candidate soak 尚无有效结果：两次本地尝试均因宿主机休眠失效，已延后到
  always-on runner 执行；这不影响本报告的 capacity-qualified 结论，但仍阻止
  release-candidate-qualified 表述。

## 12. 代码与配置验证

- `cargo fmt --all -- --check`；
- `cargo check --workspace --all-targets`；
- `cargo clippy --workspace --all-targets -- -D warnings`；
- 临时 PostgreSQL 16 上
  `TEST_POSTGRES_URL=... cargo test --workspace --all-targets -- --test-threads=1` 全绿；
- benchmark、limited、C1、C2 四组 Helm `lint` 与 `template` 全绿；
- 全部 benchmark shell 脚本通过 `bash -n`；
- `grafana/k6:1.7.1 inspect` 验证两个 k6 profile 可解析。
