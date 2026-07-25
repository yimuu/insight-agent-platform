# Kubernetes 有限资源压测报告

日期：2026-07-25

状态：实测完成，服务保持在线

对象：Insight Agent Platform `action_demo` durable lifecycle

## 1. 结论摘要

Helm 部署、Kubernetes 服务启动和完整生命周期压测均已完成。当前 release `bench` 位于
`insight-bench` namespace，runtime 与 PostgreSQL 都是 `Ready`，最终 `/health/ready` 返回
`{"code":"OK","message":"ok","data":{"status":"ok"}}`。

结论不是“触发器多所以一定扛不住”，而是更具体的：

1. **原有限资源配置确实存在明显并发瓶颈。** runtime 与 PostgreSQL 各限制为
   `500m CPU / 256Mi`，但原配置启动了 16 个 operation worker。4 VU 时 lifecycle p95 已从
   1 VU 的 405ms 上升到 5.386s，完成吞吐反而从 2.88 降到 1.50 run/s。
2. **主要瓶颈已不是 `public_event_delivery_heads` 单行 UPSERT。** `pg_stat_statements` 的主要耗时
   是 `workflow_runs ... FOR UPDATE`、late-event 全表扫描、`task_outbox` 高频发现和空队列轮询。
   `public_event_delivery_heads` 没有进入主要耗时 SQL，说明此前减少无意义 head 写锁的方向有效，
   但没有消除整个 durable scheduler 的数据库轮询成本。
3. **资源上限能够保护进程，但会带来严重排队。** 16 VU 下 PostgreSQL 达到
   `501m / 254Mi`，lifecycle p95 为 14.845s。没有 OOM、deadlock 或 Pod 重启，但已经没有延迟余量。
4. **32 VU 会触发 admission 背压。** 7,470 次迭代中只有 90 次创建成功并完成，7,380 次快速失败；
   create 成功率 1.20%。当前 API 将 `RUN_CAPACITY_EXCEEDED` 映射为 HTTP 409，压测器无退避时会形成
   快速重试风暴。
5. **状态累积会显著恶化固定轮询成本。** 原配置连续压测后，同样 1 VU/15s 从最初完成 44 个降到
   9 个，p95 从 405ms 变为 3.371s。累积库只有 415 个已成功 Run，但已有 9,533 条
   `execution_events`、14,789 条 `projection_checkpoints`，关系总大小约 95.4MiB。
6. **有限资源调优有效但未根治。** 把 operation worker 从 16 降到 2 后，4 VU 完成数从 50 提高
   到 112，完成吞吐从 1.50 提高到 3.67 run/s（约 +145%），lifecycle p95 从 5.386s 降到
   1.315s（约 -76%），锁等待峰值从 7 降到 2。PostgreSQL 在 4 VU 时仍会触达 CPU 上限，因此
   下一阶段应优化轮询/扫描，而不是继续提高 worker 数。

## 2. 测试环境

| 项目 | 配置 |
|---|---|
| Kubernetes | OrbStack 单节点，Kubernetes `v1.34.8+orb1` |
| 节点 | ARM64，宿主分配 18 CPU / 16GiB；节点本身不是瓶颈 |
| runtime | request `100m/128Mi`，limit `500m/256Mi` |
| PostgreSQL | 16-alpine，request `100m/128Mi`，limit `500m/256Mi` |
| PostgreSQL 参数 | `shared_buffers=64MB`、`max_connections=100`、`track_io_timing=on` |
| 数据库传输 | chart 内部 CA，TLS，runtime `sslmode=verify-full` |
| k6 | `grafana/k6:1.7.1`，limit `1 CPU/256Mi` |
| 资源采集 | Metrics Server 0.8.1、`kubectl top`、cgroup v2 |
| Artifact | 1Gi `ReadWriteOnce` PVC |
| PostgreSQL 数据 | benchmark `emptyDir`，明确仅用于本地测试 |

Metrics Server 用于 CPU/内存采样；其定位是 Kubernetes resource metrics pipeline，不是长期监控
系统。k6 Job 负责发起真实 HTTP 生命周期负载。工具说明见
[Metrics Server 官方文档](https://kubernetes-sigs.github.io/metrics-server/)和
[Grafana k6 官方文档](https://grafana.com/docs/k6/latest/get-started/running-k6/)。

## 3. 负载模型与指标口径

每个 k6 VU 循环执行：

1. `POST /v1/agents/action_demo/runs`；
2. 校验 HTTP 202、业务 code 和 `run_id`；
3. 轮询 `GET /v1/runs/{run_id}`；
4. 直到 `completed`，或记录失败/超时。

`action_demo` 只执行本地 `example.text_metrics` action，不调用 LLM 和外部网络。因此本报告衡量的是
durable admission、事务状态转移、scheduler、projection、public event 和查询链路，不包含模型
服务延迟。

`completed/s` 使用 `completed / (k6 总墙钟时间 - 8s 启动等待)`，包含测试结束后的 graceful drain；
它比简单使用配置 duration 更保守。资源峰值来自 5 秒抓取，但 Metrics Server 的底层分辨率约
15 秒；CPU throttle、memory limit 和 OOM 使用 cgroup 精确累计计数。

## 4. 压测结果

前七组是原配置（16 operation workers）并连续复用同一数据库；后两组是调优配置
（2 operation workers）和新 benchmark 数据库。

| profile | completed | failed/rejected | success | completed/s | create p95 | lifecycle p50 | lifecycle p95 | lifecycle p99 | runtime peak | PostgreSQL peak | max lock waiters |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| smoke, 1 VU/15s | 44 | 0 | 100.00% | 2.88 | 112ms | 323ms | 405ms | 643ms | 165m/39Mi | 475m/181Mi | 2 |
| baseline, 4 VU/30s | 50 | 0 | 100.00% | 1.50 | 2003ms | 2278ms | 5386ms | 5760ms | 181m/41Mi | 501m/223Mi | 7 |
| step, 8 VU/30s | 55 | 0 | 100.00% | 1.71 | 2717ms | 4448ms | 6952ms | 7336ms | 191m/44Mi | 501m/242Mi | 6 |
| overload, 16 VU/30s | 50 | 0 | 100.00% | 1.19 | 5489ms | 11308ms | 14845ms | 15701ms | 195m/42Mi | 501m/254Mi | 6 |
| overload, 32 VU/30s | 90 | 7380 | 1.20% | 2.36 | 99ms* | 6414ms | 8588ms | 9325ms | 289m/45Mi | 502m/249Mi | 3 |
| soak, 1 VU/120s | 74 | 0 | 100.00% | 0.60 | 1685ms | 1104ms | 4706ms | 5424ms | 126m/40Mi | 502m/250Mi | 8 |
| post-soak, 1 VU/15s | 9 | 0 | 100.00% | 0.52 | 1803ms | 1675ms | 3371ms | 3397ms | 94m/37Mi | 501m/240Mi | 8 |
| tuned fresh, 1 VU/15s | 59 | 0 | 100.00% | 3.93 | 87ms | 251ms | 301ms | 411ms | 105m/9Mi | 244m/186Mi | 2 |
| tuned, 4 VU/30s | 112 | 0 | 100.00% | 3.67 | 297ms | 1078ms | 1315ms | 1384ms | 216m/38Mi | 501m/244Mi | 2 |

\* 32 VU 的 create p95 被大量快速 409 拒绝“美化”，不能当成功请求延迟解读。

### 4.1 原配置的饱和点

1 VU 时 PostgreSQL 已接近 CPU limit；增加到 4 VU 后吞吐没有增长，延迟却增加一个数量级。
因此原配置在该资源预算下的有效饱和点低于 4 VU。16 VU 的成功率仍是 100%，但这是靠最长
15.8 秒排队换来的，不是健康容量。

### 4.2 过载行为

32 VU 没有击穿进程或数据库：

- runtime、PostgreSQL、k6 均无 workload-induced restart；
- cgroup `oom_kill=0`；
- PostgreSQL `deadlocks=0`、`conflicts=0`；
- Job 结束后 readiness 恢复并保持正常。

但当前 409 容量拒绝会被无退避客户端高速重试。生产客户端至少应指数退避并加 jitter；服务端可评估
将容量耗尽映射为 429 并返回 `Retry-After`，但这属于 API 语义变更，需要单独设计。

### 4.3 稳定性与资源限制

120 秒 soak 期间 PostgreSQL cgroup：

- `nr_periods` 增加 1,366，`nr_throttled` 同样增加 1,366，即每个统计周期都发生 CPU throttle；
- `throttled_usec` 增加约 647.6 秒，多进程累计 throttle 时间明显高于 120 秒墙钟；
- `memory.peak=268,435,456`，正好等于 256Mi limit；
- `memory.events.max` 增加 193；
- `oom=0`、`oom_kill=0`。

runtime 同期没有 CPU throttle、memory max 或 OOM 事件。瓶颈明确位于 PostgreSQL，而不是 Rust
HTTP 进程。

## 5. 数据库证据

累积状态达到 415 个 Run 时，所有 Run 都是 `succeeded/closed`。关键精确行数：

| 表 | 行数 |
|---|---:|
| `workflow_runs` | 415 |
| `execution_events` | 9,533 |
| `projection_checkpoints` | 14,789 |
| `public_event_projection_decisions` | 9,533 |
| `public_event_outbox` | 2,075 |
| `public_event_receipts` | 2,075 |
| `public_event_delivery_heads` | 415 |
| `task_outbox` | 415 |

关系总大小约 95.4MiB，最大的三张表是：

- `projection_checkpoints`：约 32.2MiB；
- `execution_events`：约 26.8MiB；
- `scheduler_checkpoints`：约 8.2MiB。

post-soak 约 25 秒采样窗口内：

- `task_outbox`：13,151 次顺序扫描，读取约 543 万 tuples；
- `execution_events`：607 次顺序扫描，读取约 576 万 tuples；
- `workflow_runs`：1,947 次顺序扫描，读取约 77 万 tuples。

主要耗时 SQL：

| SQL 形态 | calls | total | mean |
|---|---:|---:|---:|
| `SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE` | 245 | 11.09s | 45.26ms |
| late-event `execution_events` 扫描 | 218 | 5.96s | 27.34ms |
| `task_outbox` candidate discovery | 4,611 | 4.58s | 0.99ms |
| 空 `model_tool_call_batches` 轮询 | 9,192 | 1.70s | 0.19ms |

这说明当前问题由三部分叠加：

1. 16 个 worker 每 25ms 轮询 durable queues，产生高固定事务率；
2. 已终态 Run 的 durable 数据继续增加全局扫描成本；
3. 同一个 Run 的事件写入仍在 `workflow_runs` 行锁处串行化。

`public_event_delivery_heads` 更新没有进入主要耗时 SQL。当前证据支持“此前 head 快路径优化基本命中
原热点”，但不能推导为数据库并发风险已经全部解决。

## 6. 已落地的 Helm/有限资源调整

本次交付已包含：

- 多阶段 Rust 1.94.1 容器镜像，非 root runtime；
- Helm Deployment、Service、ConfigMap、Secret、Artifact PVC；
- 内置 PostgreSQL 16 StatefulSet；
- 从 canonical `database/durable/postgres/schema.sql` 初始化新数据库，不复制 schema；
- PostgreSQL UID/GID 70 非 root 与 `PGDATA` 子目录；
- 内部 CA/证书、PostgreSQL TLS、runtime `verify-full`；
- `pg_stat_statements` 与 `track_io_timing`；
- 单副本默认 `Recreate`，防止 RWO Artifact volume 升级期间出现不一致；
- Artifact PVC 持久化，避免 Pod 替换后 store authority 改变；
- k6 Job、结果 JSON、Kubernetes resource、cgroup、DB lock/SQL 采样；
- 有限资源 values 将 operation worker 从 16 调整为 2。

## 7. 容量建议

在当前 `500m/256Mi` PostgreSQL 上：

- 推荐把 `maxConcurrentOperations` 保持为 2；
- `maxConcurrentRuns=8` 只作为 admission 保护上限，不代表可同时维持 8 个低延迟 Run；
- 若目标是 lifecycle p95 ≤ 1.5s，可暂以调优后的 4 VU、约 3.5 completed run/s 作为短时上限；
- 在解决状态累积退化前，生产安全容量应保守取 **2–3 completed run/s**，并用真实保留周期进行更长
  soak 验证；
- 不能通过单纯提高 worker 数获得吞吐；它会增加 PostgreSQL 轮询和锁竞争。

下一轮优化优先级：

1. **P0：合并/唤醒 worker polling。** 用共享 dispatcher、LISTEN/NOTIFY 或空队列指数退避，避免每个
   worker 独立以 25ms 轮询。
2. **P0：修复终态数据参与全局发现扫描。** 对 `task_outbox`、late-event reconciliation 和 recovery
   查询做状态范围收敛；对候选 partial index 先用真实数据 `EXPLAIN (ANALYZE, BUFFERS)` 验证。
3. **P0：增加原生可观测性。** 暴露 admission、active runs、queue depth、poll cycles、claim latency、
   DB pool wait、capacity rejection 和 event publication latency。
4. **P1：定义 durable retention/归档策略。** 当前 415 个短 Run 已生成 95.4MiB 关系数据；应基于审计
   合同决定 checkpoint/event 的在线保留、归档和分区边界。
5. **P1：客户端/服务端背压语义。** 避免容量拒绝后的无退避重试风暴。

## 8. 限制

- 本测试是单节点本地 Kubernetes，不覆盖跨节点网络、云盘 IOPS、真实 RWX Artifact storage 或
  managed PostgreSQL；
- workload 不包含 LLM、retrieval、human task 和大 Artifact；
- 原配置阶梯连续复用数据库，适合暴露状态累积问题，但不是严格隔离的并发对比；
- 调优 A/B 使用重建后的 benchmark PostgreSQL，1 VU/4 VU 基线与原测试初始数据规模接近，但不是
  bit-for-bit 相同数据库；
- 120 秒是短 soak，只能发现快速漂移，不能替代数小时/数天稳定性测试；
- `kubectl top` 峰值受 Metrics Server 分辨率影响，cgroup 累计计数更可信。

## 9. 复现

```bash
docker build --platform linux/arm64 \
  -t insight-agent-platform:benchmark .

helm upgrade --install bench deploy/helm/insight-agent-platform \
  --namespace insight-bench \
  --create-namespace \
  --values deploy/helm/insight-agent-platform/values-benchmark.yaml \
  --wait --timeout 5m

bash bench/k8s/run-profile.sh smoke 1 15s
bash bench/k8s/run-profile.sh baseline 4 30s
bash bench/k8s/run-profile.sh soak 1 2m

bash bench/k8s/summarize-results.sh bench/results
```

原始结果位于 `bench/results/2026-07-25-k8s-limited/`，包括每个 profile 的：

- `summary.json`、`k6.log`；
- `resources.csv`、runtime/PostgreSQL cgroup before/after；
- `database-activity.csv`、`database-top-statements.csv`；
- Pod、event、最终 Helm values/manifest 和 Ready 响应。
