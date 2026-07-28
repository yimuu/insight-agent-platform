# Terminal-only 验收与 WAL 资格

状态：Current

## 当前资格进度

资格状态：**Qualified（2026-07-28）**

Phase 0、Gate A～D、最终静态验证链和规范完成定义 1～12 已全部通过。完整结论见
[资格报告](../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)，
原设计已归档为
[Implemented / capacity-qualified 规范](../archive/specs/2026-07-27-terminal-only-runtime-and-conversations.md)。
资格通过不自动授权修改默认值；独立
[rollout 决策](../archive/reviews/2026-07-28-terminal-only-default-rollout-decision.md)
仍要求 Quickstart、chart 和未声明 Deployment Revision 使用 `full`，`terminal_only` 只由兼容的
immutable Deployment Revision 显式 opt-in。

| 阶段 | 当前状态 | 正式证据 | 已闭合事实 |
|---|---|---|---|
| Phase 0 | Passed | `bench/results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/phase0-full-report.json` | 600/600 warm-up、6,000/6,000 measured 全部成功；`1,839,028,207` WAL bytes；物理记录对 `pg_stat_wal`/LSN 覆盖为 `1.000212/0.992292`，分类覆盖 `1.0` |
| Gate A | Passed | `bench/results/2026-07-28-terminal-only-qualified/gate-a-standalone-final2-pass/gate-a-report.json`；`bench/results/2026-07-28-terminal-only-qualified/gate-a-conversation-final2-pass/gate-a-report.json` | standalone `+1/+1/+0`、Conversation `+1/+1/+2`；54 个 forbidden durable table 的 row delta 与 mutation call 均为 0 |
| Gate B | Passed | `bench/results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/gate-b-report.json` | 600/600 warm-up 已排除；72,000/72,000 measured，0 drop/late/reject/interrupted；WAL `347,968,068` bytes、`4,832.89` bytes/Run；关系增长 `1,418.70` bytes/Run；lifecycle p95/p99 `57/63ms` |
| Gate C | Passed | `bench/results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/gate-c-suite-report.json` | batch `final2-pass5-20260728` 的 8 个子场景和 11 项组合断言全部为 `true` |
| Gate D | Passed | `bench/results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/gate-d-report.json` | 100×100 turns；10,000 fresh acceptances；26 次容量拒绝均按同 request/payload 重试；context、summary、1×/10× stream、百万行查询、privacy 与 encryption 全部通过 |

Gate B 同一连续窗口保持
`fsync=on`、`full_page_writes=on`、`synchronous_commit=on`，54 张 full durable 表 delta
全部为 0，物理 WAL 覆盖率为 `1.0008314585`。结束后 workload 和 port-forward 已清理，8Gi/2Gi
PVC 保留正式数据；见
[`cleanup-evidence.json`](../../bench/results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/cleanup-evidence.json)。

本页定义 terminal-only Gate A～D 的可重复验收方法。脚本位于
[`bench/terminal-only`](../../bench/terminal-only)，所有判定均 fail-closed：缺少
`pg_stat_statements`、弱化 PostgreSQL durability、没有产生故障窗口，或缺少规定规模时都会失败，
不会把 smoke 结果标成 qualification。

## 前置条件

- PostgreSQL 16 已用当前 schema 在空目标完成 provisioning；
- `pg_stat_statements` 已预加载并创建，运行用户可读取统计并执行
  `pg_stat_statements_reset()`；
- PostgreSQL 16 提供 `pg_walinspect`，运行用户可在测量边界外创建扩展并调用
  `pg_get_wal_records_info(start_lsn, end_lsn)`；
- `track_io_timing=on`、`pg_stat_statements.track=all`、
  `pg_stat_statements.track_utility=on`，且
  `pg_stat_statements` 暴露 `wal_bytes`；
- 运行用户可读取 `pg_stat_user_tables` 的 per-table `autovacuum_count`、
  `autoanalyze_count`、`last_autovacuum` 与 `last_autoanalyze`；这些字段与
  `pg_stat_database.stats_reset` 一起形成后台 maintenance 证据；
- `fsync=on`、`full_page_writes=on`，`synchronous_commit` 为 `on` 或更强；
- 一个 runtime owner replica，terminal-only concurrency 为 50；
- `action_demo` 的 Deployment Revision 显式冻结
  `execution.persistence_mode: terminal_only`；
- `BASE_URL` 指向 runtime HTTP 服务；
- 安装 `curl`、`jq`、Python 3 和 Grafana k6；
- 使用 Kubernetes 故障脚本时还需要 `kubectl` 与目标 namespace 权限。

数据库连接有两种方式。直接连接时设置
`TERMINAL_BENCH_POSTGRES_URL`；Kubernetes 内置 PostgreSQL 则设置
`BENCH_NAMESPACE`、`BENCH_RELEASE`，脚本会在确定的 PostgreSQL Pod 内运行 `psql`。资格结果目录必须
保留镜像 digest、commit、最终 Helm values/manifest、节点和 PostgreSQL 版本；可以继续使用
[`bench/k8s/run-profile.sh`](../../bench/k8s/run-profile.sh) 的环境捕获作为外层封装。

Kubernetes 资格必须显式叠加
[`values-terminal-only-qualification.yaml`](../../deploy/helm/insight-agent-platform/values-terminal-only-qualification.yaml)；
该 overlay 才会启用 provider-idempotent effect/attempt ledger、确定性故障 Agent 和私有 fault
delay，并要求 Secret-backed tenant Artifact encryption。普通 chart 默认
不发布这些测试能力。正式 Gate B 必须先由
`preflight-fresh-qualification.sh` 确认目标 namespace、同名 Helm release 和 PVC 均不存在，再创建
带不可复用 UID/preflight-id 的 namespace；`deploy-stream-fixture.sh` 随后创建（或校验已存在的）
`terminal-tenant-keyring` Secret，避免在 namespace 尚不存在时静默漏掉加密前置；它只输出
Secret 名和 active key version，不输出 key。默认自动生成一次 `qualification-v1` key；复用
namespace 时保留现有 Secret。需要显式导入时可设置
`TENANT_ARTIFACT_KEYRING_SECRET`、`TENANT_ARTIFACT_KEY_VERSION` 和
64 位小写 hex 的 `TENANT_ARTIFACT_KEY_HEX`，同时让 overlay 的 Secret 名/key version 与之相同。
qualification overlay 还固定 PostgreSQL PVC 为 `8Gi`、Artifact PVC 为 `2Gi`；前者为
`max_wal_size=4GB`、`wal_keep_size=3GB` 和两小时 WAL 上限保留空间，不能退回 chart 默认的
`2Gi` PostgreSQL PVC。普通 chart 的 `wal_keep_size` 保持 `0`，不会无条件扩大生产 WAL 保留。
runtime Pod 的 `terminationGracePeriodSeconds=40`，严格大于应用
`shutdown_hard_deadline=35s`；chart render 会拒绝 Kubernetes deadline 小于或等于应用 hard
deadline 的值，避免 graceful Gate 在应用完成终态提交前被 kubelet SIGKILL。
然后部署流式 mock 并安装 C1 资格档位：

```bash
BENCH_NAMESPACE=insight-terminal-wal \
BENCH_RELEASE=terminal-wal \
  bash bench/terminal-only/preflight-fresh-qualification.sh \
  bench/results/terminal-only/gate-b-predeploy-freshness.json

BENCH_NAMESPACE=insight-terminal-wal \
TENANT_ARTIFACT_KEYRING_SECRET=terminal-tenant-keyring \
TENANT_ARTIFACT_KEY_VERSION=qualification-v1 \
  bash bench/terminal-only/deploy-stream-fixture.sh

helm upgrade --install terminal-wal deploy/helm/insight-agent-platform \
  -n insight-terminal-wal \
  -f deploy/helm/insight-agent-platform/values-benchmark.yaml \
  -f deploy/helm/insight-agent-platform/values-benchmark-c1.yaml \
  -f deploy/helm/insight-agent-platform/values-terminal-only-qualification.yaml
```

## Phase 0：独立 full-runtime 基线

Gate A～D 之前的 Phase 0 使用独立的
[`bench/phase0-full`](../../bench/phase0-full) harness，对现有 `full` durable engine
取证；它不套用 terminal-only 的旧 ledger `+0`、16KiB/Run 或 32KiB/Run 门槛。正式 profile
固定 `action_demo` 和字节不变的请求体，先执行不计入边界的 1 分钟 warm-up，再以 10 arrivals/s
运行恰好 10 分钟，共 6,000 个 scheduled arrivals。

旧 full 结果的两小时 WAL 精确差值为 `71,033,480,938` bytes、accepted 为 `71,801`，约等于
同速率每 10 分钟 5.5GiB WAL。为了在 workload 后读取完整精确 LSN 区间，Phase 0 必须最后叠加
[`values-phase0-full-baseline.yaml`](../../deploy/helm/insight-agent-platform/values-phase0-full-baseline.yaml)：
它固定单副本 `full`、仅发布 `action_demo`，使用 PostgreSQL/Artifact `24Gi/2Gi` PVC、
`max_wal_size=4GB` 和 `wal_keep_size=8GB`。必须从
`preflight-fresh-qualification.sh` 证明不存在的 namespace/release/PVC 开始；数据库 preflight
还会拒绝已有 workload rows 或非 `full` 的 publication。

```bash
BENCH_NAMESPACE=insight-phase0-full \
BENCH_RELEASE=phase0-full \
  bash bench/terminal-only/preflight-fresh-qualification.sh \
  bench/results/phase0-full-preflight.json

helm upgrade --install phase0-full deploy/helm/insight-agent-platform \
  -n insight-phase0-full \
  -f deploy/helm/insight-agent-platform/values-benchmark.yaml \
  -f deploy/helm/insight-agent-platform/values-benchmark-c1.yaml \
  -f deploy/helm/insight-agent-platform/values-phase0-full-baseline.yaml

BASE_URL=http://127.0.0.1:3000 \
BENCH_NAMESPACE=insight-phase0-full BENCH_RELEASE=phase0-full \
PHASE0_FULL_PREFLIGHT_EVIDENCE=bench/results/phase0-full-preflight.json \
  bash bench/phase0-full/run-phase0-full.sh qualification \
  bench/results/phase0-full-10rps-10m
```

before/after snapshot 保存 `pg_stat_wal`、top-level `pg_stat_statements` top-30 与 all
aggregate、table/index/row、checkpoint/IO 和 Artifact volume bytes。nested statement 仅作诊断，
绝不与 top-level 相加。独立 `pg_walinspect` evaluator 对相同 start/end LSN 内的每条 record
只计一次，并按 resource manager/record type 分组；physical record bytes 对 `pg_stat_wal` 和
LSN byte span 的覆盖均必须在 95%～105%。block reference 会把 heap/index/TOAST 映射回 root
relation，分别报告 payload、Artifact metadata、structural、mixed 和 unmapped；payload/object
与 structural 的可解释覆盖必须至少 95%，外部 Artifact object bytes 另按 volume delta 报告。
旧两小时值只保留作对比，不能重新标成新一轮的 ≥95% 归因证据。复现、容量原因和人工报告模板见
[`bench/phase0-full/README.md`](../../bench/phase0-full/README.md) 与
[`report-template.md`](../../bench/phase0-full/report-template.md)。

## Gate A：精确写路径

先运行独立 Run，再运行 Conversation turn：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-gate-a.sh standalone \
  bench/results/terminal-only/gate-a-standalone

BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-gate-a.sh conversation \
  bench/results/terminal-only/gate-a-conversation
```

脚本在固定小型 `action_demo` 前后采样。standalone 必须得到 admission `+1`、result `+1`、
Conversation message `+0`；Conversation 必须得到 `+1/+1/+2`。判定器从 `pg_class` 自动枚举
`public` schema 的全部永久表，只排除 terminal-only 允许表、UNLOGGED owner 与 schema-contract
元数据；因此 `payloads`、`full_conversation_turns`、artifact authority/GC/retention、
definition/publication 以及 execution/checkpoint/scheduler/public ledger 都是 fail-closed
denylist，before/after 表集合必须相同且每个 row delta 都为 `0`。报告同时保存 statement 级
mutation 分类，要求同一完整 denylist 没有 INSERT/UPDATE/DELETE/TRUNCATE。admission/result
INSERT 各恰好一行，Conversation INSERT 恰好两行，并拒绝 admission/result
UPDATE/DELETE/TRUNCATE。这里的 `pg_stat_statements` 证明实际 SQL 调用与行数；admission 和
terminal-result 分属两个 repository transaction boundary 是代码契约，不从数据库总事务计数
反推。轮询次数或 SSE frame 数不能改变上述行数。

## Gate B：10 arrival/s、2 小时 WAL

正式资格：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
BENCH_NAMESPACE=insight-terminal-wal \
BENCH_RELEASE=terminal-wal \
GATE_B_PREFLIGHT_EVIDENCE=bench/results/terminal-only/gate-b-predeploy-freshness.json \
  bash bench/terminal-only/run-gate-b.sh qualification \
  bench/results/terminal-only/gate-b-10rps-2h
```

短 smoke 只验证工具链、采样和判定器，不能替代 Gate：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-gate-b.sh smoke \
  bench/results/terminal-only/gate-b-smoke
```

formal runner 在 warm-up 前重新校验 namespace UID/preflight-id、唯一 deployed release、两个新建
Bound PVC（PostgreSQL `8Gi`、Artifact `2Gi`），并保存 infrastructure freshness JSON。随后数据库
preflight 自动枚举所有旧 ledger，要求 admission/result/Conversation/历史 Run/GC 等行总数为 0，
明确包含 `artifact_gc_sweeps=0`；只允许当前 7 个 qualification Agent 的 catalog/revision/publication
行和一个 Artifact authority 行，7 个 resolved `deployment_policy` 必须全部为 `terminal_only`。
因此错误复用外部数据库、旧 PVC 或曾跑过 workload 的 namespace 会在 warm-up 前失败。

warm-up 在采样前独立完成；`pg_walinspect` 扩展也在测量边界外创建。之后重置的只有 statement
accounting，`pg_stat_wal` 和 checkpoint 计数
始终以单调 before/after 差值计算。正式 profile 固定 10 arrivals/s、单 runtime、最多 50 VU、
`action_demo`、2 小时。判定器要求：

- accepted closure 100%、scheduled success 至少 99.9%、完成吞吐至少 9/s；
- lifecycle p95/p99 不超过 1s/3s；
- WAL 不超过 32KiB/accepted 且两小时不超过 2.2GiB；
- terminal core relation 增长不超过 16KiB/accepted；
- requested checkpoint、deadlock、temp file、dropped iteration、rejection 和 forbidden durable-table delta
  都为 0。

after snapshot 内嵌的 `top_wal_statements` 是 SQL 诊断的权威输入；
`postgres-top-wal-statements.csv` 只能由该数组机械导出，报告会逐行数和总 `wal_bytes` 校验，
不接受第二次独立查询。固定 fixture 的
admission/result heap 与索引 WAL 报为 `structural WAL`；Artifact/object store 与非固定大 payload
另报 `payload/object WAL`；两者之和报 `total WAL`。不得以关闭 fsync、full-page writes、
synchronous commit，或把 admission/result 改为 UNLOGGED 的方式通过。

`pg_stat_statements.track=all` 会同时呈现 parent 与 nested statements；两者相加会双计。因此
top-30 和参与对账的 all-SQL aggregate 都严格限定 `toplevel IS TRUE`，nested calls/WAL 只单独
诊断。报告永久保存 raw top-30/total 与 top-level-all/total；top-30 必须覆盖至少 95% top-level
tracked SQL，top-level-all 不得超过 `pg_stat_wal` interval 的 105%，
`pg_stat_statements_info.dealloc` 必须全程不变。

完整 WAL 来源视图来自独立的物理证据链：after snapshot 后以其 exact before/after
`wal_insert_lsn` 调用 `pg_get_wal_records_info`，按 `resource_manager + record_type` 汇总 record
count、`record_length`、`main_data_length` 和 `fpi_length`，保存 authoritative JSON 及机械导出的
CSV。报告严格核对 LSN、extension version、每组与 totals、JSON/CSV 全字段，并要求物理
`record_length` 合计覆盖 `pg_stat_wal` delta 的 95%～105%。SQL 聚合和物理聚合是两个独立视图，
绝不相加。before/after LSN 必须为 canonical `pg_lsn` 且严格递增，statement/captured timestamps
必须满足 `before.statement <= before.captured < after.statement <= after.captured`。

`pg_stat_user_tables` 的 autovacuum/autoanalyze counters、timestamps 和连续 stats epoch 仍保存在
报告中，但它们只提供相关性诊断；PostgreSQL 没有按 maintenance operation 暴露 WAL byte counter，
所以即使观察到一次或多次 maintenance，也绝不为残差分配任何字节。C1 的
`wal_keep_size=3GB` 必须在 before/after snapshot 中稳定且不低于 3GiB，确保 ≤2.2GiB 的 exact LSN
区间在物理检查前不会回收。

正式 workload 的 configured duration 必须恰好 7200 秒，k6 实际 duration 必须在
7200～7320 秒内。runner 使用 `shared-iterations` 把 72000 个唯一 ordinal 映射到相隔 100ms 的
绝对目标时隙；要求 raw `iterations == terminal_run_arrivals_scheduled == 72000`，
`dropped_iterations == terminal_run_arrivals_late == 0`，且调度迟到
p95 ≤ p99 ≤ max < 100ms。不能用短跑结果除以固定 7200 伪造吞吐，也不能接受因 duration
边界多发或少发一个 arrival。`terminal_run_admissions` row delta 必须精确等于 accepted，
`terminal_run_results` row delta 必须精确等于 terminal observed。

脚本同时每秒采样 `terminal_run_active`，保存 admissions/results/interrupted/commit-retry 指标、
runtime `VmRSS/VmHWM/PSS`、cgroup OOM、Pod restart、Artifact volume 字节、事务数以及
`pg_stat_io` write/fsync 时间。Kubernetes 外运行时必须设置 `BENCH_RUNTIME_PID` 和
`BENCH_ARTIFACT_HOST_ROOT`；正式 qualification 缺少 restart、VmHWM 或 cgroup OOM 证据会失败。
sampler 意外退出会立即失败；每个 timestamp block 必须恰有一个 finite 非负整数 active 值，
epoch 严格递增、最大 gap 5 秒，sample count 与首尾 span 都必须覆盖至少 95% 正式区间。
Deployment desired/ready replica 和完整 selected Pod set 在 before/after 都必须是精确 1，
且唯一 Ready/non-deleting Pod UID 不变（本地运行则为同一 PID）。`pg_stat_wal`、`pg_stat_bgwriter`、
`pg_stat_database`、`pg_stat_io` 的 `stats_reset` 必须连续，关键 counter、row、runtime、OOM、
restart、relation 与 Artifact delta 均不得为负。accepted/terminal-observed/succeeded 与
lifecycle percentile 等必需 k6 metric 必须存在、可解析且为 finite；不会因字段缺失而默认为 0。
C1 的 `max_wal_size=4GB` 只防止 ≤2.2GiB 的剩余资格预算自行触发 size-checkpoint；
`wal_keep_size=3GB` 只保留物理检查区间；二者都不关闭 durability，也不能替代写路径删减。
durability 会在 warm-up 后和 workload 后再次断言；admission/result、deletion/staging、
Conversation/messages/summaries/tombstones/summary-jobs 全部必须是永久 LOGGED，
owner registry 必须保持 UNLOGGED。

## Gate C：真实进程故障

Gate C 需要一个 terminal-only 故障 fixture Agent：它必须是兼容的有界 action/LLM 调用，并保证
50 个 admission 中至少一个在 kill 时没有 result。`action_demo` 通常太快，脚本发现所有 Run 已完成
会把本次试验判为无效，而不是伪造 interruption。

runtime kill：

```bash
BASE_URL=http://127.0.0.1:3000 \
GATE_C_AGENT_ID=terminal_failure_fixture \
BENCH_NAMESPACE=insight-bench BENCH_RELEASE=bench \
  bash bench/terminal-only/run-gate-c.sh \
  bench/results/terminal-only/gate-c-runtime-kill
```

PostgreSQL restart 使用同一 harness，但必须使用持久 PVC：

```bash
BASE_URL=http://127.0.0.1:3000 \
GATE_C_AGENT_ID=terminal_failure_fixture \
GATE_C_KILL_TARGET=postgres \
BENCH_NAMESPACE=insight-bench BENCH_RELEASE=bench \
  bash bench/terminal-only/run-gate-c.sh \
  bench/results/terminal-only/gate-c-postgres-restart
```

正式资格应运行组合 harness；它会临时滚动设置 admission 后、terminal commit 后和 summary
worker 三个 qualification-only 私有 delay，并在退出时复原为零：

```bash
BASE_URL=http://runtime.example \
BENCH_NAMESPACE=insight-terminal-qual BENCH_RELEASE=terminal-qual \
  bash bench/terminal-only/run-gate-c-suite.sh \
  bench/results/terminal-only/gate-c-suite
```

harness 等待精确 50 admission、50 unresolved 且
`terminal_run_active == 50` 后才注入故障。hard runtime/PostgreSQL 路径先从目标 Pod 的
`containerStatuses` 保存正确容器的 `containerID` 与 `restartCount`，再绑定目标 PID 与
`/proc/<pid>/stat` start-time ticks 的进程 incarnation；trigger exec 必须在发信号前重新读到同一
token。脚本还会从触发前取得的精确 Pod resourceVersion 预挂 container-status watch，并预挂目标
container 的 live log follow。runtime PID 1 仅在
`INSIGHT_QUALIFICATION_ENABLED=true` 时注册 SIGUSR2 handler；harness 通过带 request timeout
且显式指定容器的 `kubectl exec -c runtime` 发送 SIGUSR2；handler 先输出唯一
`QUALIFICATION_SELF_ABORT` marker，并留出一个短 handoff 窗口让 signal sender 成功返回，
随后由 PID 1 自己调用 `abort` 并以 SIGABRT 终止。该控制没有 HTTP 路由，普通生产配置不注册
handler。PostgreSQL 则通过
`kubectl exec -c postgresql` 读取 `postmaster.pid` 并发送 postmaster 已处理的 SIGQUIT，
走其受支持的 immediate-shutdown 路径。

死亡归因是 fail-closed 的：trigger 命令必须成功且绑定同一进程 incarnation；预挂 status watch
必须观察到原 container 的后继状态，`lastState.terminated` 必须属于原 container，restart count
必须增加且新 container ID 必须不同；termination reason 必须存在且不能为 `OOMKilled`。runtime
还必须看到 signal `6`、普通进程的 exit code `134`，或容器 PID 1 上 glibc `abort` 的 trap
兜底 exit code `133`。`QUALIFICATION_SELF_ABORT` marker 允许来自预挂的 exact-container live
log 或重启后读取的 exact previous-container log，但两者聚合后必须恰好出现一次；正式 pass5 的
证据为 live `1`、previous `0`。PostgreSQL 会额外聚合 live/previous immediate-shutdown 线索，
并强制要求重启后的 `pg_postmaster_start_time()` 与故障前不同。仅收到 force-delete API
acknowledgement 不算死亡证据，也不能让碰巧发生的 OOM 或 liveness restart 冒充注入故障。确认后
脚本才正常删除 Pod，以取得新的 workload Pod UID。
graceful runtime 场景仍直接执行正常 Pod deletion，不触发 self-abort。Gate C 的 k6 和
terminal-commit/SSE 的 curl 从启动起都受 EXIT trap 管理，任意提前失败都会 kill 并 wait，
不会遗留后台负载。

组合 harness 和 summary-worker harness 在正常完成与异常退出时都必须复原三个
qualification delay。cleanup 保留场景原始退出码；若原场景成功但 reset 或验证失败，则最终
退出改为失败。成功证据同时读取 Helm release values 和唯一 Ready runtime Pod 的三个 delay
环境变量，要求全部为 `0` 并保存 JSON，不能仅依赖 `helm upgrade` 的返回码。

`process-death-evidence.json` 保存 crash 前后容器身份、trigger 命令状态和死亡判据；
`replacement-identity.json` 嵌入该证据。随后 harness 等待 lease 过期和新 owner ready，并断言：

- kill 前已提交 result 的 Run 仍为 terminal；
- missing-result admission 全部推导为 interrupted；
- 再观察一个窗口后 result 数不变，证明没有 recovery discovery；
- 同 request ID 返回原 run，不执行第二次；新 request ID 创建新 run；
- result/assistant 原子关系无孤立 assistant 或缺失 assistant。

故障证据必须保存 killed/replacement Pod name 与 UID，且 UID 必须变化；hard 路径还必须先保存并
确认原 container 进程死亡。PostgreSQL kill 仍必须保存 before/after
`pg_postmaster_start_time()` 并证明变化。terminal commit 后、SSE 前脚本的
Attached curl 有硬超时，也必须看到不同的 Ready replacement runtime UID，不能只依赖
`rollout status`。terminal-commit/SSE 与 summary-worker crash 子场景复用同一个 qualification
self-abort 和 container-death confirmation，均不得把 force-delete acknowledgement 当作旧进程
已死亡。

外部副作用场景必须用可观测的测试 provider（以 request ID 作为 provider idempotency key），分别在
“effect 前 kill”和“effect 后、terminal commit 前 kill”执行，并把 provider invocation ledger
附到报告。后者允许副作用已发生且 Run interrupted；平台不得自动重试。graceful shutdown、summary
worker crash、terminal commit 后 SSE 前 kill 也要按下表记录，HTTP 状态不能替代副作用计数：

| 故障点 | result | assistant | 可自动恢复 | 外部 effect |
|---|---|---|---|---|
| admission 后、执行前 | missing/interrupted | 无 | 否 | 0 |
| action/LLM 中 | missing/interrupted | 无 | 否 | 取决于 fixture 点 |
| effect 后、terminal commit 前 | missing/interrupted | 无 | 否 | 允许 1 |
| terminal commit 后、SSE 前 | terminal | 恰好 1 | 不需要 | 恰好 1 |
| summary worker crash | turn terminal | 恰好 1 | summary 可稍后重试 | 不影响 |

summary worker crash 由
[`run-summary-worker-crash.sh`](../../bench/terminal-only/run-summary-worker-crash.sh)
单独取证：脚本先证明达到阈值的 job 处于可观察的 delayed active 窗口且 DB 尚无 summary，再硬杀
runtime；清除 delay 后的新 turn 必须在正常预算内完成、使用精确 recent tail fallback，并触发后续
summary retry。删除 summary object 的读取失败由 `run-context-summary.sh` 另行验证，不能把该场景
报告成 worker crash。

## Gate D：Conversation、aged query 与 privacy

正式 100 × 100 turn：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-gate-d.sh qualification \
  bench/results/terminal-only/gate-d-100x100
```

qualification profile 会在主 100×100 workload 后自动串联 context/summary fault、1×/10×
Attached stream、large-object privacy delete 和 aged 1,000,000-message 查询；任何一个子报告
缺失或 `passed != true` 都使 Gate D 失败。

smoke 使用 2 × 3：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-gate-d.sh smoke \
  bench/results/terminal-only/gate-d-smoke
```

每个 Conversation 串行产生 turn，不同 Conversation 并行。每 10 个 turn 复用相同 request ID，
必须返回同一 user message/run。完成后用 17 条一页的 opaque cursor 遍历全部消息，断言无遗漏、
无重复、严格降序且每个 turn 为 assistant/user 对。数据库断言检查原子 pair、上下文 hash、
10000/10000 user/assistant、summary coverage。context harness 必须生成至少两代 summary，从 DB
记录 latest through-order/hash/ref，验证 probe 精确选中该对象值和 boundary 之后的完整 recent
窗口；删除 latest object 后则验证精确 tail fallback。真实 summary worker crash 使用上面的
独立 Gate C harness。context 内容不能仅由 message 总数或低基数指标推断。

aged 1,000,000 message 和 recent-50 p95：

```bash
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-aged-query.sh qualification \
  bench/results/terminal-only/gate-d-aged-1m
```

脚本 seed 后先从数据库读取并精确断言该 Conversation 实际有 1,000,000 rows，再保留
`EXPLAIN (ANALYZE, BUFFERS)`，要求 cursor index 被使用且 1000 次 server-side recent-50
查询 p95 ≤20ms；`aged-query-report.json` 记录实际 message/sample 数、p50/p95/p99/max。
`smoke` 只插入 10000 条并取 100 个样本。

privacy delete：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
  bash bench/terminal-only/run-privacy-delete.sh \
  bench/results/terminal-only/gate-d-privacy
```

该脚本断言 metadata/messages/summaries 不可读取、Run GET 不泄露删除内容或任何 target object
reference，且数据库级联行数为 0。每个 target/control large scoped object 在 DELETE 前都直接读取
底层 stored bytes：必须以 `IAPTEA01` 开头、header key version 等于
`TENANT_ARTIFACT_KEY_VERSION`（默认 `qualification-v1`），且 tenant ID 与 marker 都不能以明文
出现。报告只保存 active key version、magic 与布尔检查，不保存 key。target DELETE 后沿原引用逐个
确认底层文件不存在且 deletion job 清空；相同内容的另一 tenant control 的 API、Run 和对象仍可读。
Attached DELETE race 使用同一 Python 进程、同一锁记录完整 SSE frame 与完整成功 DELETE response；
response 之后出现任何 frame（包括 terminal/error）或 stream 不关闭都会失败。生成 token/chunk
1× 与 10× 的测试使用同一可按 `output_scale` 产生确定输出的 LLM fixture：

```bash
BASE_URL=http://127.0.0.1:3000 \
TERMINAL_BENCH_POSTGRES_URL='postgres://...' \
GATE_D_STREAM_AGENT_ID=terminal_stream_fixture \
  bash bench/terminal-only/run-stream-scaling.sh \
  bench/results/terminal-only/gate-d-stream-scaling
```

脚本分别保存 SSE/k6 与数据库 before/after；10× 必须产生更多 delta frame，但两次每 turn 都只能增加
两条 message，delta/chunk 不能进入 `conversation_messages`。每次还会解析 SSE JSON，拼接所有
`response.output_text.delta`，要求它逐字等于唯一 `response.completed.workflow.result`、
Run GET `output.data` 和 assistant message content。

## 报告与判定

复制 [`bench/terminal-only/report-template.md`](../../bench/terminal-only/report-template.md) 填写。
报告必须同时链接 raw before/after snapshot、k6 summary/log、top WAL SQL、table/index size、
PostgreSQL 设置、runtime metrics/RSS、Pod event/log、failure classification 和 privacy receipt。
没有实际运行两小时或缺少外部故障条件时，状态只能写 `Not run`/`Blocked`，不能写 `Passed`。

2026-07-28 的正式资格结果以
[已签署资格报告](../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)
和其链接的 immutable raw evidence 为准。Gate B 的 k6 客户端曾报告约 400,006 个 time series 的
高基数警告；它没有改变精确 arrival/closure 或数据库判定，但扩大 profile 前应先收敛客户端
URL/tag 基数，避免压测工具自身成为容量限制。
