# Terminal-only Runtime 与 Conversation 资格报告

日期：2026-07-28

状态：**Qualified。Phase 0、Gate A～D 与最终静态验证链全部通过。**

对象：Insight Agent Platform 的 `terminal_only` Run、Conversation、持久化边界、故障语义、
WAL、保留与隐私删除

对应规范和独立 rollout 决策已退出工作树，可从 Git 历史查看。

正式证据根目录：
[`bench/results/2026-07-28-terminal-only-qualified/`](../results/2026-07-28-terminal-only-qualified/)

## 1. 结论边界

本轮可以作出的资格结论是：

- `full` 基线的 WAL 来源已经通过真实 PostgreSQL 物理 WAL 记录完成拆分，分类覆盖率为 100%；
- 独立和 Conversation 两种 terminal-only 写路径都只产生规定的 admission、result 和 message
  权威行，54 张 full durable 表的写入 delta 均为 0；
- Gate B 的严格连续两小时 profile 精确完成 72,000/72,000 Run，WAL 为
  347,968,068 bytes，即 4,832.89 bytes/accepted Run，全部容量与 durability 门槛通过；
- Gate C 的 8 个子场景与 11 项 aggregate assertion 全部通过，证明了“未完成 Run 会中断、
  不会被伪装成自动恢复、同 request 不重放副作用、新 request 才能显式重试”的合同；
- Gate D 的 100 × 100 turn、幂等、分页、有界上下文、stream 放大、百万消息 aged query、
  privacy delete 和 tenant object encryption 全部通过；
- 最终源码状态上的 workspace、schema、crate boundary、公开 API、Python、Helm、shell 与文档
  验证链通过，完成定义 1～12 全部关闭；
- 默认模式保持 `full`；`terminal_only` 只允许兼容 Agent 通过 immutable Deployment Revision
  显式 opt-in，既不迁移现有 revision，也不迁移已运行或正在运行的 Run。

`Qualified` 只证明本报告限定的单 owner、固定 action fixture、WAL、故障与 Conversation 合同。
它不自动授权把平台默认值改成 `terminal_only`；默认值仍由独立 rollout 决策控制，未来修改必须
另行评审。

## 2. 总体资格状态

| 阶段 | 状态 | 当前正式证据 | 判定 |
|---|---|---|---|
| Phase 0：full 基线拆分 | Passed | [`phase0-full-report.json`](../results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/phase0-full-report.json) | 物理 WAL 分类覆盖率 100%，超过 95% 门槛 |
| Gate A：精确写路径 | Passed | [`standalone`](../results/2026-07-28-terminal-only-qualified/gate-a-standalone-final2-pass/gate-a-report.json)、[`conversation`](../results/2026-07-28-terminal-only-qualified/gate-a-conversation-final2-pass/gate-a-report.json) | 两种模式均 `passed=true` |
| Gate B：10 rps × 2h WAL | Passed | [`gate-b-report.json`](../results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/gate-b-report.json) | `passed=true`；72,000/72,000；347,968,068 WAL bytes |
| Gate C：故障语义 | Passed | [`gate-c-suite-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/gate-c-suite-report.json) | `passed=true`，8 个子场景、11/11 aggregate assertions 通过 |
| Gate D：Conversation | Passed | [`gate-d-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/gate-d-report.json) | `passed=true`，9/9 检查通过 |
| 最终静态验证与文档同步 | Passed | [`final-static-validation.json`](../results/2026-07-28-terminal-only-qualified/final-static-validation.json)、本报告第 10、12 节 | 最终验证链通过，当前文档与归档同步完成 |
| 总体资格 | **Qualified** | Phase 0、Gate A～D、完成定义 1～12 | 允许符合能力边界的 immutable revision 显式 opt-in |

## 3. 不可变构建与环境身份

### 3.1 源码与镜像

| 项目 | 身份 |
|---|---|
| Git 基线 commit | `f3158610974ebbe05dc3ded16674412451b258d1` |
| 镜像 COPY 输入指纹 | `263eae29acf7b8e4646728c7b9758675986bfe387157fe110ddeeb38adcb66ab` |
| 资格镜像 tag / build fingerprint | `insight-agent-platform:terminal-qualified-20260728-263eae29acf7` |
| 不可变镜像 digest | `sha256:f498abcd295a5c0b7cd9062edac36952c6739d1769a287a5ac7b83a72bb2d3be` |
| 镜像平台 | `linux/arm64` |
| 镜像创建时间 | `2026-07-28T04:50:16.250817412+08:00` |

资格实现是在以该 Git commit 为基线的修改工作树上构建的，因此 commit 本身不是完整的二进制
重现身份。正式部署权威必须使用上表镜像 digest；tag 只用于可读定位，不能代替 digest。

同一 digest 可在以下独立采集点交叉核对：

- Phase 0 Pod：
  [`runtime-pod-before.json`](../results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/runtime-pod-before.json)；
- Gate B 正式 Pod：
  [`runtime-pod-before.json`](../results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/runtime-pod-before.json)；
- Gate C 正式 preflight Pod：
  [`preflight-fault-zero-ready-pods.raw`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/preflight-fault-zero-ready-pods.raw)；
- Gate C 结束 Pod：
  [`fault-zero-final-ready-pods.raw`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/fault-zero-final-ready-pods.raw)。

### 3.2 Fresh infrastructure 与数据库

| 用途 | Kubernetes context | Namespace / release | Fresh preflight |
|---|---|---|---|
| terminal functional、Gate A/C/D | `orbstack` | `insight-terminal-qual-final2` / `terminal-final2` | [`final2-functional-preflight.json`](../results/2026-07-28-terminal-only-qualified/final2-functional-preflight.json)：namespace、release、PVC 均不存在于 preflight 之前 |
| Phase 0 full 基线 | `orbstack` | `insight-phase0-full-final2` / `phase0-full2` | [`phase0-full-final2-preflight.json`](../results/2026-07-28-terminal-only-qualified/phase0-full-final2-preflight.json)：fresh namespace 与 2 个 fresh PVC |
| Gate B WAL | `orbstack` | `insight-terminal-wal-final2` / `terminal-wal2` | [`terminal-wal-final2-preflight.json`](../results/2026-07-28-terminal-only-qualified/terminal-wal-final2-preflight.json)：fresh namespace、release、PVC |

PostgreSQL 为 `16.14`，`pg_walinspect` 为 `1.1`。Phase 0 与 Gate B 正式环境均保持
`fsync=on`、`full_page_writes=on`、`synchronous_commit=on`、
`pg_stat_statements.track=all` 和 `track_io_timing=on`。qualification 表为普通持久表；
只有实例级 owner registry 可以是非持久提示，不能成为 Run 恢复权威。

terminal functional 配置为单 runtime replica、最多 50 active terminal-only Runs、12 个全局
operation permits、每 Run 2 permits、PostgreSQL pool 24。`terminal_only` v1 的单 runtime
限制属于产品合同，不是本轮临时测试折扣。

## 4. Phase 0：full 模式 WAL 基线拆分

正式报告：
[`phase0-full-10rps-10m-final2/phase0-full-report.json`](../results/2026-07-28-terminal-only-qualified/phase0-full-10rps-10m-final2/phase0-full-report.json)

profile 为 `action_demo`、`full`、10 arrival/s、1 分钟 warm-up、10 分钟 measured window。
warm-up 的 600/600 Run 在 measured LSN 边界前全部闭合并排除；measured window 的
6,000/6,000 Run 全部 accepted、terminal、succeeded，0 dropped、0 rejected、0 interrupted。

| 指标 | 正式结果 |
|---|---:|
| measured duration | 600.171s |
| accepted / terminal / succeeded | 6,000 / 6,000 / 6,000 |
| completed throughput | 10.000 run/s |
| lifecycle p95 / p99 | 208ms / 235.01ms |
| PostgreSQL WAL | 1,839,028,207 bytes |
| WAL / accepted Run | 306,504.70 bytes |
| physical payload relation WAL | 27,254,370 bytes |
| physical artifact metadata WAL | 4,712,429 bytes |
| physical payload + object WAL | 31,966,799 bytes |
| physical structural WAL | 1,807,451,039 bytes |
| mixed / unmapped WAL | 0 / 0 bytes |
| requested / timed checkpoints in measured delta | 0 / 0 |
| deadlock / temp file / temp bytes | 0 / 0 / 0 |
| runtime restart before / after | 0 / 0 |

物理 WAL 记录分类覆盖率为 `1.0`；record bytes 对 measured LSN span 的比例为
`0.9922915789`，对 `pg_stat_wal` 的比例为 `1.0002118679`，均在 `[0.95, 1.05]`
物理边界范围内。top-level statement WAL 对 `pg_stat_wal` 的覆盖率为 `0.9853106815`；
top 30 SQL 单独覆盖 `0.9259032578`，但完整物理记录分类已经满足“解释至少 95% WAL 来源”的
Phase 0 完成标准。

关系增长同样按语义拆分：

| 分类 | 增长 |
|---|---:|
| structural relations + indexes | 1,219,985,408 bytes |
| payload relation | 16,883,712 bytes |
| artifact metadata relation | 3,612,672 bytes |
| external artifact store | 0 bytes |
| payload + object combined | 20,496,384 bytes |
| catalog | 0 bytes |

这组数据只用于解释 full 写放大和固定后续比较口径；它不是 terminal-only Gate B 的替代结果。

## 5. Gate A：精确写路径

最终 standalone 与 Conversation 运行都在 workload 前重置 statement statistics，并验证
before/after `stats_reset` 连续。两份正式报告均没有 failure。

| 断言 | standalone | Conversation turn | 门槛 |
|---|---:|---:|---:|
| `terminal_run_admissions` row delta | 1 | 1 | 1 |
| `terminal_run_results` row delta | 1 | 1 | 1 |
| `conversation_messages` row delta | 0 | 2 | 0 / 2 |
| admission INSERT calls / rows | 1 / 1 | 1 / 1 | 1 / 1 |
| result INSERT calls / rows | 1 / 1 | 1 / 1 | 1 / 1 |
| message INSERT calls / rows | 0 / 0 | 2 / 2 | 0 / 0 或 2 / 2 |
| forbidden durable mutation calls | 0 | 0 | 0 |
| 54 张 full durable 表 row delta | 全部 0 | 全部 0 | 全部 0 |
| terminal UPDATE/DELETE mutation calls | 0 | 0 | 0 |
| core INSERT statement WAL | 1,550 bytes | 3,602 bytes | 诊断值 |
| snapshot WAL delta | 21,606 bytes | 18,780 bytes | 诊断值 |

正式证据：

- [`gate-a-standalone-final2-pass/gate-a-report.json`](../results/2026-07-28-terminal-only-qualified/gate-a-standalone-final2-pass/gate-a-report.json)；
- [`gate-a-conversation-final2-pass/gate-a-report.json`](../results/2026-07-28-terminal-only-qualified/gate-a-conversation-final2-pass/gate-a-report.json)；
- 对应的 statement 证据分别位于
  [`postgres-write-statements.json`](../results/2026-07-28-terminal-only-qualified/gate-a-standalone-final2-pass/postgres-write-statements.json)
  和
  [`postgres-write-statements.json`](../results/2026-07-28-terminal-only-qualified/gate-a-conversation-final2-pass/postgres-write-statements.json)。

报告明确不把 database-wide transaction counter 推导为“每 Run 恰好两个数据库事务”；
它证明的是 repository transaction boundary 与所需一行 INSERT。该限定避免用背景事务计数制造
虚假精确性。

## 6. Gate B：正式两小时 WAL

<!-- GATE_B_FORMAL_RESULT_BEGIN -->

**状态：Passed**

正式报告：
[`gate-b-report.json`](../results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/gate-b-report.json)，
其中 `qualification=true`、`passed=true`、`failures=[]`。

fresh preflight 使用 namespace UID
`bb913e63-9c5d-4066-8c3b-c8c95dc5b4c3` 和 preflight ID
`20260727T231741Z-a6af13b7063f0ae1`。60 秒 warm-up 精确完成 600/600，0 dropped、late、
rejected、failed、interrupted，并在 measured LSN 边界前闭合和排除。正式窗口使用固定
`shared-iterations` profile，在 7,200.052s 内精确调度并完成 72,000/72,000 Run。

| 指标 | 门槛 | 正式结果 |
|---|---:|---:|
| accepted closure | 100% | 100%（72,000 / 72,000） |
| scheduled success | ≥99.9% | 100% |
| completed throughput | ≥9 run/s | 9.999927 run/s |
| arrival lateness p95 / p99 / max | p95 ≤ p99 ≤ max <100ms | 4 / 6 / 25ms |
| lifecycle p95 / p99 | ≤1s / ≤3s | 57 / 63ms |
| WAL / accepted Run | ≤32KiB | 4,832.89 bytes |
| 2h WAL | ≤2.2GiB | 347,968,068 bytes |
| structural DB growth / Run | ≤16KiB | 1,418.70 bytes |
| requested / timed checkpoints | 0 / diagnostic | 0 / 4 |
| deadlock / temp file / temp bytes / OOM | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| dropped / late / rejected / interrupted | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| admission / result row delta | accepted / terminal | 72,000 / 72,000 |
| 54 张 full durable 表 row delta | 全部 0 | 全部 0 |
| `fsync` / `full_page_writes` / `synchronous_commit` | on / on / on | on / on / on |

PostgreSQL WAL 全部按 structural 分类；固定 fixture 的 payload/object PostgreSQL WAL 为 0，
Artifact store 增长 294,920,192 bytes，作为独立对象字节报告，不与 PostgreSQL WAL 相加。
`pg_walinspect` 物理记录覆盖 `pg_stat_wal` 的比例为 `1.0008314585`，JSON/CSV 全字段一致；
top-30 对 top-level tracked SQL 的 coverage 为 `1.0`，top-level SQL WAL 对物理 interval 的
`0.9143527934` 只作独立诊断，不与物理记录相加。runtime Pod UID 与 container identity 在窗口前后不变，
restart、OOM、interrupted、terminal commit retry 均为 0；active 最终为 0。

运行结束后的
[`cleanup-evidence.json`](../results/2026-07-28-terminal-only-qualified/gate-b-10rps-2h-final2/cleanup-evidence.json)
为 `passed=true`：runtime、stream mock、PostgreSQL workload 均缩容为 0，Pod 与本地
port-forward/listener 已清理；8Gi PostgreSQL 与 2Gi Artifact PVC 保持 Bound，数据证据保留。

<!-- GATE_B_FORMAL_RESULT_END -->

## 7. Gate C：故障语义

正式 aggregate：
[`gate-c-suite-final2-pass5/gate-c-suite-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/gate-c-suite-report.json)

batch：`final2-pass5-20260728`

aggregate 为 `passed=true`，以下 11 个 scenario flag 全部为 `true`：

1. admission commit 后、执行前 hard kill；
2. action external effect 后 hard kill；
3. PostgreSQL restart 与 owner 重新注册；
4. graceful shutdown；
5. LLM execution hard kill；
6. terminal commit 后、SSE terminal frame 前 hard kill；
7. summary object 缺失回退；
8. summary worker hard kill 与 retry；
9. same request 不重放 effect；
10. new request 显式 effect retry；
11. Conversation 原子性。

### 7.1 Run 与副作用语义

| 场景 | 关键正式结果 | 证据 |
|---|---|---|
| admission 后、执行前 hard kill | admitted/active `50/50`；terminal `0`；interrupted `50`；kill 前 effect `0`；无自动恢复；same request 返回原 Run；new request 创建新 Run | [`gate-c-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/admission-before-execution/gate-c-report.json) |
| action effect 后 hard kill | admitted/active `50/50`；terminal `0`；interrupted `50`；kill 前 effect `50`；same request occurrence/attempt `1/1`；new request 显式 retry `2/2` | [`gate-c-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/action-hard-kill/gate-c-report.json) |
| PostgreSQL immediate restart | admitted/active `50/50`；terminal `0`；interrupted `50`；effect `50`；postmaster start time 改变；没有 owner 接管或自动恢复 | [`gate-c-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/postgres-restart/gate-c-report.json) |
| graceful shutdown | admitted/active `50/50`；terminal `50`；interrupted `0`；50 个 public GET 全部校验 | [`gate-c-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/graceful-shutdown/gate-c-report.json) |
| LLM hard kill | provider active before kill `50`；terminal `0`；interrupted `50`；provider observed count `203` 在 restart 和 same-request replay 后保持 `203`，new-request 显式 retry 后为 `204` | [`gate-c-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/llm-hard-kill/gate-c-report.json) |
| commit 后、SSE 前 hard kill | result 与 assistant message 在 kill 前已提交；kill 前 terminal SSE 不存在；restart 后 GET 与 messages 内容校准 | [`commit-before-sse-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/commit-before-sse/commit-before-sse-report.json) |

所有适用场景的 Conversation 原子性计数均为 0：
`admission_without_user`、`user_without_admission`、`assistant_without_result`、
`result_without_assistant`、duplicate result、duplicate assistant、assistant without admission
以及 admission/user reuse 都没有发生。

### 7.2 hard-death 证明链

Gate C 不把 `kubectl delete pod --force` 或“Pod 名字改变”当成 hard process death。pass5
对每个 hard-kill 场景执行以下绑定：

1. 触发前记录原 Pod UID、container ID、restart count；
2. 在原 container 中读取并绑定 `(pid, start_time_ticks)` process incarnation；
3. 触发前挂接精确 Pod status watch；
4. 触发前挂接 exact-container `logs --follow --tail=0`；
5. trigger 再读取并回显同一 process token 后才发送资格 signal；
6. 证明原 container 身份终止、restart/container identity 改变、termination 非 OOM；
7. 证明 status watch 捕获了原 `lastState`；
8. 证明资格 marker 在预挂 live stream 或 exact previous-container logs 中整体唯一。

例如 admission 场景的正式
[`process-death-evidence.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/admission-before-execution/process-death-evidence.json)
显示原 runtime exit code `133`，status watch 在 trigger 前已挂接，live marker count 为 `1`，
previous-container marker count 为 `0`，cause calibration 的所有必需字段均为 `true`。这正是
本地 CRI 可能丢失 previous logs 时仍能绑定原进程的正式证明模型；不得把它误写成“marker
必须只来自 `kubectl logs --previous`”。

PostgreSQL 场景还要求原 postmaster start time 改变，其值从
`2026-07-27 22:06:35.562388+00` 变为 `2026-07-27 22:12:44.422183+00`。

### 7.3 Conversation summary 与 fallback

missing-summary-object 场景：

- 生成两代 summary，正式选择最新 `through_message_order=22`；
- object framing magic 为 `IAPTEA01`，active key 为 `qualification-v1`；
- tenant ID 与 marker plaintext 都不出现在 raw encrypted envelope；
- 注入时删除真正的最新 summary object，边界为 `through_message_order=24`；
- 后续 context 中 summary 为 `null`，从 Conversation 起点安全回退，精确 recent tail 为
  message order `43, 44`；
- read-failure metric 确认递增；
- context 为 1,652 tokens，低于 24,000 token budget。

证据：
[`context-summary-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/summary-object-fallback/context-summary-report.json)。

summary-worker hard-kill 场景：

- kill 窗口内 active job 为 `1`、eligible messages 为 `32`、已提交 summary 为 `0`；
- 原 Pod 与 replacement UID 不同，hard-death cause 完整校准；
- restart 后新 turn 用时 `0s`，没有被 summary 阻塞，精确 recent tail 为 `31, 32`；
- 新 terminal turn 后 worker retry 成功，最新 summary 到 `through_message_order=14`；
- terminal result 与 assistant pair 保持原子。

证据：
[`summary-worker-crash-report.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/summary-worker-crash/summary-worker-crash-report.json)。

### 7.4 清理

[`suite-cleanup.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/suite-cleanup.json)
的 original/reset/final status 全为 `0`。
[`fault-zero-final.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/fault-zero-final.json)
和
[`fault-zero-exit.json`](../results/2026-07-28-terminal-only-qualified/gate-c-suite-final2-pass5/fault-zero-exit.json)
均证明 Helm 与唯一 Ready runtime Pod 中的 admission、post-commit、summary delay 全部为 `0`。

## 8. Gate D：Conversation

正式 aggregate：
[`gate-d-conversations-final2-pass5/gate-d-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/gate-d-report.json)

profile 为 `qualification`，tenant 为 `gate-d-final2-pass5-20260728`，100 个 Conversation
各执行 100 个 turn。aggregate 为 `passed=true`、`qualification_composite=true`，9/9 检查
全部为 `true`。

### 8.1 容量、幂等、分页与原子性

| 指标 | 正式结果 |
|---|---:|
| Conversation / fresh accepted turns | 100 / 10,000 |
| capacity retry attempts / rejected / accepted | 10,026 / 26 / 10,000 |
| fresh non-replayed acceptances | 10,000 |
| idempotent replays verified | 1,000 |
| pagination conversations / pages | 100 / 1,200 |
| k6 checks pass / fail | 12,426 / 0 |
| workload success pass / fail | 100 / 0 |

26 个非成功 HTTP 都是显式 capacity rejection，均要求正整数 `Retry-After`；harness 使用相同
request ID 与相同 payload 重试，并禁止 rejection 返回 Run/message identity。它们不是已受理
workload failure。

数据库断言：

| 行或违例 | 数量 |
|---|---:|
| admissions / results / succeeded results | 10,000 / 10,000 / 10,000 |
| messages | 20,000 |
| user / assistant messages | 10,000 / 10,000 |
| distinct admission user messages | 10,000 |
| conversations with summary | 100 |
| max messages per Conversation | 200 |
| turn order violations | 0 |
| admission without user / user without admission | 0 / 0 |
| assistant without result / result without assistant | 0 / 0 |
| missing context hash after first turn | 0 |

证据：
[`k6-summary.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/k6-summary.json)
和
[`database-assertions.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/database-assertions.json)。

### 8.2 有界 context 与 summary failure

正式 context 场景生成两代 summary，选择最新 `through_message_order=22`；raw object 使用
`IAPTEA01` / `qualification-v1`，tenant 与 marker plaintext 均不存在。删除注入时真正的
latest boundary 为 24，回退后的精确 tail 为 `43, 44`；context 使用 1,652 / 24,000 tokens。

证据：
[`context-summary-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/context-summary/context-summary-report.json)。

### 8.3 SSE/token 放大不增加数据库消息

| profile | SSE delta frames | admission | result | messages | forbidden full durable rows |
|---|---:|---:|---:|---:|---:|
| 1× | 4 | 1 | 1 | 2 | 0 |
| 10× | 40 | 1 | 1 | 2 | 0 |

frame 数量严格放大 10 倍，而每 turn message 始终为 2，terminal GET/messages 内容已校准。

证据：

- [`stream-scaling-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/stream-scaling/stream-scaling-report.json)；
- [`1x/write-path-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/stream-scaling/1x/write-path-report.json)；
- [`10x/write-path-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/stream-scaling/10x/write-path-report.json)。

### 8.4 百万消息 aged query

配置与实际 seed 都是 1,000,000 条 message，1,000 次查询的 p50/p95/p99/max 分别为
`0.011 / 0.015 / 0.01601 / 0.208ms`，低于 20ms 门槛。

admission lookup 精确使用
`terminal_run_admissions_tenant_request_key`；derived Run lookup 使用 admission/result 主键。
唯一 Seq Scan 是只有 1 行的 `terminal_runtime_instances` owner registry，没有 growing
relation Seq Scan。

证据：
[`aged-query-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/aged-query/aged-query-report.json)。

### 8.5 Privacy delete 与 tenant encryption

| 断言 | 正式结果 |
|---|---|
| target large object refs deleted | 3 |
| control object refs preserved | 3 |
| tenant-scoped content hashes distinct | true |
| encrypted framing / active key | `IAPTEA01` / `qualification-v1` |
| target/control tenant与marker plaintext absent | true / true |
| control Conversation 与 Run 删除后仍可读 | true |
| attached stream delete fenced | true |
| stream delta / frames before-or-at / frames after delete | 1 / 5 / 0 |
| target Conversation/messages/Run status | 404 / 404 / 404 |
| deletion jobs remaining | 0 |
| target DB rows / stream DB rows deleted | true / true |
| target content/object refs absent from HTTP | true |

证据：

- [`privacy-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/privacy-delete/privacy-report.json)；
- [`stream-probe-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/privacy-delete/stream-probe-report.json)；
- [`tenant-encryption-report.json`](../results/2026-07-28-terminal-only-qualified/gate-d-conversations-final2-pass5/privacy-delete/tenant-encryption-report.json)。

## 9. 失败与非资格证据保留

失败、partial 和 diagnostic 目录没有删除，也不会被重命名成通过。分类如下：

| 证据 | 分类 | 观察与处理 |
|---|---|---|
| `gate-a-standalone-final2/`、`gate-a-conversation-final2/` | 资格采集失败 | 报告因 database `stats_reset` 改变或缺失而失败；在显式 initial reset 后从头重跑，只有 `*-final2-pass/` 被接受 |
| `gate-b-10rps-2h-failed-attribution-gc/` | 正式失败，不计资格 | `artifact_gc_sweeps +120`，且 top-WAL attribution/raw accounting coverage 只有 `0.912936`；该运行使用旧镜像 `...-5141d47c5258@sha256:252f3318...`，不能与 final2 正式结果拼接 |
| `gate-b-10rps-2h-final2/` | 正式通过 | fresh preflight、同一不可变镜像、600/600 warm-up 排除、72,000/72,000 measured closure、物理 WAL 对账与清理证据完整，`gate-b-report.json passed=true` |
| `gate-c-suite-failed-unconfirmed-force-delete/` | fault oracle 不充分 | force-delete 不能证明目标进程精确死亡；结果被拒绝 |
| `gate-c-suite-final2` 至 `pass2` | partial / harness hardening | 没有最终 aggregate；只保留诊断价值，不计 suite pass |
| `gate-c-suite-final2-pass3/` | 证据采集失败 | 原 container 的 `lastState` 在快速多次 restart 后被覆盖，marker 可见但无法证明精确原 termination；由此增加 trigger 前 Pod status watch |
| `gate-c-suite-final2-pass4/` | benchmark infrastructure/readiness failure | status watch 已捕获死亡，但 replacement rollout/readiness 阶段中止；cleanup 成功，仍不计资格 |
| `gate-c-suite-final2-pass5/` | 正式通过 | 首个同时具备完整 aggregate、精确 hard-death chain 和 fault-zero exit 的 Gate C 结果 |
| `gate-d-conversations-final2` 至 `pass4` 与 `diagnostic-*` | partial / harness calibration | capacity retry accounting、stream timestamp、privacy port-forward 等 oracle 逐步校准；缺少最终 aggregate 的目录不计资格 |
| `gate-d-conversations-final2-pass5/` | 正式通过 | 首个 `qualification_composite=true` 且 9/9 checks 全通过的 Gate D 结果 |

Gate C pass3/pass4 属于证据获取或基础设施失败，不应写成产品正确性失败；同样，它们也不能被隐藏。
最终判断只引用 pass5 aggregate。

## 10. 静态验证与合同核对

最终源码状态上的 consolidated validation 已通过，不以 benchmark 的中间结果代替静态合同：
机器可读清单位于
[`final-static-validation.json`](../results/2026-07-28-terminal-only-qualified/final-static-validation.json)。

- `scripts/check-cutover-residuals.sh`、`scripts/check-crate-boundaries.sh` 和
  `scripts/check-public-api-baseline.sh` 通过；
- `cargo fmt --all -- --check`、workspace/all-targets/all-features `cargo check` 与
  `cargo clippy -D warnings` 通过；
- PostgreSQL 16 上的 workspace/all-targets/all-features tests 与 workspace doctests 通过，覆盖
  PostgreSQL/SQLite schema parity、repository contract、Conversation 原子边界、full mode
  conformance、公开 API baseline 与 real-process 行为；
- terminal-only 与 Phase 0 Python suites、Python bytecode compile、所有相关 shell `bash -n`
  检查通过；两组 Python suite 分别为 51/51 与 14/14，17 个 shell 和 14 个 Python 文件
  语法检查全部通过；
- `cargo audit` 扫描 350 个依赖且 0 vulnerability，`cargo deny check` 的 advisories、bans、
  licenses、sources 全部为 `ok`；13 个 multiple-version warning 是 `deny.toml` 明确允许的
  warning，不是策略失败；
- qualification、Phase 0、默认 full 与 quickstart 四组 Helm lint/render 通过，k6 profile
  inspect 确认正式 workload 的 executor、iteration 和 duration 配置；
- Markdown 相对链接检查覆盖 126 个文件和 333 个相对链接，缺失目标为 0；当前文档、归档规范、
  配置默认值与 rollout 决策一致。

验证过程中发现并修复了 crate contract 所属层级、并发测试监听到其他隔离 schema 的通知后误判
目标 Run 的测试 oracle，以及没有限定 `current_schema()` 的 trigger catalog 查询；修复后以默认
并行线程完整重跑 workspace/all-targets/all-features 测试并通过。这里不以测试计数替代命令级通过
结论。

## 11. Rollout 决定

独立决定记录已经接受以下策略：

- 平台、quickstart 和 Helm 默认值均保持 `full`；
- terminal-only feature 保持启用，使兼容 Agent 可以显式 opt-in；
- `terminal_only` 只能由 immutable Deployment Revision 明确选择；
- 现有 revision、已完成 Run、正在运行的 full Run 均不迁移；
- 需要 recovery、event replay、durable timer/signal/human wait、fork/redrive/migration 的 Agent
  必须继续使用 `full`；
- Gate B 已通过，但不会自动触发默认值切换。

配置核对：

| 配置入口 | 默认值 | terminal-only feature |
|---|---|---|
| `config/platform.yaml` | `default_persistence_mode: full` | `enabled: true` |
| `config/platform.quickstart.yaml` | `default_persistence_mode: full` | `enabled: true` |
| Helm `values.yaml` | `defaultPersistenceMode: full` | `enabled: true` |

完整理由、回退和复审条件见
独立 rollout 决策可从 Git 历史查看。

## 12. 规范完成定义 1～12

| # | 完成定义 | 当前状态 | 证据或剩余动作 |
|---:|---|---|---|
| 1 | Store 与 PostgreSQL/SQLite schema parity | **Passed** | schema、ports、repository contracts 与最终 workspace 验证通过 |
| 2 | TerminalOnlyRunEngine 与 DurableRunEngine 完全分离 | **Passed** | 独立 engine/port；Gate A 的 54 张 full durable 表 delta 全 0 |
| 3 | persistence capability、unsupported API、故障语义进入公开合同 | **Passed** | `docs/current/api.md`、DTO capability、typed 422 与故障语义同步 |
| 4 | 两个 Conversation 原子边界 tests 全绿 | **Passed** | repository tests 与 Gate A/C/D 的孤立、重复、顺序违例均为 0 |
| 5 | full conformance 无回退 | **Passed** | 最终 full conformance 通过；Phase 0 full workload 6,000/6,000 |
| 6 | Gate A～D 全部通过 | **Passed** | A/B/C/D 正式 aggregate 均为 `passed=true` |
| 7 | 2h WAL ≤2.2GiB 且 ≤32KiB/accepted | **Passed** | 347,968,068 bytes；4,832.89 bytes/accepted |
| 8 | 未关闭 PostgreSQL durability 参数 | **Passed** | Gate B 同一连续窗口前后均为 `fsync/full_page_writes/synchronous_commit=on` |
| 9 | current docs、配置、Helm、API baseline、运维同步 | **Passed** | 当前指南、索引、归档、默认值、Helm 与 API baseline 同步 |
| 10 | 明确不支持 recovery、event replay、durable wait | **Passed** | capability DTO、API、架构与本报告均明确记录 |
| 11 | Agent memory 未进入 Conversation/Run 热路径 | **Passed** | Conversation 只保存 user/final assistant/summary；Gate A/D 证明 chunk/token 不增加行 |
| 12 | 默认值由独立 rollout 决策确认 | **Passed / Accepted** | 默认保留 `full`；见独立 rollout 决策 |

第 1～12 项已于 2026-07-28 全部关闭；本报告状态为 `Qualified`，规范已标记
`Implemented / capacity-qualified` 并归档。

## 13. 限制与不外推范围

- 环境为本地 OrbStack 单节点 ARM64，不是多节点云生产集群。
- terminal-only v1 只有一个 runtime owner；本报告不证明多 runtime ownership handoff 或全局容量
  协调。
- Gate A/B/Phase 0 的热路径使用本地小型 action fixture；Gate C 的 provider 场景使用可校准
  mock/fixture。本报告不代表 50 路真实 LLM、retrieval 或第三方 API 的容量。
- terminal-only 明确不提供 crash recovery、event replay、durable wait、fork/redrive/migration
  或外部副作用 exactly-once。
- provider 已发生外部副作用后 crash，显式新 request retry 仍可能重复副作用；调用方需要自己的
  idempotency key。
- summary 是有界 context 优化，不是长期 Agent memory、事实库、embedding 或 vector store。
- tenant object encryption 的本轮 key 为 qualification fixture `qualification-v1`；生产 keyring
  轮换、托管和灾备必须按生产密钥流程执行。
- privacy delete 证明本轮关系与 object refs 不再可读，不等同于备份介质的立即物理擦除承诺。
- 资格源码来自修改工作树，commit 不能单独复现；不可变镜像 digest 是运行证据的部署权威。
- Gate B 的 k6 客户端在两小时运行中报告高基数警告，最多约 400,006 个 time series；精确 arrival、
  closure 与数据库判定不受影响，但继续扩大 profile 前应收敛客户端 URL/tag 基数，避免 harness
  内存成为限制。该限制不表示 runtime 的低基数平台指标含有 Run/Conversation ID label。

本次 Qualified 范围内的可靠表述是：

> 平台支持低写入的 terminal-only Run 和持久化 Conversation。terminal-only Run 只持久化
> admission、最终结果、用户消息和最终 assistant 消息；进程失败会中断未完成 Run，平台不提供
> checkpoint 恢复或中间事件重放。需要 durable wait、recovery、fork/redrive/migration 的部署
> 必须继续使用 full 模式。
