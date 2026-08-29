# M4 黄金场景证据合同

状态：**In Progress**。本文件定义 M4 的 machine-readable evidence，不表示十条场景已经通过。

## 1. 权威输入与输出

- [`examples/productization/scenarios.json`](../../../examples/productization/scenarios.json) 是十条 required scenario
  的 checked manifest；
- [`scenario-report.schema.json`](../../../examples/productization/scenario-report.schema.json) 是单次 fresh-profile
  journey 的 closed report schema；
- 每条报告文件名必须为 `<scenario-id>.json`，并由
  [`check-productization-scenario-reports.py`](../../../scripts/check-productization-scenario-reports.py) 对 manifest
做 exact ID、顺序、profile、automation layer 和 source revision 复核。
校验器默认要求报告的 `source_revision` 等于当前 Git `HEAD`；资格流水线也可用
`--source-revision <40-char-commit>` 显式固定同一候选 revision。

报告不能只记录测试进程 exit code。每个 manifest entrypoint、assertion 和 failure probe 都必须逐项给出
`passed | failed | not_run` 与 bounded evidence。只有全部 required check 为 `passed` 时，报告顶层 `status`
才能为 `passed`；`not_run` 必须使报告保持 `incomplete`。

## 2. 两种校验模式

开发中的单条/部分报告可以执行：

```console
python3 scripts/check-productization-scenario-reports.py \
  target/productization-reports --allow-incomplete
```

M4 gate 不使用该开关：

```console
python3 scripts/check-productization-scenario-reports.py \
  target/productization-reports
```

严格模式要求十个文件全部存在且全部 Passed；任何 required scenario 缺失、skip、`not_run`、未知字段、
working-tree revision 或 manifest drift 都失败。

## 3. 当前证据

`deterministic_first_run.rs` 的真实 P2 journey 已覆盖 CLI、checked curl 七步 Resource lifecycle、独立 raw public HTTP、
terminal Run、durable SSE、exact binding、Orchestration Worker replacement、Artifact S3/KMS I/O、invalid Receipt conflict、Gateway unavailable diagnostic
与角色重启。Human Task 子旅程已提取到独立 [`approval_task_resume.rs`](../../../tests/productization/approval_task_resume.rs)，
覆盖 waiting Task、first-winner、exact CLI journal replay、stale ETag/new Receipt fence、durable SSE resume 与 terminal result，
并从同一明确 fresh base authority 生成 `approval-task-resume` report。独立
[`timer_signal_restart_recovery.rs`](../../../tests/productization/timer_signal_restart_recovery.rs) 又覆盖 TimerWait 到期、
SignalWait exact key、Worker 缺席时两次相同 Receipt 的 204 replay、durable ready continuation、替代 Worker 恢复与 terminal
result，生成第三份 `timer-signal-restart-recovery` report。远端 fresh Linux journey 已为 Human Task 场景补齐真实
Console，使其完整 Passed；deterministic 与 Timer/Signal 两份报告仍缺各自场景的 Console，后者还诚实保留 stale
Job fence 为 `not_run`，因此必须保持 `incomplete`。其余七条场景尚未产生报告。

这一区分防止把一个覆盖多项行为的集成测试误报为十条黄金场景，或用普通单元测试替代 fresh base/full
profile evidence。

Git revision `591baf00c9b5bac04826f84b58ee96032aa2749b` 的 fresh Linux run `33279353000` 已使
`approval-task-resume` 成为第一份完整 Passed report：CLI、raw `/v1` 和真实 Console entrypoint，三项 assertion
以及 Task replay/stale fence 均 Passed。`deterministic-first-run` 与 `timer-signal-restart-recovery` 仍分别保留
Console，以及 Console/stale Job fence 的 `not_run`，因此继续 Incomplete。下载 artifact 后的 canonical report
摘要与 SHA-256 见 [`base-journey-evidence.md`](base-journey-evidence.md)。严格 M4 gate 仍因其余九条没有全部
Passed 而失败。

后续 runner 已把 exact deterministic Run ID 传入同一个真实 Console 会话；浏览器在 Task mutation 前必须从 public
authority 读取该 Run 的 `succeeded` 与预期 Inline result，才允许将 `deterministic-first-run` 的 Console entrypoint
记为 Passed。该增量已通过 stateful browser regression 和 Rust fixture 编译，但尚未取得新的 fresh remote report；
因此上述 revision `591baf00...` 的 deterministic 报告仍保持 Incomplete，不能预先升级。

Timer/Signal 子旅程现调整为先完成 replacement-Worker recovery，再把 exact Timer/Signal Run ID 交给同一真实
Console session；浏览器必须读取 `succeeded` 与 `resume after signal` Inline result，报告才会把该场景的 Console
entrypoint 记为 Passed。报告仍固定保留未执行的 `stale_job_fence`，所以即使 Console 通过也只能是 Incomplete。
该增量已通过 stateful browser regression 与 Rust fixture 编译，fresh remote report 尚待下一次 base journey。

可从仓库根目录用下列单一入口复现当前 base journey；不带 `--report-directory` 时只运行测试，不写资格报告：

```console
scripts/run-productization-base-journey.sh \
  --report-directory target/productization-reports
```

runner 会保留其 fresh project 路径以便检查日志和 journal；默认只停止 exact Platform/Compose process，不删除持久卷。
为避免旧 Worker 通过固定本地 PostgreSQL 端口跨 profile 抢占 Job，runner 在任何构建或启动前检查本仓库 release
Platform process；发现孤儿或另一活动 profile 时 fail closed，并要求先从 owner project 执行 `insight stop`。
