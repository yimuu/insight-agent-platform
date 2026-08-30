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
result，生成第三份 `timer-signal-restart-recovery` report。受控 PostgreSQL 探针会领取 exact durable continuation，
只替换 lease-token digest，证明 `RepositoryError::StaleFence` 且 Job version/token 不变，再由替代 Worker 在真实 lease
到期后恢复。远端 fresh Linux journey 已为 Human Task、deterministic 和 Timer/Signal 三条场景补齐真实 Console，
三份报告现均完整 Passed。其余七条场景尚未产生 Passed 报告。

这一区分防止把一个覆盖多项行为的集成测试误报为十条黄金场景，或用普通单元测试替代 fresh base/full
profile evidence。

Git revision `e03b6cc123f5f1ada2c96a47f167956adde7a095` 的 fresh Linux run `33284301192` 已使
`approval-task-resume`、`deterministic-first-run` 和 `timer-signal-restart-recovery` 成为三份完整 Passed report：
CLI、raw `/v1`、真实 Console、全部 assertion 与 failure probe 均 Passed。下载 artifact 后的 canonical report
摘要与 SHA-256 见 [`base-journey-evidence.md`](base-journey-evidence.md)。严格 M4 gate 仍因其余七条没有全部
Passed 而失败。

可从仓库根目录用下列单一入口复现当前 base journey；不带 `--report-directory` 时只运行测试，不写资格报告：

```console
scripts/run-productization-base-journey.sh \
  --report-directory target/productization-reports
```

runner 会保留其 fresh project 路径以便检查日志和 journal；默认只停止 exact Platform/Compose process，不删除持久卷。
为避免旧 Worker 通过固定本地 PostgreSQL 端口跨 profile 抢占 Job，runner 在任何构建或启动前检查本仓库 release
Platform process；发现孤儿或另一活动 profile 时 fail closed，并要求先从 owner project 执行 `insight stop`。
