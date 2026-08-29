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

`deterministic_first_run.rs` 的真实 P2 journey 已覆盖 CLI、独立 raw public HTTP、terminal Run、durable SSE、exact
binding、Orchestration Worker replacement、Human Task resume、Artifact S3/KMS I/O、invalid Receipt conflict、Gateway
unavailable diagnostic 与角色重启。共享 fresh journey 也已执行 Human Task exact journal replay 与 stale ETag/new
Receipt fence，但尚未拆出 `approval-task-resume` 的独立 report。它仍未覆盖首条场景 manifest 要求的真实 Console
browser journey，因此即使其余
断言通过，M4 报告也必须保持 `incomplete`。其余九条场景尚未产生报告。

这一区分防止把一个覆盖多项行为的集成测试误报为十条黄金场景，或用普通单元测试替代 fresh base/full
profile evidence。

可从仓库根目录用下列单一入口复现当前 base journey；不带 `--report-directory` 时只运行测试，不写资格报告：

```console
scripts/run-productization-base-journey.sh \
  --report-directory target/productization-reports
```

runner 会保留其 fresh project 路径以便检查日志和 journal；默认只停止 exact Platform/Compose process，不删除持久卷。
为避免旧 Worker 通过固定本地 PostgreSQL 端口跨 profile 抢占 Job，runner 在任何构建或启动前检查本仓库 release
Platform process；发现孤儿或另一活动 profile 时 fail closed，并要求先从 owner project 执行 `insight stop`。
