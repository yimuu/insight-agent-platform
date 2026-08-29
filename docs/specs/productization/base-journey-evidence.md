# Fresh base journey 资格证据

状态：**Passed with partial scenario closure**。本文件记录一次精确 revision 的真实 base profile 资格运行，
不表示 M1、M3、M4 或 Platform v2 spec00～18 已全部 Verified。

## 权威运行

- Git revision：`591baf00c9b5bac04826f84b58ee96032aa2749b`；
- GitHub Actions run：[`33279353000`](https://github.com/yimuu/insight-agent-platform/actions/runs/33279353000)；
- job：[`99171748184`](https://github.com/yimuu/insight-agent-platform/actions/runs/33279353000/job/99171748184)；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- 结果：2026-08-29T22:45:30Z 开始，2026-08-29T23:00:08Z 成功结束。

工作流只由 `workflow_dispatch` 显式触发。它没有构建或推送 candidate image，也没有执行 cosign、SBOM、
provenance 或 GitOps promotion。核心步骤执行 fresh `doctor -> init -> dev --profile base -> status`，通过 public
`/v1` 运行 CLI、checked raw HTTP、Orchestration replacement/restart 与真实 headless Chrome Console journey，最后
停止 exact Platform/Compose closure。

## Machine-readable reports

上传 artifact `productization-base-scenario-reports-591baf00c9b5bac04826f84b58ee96032aa2749b`
包含三份 canonical report。下载后以
`check-productization-scenario-reports.py --allow-incomplete --source-revision
591baf00c9b5bac04826f84b58ee96032aa2749b` 重验通过：

| 场景 | 状态 | SHA-256 | 已关闭边界 |
|---|---|---|---|
| `approval-task-resume` | Passed | `979fb29b135deecc09da6500c4c048f9cc24cac25cade5dff21411e25a6a9ce2` | CLI、raw `/v1`、真实 Console；waiting/first-winner/resume；replay 与 stale Task fence |
| `deterministic-first-run` | Incomplete | `f48b6b30906836467f2b10a23c0d002331fc21a6f96ac37b161dc57f5ae16ced` | CLI、raw `/v1`、terminal/SSE/exact binding、Receipt conflict 与 Gateway unavailable；该场景自身的 Console entrypoint 仍 Not run |
| `timer-signal-restart-recovery` | Incomplete | `0c56cd06e50258174ee615be539aadb7902fdcf50af26b7b74a2292c754c43f6` | CLI、raw signal、replacement Worker、Event sequence 与 no duplicate effect；Console 和 stale Job fence 仍 Not run |

## 时延与未关闭项

核心 journey step 从 22:45:46Z 到 22:59:47Z，用时约 14 分 1 秒；整个 job 用时约 14 分 38 秒。
三份报告覆盖的业务 journey 从 22:59:09Z 到 22:59:42Z。首次 cold release build 因此仍使 clone-to-first-Run
超过 G1 的 10 分钟目标；本次成功不能作为 G1 wall-clock Passed。后续必须用可安装 CLI/预构建开发产物或更强的
可复现缓存闭合冷启动路径，并用新的 fresh runner evidence 复测。

本证据也不覆盖 full profile 的真实 remote Model/Capability/MCP/Context/WASI workload，不覆盖剩余七条黄金场景，
不覆盖正式静态部署、生产 telemetry/accessibility/慢依赖资格，也不覆盖用户已明确排除的真实多节点 Kubernetes
L4～L6。
