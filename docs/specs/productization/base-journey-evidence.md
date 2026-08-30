# Fresh base journey 资格证据

状态：**Passed with three-scenario closure**。本文件记录一次精确 revision 的真实 base profile 资格运行，
不表示 M1、M3、M4 或 Platform v2 spec00～18 已全部 Verified。

## 权威运行

- Git revision：`e03b6cc123f5f1ada2c96a47f167956adde7a095`；
- GitHub Actions run：[`33284301192`](https://github.com/yimuu/insight-agent-platform/actions/runs/33284301192)；
- job：[`99184695618`](https://github.com/yimuu/insight-agent-platform/actions/runs/33284301192/job/99184695618)；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- 结果：2026-08-30T00:51:28Z 开始，2026-08-30T01:01:25Z 成功结束。

工作流只由 `workflow_dispatch` 显式触发。它没有构建或推送 candidate image，也没有执行 cosign、SBOM、
provenance 或 GitOps promotion。核心步骤执行 fresh `doctor -> init -> dev --profile base -> status`，通过 public
`/v1` 运行 CLI、checked raw HTTP、Orchestration replacement/restart 与真实 headless Chrome Console journey，最后
停止 exact Platform/Compose closure。

## Machine-readable reports

上传 artifact `productization-base-scenario-reports-e03b6cc123f5f1ada2c96a47f167956adde7a095`
包含三份 canonical report。下载后以
`check-productization-scenario-reports.py --allow-incomplete --source-revision
e03b6cc123f5f1ada2c96a47f167956adde7a095` 重验通过：

| 场景 | 状态 | SHA-256 | 已关闭边界 |
|---|---|---|---|
| `approval-task-resume` | Passed | `8b0981f30328a7c8d09f6bf766c3dd9213ed17ec3c451fa5c9fe8e957ee6e919` | CLI、raw `/v1`、真实 Console；waiting/first-winner/resume；replay 与 stale Task fence |
| `deterministic-first-run` | Passed | `1694f1378a8fbea6680c1059802ac5669d6dceb1bfca0dee48331d75a5824eaf` | CLI、raw `/v1`、真实 Console；terminal/SSE/exact binding、Receipt conflict 与 Gateway unavailable |
| `timer-signal-restart-recovery` | Passed | `de8feeba211b394e212613d6de9453bbeb936ba44f947017367eccff6b7d86ec` | CLI、raw signal、真实 Console、replacement Worker、Event sequence、no duplicate effect 与受控 stale Job fence |

## 时延与未关闭项

核心 journey step 从 00:52:08Z 到 01:01:10Z，用时约 9 分 2 秒；整个 job 用时约 9 分 57 秒。
三份报告覆盖的业务 journey 从 01:00:24Z 到 01:01:03Z。该 run 使用受控 Cargo cache，但报告尚未记录 clone 与
first Run authority commit 的独立时间点，也不是无缓存机器；因此
仍不能把 G1 的 cold clone-to-first-Run 门禁升级为 Passed。后续应直接记录 first Run commit timestamp，并补一条
明确 cache state 的 fresh runner evidence。

本证据也不覆盖 full profile 的真实 remote Model/Capability/MCP/Context/WASI workload，不覆盖剩余七条黄金场景，
不覆盖正式静态部署、生产 telemetry/accessibility/慢依赖资格，也不覆盖用户已明确排除的真实多节点 Kubernetes
L4～L6。
