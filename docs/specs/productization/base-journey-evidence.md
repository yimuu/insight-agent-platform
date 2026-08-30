# Fresh base journey 资格证据

状态：**Passed with partial scenario closure**。本文件记录一次精确 revision 的真实 base profile 资格运行，
不表示 M1、M3、M4 或 Platform v2 spec00～18 已全部 Verified。

## 权威运行

- Git revision：`972a37f67cf22406c5064418aa1d759cc16e3c72`；
- GitHub Actions run：[`33281976729`](https://github.com/yimuu/insight-agent-platform/actions/runs/33281976729)；
- job：[`99178547427`](https://github.com/yimuu/insight-agent-platform/actions/runs/33281976729/job/99178547427)；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- 结果：2026-08-29T23:52:09Z 开始，2026-08-30T00:02:21Z 成功结束。

工作流只由 `workflow_dispatch` 显式触发。它没有构建或推送 candidate image，也没有执行 cosign、SBOM、
provenance 或 GitOps promotion。核心步骤执行 fresh `doctor -> init -> dev --profile base -> status`，通过 public
`/v1` 运行 CLI、checked raw HTTP、Orchestration replacement/restart 与真实 headless Chrome Console journey，最后
停止 exact Platform/Compose closure。

## Machine-readable reports

上传 artifact `productization-base-scenario-reports-972a37f67cf22406c5064418aa1d759cc16e3c72`
包含三份 canonical report。下载后以
`check-productization-scenario-reports.py --allow-incomplete --source-revision
972a37f67cf22406c5064418aa1d759cc16e3c72` 重验通过：

| 场景 | 状态 | SHA-256 | 已关闭边界 |
|---|---|---|---|
| `approval-task-resume` | Passed | `07e02990de10b7b3445b2033e3b7a4dc278c681a437da3505e6bf35c2795e1a8` | CLI、raw `/v1`、真实 Console；waiting/first-winner/resume；replay 与 stale Task fence |
| `deterministic-first-run` | Passed | `3d4045331f995c18af40791909c7973fa44f187123667dd8203bda82686ed7a4` | CLI、raw `/v1`、真实 Console；terminal/SSE/exact binding、Receipt conflict 与 Gateway unavailable |
| `timer-signal-restart-recovery` | Incomplete | `e82836c34674bdd1d1de631a4ae9a5a6ad6470d4d17ef8c2ffdc42cfbf29161d` | CLI、raw signal、真实 Console、replacement Worker、Event sequence 与 no duplicate effect；仅 stale Job fence 仍 Not run |

## 时延与未关闭项

核心 journey step 从 23:52:41Z 到 00:02:17Z，用时约 9 分 36 秒；整个 job 用时约 10 分 12 秒。
三份报告覆盖的业务 journey 从 00:01:45Z 到 00:02:13Z。该 run 使用受控 Cargo cache，显著优于上一轮的
14 分 1 秒核心步骤，但报告尚未记录 clone 与 first Run authority commit 的独立时间点，也不是无缓存机器；因此
仍不能把 G1 的 cold clone-to-first-Run 门禁升级为 Passed。后续应直接记录 first Run commit timestamp，并补一条
明确 cache state 的 fresh runner evidence。

本证据也不覆盖 full profile 的真实 remote Model/Capability/MCP/Context/WASI workload，不覆盖剩余七条黄金场景，
不覆盖正式静态部署、生产 telemetry/accessibility/慢依赖资格，也不覆盖用户已明确排除的真实多节点 Kubernetes
L4～L6。
