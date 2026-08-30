# Fresh base journey 资格证据

状态：**Passed with four-scenario closure**。本文件记录一次精确 revision 的真实 base profile 资格运行，
不表示 M1、M3、M4 或 Platform v2 spec00～18 已全部 Verified。

## 权威运行

- Git revision：`5a12a3deb8658e1dd496313b3f5bab9e352d5efe`；
- GitHub Actions run：[`33289764921`](https://github.com/yimuu/insight-agent-platform/actions/runs/33289764921)；
- job：[`99199229740`](https://github.com/yimuu/insight-agent-platform/actions/runs/33289764921/job/99199229740)；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- 结果：2026-08-30T03:13:26Z 开始，2026-08-30T03:24:41Z 成功结束。

工作流只由 `workflow_dispatch` 显式触发。它没有构建或推送 candidate image，也没有执行 cosign、SBOM、
provenance 或 GitOps promotion。核心步骤执行 fresh `doctor -> init -> dev --profile base -> status`，通过 public
`/v1` 运行 CLI、checked raw HTTP、Orchestration replacement/restart 与真实 headless Chrome Console journey，最后
停止 exact Platform/Compose closure。

## Machine-readable reports

上传 artifact `productization-base-scenario-reports-5a12a3deb8658e1dd496313b3f5bab9e352d5efe`
包含四份 canonical report。下载后以
`check-productization-scenario-reports.py --allow-incomplete --source-revision
5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 重验通过：

| 场景 | 状态 | SHA-256 | 已关闭边界 |
|---|---|---|---|
| `approval-task-resume` | Passed | `13acc64b2f37d01b413872c227265fb28c8994ae875fb5d201b2334e7f91eca9` | CLI、raw `/v1`、真实 Console；waiting/first-winner/resume；replay 与 stale Task fence |
| `deterministic-first-run` | Passed | `11824d3ce537407da71aa1fe2e0641024008b0fda9d1b5b090121425a356b067` | CLI、raw `/v1`、真实 Console；terminal/SSE/exact binding、Receipt conflict 与 Gateway unavailable |
| `subagent-quota-and-cancel` | Passed | `fb3f0513523357c217b62bf76ab4335e2dc3eae3b078f26063e1c7206006d82a` | typed child Run、exact Deployment、quota reservation、cascade cancel、late Timer first-winner 与 quota exhaustion 原子拒绝 |
| `timer-signal-restart-recovery` | Passed | `58fe3956065db606b7bd42aa38d24e7cfb879e0e043dab1a4eee56b4c8ef155f` | CLI、raw signal、真实 Console、replacement Worker、Event sequence、no duplicate effect 与受控 stale Job fence |

## 时延与未关闭项

核心 journey step 从 03:14:04Z 到 03:24:19Z，用时约 10 分 15 秒；整个 job 用时约 11 分 15 秒。
四份报告覆盖的业务 journey 从 03:23:37Z 到 03:24:15Z。该 run 使用受控 Cargo cache，但报告尚未记录 clone 与
first Run authority commit 的独立时间点，也不是无缓存机器；因此
仍不能把 G1 的 cold clone-to-first-Run 门禁升级为 Passed。后续应直接记录 first Run commit timestamp，并补一条
明确 cache state 的 fresh runner evidence。

本证据也不覆盖 full profile 的真实 remote Model/Capability/MCP/Context/WASI workload，不覆盖剩余六条黄金场景，
不覆盖正式静态部署、生产 telemetry/accessibility/慢依赖资格，也不覆盖用户已明确排除的真实多节点 Kubernetes
L4～L6。
