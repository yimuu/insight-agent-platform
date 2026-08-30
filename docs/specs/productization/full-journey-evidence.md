# Full profile fresh journey evidence

状态：**Passed（composition + six-scenario closure）**。这不是十条黄金场景的完成证据。

## 1. 权威运行

最新 exact-revision 运行：

- Git revision：`63fa4132ba274460d94ef03a44a21cec9f5017fc`；
- 命令：`scripts/run-productization-base-journey.sh --profile full --console-browser --node-bin
  /Users/cc/.nvm/versions/node/v24.14.1/bin/node --report-directory target/productization-reports-63fa4132`；
- 环境：macOS Darwin 25.6.0 arm64、Rust 1.94.1、Node 24.14.1、pnpm 11.19.0、Chrome 151、
  Docker Engine 29.4.0、Compose v5.1.2；
- 场景报告时间：`2026-08-30T11:12:22Z` 至 `11:13:12Z`；测试主体 49.84 秒；
- report checker：`validated 6 productization scenario report(s); complete_gate=false`。

runner 从不存在的 project path 执行 `doctor -> init -> dev --profile full -> status -> token ->
productization test -> report checker -> stop`。`token` 在全部角色 ready 后重新签发，stdout 被丢弃，因此不会把
bearer credential 写入日志。执行前 working tree 为空，六份 report 的 `source_revision` 均精确等于上述 40 字节 revision。

此前 GitHub Linux composition 证据仍保留：revision `5a12a3deb8658e1dd496313b3f5bab9e352d5efe`、run
[`33290248516`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516)、job
[`99200524415`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516/job/99200524415) 为 Passed；它是五场景
闭包，不能替代本次新增的 Context report。

## 2. 已证明的闭包

同一 fresh PostgreSQL、NATS、LocalStack authority 上，以下 25 个独立角色全部报告 `ready`：

`artifact-data`、`artifact-gateway`、`artifact-maintenance`、`callback-api`、`capability-native`、
`capability-remote`、`context-dataset`、`context-native`、`context-remote`、`context-subscription`、
`egress-broker`、`gateway-management`、`gateway-runtime`、`mcp-cleanup`、`mcp-discovery`、`mcp-host`、
`mcp-resource-host`、`mcp-subscription`、`model-worker`、`orchestration`、`registry-validation`、
`sandbox-attestor`、`sandbox-controller`、`sandbox-executor`、`security-authority`。

真实 Management/Runtime Gateway、CLI、raw `/v1` 与 headless Chrome共同完成四条 base journey，以及 full-only
Artifact lifecycle/rejection 与 Context retrieval/citation journey。Context 路径通过 public authoring lifecycle发布
Interface/Implementation/Deployment，等待 Dataset Operation 的 typed Generation result，冻结 exact Generation 与 policy
binding，随后由独立 Context Worker提交 citation result。结束时 runner停止 exact Platform进程并移除 fresh Compose closure。

## 3. Machine-readable reports

目录 `target/productization-reports-63fa4132` 由 report checker按 manifest、closed schema、exact source revision、
entrypoint、assertion与 failure probe重验通过：

| 场景 | 状态 | SHA-256 |
|---|---|---|
| `approval-task-resume` | Passed | `0bf92c1522eff3c945de29b25e609b43cda295c1e0226a58c6cfdfc16675428e` |
| `artifact-lifecycle-and-rejection` | Passed | `61061b0eb9017c0b5e8f080d04ae24252e3c1daf3b41686392502d629977ceed` |
| `context-retrieval-and-citation` | Passed | `c16c68cacfeecafae03cfd9bba3c899ea09807e7994048537fb10c12eccec067` |
| `deterministic-first-run` | Passed | `6c32fda248fd19f3ef588b759cbe1544f575a037192d0f8e1c3c4d4e003bfe61` |
| `subagent-quota-and-cancel` | Passed | `dfab5c604970a813457871f10ec95bd57461fe4c45f55fc6316e54c9d40c458d` |
| `timer-signal-restart-recovery` | Passed | `5aef21a6b6c39020531fda90e8cfb92b1490222992da40fe306cf70836dc92a3` |

## 4. 明确边界

本次运行证明 full profile 可以在 fresh authority 上完成整体构建、25 角色监督启动、public `/v1`、CLI、真实
Console、Artifact 与 Context 用户旅程及受控清理。严格 M4 仍缺 `exact-model-streaming-chat`、
`native-and-remote-capability`、`remote-mcp-tool-and-resource` 与 `wasi-and-remote-framework-capability` 四份 Passed report；
因此 `complete_gate=false`，不得宣称十场景或 Productization Convergence 完成。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
