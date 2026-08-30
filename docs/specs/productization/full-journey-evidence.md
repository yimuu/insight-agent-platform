# Full profile fresh journey evidence

状态：**Passed（composition + seven-scenario closure）**。这不是十条黄金场景的完成证据。

## 1. 权威运行

最新 exact-revision 运行：

- Git revision：`2e4402a3a9f0574f8fea445a34f6675869d02f42`；
- 命令：`scripts/run-productization-base-journey.sh --profile full --console-browser
  --report-directory target/productization-reports-2e4402a3`；
- 环境：macOS Darwin 25.6.0 arm64、Rust 1.94.1、Node 24.14.1、pnpm 11.19.0、Chrome 151、
  Docker Engine 29.4.0、Compose v5.1.2；
- 场景报告时间：`2026-08-30T12:16:27Z` 至 `12:17:25Z`；测试主体 57.79 秒；
- report checker：`validated 7 productization scenario report(s); complete_gate=false`。

runner 从不存在的 project path 执行 `doctor -> init -> dev --profile full -> status -> token ->
productization test -> report checker -> stop`。`token` 在全部角色 ready 后重新签发，stdout 被丢弃，因此不会把
bearer credential 写入日志。执行前 working tree 为空，七份 report 的 `source_revision` 均精确等于上述 40 字节 revision。

此前 GitHub Linux composition 证据仍保留：revision `5a12a3deb8658e1dd496313b3f5bab9e352d5efe`、run
[`33290248516`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516)、job
[`99200524415`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516/job/99200524415) 为 Passed；它是五场景
闭包，不能替代本次新增的 Context 与 Model report。

## 2. 已证明的闭包

同一 fresh PostgreSQL、NATS、LocalStack authority 上，以下 25 个独立角色全部报告 `ready`：

`artifact-data`、`artifact-gateway`、`artifact-maintenance`、`callback-api`、`capability-native`、
`capability-remote`、`context-dataset`、`context-native`、`context-remote`、`context-subscription`、
`egress-broker`、`gateway-management`、`gateway-runtime`、`mcp-cleanup`、`mcp-discovery`、`mcp-host`、
`mcp-resource-host`、`mcp-subscription`、`model-worker`、`orchestration`、`registry-validation`、
`sandbox-attestor`、`sandbox-controller`、`sandbox-executor`、`security-authority`。

真实 Management/Runtime Gateway、CLI、raw `/v1` 与 headless Chrome共同完成四条 base journey，以及 full-only
Artifact lifecycle/rejection、Context retrieval/citation 与 exact Model streaming journey。Model 路径通过 public
authoring lifecycle 发布 exact Provider/Profile/Agent，在默认拒绝的 Egress catalog 中安装显式 localhost TLS
fixture，并保留 exact SecretBinding metadata 但不解析开发 fixture 的物理凭据。Context 路径通过 public authoring lifecycle发布
Interface/Implementation/Deployment，等待 Dataset Operation 的 typed Generation result，冻结 exact Generation 与 policy
binding，随后由独立 Context Worker提交 citation result。结束时 runner停止 exact Platform进程并移除 fresh Compose closure。

## 3. Machine-readable reports

目录 `target/productization-reports-2e4402a3` 由 report checker按 manifest、closed schema、exact source revision、
entrypoint、assertion与 failure probe重验通过：

| 场景 | 状态 | SHA-256 |
|---|---|---|
| `approval-task-resume` | Passed | `a8a26ab8d47f598747b0835c7e03b2a326f705c68c26d0841c48f4c3a206b9cf` |
| `artifact-lifecycle-and-rejection` | Passed | `b75f53ac2fc87997016989c82c2eceb509964de70b9f333be471d12c6681ee5d` |
| `context-retrieval-and-citation` | Passed | `131f1e7d1d677811d57119a2c05cf61065d1f81a4804bfab588268746f44378b` |
| `deterministic-first-run` | Passed | `6ea2c9b51fd075e34af64ccb47094946ad42097c42a42cdd6627d2c6e3c5a53a` |
| `exact-model-streaming-chat` | Passed | `7cd00a1d7ae889f57805376d62b7e2fba59c8a546863364e4cc28c3cc9812fe3` |
| `subagent-quota-and-cancel` | Passed | `c2236cb68ba3f56aa3a1f43236eb9661e717757780efad6b32b304421eef1249` |
| `timer-signal-restart-recovery` | Passed | `c61eb7ef4eb46db72d72a2c5e5c9723fe3774559ddd817210eef9ace9531b311` |

## 4. 明确边界

本次运行证明 full profile 可以在 fresh authority 上完成整体构建、25 角色监督启动、public `/v1`、CLI、真实
Console、Artifact、Context 与 Model 用户旅程及受控清理。严格 M4 仍缺 `native-and-remote-capability`、
`remote-mcp-tool-and-resource` 与 `wasi-and-remote-framework-capability` 三份 Passed report；
因此 `complete_gate=false`，不得宣称十场景或 Productization Convergence 完成。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
