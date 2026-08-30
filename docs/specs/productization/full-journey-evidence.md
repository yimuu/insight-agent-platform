# Full profile fresh journey evidence

状态：**Passed（composition + ten-scenario manifest gate + durable Sandbox admission + LangGraph.js reference）**。
外部 L4～L6 发布门禁仍单独保持未完成。

## 1. 权威运行

最新 exact-revision 运行：

- Git revision：`a70a9f99f58b8fd9fecb4c309f910aa99434b122`；
- 命令：`scripts/run-productization-base-journey.sh --profile full --console-browser
  --report-directory target/productization-reports --node-bin
  /Users/cc/.nvm/versions/node/v24.14.1/bin/node --browser-bin
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'`；
- 环境：macOS Darwin 25.6.0 arm64、Rust 1.94.1、Node 24.14.1、pnpm 11.19.0、Chrome 151、
  Docker Engine 29.4.0、Compose v5.1.2；
- 场景报告时间：`2026-08-30T17:36:15Z` 至 `17:37:39Z`；测试主体 84.80 秒；
- 严格 report checker：`validated 10 productization scenario report(s); complete_gate=true`。

runner 从不存在的 project path 执行 `doctor -> init -> dev --profile full -> status -> token ->
productization test -> report checker -> stop`。`token` 在全部角色 ready 后重新签发，stdout 被丢弃，因此不会把
bearer credential 写入日志。执行前 working tree 为空，十份 report 的 `source_revision` 均精确等于上述 40 字节 revision。

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
Artifact lifecycle/rejection、Context retrieval/citation、exact Model streaming、Native/Remote Capability、remote MCP 和
WASI/remote-framework journey。Model 路径通过 public
authoring lifecycle 发布 exact Provider/Profile/Agent，在默认拒绝的 Egress catalog 中安装显式 localhost TLS
fixture，并保留 exact SecretBinding metadata 但不解析开发 fixture 的物理凭据。Context 路径通过 public authoring lifecycle发布
Interface/Implementation/Deployment，等待 Dataset Operation 的 typed Generation result，冻结 exact Generation 与 policy
binding，随后由独立 Context Worker提交 citation result。Capability 路径通过 public authoring lifecycle 发布 exact
Interface/Implementation/Deployment，并让 Native output 成为经 Egress TLS fixture 执行的 Remote input；非幂等 timeout
保留为 reconciliation，deny-all catalog 在 dispatch 前拒绝。MCP 路径完成 public Server authoring、Discovery、Tool
调用、Resource descriptor、remote JSON-RPC error 与 TLS 拒绝。固定 `@langchain/langgraph` 1.4.13 的
`StateGraph` reference service 以独立 Node 进程运行，进程环境被清空后只注入监听、TLS 和 trace 四项配置，不获得
Platform DB 或内部 credential；成功调用只经 exact Egress Deployment。WASI 路径从 public Agent Run 的 exact
Sandbox Capability binding 开始，由 Controller 原子创建 durable Sandbox Job，再由独立 Executor 使用 production
Wasmtime adapter 执行 closed ABI module、合并 typed outcome，并对无限循环 module 证明 exact fuel exhaustion。
结束时 runner 停止 exact Platform 进程并移除 fresh Compose closure。

## 3. Machine-readable reports

目录 `target/productization-reports` 由 report checker按 manifest、closed schema、exact source revision、
entrypoint、assertion与 failure probe重验通过：

| 场景 | 状态 | SHA-256 |
|---|---|---|
| `approval-task-resume` | Passed | `0211a235db0244588721aee81c90b9783339d3ca365fb0de09ed49345fd6a6c1` |
| `artifact-lifecycle-and-rejection` | Passed | `85ab48c2eb911db4002797de27a3551ee89ff1fd28149420d8958a8205fc0619` |
| `context-retrieval-and-citation` | Passed | `d0ac4c0e820f1be27f32b25bfd1a2cac43b0daf4aafd586fc945a5196831de9a` |
| `deterministic-first-run` | Passed | `9188e7fa08e38908534eefa80d9580e3a382c945dcb0f0c8fb018d9a14f8d758` |
| `exact-model-streaming-chat` | Passed | `52b8f7cca7f8d0662f832d42a50b8219208c3304694c1ce45ed1ac07f0003fc5` |
| `native-and-remote-capability` | Passed | `38c5453bf6b7700fec517a581b54d0d27c30c4be3278f71764ed4a153045762b` |
| `remote-mcp-tool-and-resource` | Passed | `ed094735c3c56edf4452784ed7151a7eef6807cb294b1c0aa3633a192346fef2` |
| `subagent-quota-and-cancel` | Passed | `d95cc9b11e3f0460161382db3a780f952dbb400e4ff1326b054c2ea7e33ebaea` |
| `timer-signal-restart-recovery` | Passed | `9847bfe8d99f3034e20cb034d89d45820cf7d66437f0d852c71a656b871a4d5c` |
| `wasi-and-remote-framework-capability` | Passed | `d77917f21edb7e44b6ca662ecb429e7a69c861492f48807945152fb01e755c66` |

## 4. 明确边界

本次运行证明 full profile 可以在 fresh authority 上完成整体构建、25 角色监督启动、public `/v1`、CLI、真实
Console 以及 manifest 定义的十条用户旅程，严格十报告门禁为 `complete_gate=true`。

WASI 部分已证明普通 public Agent `CapabilityCall` 经 Controller 原子创建 durable Sandbox Job、独立 Executor
领取和执行、fenced outcome merge、Run permit 恢复及 terminal typed result 的端到端链路。remote framework 部分已使用
固定依赖的 LangGraph.js `StateGraph`，而不是平台内的框架模拟器；它仍保持独立发布与 Egress 隔离，不链接进
Gateway/Worker。Python SDK 已按产品决策取消，不属于本阶段完成条件。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
