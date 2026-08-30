# Full profile fresh journey evidence

状态：**Passed（composition + ten-scenario manifest gate）**。Sandbox durable admission 与外部 L4～L6
发布门禁仍单独保持未完成。

## 1. 权威运行

最新 exact-revision 运行：

- Git revision：`3f2ee593c75ff81c96a4b2968118d411ff89b2f8`；
- 命令：`scripts/run-productization-base-journey.sh --profile full --console-browser
  --report-directory target/productization-reports --node-bin
  /Users/cc/.nvm/versions/node/v24.14.1/bin/node --browser-bin
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'`；
- 环境：macOS Darwin 25.6.0 arm64、Rust 1.94.1、Node 24.14.1、pnpm 11.19.0、Chrome 151、
  Docker Engine 29.4.0、Compose v5.1.2；
- 场景报告时间：`2026-08-30T15:29:30Z` 至 `15:30:51Z`；测试主体 80.28 秒；
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
调用、Resource descriptor、remote JSON-RPC error 与 TLS 拒绝。framework-neutral reference service 以独立 Node 进程
运行，进程环境被清空后只注入监听、TLS 和 trace 四项配置，不获得 Platform DB 或内部 credential；成功调用只经
exact Egress Deployment。WASI 路径用 production Wasmtime adapter 执行 closed ABI module，并对无限循环 module 证明
exact fuel exhaustion。结束时 runner停止 exact Platform进程并移除 fresh Compose closure。

## 3. Machine-readable reports

目录 `target/productization-reports` 由 report checker按 manifest、closed schema、exact source revision、
entrypoint、assertion与 failure probe重验通过：

| 场景 | 状态 | SHA-256 |
|---|---|---|
| `approval-task-resume` | Passed | `10f73cba7e9e117fc3270e6825b98525743d7b22ca7265a2f714d97d45db619c` |
| `artifact-lifecycle-and-rejection` | Passed | `87416f3e17677ef787ddfccfe85aec96906cdff5cc46f4eea379be4c702591a9` |
| `context-retrieval-and-citation` | Passed | `a036d4d6870758b1fbd4e270f8f3693496b576ab53778e985ca8575d02c06f69` |
| `deterministic-first-run` | Passed | `407e8d237e4f9ce53090d4151a44aba706ea05a352fbfc4cebd787b14804c133` |
| `exact-model-streaming-chat` | Passed | `fc9db606517755d723c00f51ac92b612de19a95942c3701a2b8dbeb767260bcd` |
| `native-and-remote-capability` | Passed | `aa97872d3dd387b0432daa47fb662326673a803ec248012aedd519f6d38489f9` |
| `remote-mcp-tool-and-resource` | Passed | `24f88418ac3778850e6c6c4f9636310b05335bbbbf6f96909bbf72fdfdb2e8d1` |
| `subagent-quota-and-cancel` | Passed | `2189d12ad111c6d48175024638c5b1a3939acc29b70b0ae41ef47dff84eb4f6f` |
| `timer-signal-restart-recovery` | Passed | `353338bcfb4a39784bd96c4bde58f45b12bea8053e0376066fd874f7e4c01bae` |
| `wasi-and-remote-framework-capability` | Passed | `b584b2e526159f25bc554d63d33d75c095bde38f0af84709f9e4d0ee58f872ef` |

## 4. 明确边界

本次运行证明 full profile 可以在 fresh authority 上完成整体构建、25 角色监督启动、public `/v1`、CLI、真实
Console 以及 manifest 定义的十条用户旅程，严格十报告门禁为 `complete_gate=true`。

WASI 部分当前证明的是 production Wasmtime adapter 的真实 closed-request 执行、资源计量、fuel-limit 失败与 destroy，
不是普通 Agent `CapabilityCall` 经 Controller 原子创建 durable Sandbox Job 的端到端证据；生产编排目前仍拒绝
`Sandbox` backend 的普通 Capability dispatch。remote framework 部分是 framework-neutral typed HTTP reference service，
不是 Agno/LangGraph SDK 本身。Python SDK 已按产品决策取消；若未来要求具体 Agno/LangGraph adapter，需另立受冻结依赖
与独立发布物的目标，不得把它链接进 Gateway/Worker。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
