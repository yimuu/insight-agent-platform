# Full profile fresh Linux journey evidence

状态：**Passed（composition + base journey scope）**。这不是七条 full-only 黄金场景的完成证据。

## 1. 权威运行

- Git revision：`00310f9ff5162c2c2aa259dd8565b133a32568ca`；
- GitHub Actions run：[`33283147235`](https://github.com/yimuu/insight-agent-platform/actions/runs/33283147235)；
- job：`99181622944`，结论 `success`；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- job：`2026-08-30T00:22:11Z` 至 `00:42:15Z`；核心 journey step：`00:22:41Z` 至
  `00:42:13Z`，约 19 分 32 秒。

runner 从不存在的 project path 执行 `doctor -> init -> dev --profile full -> status -> token ->
productization test -> stop`。`token` 在全部角色 ready 后重新签发，stdout 被丢弃，因此长时间 full build/start
不会使用过期 bearer credential，也不会把 token 写入 CI 日志。

## 2. 已证明的闭包

同一 fresh PostgreSQL、NATS、LocalStack authority 上，以下 24 个独立角色全部报告 `ready`：

`artifact-data`、`artifact-gateway`、`artifact-maintenance`、`callback-api`、`capability-native`、
`capability-remote`、`context-native`、`context-remote`、`context-subscription`、`egress-broker`、
`gateway-management`、`gateway-runtime`、`mcp-cleanup`、`mcp-discovery`、`mcp-host`、
`mcp-resource-host`、`mcp-subscription`、`model-worker`、`orchestration`、`registry-validation`、
`sandbox-attestor`、`sandbox-controller`、`sandbox-executor`、`security-authority`。

真实 Runtime Gateway 和 headless Chrome 随后完成 deterministic Run、Human Task resume 和 Timer/Signal restart
三条 base journey。结束时 runner 停止 exact Platform 进程，并移除该 fresh Compose dependency closure。

## 3. 明确边界

本次运行证明 full profile 可以在 Linux fresh authority 上完成整体构建、24 角色监督启动、public `/v1` base
journey、真实 Console 与受控清理。它没有安装或调用 remote Model、Capability、MCP、Context、WASI 或 framework
fixture，因此不能替代剩余七条 M4 report，也不证明 OAuth lifecycle、WASI limit、remote UnknownOutcome 或生态适配。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
