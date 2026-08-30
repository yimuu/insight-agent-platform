# Full profile fresh Linux journey evidence

状态：**Passed（composition + five-scenario closure）**。这不是十条黄金场景的完成证据。

## 1. 权威运行

- Git revision：`5a12a3deb8658e1dd496313b3f5bab9e352d5efe`；
- GitHub Actions run：[`33290248516`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516)；
- job：[`99200524415`](https://github.com/yimuu/insight-agent-platform/actions/runs/33290248516/job/99200524415)，结论 `success`；
- 环境：GitHub `ubuntu-24.04`、Rust 1.94.1、Node 24、pnpm 11.19.0、预装 Chrome；
- job：`2026-08-30T03:26:06Z` 至 `03:47:34Z`；核心 journey step：`03:26:43Z` 至
  `03:47:31Z`，约 20 分 48 秒。

该 revision 的主 CI 暴露了一个只影响后段 WASI qualification fixture 的生命周期字段污染；后续 test-only
revision `49fa46cca6e15858bc2ff1c8bb020a8862005ab1` 清空派生 Invocation 的终态字段，未改变 runtime 或 journey
实现。对应主 CI run [`33291399441`](https://github.com/yimuu/insight-agent-platform/actions/runs/33291399441) 的
lint/check、全量测试、文档测试和 Required CI summary 全部 Passed。

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

真实 Runtime Gateway 和 headless Chrome 随后完成 deterministic Run、Human Task resume、Timer/Signal restart、
Subagent quota/cancel 四条 base journey，以及 full-only Artifact lifecycle/rejection journey。结束时 runner 停止 exact
Platform 进程，并移除该 fresh Compose dependency closure。

## 3. Machine-readable reports

上传 artifact `productization-full-scenario-reports-5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 包含五份
canonical report。下载后以 `check-productization-scenario-reports.py --allow-incomplete --source-revision
5a12a3deb8658e1dd496313b3f5bab9e352d5efe` 重验通过：

| 场景 | 状态 | SHA-256 |
|---|---|---|
| `approval-task-resume` | Passed | `93b360a8b226c34a091a1e629262bf0cd409ed144168e6c220c8eb785c7442a1` |
| `artifact-lifecycle-and-rejection` | Passed | `ce13b78dd8d8671f35d3b700f3254d0c9130517c3ba4f19d006f9ec8af783d4b` |
| `deterministic-first-run` | Passed | `a38c4f68acbf228dfb39c97bd46bb1a62901f2673fd24cede20bd46ae97e8204` |
| `subagent-quota-and-cancel` | Passed | `795bb8fd598d6a61aeefa97646d7d863304d61e77aec0f6973b7aedfeeaf8c5d` |
| `timer-signal-restart-recovery` | Passed | `405daacb927e7a4daaa08de4f23d800031e15b52ffd73bb9bba724eb37ead525` |

## 4. 明确边界

本次运行证明 full profile 可以在 Linux fresh authority 上完成整体构建、24 角色监督启动、public `/v1` base
journey、真实 Console、Artifact lifecycle/rejection 与受控清理。它没有安装或调用 remote Model、Capability、MCP、
Context、WASI 或 framework fixture，因此不能替代剩余五条 M4 report，也不证明 OAuth lifecycle、WASI limit、remote
UnknownOutcome 或生态适配。

真实多节点 Kubernetes、runsc、容量、混沌、restore 与 soak 依照阶段范围继续为 **Not run**。
