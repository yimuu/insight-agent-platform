# Console 浏览器 fixture evidence

| 属性 | 值 |
|---|---|
| 日期 | 2026-08-29 |
| 范围 | `web/console` 静态 bundle + stateful public `/v1` fixture |
| 浏览器 | Codex 内置 Chromium，会话内自动化 |
| 结论 | Passed within fixture boundary |

## 已检查行为

- `/readyz` 不携带 Authorization；所有 `/v1` 请求携带内存态 bearer token，日志只记录 presence；
- Run 首次读取接收 `cur-browser-1`，重连发送 exact `Last-Event-ID: cur-browser-1` 并接收
  `cur-browser-2`；
- Run Event 发现 Task，Task mutation 携带 `If-Match: "task-v1"` 与唯一 `Idempotency-Key`，随后
  Task version 变为 2、Run 变为 `succeeded`；
- pause、cancel、resume 分别投影 closed 409、412、429 Problem，429 保留 retryable 与 trace ID；
- checked `access_token`、`credential`、`raw_prompt`、`tool_output` 不出现在 DOM 或 console，DOM 使用
  `[redacted]`；
- 页面刷新清除 token 和已读取 Run projection；token 不进入 URL、`localStorage` 或 `sessionStorage`；
- skip link 是首个可聚焦元素，目标 `main#console-main` 可接收焦点，当前导航项携带
  `aria-current="page"`，状态消息使用 atomic live region。
- 对无 Event 的非终态 Run 显示明确空 timeline，不把“尚无 committed event”伪装成加载失败；
- fixture 延迟 Run authority 响应时，Inspect 按钮显示 `Loading…` 并保持 disabled，响应完成后再投影 authority。

## 重现

```bash
cd web/console
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
pnpm run browser:fixture:qualify
```

`browser:fixture:qualify` 会自行启动 fixture、构建后的同源代理和 headless Chrome，执行完整旅程，并对
fixture 请求日志做闭合断言。GitHub `console` job 固定在带 Chrome 的 `ubuntu-24.04` runner 上执行同一命令；
浏览器可执行文件从显式环境变量或受限的 macOS/Linux 标准位置解析，不在 CI 中临时下载浏览器。fixture
使用固定 Run/Task 标识和内存态转换，因此每次服务重启都会回到 waiting/pending 初态；它只用于浏览器
契约验证，不进入生产产物。

远端权威记录为 Git revision `d6dca5c180f9027441284b29b2c2684b3fd0c795`、GitHub Actions run
[`33274451057`](https://github.com/yimuu/insight-agent-platform/actions/runs/33274451057)、job
[`99158633401`](https://github.com/yimuu/insight-agent-platform/actions/runs/33274451057/job/99158633401)。该 job 在
`ubuntu-24.04` 的预装 Chrome 中返回 closed `passed` evidence，记录 `request_count=15`，并逐项通过
`gateway_ready`、`sse_task_discovery`、`task_mutation`、`terminal_run`、`reload_authority_read` 与
`memory_only_token`。workflow 后因更晚提交取代而取消，不改变已完成 job 的结果；该证据只适用于上述 exact revision
且仅适用于 stateful fixture boundary。2026-08-31 的增强 fixture 本地闭合运行返回 `request_count=17`，并新增
`empty_run_timeline` 与 `slow_dependency_busy_state`；远端 CI 结果在对应提交完成后按 exact revision 记录。

## 证据边界

本报告不证明 PostgreSQL transaction、真实 Gateway authentication/authorization、进程重启恢复、Ingress
同源承载或 telemetry sink 脱敏。因此它只关闭 M3 浏览器交互矩阵的一部分，不把 M3 或 Platform
spec00～18 升级为 Verified。

## 真实 Gateway 模式

[`gateway-server.mjs`](../../../web/console/tests/gateway-server.mjs) 现提供仅用于开发/资格测试的静态同源入口：
upstream 必须是 origin-only loopback HTTP，只有 `/readyz` 与 `/v1` 会透明转发，其余路径只读取已构建的
`dist/`。代理不解码或记录 Authorization，不连接数据库或内部 RPC，也不进入生产 bundle。

[`real-gateway-journey.mjs`](../../../web/console/tests/real-gateway-journey.mjs) 使用 headless Chromium 操作真实页面，
检查 Gateway readiness、Run SSE Task 发现、typed Task mutation、terminal Run/result、刷新清除 token/Run projection，
以及重新输入内存 token 后按同一 authority Run ID 读取终态。base runner 的显式 `--console-browser` 会在同一 fresh
PostgreSQL/Gateway authority 中额外创建一条 Human Task Run；只有浏览器脚本返回 closed Passed evidence 时，
`approval-task-resume` 报告才把 `console` 与顶层状态升级为 `passed`。

2026-08-30 自动化和透明代理已对 stateful fixture 完整通过，并已接入受影响 Console 的 CI job；CI 只有在
真实 headless Chrome 旅程及请求日志闭合断言同时通过后才会成功。一次本地 fresh real-Gateway 执行在进入
`init/dev` 前发现 OrbStack Docker API 无响应；旧 `doctor` 因无 timeout 挂起，该缺口已修复为每条外部命令 5 秒
有界失败并由真实无响应 daemon 验证。之后 GitHub fresh Linux run `33284301192` / job `99184695618` 已在真实
PostgreSQL + Gateway authority 上通过 Console Task journey，后续 full-profile 10 场景又按 exact ID 读取全部旅程。
因此真实 Console 的仓库范围状态为 **Passed**；fixture 仍不替代外部多节点 Ingress 与 L4～L6 资格。
