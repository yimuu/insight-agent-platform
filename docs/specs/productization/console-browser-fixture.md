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

## 重现

```bash
cd web/console
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
pnpm run browser:fixture
```

浏览器访问 `http://127.0.0.1:4173/`。fixture 使用固定 Run/Task 标识和内存态转换，因此每次服务重启
都会回到 waiting/pending 初态；它只用于浏览器契约验证，不进入生产产物。

## 证据边界

本报告不证明 PostgreSQL transaction、真实 Gateway authentication/authorization、进程重启恢复、Ingress
同源承载或 telemetry sink 脱敏。因此它只关闭 M3 浏览器交互矩阵的一部分，不把 M3 或 Platform
spec00～18 升级为 Verified。
