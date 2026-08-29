# M3 最小运行控制台

| 属性 | 值 |
|---|---|
| 状态 | In Progress |
| 合同 | `insight.platform/v1` public HTTP 与 bounded SSE |
| 实现 | [`web/console/`](../../../web/console/) |
| authority | 无；静态 Console 只投影 Gateway authority |

## 已实现产品面

- readiness 与 session context：Gateway origin、人工 tenant 标签、内存态 OIDC access token；
- Agent/Deployment：按 authority ID 读取 Resource 与 immutable Deployment metadata；
- Run：按 ID 读取 authority、执行 pause/resume/cancel、读取 typed result，并用 opaque
  `Last-Event-ID` 拉取有限 SSE page；
- Task：从 Run public Event 发现 Task ID，按 ID 读取，并用 current ETag 与唯一 Receipt 提交
  input/approve/reject/cancel；
- Artifact：只展示 safe metadata，Ready 时显式下载并重验 public `Content-Length` 与 authority size；
- Operation：按 shared Job ID 展示状态、进度、result digest 和 safe error；
- low-sensitivity diagnostics：保留 closed Problem code、retryability 与 trace ID；Event/result
  在 DOM 前递归脱敏 credential、Secret、token、raw prompt/body 和 tool body。

浏览器不使用 `localStorage`、`sessionStorage`、URL 或日志持久化 token。客户端对 redirect 使用
fail-closed，HTTP mutation 必须携带 exact `If-Match` 与 `Idempotency-Key`。Console 没有 BFF、数据库、
worker identity 或内部 RPC client。

## 开发与检查

Node.js 只用于构建静态 bundle 和运行开发期浏览器 fixture，不是平台运行时依赖。开发机可复用已有
Node.js；仓库不要求再安装一套项目内运行时：

```bash
cd web/console
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
```

构建完成后，可在另一个终端运行 `pnpm run browser:fixture`。该 fixture 只服务 `dist/` 和一组
stateful `/v1` 契约响应，用于浏览器自动化；它不进入生产 bundle，也不是 Gateway 的替代品。

同源部署时页面默认访问当前 origin 的 `/readyz` 与 `/v1`。本地 Vite 与 Gateway 分离时，Gateway 必须按
Platform credential mode 配置受审查的 CORS origin；不得用关闭浏览器安全策略或把 token 写入 URL 的方式绕过。

## 当前未关闭项

2026-08-29 已使用真实浏览器和开发期 stateful fixture 完成一轮可重复检查：Run/SSE 发现等待 Task，
使用 exact `If-Match` 和唯一 Receipt 提交 input，随后重新读取 terminal Run/result；409、412、429 Problem
均按 closed contract 投影；SSE 重连发送 `Last-Event-ID: cur-browser-1` 并读取下一页；checked Secret、credential、
raw prompt 与 tool output 在 DOM 中均为 `[redacted]`，浏览器 console 无泄漏；刷新后 token 与 Run projection
清空；跳转链接、main focus target、active navigation `aria-current` 也已检查。fixture 请求日志只记录凭据是否
存在，不记录 Authorization 内容。证据边界见 [`console-browser-fixture.md`](console-browser-fixture.md)。

这仍不是 fresh PostgreSQL + 真实 Gateway 证据，尚不能关闭 M3。剩余门禁包括：

1. 真实 Gateway + fresh PostgreSQL 下的浏览器 journey：失败/等待 Run -> Task -> mutation -> terminal Run；
2. Gateway restart 后使用 SSE cursor 恢复，页面刷新后从 authority ID 重新读取状态；
3. telemetry sink 的 checked sensitive fixture 负向检查；
4. 空状态、慢依赖的浏览器契约测试，以及正式 accessibility audit；
5. 静态 bundle 由 Gateway/Ingress 同源承载的部署清单与 CI lane。

真实 authority 自动化入口现已实现但尚未取得 fresh Passed evidence。
`scripts/run-productization-base-journey.sh --console-browser` 会使用现有全局 Node/Corepack 构建静态 bundle，通过严格 loopback 透明代理连接同一次 fresh Runtime
Gateway，并在独立 Human Task Run 上驱动浏览器 mutation；NVM 路径可用 `--node-bin` 显式传入，不把 Node 变成平台
runtime 依赖。2026-08-30 首次 fresh 尝试被本机无响应的 OrbStack Docker API 阻断在 `doctor`，没有启动 Gateway，
因此当前状态仍为 Not run。该尝试同时促成 `doctor` 外部命令的 5 秒 timeout 与可操作失败诊断。

这些证据完成前，M3 与 Platform spec00～18 均不升级为 Verified。
