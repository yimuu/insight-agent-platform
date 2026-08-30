# M3 最小运行控制台

| 属性 | 值 |
|---|---|
| 状态 | Passed / repository and fresh-authority scope |
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
`pnpm run browser:fixture:qualify` 会自行启动 fixture 和 headless Chrome，执行同一浏览器旅程并校验请求
日志中的认证、CAS、Receipt 与脱敏不变量；受影响 Console 的 GitHub CI 固定在带 Chrome 的
`ubuntu-24.04` runner 上执行该闭合资格命令。Git revision
`d6dca5c180f9027441284b29b2c2684b3fd0c795` 的远端 Console job `99158633401` 已返回 closed Passed evidence；
对应 run `33274451057` 后续因更新提交取代而取消，但已完成 job 的结果保持可审计。

同源部署时页面默认访问当前 origin 的 `/readyz` 与 `/v1`。本地 Vite 与 Gateway 分离时，Gateway 必须按
Platform credential mode 配置受审查的 CORS origin；不得用关闭浏览器安全策略或把 token 写入 URL 的方式绕过。

## 资格演进记录

2026-08-29 已使用真实浏览器和开发期 stateful fixture 完成一轮可重复检查：Run/SSE 发现等待 Task，
使用 exact `If-Match` 和唯一 Receipt 提交 input，随后重新读取 terminal Run/result；409、412、429 Problem
均按 closed contract 投影；SSE 重连发送 `Last-Event-ID: cur-browser-1` 并读取下一页；checked Secret、credential、
raw prompt 与 tool output 在 DOM 中均为 `[redacted]`，浏览器 console 无泄漏；刷新后 token 与 Run projection
清空；跳转链接、main focus target、active navigation `aria-current` 也已检查。fixture 请求日志只记录凭据是否
存在，不记录 Authorization 内容。证据边界见 [`console-browser-fixture.md`](console-browser-fixture.md)。
该 stateful fixture 资格命令已进入受影响 Console 的 CI，不再依赖人工打开浏览器；这仍只证明 fixture
边界，不能替代真实 Gateway/PostgreSQL authority。远端 job 的 exact 输出为 `request_count=15`，六项 journey
check 全部通过，证据链接见 [`console-browser-fixture.md`](console-browser-fixture.md)。

上述 stateful fixture 本身不是 fresh PostgreSQL + 真实 Gateway 证据。后续真实 authority 结果如下。

Git revision `e03b6cc123f5f1ada2c96a47f167956adde7a095` 的手动 GitHub run
[`33284301192`](https://github.com/yimuu/insight-agent-platform/actions/runs/33284301192) 随后已在 fresh
`ubuntu-24.04` authority 上完成真实 Gateway/PostgreSQL 与 headless Chrome Task journey，job
[`99184695618`](https://github.com/yimuu/insight-agent-platform/actions/runs/33284301192/job/99184695618) 为 Passed。
其 machine-readable `approval-task-resume`、`deterministic-first-run` 与 `timer-signal-restart-recovery` report 均
完整 Passed；同一浏览器按 exact ID 读取 deterministic 与 replacement-Worker 恢复后的 Timer/Signal Run。证据摘要见
[`base-journey-evidence.md`](base-journey-evidence.md)。同一 full-profile 十场景复跑随后让 Console 按 exact ID
读取全部用户旅程，并与 durable role restart、opaque SSE cursor、刷新后 authority 重读、生产 telemetry redaction
checker 和浏览器安全负向测试形成闭包。Console 不拥有状态，Gateway/页面重启后以 exact authority ID 重读
PostgreSQL 事实；因此 repository/local M3 门禁已关闭。

以下项目继续作为部署环境 hardening，而不是 repository M3 阻塞项：

1. Gateway restart 后使用 SSE cursor 恢复，页面刷新后从 authority ID 重新读取状态；
2. telemetry sink 的 checked sensitive fixture 负向检查；
3. 空状态、慢依赖的浏览器契约测试，以及正式 accessibility audit；
4. 静态 bundle 由 Gateway/Ingress 同源承载的部署清单与 CI lane。

`scripts/run-productization-base-journey.sh --console-browser` 使用现有全局 Node/Corepack 构建静态 bundle，通过严格
loopback 透明代理连接同一次 fresh Runtime Gateway，并在独立 Human Task Run 上驱动浏览器 mutation；NVM 路径可用
`--node-bin` 显式传入，不把 Node 变成平台 runtime 依赖。2026-08-30 首次本地 fresh 尝试被无响应的 OrbStack
Docker API 阻断在 `doctor`，并促成外部命令的 5 秒 timeout 与可操作失败诊断；上述 GitHub Linux run 随后关闭了
真实 authority 的 Not run 状态；后续 fresh Linux 与本地 full journey 已关闭该状态。

真实多节点部署中的 accessibility/slow-network audit 与 Ingress 静态托管属于环境资格，不把 Platform spec00～18
升级为 production Verified。
