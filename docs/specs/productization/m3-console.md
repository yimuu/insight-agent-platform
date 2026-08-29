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

Node.js 只用于构建静态 bundle，不是平台运行时依赖：

```bash
cd web/console
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
```

同源部署时页面默认访问当前 origin 的 `/readyz` 与 `/v1`。本地 Vite 与 Gateway 分离时，Gateway 必须按
Platform credential mode 配置受审查的 CORS origin；不得用关闭浏览器安全策略或把 token 写入 URL 的方式绕过。

## 当前未关闭项

本批次只有 P0 unit/build evidence，尚不能关闭 M3。剩余门禁包括：

1. 真实 Gateway + fresh PostgreSQL 下的浏览器 journey：失败/等待 Run -> Task -> mutation -> terminal Run；
2. Gateway restart 后使用 SSE cursor 恢复，页面刷新后从 authority ID 重新读取状态；
3. DOM、console 与 telemetry 的 checked sensitive fixture 负向检查；
4. keyboard/accessibility、空状态、慢依赖与 409/412/429 浏览器契约测试；
5. 静态 bundle 由 Gateway/Ingress 同源承载的部署清单与 CI lane。

这些证据完成前，M3 与 Platform spec00～18 均不升级为 Verified。
