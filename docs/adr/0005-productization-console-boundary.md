# ADR-0005：最小 Console 是静态 `/v1` 客户端

| 属性 | 值 |
|---|---|
| 状态 | Accepted |
| 日期 | 2026-08-29 |
| 影响阶段 | Productization M3、M4 |

## 决策

M3 在 `web/console/` 交付 TypeScript/React/Vite 静态单页应用。开发时由 Vite 提供静态文件；正式部署由
Ingress 或静态文件服务交付不可变 bundle。它只调用 Gateway public `/v1`、使用其 OIDC access token 和
SSE/cursor 合同；不引入 SSR、BFF、Console database、worker credential 或内部 RPC client。

首批页面固定为 readiness、Agent/Deployment、Run timeline、Task inbox、Artifact 与 low-sensitivity Trace。
写操作必须使用 Receipt/ETag，SSE 断开按 public cursor/replay 语义恢复。浏览器持久化不得包含长期 Secret、
worker token、database URL 或 raw credential。

## 后果

Console 能降低诊断和人工 Task 操作门槛，但不会变成新的业务 authority。React/Vite 只属于静态产品面；不能
为它扩张 Platform control-plane 角色或 `/v1` 之外的 API。
