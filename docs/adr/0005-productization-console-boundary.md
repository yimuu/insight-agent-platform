# ADR-0005：最小 Console 是静态 `/v1` 客户端

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-207 |
| 日期 | 2026-08-31 |
| 影响阶段 | Productization、Agent Product Experience |

## 决策

M3 在 `web/console/` 交付 TypeScript/React/Vite 静态单页应用。开发时由 Vite 提供静态文件；正式部署由
Ingress 或静态文件服务交付不可变 bundle。它只调用 Gateway public `/v1`、使用其 OIDC access token 和
SSE/cursor 合同；不引入 SSR、BFF、Console database、worker credential 或内部 RPC client。

默认一级页面固定为Agents、Runs、Tasks与Settings。Console提供Agent列表、新建/导入、closed manifest编辑、校验、
发布、Run输入、durable timeline、Task处理与结果读取；Artifact、Deployment、Operation、Receipt、ETag、cursor与
low-sensitivity Trace只在关联对象的Advanced diagnostics中显示。

Agent/Run历史使用public OpenAPI的bounded authority list route和opaque分页envelope；不允许用localStorage、Event
重建或Console数据库伪造current state。表单与YAML导入必须通过
`contracts/product-experience/agent-compiler/v1`的conformance corpus，不能维护宽松的第二份字段语义。浏览器reload只从
publication handle、public authority与SSE cursor恢复，不把React state当作持久事实。

写操作必须使用 Receipt/ETag，SSE 断开按 public cursor/replay 语义恢复。浏览器持久化不得包含长期 Secret、
worker token、database URL、manifest正文、输入/结果正文或 raw credential。

## 后果

Console成为默认Agent authoring与Run产品入口，但不会变成新的业务authority。React/Vite只属于静态产品面；不能
为它扩张Platform control-plane role、BFF、数据库或`/v1`之外的API。默认隐藏不改变服务端permission projection。
