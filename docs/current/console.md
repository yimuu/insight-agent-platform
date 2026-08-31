# 运行控制台

[`web/console`](../../web/console/) 是无 BFF、无数据库的静态 React 客户端，只访问同源 `/readyz` 与 public `/v1`。
它展示 Agent/Deployment、Run timeline/result、Task inbox、Artifact、Operation 与低敏 diagnostics。

OIDC token 只保存在浏览器内存；页面不使用 localStorage/sessionStorage/URL 持久化 credential。mutation 携带 exact
ETag/Receipt，SSE 以 opaque cursor 重连，DOM 投影递归脱敏 Secret、token、raw prompt/body 与 tool body。

Node.js 只用于构建静态 bundle 和运行浏览器测试，不是平台服务运行时：

```bash
cd web/console
corepack pnpm install --frozen-lockfile
corepack pnpm test
corepack pnpm run lint
corepack pnpm run build
```

fresh `all` feature journey 已用 headless Chrome 对真实 Gateway/PostgreSQL 完成 Run/Task/Artifact/Operation 读取和 mutation。
