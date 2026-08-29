# Insight Operations Console

`web/console` 是 `insight.platform/v1` 的 React/Vite 静态客户端。它只调用 Gateway public `/v1`
和 `/readyz`，不拥有业务状态，也不连接数据库、worker 或内部 RPC。

## 本地开发

```bash
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
pnpm run dev
```

打开页面后填写 Gateway origin 与短期 OIDC access token。token 只保存在当前 React 内存状态，刷新页面即清除；
不要在 URL、环境构建变量或静态文件中嵌入 credential。

生产 bundle 位于忽略提交的 `dist/`，应由 Gateway/Ingress 同源托管。完整边界、当前证据和未关闭门禁见
[`docs/specs/productization/m3-console.md`](../../docs/specs/productization/m3-console.md)。

资格测试可用透明 loopback 同源代理把同一静态 bundle 接到 fresh 本地 Gateway。代理只转发 `/readyz`
和 `/v1`，不保存 token、不改写业务响应，也不拥有状态：

```bash
scripts/run-productization-base-journey.sh --console-browser \
  --node-bin "$(command -v node)" \
  --browser-bin "/path/to/Chromium-or-Chrome"
```

NVM 用户应显式传入 `--node-bin`；runner 会从同一 Node 安装目录解析 Corepack，不要求把 Node 加入平台
runtime image。
