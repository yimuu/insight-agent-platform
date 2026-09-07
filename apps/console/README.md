# Insight Operations Console

`apps/console` 是 `insight.platform/v1` 的 React/Vite 静态客户端。它只调用 Gateway public `/v1`
和 `/readyz`，不拥有业务状态，也不连接数据库、worker 或内部 RPC。

## 本地开发

构建和测试会先构建共享 Rust 编译器的 WASM。需要 Rust 的 `wasm32-unknown-unknown` target，以及与
[构建脚本](scripts/build-agent-compiler.mjs)匹配的 `wasm-bindgen`；可用 `WASM_BINDGEN` 指定其路径。
浏览器与 CLI 使用同一编译语义，没有 TypeScript 编译器替代实现。

```bash
pnpm install --frozen-lockfile
pnpm test
pnpm run lint
pnpm run build
pnpm run dev
```

打开页面后填写 Gateway origin 与短期 OIDC access token。token 只保存在当前 React 内存状态，刷新页面即清除；
不要在 URL、环境构建变量或静态文件中嵌入 credential。

生产 bundle 位于忽略提交的 `dist/`，应由 Gateway/Ingress 同源托管。用户行为和边界见
[`docs/current/console.md`](../../docs/current/console.md)。

## 测试范围

`pnpm test` 覆盖 API、SSE、编辑器、共享 WASM 编译和 Task 表单等独立行为。真实浏览器 fixture 测试需先构建 bundle，
显式指定 bundle 和浏览器；缺少浏览器配置的跳过结果不计为通过证据：

```bash
INSIGHT_CONSOLE_BUNDLE_ROOT="$PWD/dist" \
INSIGHT_CONSOLE_BROWSER_BIN="/path/to/Chromium-or-Chrome" \
node --test tests/agent-editor-browser.test.mjs tests/authoring-values-browser.test.mjs \
  tests/sse-browser.test.mjs tests/task-inbox-browser.test.mjs
```

这些测试使用真实浏览器、静态应用和 Rust/WASM，但 public API 由隔离的模拟 HTTP 服务提供。
`browser:fixture:qualify` 也属于这一类：即使复用了 `real-gateway-journey.mjs` 驱动，也不能据文件名声称已验证真实 Gateway/PostgreSQL。

节点编辑器的控件来自 owning Rust 生成的 draft descriptor，所有字段直接写回原 Plan/框架 source JSON。
source map 和 expression rebuild 都调用同一 Rust/WASM 核心。浏览器测试覆盖节点模板、分支/Join/Loop 字段、exact published source 恢复，
以及 signal 重试、typed schema tree、受权正文与撤权清理；这些新增用例仍使用模拟 public API。

[Task/Receipt PostgreSQL 测试](../../crates/adapters/platform-postgres/tests/product_task_reads_pg.rs)是另一类证据：它要求显式测试数据库，
验证当前 Task inbox 扫描边界、列表/详情资格、表单正文权限及成员撤权。其种入的数据符合 owning Task 合同，测试范围是直接读取，
不代表完整 Run 创建、Task 响应或后续 Operation 正文链路。

[作者依赖 PostgreSQL 查询助手](../../crates/adapters/platform-postgres/tests/product_authoring_query_fixture.rs)由资源生命周期测试调用，
复用实际发布并激活的 Agent，验证 Active/Exact 解析、契约与调用权限分离及查询不追加业务记录。
Selection Policy 是经过 owning 类型校验的 SQL 读取测试数据；该测试不声称其发布准入或浏览器端到端旅程已经通过。

真实 Gateway 资格测试可用透明 loopback 同源代理把同一静态 bundle 接到 fresh 本地 Gateway。代理只转发 `/readyz`
和 `/v1`，不保存 token、不改写业务响应，也不拥有状态。runner 从实际 runtime profile 取得两个 Gateway 端口：
作者与发布请求转发到 Management，Run/Task/Artifact 请求转发到 Runtime。直接运行 `gateway-server.mjs` 时必须显式提供
`INSIGHT_CONSOLE_GATEWAY_ORIGIN`（Runtime）与 `INSIGHT_CONSOLE_MANAGEMENT_GATEWAY_ORIGIN`（Management），两者都必须是
不含路径或凭据的 loopback HTTP origin；模拟服务如合并了两种角色，也须显式传入同一地址两次：

```bash
tools/qualification/run-productization-journey.sh --console-browser \
  --node-bin "$(command -v node)" \
  --browser-bin "/path/to/Chromium-or-Chrome"
```

NVM 用户应显式传入 `--node-bin`；runner 会从同一 Node 安装目录解析 Corepack，不要求把 Node 加入平台
runtime image。

只有该实际 Gateway/数据库运行产出的证据，才能用于报告对应端到端路径通过；新增 UI fixture 的通过不会自动扩大已有资格结论。
