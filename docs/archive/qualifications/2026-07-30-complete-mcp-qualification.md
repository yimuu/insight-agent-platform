# MCP 2026-07-28 完整支持资格验收

| 属性 | 结果 |
|---|---|
| 状态 | Qualified |
| 验收日期 | 2026-07-30 |
| Modern profiles | `mcp-modern-client-v1`、`mcp-modern-server-v1` |
| Extension profile | `mcp-tasks-v1` |
| Compatibility profile | `mcp-legacy-client-v1`（MCP `2025-11-25`） |
| 数据库 | SQLite、PostgreSQL 16 |
| Rust | `rustc` / `cargo` 1.94.1 |

## 结论

MCP 完整支持规范定义的 modern Host/Client、modern Server、官方 Tasks extension 和独立 legacy
client profile 已完成实现并通过资格验收。当前生产合同见
[MCP 使用、运行与安全合同](../../current/mcp.md)，历史设计见
[MCP 完整支持规范](../specs/2026-07-30-complete-mcp-support.md)。

验收覆盖：

- MCP `2026-07-28` stateless wire、`server/discover`、stdio 与 Streamable HTTP；
- Tools、Resources、Prompts、Completion、Subscriptions、Elicitation、进度、取消与分页；
- `/mcp` 显式 exports、OAuth protected-resource 行为和 principal/scope 隔离；
- 双向 Tasks get/update/cancel/status、断线恢复、lease/fence 与 terminal first-winner；
- SQLite/PostgreSQL 等价的 interaction、task、OAuth transaction 和加密 credential 状态机；
- MCP `2025-11-25` initialize/session fallback 与 modern path 的严格隔离；
- `run-stream/v2` interaction 协商，且不改变闭合的 `run-stream/v1`；
- catalog/revision 冻结、secret non-interference、SSRF/TLS/body/depth/content bounds 与可观测性。

## 上游与供应链证据

Vendored `2026-07-28` schema snapshot 固定到上游 commit
`f817239f4d6b1efff2c4dfc2f7af85c985d73076`，snapshot SHA-256 为
`2ee387342f81e9f38a87ece7abeaf29d9fe3769cd7400ccad1fb1f0b80966bb0`。wire types 由项目使用
Serde 手工维护并由 snapshot conformance tests 校验，没有本地 schema 补丁；完整来源记录见
[`mcp-2026-07-28.PROVENANCE.md`](../../../schemas/vendor/mcp-2026-07-28.PROVENANCE.md)。

OAuth JWT 使用 `jsonwebtoken` 11 的 AWS-LC backend，不引入无修复版本的 `rsa` advisory。
`cargo audit` 加载 1174 条 RustSec advisory 并扫描 356 个依赖，结果为零漏洞；
`cargo deny check` 的 advisories、bans、licenses、sources 全部通过。已配置的重复依赖仅产生 warning。

## 外部 SDK 互操作

固定版本：

- TypeScript：`@modelcontextprotocol/sdk@1.30.0`；
- Go：`github.com/modelcontextprotocol/go-sdk@91e4e1a0b8ca01cfa680f142815b1152a0513326`。

| 调用方向 | stdio | Streamable HTTP | Tasks |
|---|---:|---:|---:|
| 平台 Client → TypeScript Server | 通过 | 通过 | 通过 |
| 平台 Client → Go Server | 通过 | 通过 | 通过 |
| TypeScript Client → 平台 Server | 不适用 | 通过 | 通过 |
| Go Client → 平台 Server | 不适用 | 通过 | 通过 |

固定 SDK 的高层 client/server helper 默认仍面向 session-era 协议。qualification fixture 使用 SDK
拥有的 JSON-RPC、content、task types 与 validators，并增加最小 stateless `2026-07-28` transport
adapter；它不复制平台 wire types。原始机器报告由
`bash scripts/qualify-mcp-external-sdk.sh` 生成到
`target/mcp-qualification/report.json`，该目录是本地/CI artifact；本记录是 checked-in 的持久摘要。

## 验收门禁

以下门禁均通过：

```text
CI=1 TEST_POSTGRES_URL=postgresql://... cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
cargo fmt --all -- --check
cargo metadata --locked --all-features --format-version 1
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
bash scripts/check-cutover-residuals.sh
bash scripts/check-crate-boundaries.sh
bash scripts/check-public-api-baseline.sh
bash scripts/qualify-mcp-external-sdk.sh
cargo audit
cargo deny check
helm lint deploy/helm/insight-agent-platform
helm template ... deploy/helm/insight-agent-platform
git diff --check
```

完整 workspace gate 在 PostgreSQL 16 上执行，覆盖 74 张表和 216 个索引的最终 schema contract。
Helm lint 及启用 MCP 的 render 验证 keyring 与 bearer secret 都通过现有 Kubernetes Secret key
注入，不在 values、ConfigMap 或 Deployment Revision 中保存明文。

## 发布判定

- `mcp-modern-client-v1`：Qualified；
- `mcp-modern-server-v1`：Qualified；
- `mcp-tasks-v1`：Qualified，仍按部署显式启用；
- `mcp-legacy-client-v1`：Qualified，默认关闭；
- 未包含 Roots、Sampling、MCP Apps/UI、Registry 市场或任意第三方 extension；
- MCP 不扩大外部副作用 exactly-once 承诺，远程副作用仍为 at-least-once。
