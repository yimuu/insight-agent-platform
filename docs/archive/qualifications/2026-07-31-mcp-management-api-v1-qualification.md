# MCP 管理 API v1 资格验收

| 属性 | 结果 |
|---|---|
| 状态 | Qualified |
| 验收日期 | 2026-07-31 |
| 管理协议 | `mcp-management/v1` |
| MCP profiles | `2026-07-28` modern；`2025-11-25` 独立 legacy fallback |
| 配置 | `mcp.version: 2` clean-cut |
| Durable schema contract | `durable-schema-eb07a629-e22a-4935-9bba-4835c7b027f1` |
| 数据库 | SQLite、PostgreSQL 16 |
| Rust | `rustc` / `cargo` 1.94.1 |

## 结论

MCP 管理 API v1 与显式导入规范已经完整实施并通过资格验收。第三方 MCP Client Server 的唯一权威
从 YAML clean-cut 到 durable management store；`/v1/admin/mcp/**` 提供独立 Operator 控制面，
覆盖 Draft、signed manifest、durable discovery、候选审阅、逐项 Tool/Resource/Prompt import、
validation、不可变 Revision、publish、CAS activate/disable 与不可逆 retire。当前合同见
[MCP 使用、运行与安全合同](../../current/mcp.md)，历史设计见
[MCP 管理 API v1 与显式导入规范](../specs/2026-07-31-mcp-management-api-v1.md)。

系统没有持久化或运行时 `tools: ["*"]`、`auto_import`、regex/pattern import。`all` 只生成当次
discovery snapshot 的显式 preview，Operator 必须把逐项 schema hash 和保守 policy 写回 Draft。

## 控制面与数据证据

- checked-in OpenAPI 3.1 覆盖 28 个 operation；请求、成功 envelope、错误 envelope、分页 cursor 和
  DELETE body-free 合同均由 closed schema/sample 测试验证；
- 独立 Operator Bearer token 仅授予闭合集合中的 `read/write/discover/publish` capability；普通
  tenant/user token 和 MCP OAuth token 无法进入管理面；
- 所有 mutation 使用 `X-Request-ID` 幂等 receipt；Draft/Server mutation 使用强 ETag CAS；成功 mutation、
  audit 和必要 outbox 在短事务中原子提交；已识别 Operator 的拒绝请求生成 body-free audit；
- SQLite/PostgreSQL 使用等价的 normalized server、draft、discovery operation/snapshot、candidate、
  validation、revision、active pointer、request receipt、audit 和 outbox authority；不可变 snapshot/
  Revision 由数据库 trigger 保护；
- discovery 使用 durable claim/lease/fence/cancel/retry takeover，远程 I/O 不持有数据库事务；policy
  fingerprint 或 list change 会使旧 evidence stale；
- publish 不隐式激活。active Revision 只投影显式导入项；Agent Deployment 固定精确
  `server_revision_id + binding_hash`，active pointer 改变不会改写旧 Deployment/Run；
- Run admission 在创建 Run 的同一数据库事务内校验 exact active fence；disable 阻止新 admission 和
  尚未 dispatch 的调用，并使 in-flight 调用按 uncertain-outcome 合同有界取消。

## 安全与运维证据

- HTTP endpoint 拒绝 userinfo、query、fragment、非允许明文、redirect、private/link-local/metadata IP；
  DNS pin 要求所有解析结果同属允许网络类别，并覆盖 IPv4-mapped IPv6 与 IDNA canonicalization；
- stdio 只能引用平台预批准的绝对 executable launch profile、固定 argv/workdir/secret slots 和隔离
  fingerprint；管理请求不能提交 shell 或 executable；
- secret value、OAuth token、远程 body/stderr、schema/description 正文不进入 Revision hash、响应、
  日志、trace、metric label 或 audit；keyring 与 resolver policy 在启用时 fail-fast；
- metrics 使用闭合低基数 route/result/operation/outcome/kind/state label，覆盖管理请求/延迟、生命周期、
  catalog histogram、pending/running/oldest discovery 和 active/disabled/stale Server；
- readiness 检查 store/schema、Operator auth、keyring、resolver/worker policy 与被 Agent 引用的 binding；
  shutdown 先停止新 mutation/claim，再取消 worker/归还 lease，最后关闭 projection、transport 和
  RunService；running discovery 可在 lease 到期后接管；
- Helm 默认保持管理面关闭；启用 overlay 要求 client、Operator credential、keyring existing Secret 和
  resolver Secret 引用齐备，render 后 ConfigMap 不含 credential value。

## 外部 SDK 互操作

`bash scripts/qualify-mcp-external-sdk.sh` 使用真实子进程并通过：

- TypeScript `@modelcontextprotocol/sdk@1.30.0`；
- Go `github.com/modelcontextprotocol/go-sdk@91e4e1a0b8ca01cfa680f142815b1152a0513326`；
- 平台 Client → 两个 SDK Server 的 stdio、Streamable HTTP 与 Tasks；
- 两个 SDK Client → 平台 `/mcp` 的 Streamable HTTP 与 Tasks。

机器报告生成于 `target/mcp-qualification/report.json`；该目录是本地/CI artifact，本记录是持久摘要。

## 验收门禁

以下门禁均通过：

```text
bash scripts/check-cutover-residuals.sh
bash scripts/check-crate-boundaries.sh
bash scripts/check-public-api-baseline.sh
cargo fmt --all -- --check
cargo metadata --locked --all-features --format-version 1
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
bash scripts/qualify-mcp-external-sdk.sh
cargo test --release --locked --workspace --all-targets --all-features
cargo audit
cargo deny check
helm lint/template（默认 values 与 values-mcp-management-example.yaml）
git diff --check
```

完整 workspace gate 使用可用的 PostgreSQL 16 测试 authority，双数据库测试没有降级为 SQLite-only。
`cargo deny check` 的 advisories、bans、licenses 和 sources 均通过，仅保留既有 duplicate dependency
warning。`cargo audit` 扫描 356 个依赖并通过；当前 RustSec 数据库报告一条项目已显式允许的
`event-listener 5.4.1` warning（`RUSTSEC-2026-0221`），没有未允许 vulnerability。

## 发布判定

- `mcp-management/v1`：Qualified；
- `mcp.version: 2` durable Client Server authority：Qualified；
- SQLite/PostgreSQL 管理生命周期、CAS、幂等与 Revision 不可变性：Qualified；
- Agent binding、Run admission/dispatch disable fence：Qualified；
- UI、MCP 市场、Registry 搜索、自动安装与 wildcard/auto-import：未包含，且不属于本次交付。
