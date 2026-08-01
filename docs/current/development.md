# 开发指南

状态：Current

## 环境与验证

仓库固定 Rust `1.94.1`。完整 workspace 的最低验证命令为：

```bash
bash scripts/check-cutover-residuals.sh
bash scripts/check-crate-boundaries.sh
bash scripts/check-public-api-baseline.sh
cargo fmt --all -- --check
cargo metadata --locked --all-features --format-version 1
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
cargo audit
cargo deny check
bash scripts/qualify-mcp-external-sdk.sh
```

其中：

- `scripts/check-cutover-residuals.sh`，阻止旧 parser、旧 runtime 和旧正向输入回流；
- `scripts/check-crate-boundaries.sh`，校验 member 依赖 DAG、禁止的 I/O/clock 使用与第三方 feature 基线；
- `scripts/check-public-api-baseline.sh`，验证根 facade 的公开 Rust API；
- 真实 PostgreSQL 16 repository、恢复与竞态合同测试；
- real-process restart、SIGINT 和 shutdown 测试；
- 空数据库 Schema 安装、contract 校验、运行时零 DDL 和依赖策略检查。

MCP 外部互操作门禁固定 TypeScript `@modelcontextprotocol/sdk@1.30.0` 和 Go SDK commit
`91e4e1a0b8ca01cfa680f142815b1152a0513326`。依赖 lockfile 位于 `tests/interop/`，runner 使用真实
子进程，覆盖平台 Client 到两个 SDK Server 的 stdio/Streamable HTTP、两个 SDK Client 到平台
`/mcp` 的 Streamable HTTP，以及 Tasks。上游 SDK 在该固定版本尚未提供 modern high-level API 的
部分由 fixture 中使用 SDK JSON-RPC/types/validator 的 `2026-07-28` adapter 承接，不能误报为
high-level API 覆盖；完整边界见 [`tests/interop/README.md`](../../tests/interop/README.md)。
成功运行会生成 `target/mcp-qualification/report.json`，发布证据另保存于
[`docs/archive/qualifications/2026-07-30-complete-mcp-qualification.md`](../archive/qualifications/2026-07-30-complete-mcp-qualification.md)。

完整 PostgreSQL 门禁必须在 PostgreSQL 16 上以 `CI=1` 运行，并设置
`RUN_HISTORY_POSTGRES_URL` 和 `TEST_POSTGRES_URL`。CI 中这些变量必须存在，相关门禁不能静默跳过。

容量、WAL、真实进程故障和百万行 Conversation 查询不是普通单元 CI 的替代品，也不能由 smoke
结果代替。Terminal-only 的 Phase 0 与 Gate A～D 使用独立的 fresh namespace、固定 workload 和
fail-closed evaluator；已完成的复现命令及正式证据路径保存在
[Terminal-only 验收与 WAL 资格归档](../archive/qualifications/2026-07-28-terminal-only-qualification.md)。

## 代码导航

| 路径 | 职责 |
|---|---|
| `crates/engine/src/` | 无 I/O 的 Plan、scheduler、状态机和公开合同内核 |
| `crates/dsl/src/` | 作者文档、表达式、类型检查、lowering 与 Graph authoring |
| `crates/durable/src/` | 后端中立的 repository ports、commands、claims、receipts 与 projection models |
| `crates/resources/src/` | Model/Action/Retrieval SPI、registry 与 OpenAI-compatible adapter |
| `crates/mcp/src/` | MCP codec、wire、transport、OAuth、Tasks 与 Server dispatcher |
| `crates/storage/src/` | SQLite/PostgreSQL、Graph SQL、Artifact store 与 PostgreSQL live broker adapter |
| `crates/runtime/src/` | catalog/deployment、leaf adapter、scheduler/worker pump、RunService 与 live Run stream |
| `crates/api/src/v1/` | `/v1` Axum HTTP、认证、错误映射与 SSE transport |
| `catalog/provider-catalog.yaml` | 平台版本化的内置 Provider route 与最小模型事实 |
| `src/` | 根 facade、Provider extension/平台配置、Catalog loader、严格 YAML 解码和 binary composition |
| `database/durable/` | SQLite/PostgreSQL 完整 Schema、安装与权限合同 |
| `schemas/mcp-management-v1.json` | MCP Operator 管理请求的闭合 JSON Schema |
| `schemas/mcp-management-v1.openapi.json` | 全部 MCP Operator 管理 endpoint 的 OpenAPI 3.1 合同 |
| `schemas/mcp-management-v1.samples.json` | MCP 管理合法/非法 checked-in fixtures |
| `schemas/agent-management-v1*.json` | Agent 管理 JSON Schema、OpenAPI 与正/负样例 |
| `schemas/provider-management-v1*.json` | Provider 管理 JSON Schema、OpenAPI 与正/负样例 |
| `agents/` | 随仓库交付的 Agent |
| `tests/fixtures/dsl/` | DSL compiler 正向和负向 fixtures |
| `crates/*/{src,tests}` | owner crate 的单元与合同测试 |
| `tests/` | 跨层 E2E、根 facade、real-process 与 binary smoke 门禁 |

## 修改合同时

1. 先确认受影响的公开合同、实现、测试和当前文档；
2. Breaking 语义同步更新 compiler/schema、runtime、正向示例和负向 fixtures；
3. 为 durable 行为补充 SQLite 与真实 PostgreSQL 证据；
4. 更新对应 `docs/current/` 文档；
5. 设计和迁移记录写入 `docs/archive/`，不要让历史示例重新成为正向输入。

修改 Provider template catalog 时必须验证 manifest digest、显式 import 和“启动不注册 live route”；
修改 durable Provider 时必须验证 route/模型解析、secret reference、Revision restore 与 Deployment
non-interference。调用级参数不得下沉为 template 默认值；添加 OpenAI-compatible 行为时保持 Provider
身份与 adapter 协议分离。

修改 MCP wire 时必须同步 vendor schema provenance、bounded codec、Client/Server capability
negotiation、stdio 与 Streamable HTTP tests，以及至少两个固定版本外部 SDK 的 interoperability
fixture。MCP schema、body、secret、tenant、SSRF、header injection 和 prompt injection 均属于发布
门禁；不能用 loopback mock 代替 real-process/外部 SDK 证据。

修改 MCP 管理控制面时还必须运行 `cargo test -p insight-api mcp_management`、
`cargo test --test mcp_management_api` 和
`cargo test -p insight-storage --test mcp_management`。根级测试验证完整 HTTP 生命周期，后者在
`TEST_POSTGRES_URL` 存在时对 SQLite 与
PostgreSQL 16 执行同一 Draft、discovery、publish、activate、disable、retention、CAS、幂等和不可变
Revision 合同；CI 缺少 PostgreSQL URL 会失败，不能静默降级为 SQLite-only 证据。

修改 Agent 或 Provider 管理控制面时必须分别运行 API contract、根 HTTP E2E 和 storage parity tests：

```bash
cargo test -p insight-api agent_management
cargo test --test agent_management_api
cargo test -p insight-storage --test agent_management
cargo test -p insight-api provider_management
cargo test --test provider_management_api
cargo test -p insight-storage --test provider_management
```

Definition publish、Deployment create 与 route activate 是三个独立事务边界；测试不得用旧 Graph API 或
public historical Deployment admission 绕过它们。Provider/MCP active 变化不得改写已有 binding hash，
suspension/disable/archive 必须同时覆盖 admission 与 leaf-start fence。
`schemas/run-stream-v1.samples.json` 必须覆盖 `run-stream/v1` 的全部 27 个事件，并由
`insight-engine` 测试逐条按 `schemas/run-stream-v1.json` 验证；样本不得包含 interaction body、
credential、requestState 或远程原始错误。

Repository 测试必须显式区分数据库安装和连接：先在新的空目标执行
`database/durable/{postgres,sqlite}/schema.sql`，再创建 repository。生产构造函数没有隐式建表
捷径。修改 1.0 前 Schema 时应同时修改两个后端和 contract ID，然后重建所有开发/CI 数据库；
不得为尚未发布的数据库保留伪 migration 历史。

文档分类与权威顺序见[文档首页](../README.md)。
