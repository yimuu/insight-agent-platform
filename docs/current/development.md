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
```

其中：

- `scripts/check-cutover-residuals.sh`，阻止旧 parser、旧 runtime 和旧正向输入回流；
- `scripts/check-crate-boundaries.sh`，校验 member 依赖 DAG、禁止的 I/O/clock 使用与第三方 feature 基线；
- `scripts/check-public-api-baseline.sh`，验证根 facade 的公开 Rust API；
- 真实 PostgreSQL 16 repository、恢复与竞态合同测试；
- real-process restart、SIGINT 和 shutdown 测试；
- 依赖策略与 migration manifest 检查。

完整 PostgreSQL 门禁必须在 PostgreSQL 16 上以 `CI=1` 运行，并设置
`RUN_HISTORY_POSTGRES_URL` 和 `TEST_POSTGRES_URL`。CI 中这些变量必须存在，相关门禁不能静默跳过。

## 代码导航

| 路径 | 职责 |
|---|---|
| `crates/engine/src/` | 无 I/O 的 Plan、scheduler、状态机和公开合同内核 |
| `crates/dsl/src/` | 作者文档、表达式、类型检查、lowering 与 Graph authoring |
| `crates/durable/src/` | 后端中立的 repository ports、commands、claims、receipts 与 projection models |
| `crates/resources/src/` | Model/Action/Retrieval SPI、registry、builtin 与 OpenAI provider |
| `crates/storage/src/` | SQLite/PostgreSQL、Graph SQL、Artifact store 与 PostgreSQL live broker adapter |
| `crates/runtime/src/` | catalog/deployment、leaf adapter、scheduler/worker pump、RunService 与 live response |
| `crates/api/src/v1/` | `/v1` Axum HTTP、认证、错误映射与 SSE transport |
| `src/` | 根兼容 facade、平台配置、严格 YAML 解码和 binary composition |
| `migrations/durable/` | SQLite/PostgreSQL 持久化 schema |
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

文档分类与权威顺序见[文档首页](../README.md)。
