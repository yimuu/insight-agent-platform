# 开发指南

状态：Current

## 环境与验证

仓库固定 Rust `1.94.1`，并要求 `rustfmt` 与 `clippy`：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

CI 额外运行：

- `scripts/check-v3-cutover-residuals.sh`，阻止旧 parser、旧 runtime 和旧正向输入回流；
- 真实 PostgreSQL 16 repository、恢复与竞态合同测试；
- real-process restart、SIGINT 和 shutdown 测试；
- 依赖策略与 migration manifest 检查。

PostgreSQL 测试使用 `V3_TEST_POSTGRES_URL`，Artifact repository 专项门禁使用
`V3_ARTIFACT_TEST_POSTGRES_URL`。CI 中这些变量必须存在，相关门禁不能静默跳过。

## 代码导航

| 路径 | 职责 |
|---|---|
| `src/dsl/v3/` | 作者文档、表达式、类型检查与 lowering |
| `src/engine/plan/` | Canonical Typed Plan、链接与 verifier |
| `src/engine/scheduler/` | 纯计划决策、稳定身份与工作生成 |
| `src/engine/repository/` | SQLite/PostgreSQL 状态机、lease、checkpoint、recovery |
| `src/runtime/v3_service.rs` | Run 服务、Worker pump 与外部 ingress |
| `src/runtime/response_stream*` | Attached response broker 与公开投影 |
| `src/api/formal/` | HTTP、认证与 SSE 路由 |
| `src/catalog_v3.rs` | Agent 编译、资源绑定和 revision 固定 |
| `migrations/durable_v3/` | SQLite/PostgreSQL 持久化 schema |
| `agents/` | 随仓库交付的 Agent |
| `tests/fixtures/v3/` | v3 compiler 正向和负向 fixtures |
| `tests/v3_*` | compiler、repository、scheduler、恢复与数据库门禁 |

## 修改合同时

1. 先确认变更属于使用者文档、规范、实现还是三者共同变更；
2. Breaking 语义先更新 `docs/current/specifications/` 中的主规范或新增窄增量；
3. 同步 compiler/schema、runtime、正向示例和负向 fixtures；
4. 为 durable 行为补充 SQLite 与真实 PostgreSQL 证据；
5. 更新对应 `docs/current/` 使用者文档；
6. 已被替代的设计移入 `docs/archive/`，不要让历史示例重新成为正向输入。

文档分类与权威顺序见[文档首页](../README.md)。
