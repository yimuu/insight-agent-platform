# Agent 与 Provider 管理 Control Plane v1 资格验收

日期：2026-08-01（最终复验：2026-08-02 CST）

状态：Archived / Qualified

对应规范：[Agent 与 Provider 管理 Control Plane v1](../specs/2026-08-01-agent-and-provider-management-control-plane.md)

## 结论

Agent 与 Provider durable 管理面已通过规范完成定义 1～15。最终合同包括：严格 Operator API、
SQLite/PostgreSQL 等价状态机、Definition/Deployment/Activate 分离、Provider Revision 动态投影、
exact Provider/MCP/Action/Retrieval/Subflow/worker binding、公开历史 Deployment clean cut、Debug Session、
迁移工具、低基数可观测性以及 body-free audit/outbox。

最终全仓命令以退出码 `0` 完成：

```bash
cargo test --workspace --all-targets --quiet
```

该命令是在修正旧公开 historical Deployment 测试以及 LLM 测试 fixture 的 exact Provider binding 后，
从头重新执行；关键 `insight-runtime` LLM 组为 `72 passed`，根运行时组为 `43 passed`，API 组为
`40 passed`，Conversation 组为 `14 passed`，其余 workspace target 也全部为零失败。

## 验收环境

| 项目 | 版本/事实 |
|---|---|
| 主机 | Apple ARM64，Darwin 25.5.0 |
| Rust | `rustc 1.94.1 (e408947bf 2026-03-25)` |
| Cargo | `cargo 1.94.1 (29ea6fb6a 2026-03-24)` |
| SQLite | 3.51.0 |
| PostgreSQL | 16.14，隔离 schema 测试 |
| 基线提交 | `d134586`；本报告对应其后的 Agent/Provider 工作树变更 |

PostgreSQL 测试通过 `TEST_POSTGRES_URL` 使用本地 PostgreSQL 16 容器；各测试创建隔离 schema，并在结束
时删除。CI gate 会在 CI 环境缺少 PostgreSQL URL 时失败，不把跳过当作资格通过。

## 完成定义矩阵

| # | 结果 | 主要证据 |
|---|---|---|
| 1 | Passed | `schemas/agent-management-v1*`、`schemas/provider-management-v1*`；API contract tests 覆盖全部 route、严格 schema、duplicate key、unknown field、body-free DELETE 和正负样例。 |
| 2 | Passed | Agent/Provider API 与 storage tests 覆盖 Operator auth、闭合 capability、强 ETag、`X-Request-ID`、opaque cursor、分页上限、exact replay/conflict、同事务 audit/outbox；认证后失败也写 body-free rejection audit。 |
| 3 | Passed | `crates/storage/tests/agent_management.rs` 对 SQLite/PostgreSQL 运行 Draft → Validation → Definition → Resolution → Deployment → Activate → Archive/Restore，并验证相同 ETag、receipt、错误与 pin。 |
| 4 | Passed | `crates/storage/tests/provider_management.rs` 对两库运行 Draft → Discovery/Import/Test/Validation → Revision → Activate/Suspend/Resume/Retire；operation claim/lease 可恢复。 |
| 5 | Passed | `tests/graph_product.rs`、`tests/agent_management_api.rs`：Graph semantic edit 只改 Draft；publish、deploy、activate 是三个边界；旧 `/v1/graph-agents/**` 为 404。 |
| 6 | Passed | Provider/MCP active pointer 变化不重写既有 Deployment/Run；历史 `resolved_bindings` 与 binding hash 在 migration/restart tests 中逐字保持。 |
| 7 | Passed | Agent resolution/deployment tests 验证 exact Provider revision/model/adapter、MCP revision/action、Retrieval/Subflow head 与 worker contract 进入不可变 evidence。 |
| 8 | Passed | 普通用户只可调用 `/v1/agents/{id}/runs`；`/deployments/{revision}/runs` clean-cut 为 404；Debug 使用独立 `debugrun_*` namespace。 |
| 9 | Passed | Agent archive/admission、Provider suspension/admission、MCP disable/admission 均以并发 `tokio::join!` 验证线性化；leaf-start 还复验 durable suspension/disable fence。 |
| 10 | Passed | Agent API/storage tests 覆盖 sandbox/live capability、live confirmation、临时 exact Deployment、不写 public head、TTL/cancel、public 404、retention tombstone、保留 hash/identity 与到期 stream `410 Gone`。 |
| 11 | Passed | Provider/Agent audit-outbox扫描、binary metrics 扫描、`secret_noninterference`、MCP/stream tests 证明 secret、Prompt、endpoint/model body、tool argument 不进入未授权响应、日志、metric label、audit、outbox 或 public stream。 |
| 12 | Passed | PostgreSQL Provider outbox trigger 发 schema-scoped opaque `NOTIFY`；重复提示不含 identity/order；丢失提示由 generation poll 收敛。migration/production tests 覆盖 active/archived exact revision restart recovery；缺少 exact adapter 的投影启动/就绪 fail closed。 |
| 13 | Passed | `agentctl`、`providerctl` dry-run 不读 token/secret value；`management-migrate` 在 SQLite/PostgreSQL 覆盖 dry-run rollback、整批失败回滚、Agent head 接管和 legacy Provider history mapping，并保持历史 Run pin。 |
| 14 | Passed | Quickstart、生产 YAML、Helm values/template、operations、architecture、DSL、API、configuration、management 与 migration 文档已同步；Helm 默认及 MCP management overlay 均 lint/render 通过。 |
| 15 | Passed | 本报告记录命令、平台、数据库、并发/race、故障与剩余边界。 |

## 并发、通知与故障证据

- Agent/Provider Draft CAS race 各只有一个 winner；loser 得到稳定 precondition conflict。
- Archive/suspension/disable 与 Run admission 在同一数据库锁序中线性化，不产生半个 Run。
- SQLite audit insert 故障注入证明 entity、receipt、audit、outbox 全部回滚；PostgreSQL 用同一事务合同。
- Provider operation lease 过期后由新 claim token 接管，旧 worker 不能提交。
- PostgreSQL Provider notification 是不带对象 ID/正文的相同 opaque hint；consumer 每次都全量复读 durable
  final state。listener 丢失或断开时 safety poll 继续收敛并尝试重连。
- binary smoke 覆盖 SQLite/无 DDL PostgreSQL 启动、普通进程重启、CLI import、管理指标与 server-only
  Provider secret；`providerctl` 子进程显式移除 secret value 环境变量。

## 复现命令

```bash
cargo fmt --all
cargo check --workspace --all-targets
cargo test -p insight-api -- --nocapture
cargo test --test agent_management_api --test mcp_management_api --test provider_management_api -- --nocapture
cargo test -p insight-storage --test agent_management --test provider_management --test schema_layout -- --nocapture
cargo test --test management_import_tools --test management_migration -- --nocapture
cargo test --test binary_smoke binary_starts_and_observes_success_and_workflow_failure_runs -- --nocapture
cargo test --workspace --all-targets --quiet
helm lint deploy/helm/insight-agent-platform
helm template insight deploy/helm/insight-agent-platform
helm template insight deploy/helm/insight-agent-platform \
  -f deploy/helm/insight-agent-platform/values-mcp-management-example.yaml
git diff --check
```

## 剩余边界

- 本次交付是 API/CLI 控制面，不包含 UI。
- 自动导入与 `tools: ["*"]` 仍故意不支持；MCP 与 Provider discovery 必须显式选择并发布 immutable
  Revision。
- Debug profile retention 到期时先在管理事务内把 source/input 变成 content tombstone，并立即阻止
  admin stream；底层 Run payload/artifact 的物理回收继续由既有 bounded Run/artifact retention worker
  完成。不可变 ID、hash、状态、计数和引用不会被破坏。
- SQLite 仅支持 single-process development；生产多 runtime 管理面要求 PostgreSQL 16。
- Provider notification 是加速提示，不是事件总线；不提供逐对象顺序、exact-once 或正文 payload。
