# Insight Agent Platform

Insight Agent Platform 是一个以 Rust 实现的 DSL v1 图执行运行时。Agent 作者编写结构化 YAML，
平台在启动时将其编译为不可变、类型化的 Canonical Plan；Deployment Revision 同时冻结 `full`
或 `terminal_only` persistence policy。`full` 使用数据库驱动的 scheduler、checkpoint、
lease/fence 与恢复；显式 opt-in 的 `terminal_only` 在 owner 进程内执行，只持久化 admission 和
最终结果，进程失败会中断未完成 Run。Quickstart 和平台默认值继续为 `full`。

当前只支持 `insight.agent/v1`，不提供旧 DSL 或旧运行内核兼容层。

## 快速启动

仓库要求 Rust `1.94.1`。Quickstart 只启用本地 `action_demo`，不需要模型密钥：

首次启动或明确重建数据库时：

```bash
bash scripts/provision-sqlite-schema.sh
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

第一条命令在服务启动前从
[`database/durable/sqlite/schema.sql`](database/durable/sqlite/schema.sql) 创建
`data/quickstart.sqlite3`。服务本身不会创建数据库文件或表；如果目标文件已经存在，provisioner
会拒绝覆盖。已按当前 contract provision 的数据库在普通服务重启时直接运行第二条命令；只有明确
重建或 Schema contract 变化时，才先移动/删除 pre-1.0 开发数据库并重新执行 provisioner。

服务默认监听 `127.0.0.1:3000`：

```bash
curl http://127.0.0.1:3000/health/ready

curl -X POST http://127.0.0.1:3000/v1/agents/action_demo/runs \
  -H 'content-type: application/json' \
  -H 'x-request-id: example-1' \
  -d '{"text":"hello durable graph"}'
```

创建接口返回 `202 Accepted` 和 `run_id`。随后查询：

```bash
curl http://127.0.0.1:3000/v1/runs/RUN_ID
```

## 最小 Agent

```yaml
api_version: insight.agent/v1
kind: agent

inputs:
  text: string

output: TextMetrics

types:
  TextMetrics:
    fields:
      characters: integer
      words: integer
      lines: integer

workflow:
  steps:
    - type: action
      id: analyze_text
      call: example.text_metrics
      inputs:
        text: $text
      response: TextMetrics

    - return: $analyze_text
```

完整示例见 [`agents/`](agents) 和 [`tests/fixtures/dsl/`](tests/fixtures/dsl)。

## 文档

- [文档首页](docs/README.md)：阅读路线、现行文档与历史档案边界；
- [架构概览](docs/current/architecture.md)：执行模型、核心不变量与权威边界；
- [DSL v1 指南](docs/current/dsl.md)：Agent 结构、类型、表达式和控制流；
- [HTTP 与 SSE API](docs/current/api.md)：路由、幂等要求和响应流；
- [部署与运维](docs/current/operations.md)：Schema 预置、配置、存储、认证和生命周期；
- [开发指南](docs/current/development.md)：代码导航和验证命令；
- [变更记录](CHANGELOG.md)：发布版本的重要变化与兼容性说明。

当前行为由上述文档、公开 schema、编译器与 verifier、数据库约束及测试共同定义。历史设计、
实施计划、评审和已完成资格验收记录集中保存在
[`docs/archive/`](docs/archive/README.md)，不代表当前生产合同。

## 验证

```bash
bash scripts/check-cutover-residuals.sh
bash scripts/check-crate-boundaries.sh
bash scripts/check-public-api-baseline.sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
```

CI 使用同一组 workspace 门禁，并在 PostgreSQL 16 上运行数据库合同与 real-process
restart/shutdown 测试，同时执行依赖策略检查。具体环境要求见[开发指南](docs/current/development.md)。

## License

本项目采用 [Apache License 2.0](LICENSE)。
