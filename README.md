# Insight Agent Platform

Insight Agent Platform 是一个以 Rust 实现的 DSL v3 持久化图执行运行时。Agent 作者编写结构化
YAML，平台在启动时将其编译为不可变、类型化的 Canonical Plan，再由数据库驱动的调度器执行。
进程重启、Worker lease 过期、信号/超时竞态与取消都通过持久化状态和 first-winner 事务恢复，
不依赖进程内执行栈重放。

当前只支持 `insight.agent/v3`，不提供旧 DSL 或旧运行内核兼容层。

## 快速启动

仓库要求 Rust `1.94.1`。Quickstart 只启用本地 `action_demo`，不需要模型密钥：

```bash
PLATFORM_CONFIG=config/platform.quickstart.yaml cargo run
```

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
api_version: insight.agent/v3
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

完整示例见 [`agents/`](agents) 和 [`tests/fixtures/v3/`](tests/fixtures/v3)。

## 文档

- [文档首页](docs/README.md)：阅读路线、现行文档与历史档案边界；
- [架构概览](docs/current/architecture.md)：执行模型、核心不变量与权威边界；
- [DSL v3 指南](docs/current/dsl.md)：Agent 结构、类型、表达式和控制流；
- [HTTP 与 SSE API](docs/current/api.md)：路由、幂等要求和响应流；
- [部署与运维](docs/current/operations.md)：配置、存储、迁移、认证和生命周期；
- [开发指南](docs/current/development.md)：代码导航和验证命令。

规范性合同见 [DSL v3 持久化图执行架构规范](docs/current/specifications/2026-07-18-dsl-v3-durable-graph-execution-design.md)。
内部代码组织和依赖边界见
[Rust Workspace 与 Crate 边界拆分规范](docs/current/specifications/2026-07-21-rust-workspace-crate-boundaries-design.md)。
历史设计、实施计划和评审记录集中保存在 [`docs/archive/`](docs/archive/README.md)，不代表当前生产合同。

## 验证

```bash
bash scripts/check-v3-cutover-residuals.sh
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
