# Insight Agent Platform

Insight Agent Platform 是面向关键业务 Agent 的高保证 durable execution backend。当前公共协议是
`insight.platform/v1` 与 `/v1`：Resource、immutable ResourceVersion、Deployment、Run、Invocation、Job、Task、
Event、Receipt 和 Artifact 分别由 PostgreSQL 与独立 worker 持有，不依赖 Gateway 进程内状态。

旧 `insight.agent/v1` DSL runtime 已退出默认构建、镜像和当前文档；历史源码与文档仅用于追溯，不构成兼容层。

## 两条命令完成首次 Run

前置条件：Rust 1.94.1、Docker Engine/Compose。Console 和 LangGraph.js 参考集成另外使用已有的 Node.js 24 与
Corepack；Node 不是 Rust 平台服务运行时。

在尚未 clone 的目录中执行以下两条人工命令；第二条会构建 `insight`，依次执行
`doctor -> init -> dev --profile base`，通过 public `/v1` 发布 deterministic Agent、完成首个 Run 与 Task/restart
负向旅程，然后停止精确 Platform 进程并保留 fresh project 和日志：

```bash
git clone https://github.com/yimuu/insight-agent-platform.git && cd insight-agent-platform
scripts/run-productization-base-journey.sh --profile base
```

该入口不需要模型 API key，也不要求理解 workspace crate。fresh-checkout 的 10 分钟门禁由
[`north-star-report.schema.json`](examples/productization/north-star-report.schema.json) 和独立 GitHub qualification
生成机器可读报告；普通工作树不能自称 fresh checkout。

## 交互式本地平台

需要保留平台继续开发时，使用显式低层命令：

```bash
cargo build --locked
target/debug/insight doctor
target/debug/insight init --path ./insight-local
target/debug/insight dev --path ./insight-local --profile base
```

`init` 生成显式的非生产 OIDC、mTLS、配置摘要和本地 project；`dev` 预置 fresh PostgreSQL schema并监督独立
Gateway、Orchestration、Artifact 和 worker 角色。服务进程不会隐式建表。运行后可用：

```bash
target/debug/insight status --path ./insight-local
target/debug/insight token --path ./insight-local
target/debug/insight apply --path ./insight-local --file request.json
target/debug/insight run create --path ./insight-local --file run.json
target/debug/insight stop --path ./insight-local
```

完整、可复制的 Resource lifecycle curl 示例见
[`examples/productization/http-resource-lifecycle.sh`](examples/productization/http-resource-lifecycle.sh)。CLI 会输出
每个 authority ID，并保留 Problem code、request ID、Receipt、ETag 与 retryability；不会通过 SQL 或内部 RPC 走捷径。

## 产品面

- [`insight` CLI](docs/current/cli.md)：本地 profile、Resource lifecycle、Run、Task、Artifact 和 Operation；
- [公开 HTTP API](docs/current/api.md)：authoritative OpenAPI、认证、Receipt/CAS、SSE 与 Problem；
- [运行控制台](docs/current/console.md)：只访问 public `/v1` 的静态 React 客户端；
- [架构](docs/current/architecture.md)：Control、durable orchestration 与 Sandbox execution plane；
- [运维](docs/current/operations.md)：base/full profile、镜像、GitOps 与资格边界；
- [黄金场景证据](docs/specs/productization/full-journey-evidence.md)：fresh authority 上 10/10 Passed。

Python SDK 已取消。仓库提供固定 `@langchain/langgraph` 1.4.13 的独立 remote Capability reference；它不链接进
Gateway/Worker，也不获得平台数据库凭据。

## 验证边界

本地产品化 P0～P4 已覆盖真实 PostgreSQL/NATS/Object Storage、25 个独立角色、公开 `/v1`、CLI、headless
Chrome、durable restart、MCP、Artifact、WASI 和 LangGraph.js。真实多节点 Kubernetes、runsc、容量、混沌、restore、
soak 与 production GitOps promotion 仍是明确的外部 L4～L6 门禁，当前为 **Not run**；Platform spec00～18 因此继续
保持 Accepted/In Progress，而不是 Verified。

```bash
bash scripts/check-cutover-residuals.sh
python3 scripts/check-productization-ci.py
cargo check --locked --workspace --all-targets
```

历史 DSL 文档位于 [`docs/archive/current-dsl-v1/`](docs/archive/current-dsl-v1/)，旧 Helm 拓扑位于
[`deploy/archive/helm/insight-agent-platform/`](deploy/archive/helm/insight-agent-platform/)。

## License

[Apache License 2.0](LICENSE)
