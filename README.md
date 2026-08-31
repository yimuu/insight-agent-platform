# Insight Agent Platform

Insight Agent Platform 是面向关键业务 Agent 的高保证 durable execution backend。公共协议保持
`insight.platform/v1` 与 `/v1`；Resource、immutable ResourceVersion、Deployment、Run、Job、Task、Event、Receipt
与 Artifact 的 authority 仍由 PostgreSQL 和独立 worker 持有。

## 四条命令完成首次 Run

安装与本机架构匹配的官方签名 `insight` CLI 后，只需 Docker Engine 与 Docker Compose v2；普通用户不需要 Rust、
Node.js、Kubernetes 或数据库客户端。

```bash
insight init --path ./my-agent --name my-agent
insight dev --path ./my-agent
insight agent publish --path ./my-agent --file ./my-agent/agent.yaml
insight agent run my-agent --path ./my-agent --input '{"message":"hello"}'
```

默认 `starter` 只启动 deterministic Agent 所需的真实 `/v1`、PostgreSQL、NATS、S3/KMS-compatible dependency、
Gateway、Orchestration、Registry Validation、Native Capability 和 Artifact 角色。需要额外能力时显式追加 feature：

```bash
insight dev --path ./my-agent --features model,context
insight dev --path ./my-agent --features all
```

feature 集合按 canonical 顺序冻结到 profile digest；未知、重复或移除既有 feature 都会在拉取 image 或启动服务前
fail closed。`insight stop` 保留数据，`insight start` 重验并恢复同一 closure；`insight reset` 先显示精确删除范围，
只有再次提供 project name 才删除 project-local authority 和 volume。

## 安装、更新与诊断

每个 release 提供四个平台 CLI archive、checksum、SBOM、provenance、签名以及 digest-pinned runtime、Sandbox guest
和 Console image。CLI 只接受与当前平台、版本、profile/schema 和自身 binary digest 完全匹配的签名 ReleaseBundle。

```bash
insight version --json
insight update check
insight update apply --version 1.2.3
insight doctor --json
```

`doctor` 的预构建路径要求 Docker/Compose、可用端口、至少 4 CPU、8 GiB memory 与 8 GiB free disk；Rust 仅作为
`--from-source` contributor 路径的可选检查，Node.js 只用于构建 Console 和远端框架 reference。

## Contributor 资格入口

仓库贡献者可显式选择源码构建；它不是普通用户的 fallback：

```bash
cargo build --locked -p insight-cli --bin insight
target/debug/insight dev --path ./insight-local --from-source
scripts/run-productization-journey.sh --console-browser
```

`--from-source` 只构建所选 role closure。发行版或 image 验证失败时默认路径不会静默编译源码。

## 产品文档

- [`insight` CLI](docs/current/cli.md)：Agent 发布、Run、结果与高级 `/v1` 自动化；
- [公开 HTTP API](docs/current/api.md)：认证、Receipt/CAS、SSE、cursor 与 Problem；
- [Agent Console](docs/current/console.md)：Agents、Runs、Tasks、Settings 的静态 `/v1` 客户端；
- [架构](docs/current/architecture.md)：Control、durable orchestration 与 Sandbox execution plane；
- [运维](docs/current/operations.md)：starter/features、签名发行物、生命周期与资格边界。

本地 profile 始终声明 `single-node-development`、`production=false`；真实多节点 Kubernetes、runsc、容量、混沌、
restore、soak 与 production GitOps promotion 仍是外部 L4～L6 门禁，当前为 **Not run**。

## License

[Apache License 2.0](LICENSE)
