# Insight Agent Platform

Insight Agent Platform 是面向关键业务 Agent 的高保证 durable execution backend。公共协议保持
`insight.platform/v1` 与 `/v1`；Resource、immutable ResourceVersion、Deployment、Run、Job、Task、Event、Receipt
与 Artifact 的 authority 仍由 PostgreSQL 和独立 worker 持有。

## 安装与配置

在仓库根目录运行一条命令，构建并启动完整平台：

```bash
docker compose up -d --build
```

打开 http://127.0.0.1:8088，首次创建管理员账号，之后使用邮箱和密码登录。
在“模型配置”选择服务并填写 API Key 和模型 ID，再创建智能体或工作流。
不需要先生成配置文件、填写模型地址到安装文件，或导入浏览器证书和登录令牌。

首次构建由 Docker 完成，主机无需 Python、Rust 或 Node.js。依赖初始化由 Compose 的一次性服务完成；
重复启动保留数据库、账号、密钥和模型配置。可选的安装名称、端口与镜像设置见 [.env.example](.env.example)。
详细启动、验证、停止和 Kubernetes 操作见[安装指南](docs/current/installation.md)，
模型配置及中断恢复见[模型配置](docs/current/model-configuration.md)。
从[交付入口索引](docs/current/README.md#统一安装交付入口)进入公共 CLI、Console 和文档检索/人工审阅样例。
实现、本地回归与实际安装/业务验收分别记录，当前[完整验收](docs/specs/unified-installation/README.md)尚未全部完成。
安装本身不要求公开平台端口；远端模型和检索请求仍遵守部署允许的 HTTPS 目标及出站策略。

原生进程使用[统一 Native 启动器](docs/current/installation.md#start-native-processes)，依赖仍由 Docker 运行。
CLI 通过安装输出的连接和会话文件操作已有服务：

```bash
mkdir -m 700 ./my-agent
insight connect --path ./my-agent --endpoint "$CONSOLE_ORIGIN" \
  --tenant "$TENANT_ID" --token-file "$SESSION_FILE" --ca-file "$PUBLIC_CA_FILE"
insight agent publish --path ./my-agent --file ./my-agent/agent.yaml
insight agent run my-agent --path ./my-agent --input '{"message":"hello"}'
```

当前 Native 启动器冻结主机产物和安装身份，前台退出保留依赖数据；公开 CLI 命令见 [CLI 文档](docs/current/cli.md)。
模型、文档检索与普通 Agent 不要求启用 Sandbox；不可信代码执行使用独立 Kubernetes/OpenSandbox 安装。

## 安装、更新与诊断

通过当前发行门禁的版本提供四个平台 CLI archive、checksum、SBOM、provenance、签名以及 digest-pinned runtime、Sandbox runner
和 Console image。CLI 只接受与当前平台、版本、profile/schema 和自身 binary digest 完全匹配的签名 ReleaseBundle。

```bash
insight version --json
insight update check
insight update apply --version 1.2.3
insight doctor --json
```

`update apply` 只原子安装已签名的 exact CLI，不会隐式改写现有服务安装。服务更新由部署工具管理；
显式 AWS 资格环境的 release transition 使用 `qualification-aws stop/dev`，不是普通启动入口。

`doctor` 检查 Docker/Compose、可用端口、至少 4 CPU、8 GiB memory 与 8 GiB free disk；其 `ready` 只汇总已列依赖检查，
不证明 runtime 可在当前主机执行。Rust 是报告中的可选检查，但 `--from-source` 启动必需；Console 镜像内使用 Node.js 提供静态资源与同源转发，主机无需安装 Node.js，除非在宿主机开发 Console 或运行远端框架 reference。

## Contributor 资格入口

仓库贡献者可显式选择源码构建；它不是普通用户的 fallback：

```bash
cargo build --locked -p insight-cli --bin insight
target/debug/insight qualification-aws dev --path ./insight-local --from-source
tools/qualification/run-productization-journey.sh --console-browser
```

`--from-source` 只构建所选 role closure。发行版或 image 验证失败时默认路径不会静默编译源码。
仓库根是 virtual Cargo workspace；可执行实现只来自显式 `platform-*` role 与 `insight` CLI，跨平面
journey 由 `insight-platform-qualification-tests` 持有，不存在根级 runtime facade。

## 产品文档

- [安装](docs/current/installation.md)：Compose、Helm、私有身份与恢复；
- [模型配置](docs/current/model-configuration.md)：多厂商来源、密钥、额度与默认模型；
- [`insight` CLI](docs/current/cli.md)：Agent 发布、Run、结果与高级 `/v1` 自动化；
- [公开 HTTP API](docs/current/api.md)：认证、Receipt/CAS、SSE、cursor 与 Problem；
- [Agent Console](docs/current/console.md)：Agents、Runs、Tasks、Models、Settings 的静态 `/v1` 客户端；
- [架构](docs/current/architecture.md)：Control、durable orchestration 与 Sandbox execution plane；
- [运维](docs/current/operations.md)：初始化、持久依赖、生命周期与资格边界。

本地 profile 始终声明 `single-node-development`、`production=false`；真实多节点 Kubernetes、固定的
containerd/runc 与 CNI 闭包、容量、混沌、restore、soak 和 production GitOps promotion 仍是外部 L4～L6 门禁，
当前为 **Not run**。

## License

[Apache License 2.0](LICENSE)
