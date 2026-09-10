# 工程指南

从 [AGENTS.md](../../AGENTS.md) 的工程规则、[当前架构](../current/architecture.md) 和对应 owning 合同开始。

## 代码归属

| 目录 | 职责 |
|---|---|
| `apps/services` | 可部署服务入口、配置与物理依赖装配 |
| `apps/sandbox` | 隔离环境中的固定 runner |
| `apps/insight-cli`、`apps/console` | 公开 API 的产品客户端 |
| `crates/foundation` | 共享身份、严格边界类型与纯观测模型 |
| `crates/definitions` | Plan 定义和 Registry 生命周期 |
| `crates/authoring` | native 与 WASM 共用的作者编译语义 |
| `crates/execution` | Job、调度、解释器、执行上下文 |
| `crates/domains` | 各领域的合同、决策和用例端口 |
| `crates/application` | 跨领域用例编排与进程内执行容量 |
| `crates/protocols` | HTTP、RPC、trace 等传输边界 |
| `crates/adapters` | PostgreSQL、网络协议、凭证和执行提供方 |
| `crates/deployment` | 签名发行与部署证据合同 |
| `tools/rust` | 合同生成、建库和发布工具 |
| `tests/qualification` | 真实依赖的组合场景 |
| `deploy` | 镜像、Helm、开发环境和发行输入 |

Plan、领域决策和跨边界类型不依赖物理 I/O。应用消费 owning port，PostgreSQL adapter 实现事务；服务入口装配
数据库、RPC client 和 handler。修改边界后运行 `tools/checks/check-crate-boundaries.sh`，不要为通过检查而扩大允许边。

## 修改合同

先阅读 owning Rust type、生成合同、当前 schema、关联 ADR 和对应 `docs/current` 页面。先共同评审上游合同与
架构，再生成消费者并实施。评审覆盖 authority、身份、字段、错误、事务、事件、权限、容量、恢复和验证证据。

当前 schema 位于 `crates/adapters/platform-postgres/schema.sql`，结构 inventory 与 schema contract 位于同目录。
`platform-schema provision` 只用于空目标，`verify` 只读核验实际结构，`inventory` 导出实际结构。
运行服务不执行 DDL。项目不保留旧迁移链、历史 reader 或数据转换；工具也不自动删除已有数据。

使用 `check-platform-contracts --write` 从 owning type 生成机器合同，再运行相应 conformance 检查。公开 API 名称、binary
名称与 schema ID 不从目录名推导。生成合同之外仍需要独立负向、并发、权限和恢复测试。

## 构建与验证

先执行 `cargo check --workspace --all-targets` 检查所有消费者。无外部依赖的 Rust 单测与真实 PostgreSQL、消息、TLS、
提供方及进程测试分别运行。真实依赖测试必须显式配置其声明的环境，缺配置是失败；不要以本地跳过冒充验证。
CI 的具体启动和测试命令以 `.github/workflows` 及 `tools/qualification` 的实际脚本为准。

数据库测试使用独占空 fixture。真实服务启动检查当前 executable 的 SHA256 与安装能力目录，所以进程 fixture 必须
先构建目标 binary，再根据真实字节构造配置。改变配置中的摘要不能让未安装的解释器或适配器获得执行能力。

Console 构建需要 `wasm32-unknown-unknown` target 和 owning WASM crate 固定版本的 `wasm-bindgen-cli`；CI 与
发行流程显式安装这两个工具。共享编译器的资源观测可在构建 Console WASM 后执行：

```bash
cargo build --locked -p insight-platform-contract-tooling --bin agent_compiler_resources
node apps/console/tests/compiler-resources.ts target/debug/agent_compiler_resources /tmp/compiler-resources.json
```

报告校验真实 native/WASM 的编译与源码预检响应一致性，并记录输入、编译器与执行文件摘要，以及两个阶段各自的耗时和 WASM 线性内存高水位。
native 报告标明是否启用 debug assertions。它覆盖允许的源文件边界、超限及深层输入拒绝，是本地资源观测，不能当作
优化构建之间的性能排名、整个浏览器内存或生产容量证明。输出路径必须尚不存在，CI 将报告保留为独立产物。

文档解释行为和不变量；字段闭集、版本和限制由 owning 合同定义。只有实现、合同、当前文档和证据一致后才删除临时
spec；Git 保留历史。生产容量、恢复和晋级资格必须绑定实际候选与真实环境，本地功能验证不能替代。
