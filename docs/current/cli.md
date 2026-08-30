# `insight` CLI

默认 Cargo 构建产物是 `insight`。它是 public `/v1` 客户端和本地多进程 supervisor，不拥有业务 authority。

```bash
cargo build --locked
target/debug/insight doctor
target/debug/insight init --path ./insight-local
target/debug/insight dev --path ./insight-local --profile base
target/debug/insight status --path ./insight-local
```

`base` 覆盖无模型密钥的 deterministic Run；`full` 增加 Model、Context、remote Capability、MCP、Artifact maintenance、
Security/Egress 与 Sandbox。源码、lockfile 和 profile digest 不变时，`dev` 复用同一 release build。

业务命令包括 `apply`、`operation wait`、`run create|get|watch|pause|resume|cancel|result`、
`task get|submit-input|approve|reject|cancel`、`artifact upload|get|read`。所有 mutation 使用 exact Receipt 与 ETag；
本地 journal 为 bounded `0600` 文件，不保存 bearer token。

详细 closed manifest 与恢复语义见 productization 的
[`m2-cli-apply.md`](../specs/productization/m2-cli-apply.md)、
[`m2-cli-run.md`](../specs/productization/m2-cli-run.md)、
[`m2-cli-task.md`](../specs/productization/m2-cli-task.md) 和
[`m2-cli-artifact.md`](../specs/productization/m2-cli-artifact.md)。
