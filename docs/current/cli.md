# `insight` CLI

默认 Cargo 构建产物是 `insight`。它是 public `/v1` 客户端和本地多进程 supervisor，不拥有业务 authority。

fresh checkout 到首个 deterministic Run 的低摩擦入口是：

```bash
scripts/run-productization-base-journey.sh --profile base
```

该单一入口构建 CLI、运行 `doctor/init/dev`、只经 public `/v1` 完成首次 Run 和 Task/restart 旅程，并在结束时
停止精确 Platform 进程。连同 `git clone … && cd …`，文档路径共两条人工命令且不需要模型 key。资格 workflow
在 checkout 前启动时钟，在首个 terminal Run 验证结果时写 marker，并以 closed north-star report 检查 600 秒上限；
工作树运行不能伪装成 fresh-checkout 报告。

需要保留平台做交互式开发时再使用下列显式命令：

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
