# `insight` CLI

`insight` 是 public `/v1` 客户端和本地多进程 supervisor，不拥有业务 authority。官方预构建 CLI 是默认入口；
源码构建只在显式 `--from-source` 时发生。

## Agent 北极星旅程

```bash
insight init --path ./my-agent --name my-agent
insight dev --path ./my-agent
insight agent validate --path ./my-agent --file ./my-agent/agent.yaml
insight agent publish --path ./my-agent --file ./my-agent/agent.yaml
insight agent run my-agent --path ./my-agent --input '{"message":"hello"}'
```

`agent publish` 代管 Artifact materialization、Draft validation、immutable publish、Deployment activation、Receipt 与 ETag；
崩溃后按 0600 journal 和服务端 authority 恢复。默认文本输出只显示 Agent、环境、状态和下一条命令；
`--verbose` 增加 manifest digest，`--debug-authority` 才显示 ResourceVersion、Deployment 与 Operation ID。

产品命令为：

- `agent validate|publish|list|get|adopt|run|logs|result`；
- `run` 默认等待终态并直接输出 Inline result，`--detach` 显式返回；
- `agent logs --follow` 使用 durable SSE cursor 重连，Ctrl-C 后可恢复；
- `text|json` 输出使用同一 closed DTO，token、Secret、signed URL 和 Artifact body 不进入 journal。

高级 `apply`、`operation`、`run`、`task`、`artifact` 命令保留在 `insight advanced`，仍完整执行 public `/v1`
Receipt/CAS/Operation 语义。

## `agent.yaml`

普通作者只描述名称、执行方式、输入输出与发布环境。CLI负责确定性编译、Artifact上传、Draft校验、immutable publish、
Deployment activation和恢复；manifest、`insight.lock`都不是服务端authority。

```yaml
apiVersion: insight.platform/v1
kind: Agent
metadata:
  name: support-agent
  displayName: Support Agent
spec:
  execution:
    kind: model_chat
  instructions: |
    Answer using only the provided user input.
  model:
    ref: project/default-model
  input:
    schema: schemas/input.json
    classification: internal
  output:
    schema: schemas/output.json
  limits:
    deadlineSeconds: 120
  publish:
    environment: development
```

首版 execution kind 只有：

- `deterministic`：`start -> return`，不允许 `instructions` 或 `model`，且 input/output schema 必须相同；
- `model_chat`：`start -> model_loop -> return`，必须提供 `instructions` 和一个 Model binding。

`metadata.name` 匹配 `[a-z][a-z0-9-]{0,62}`；schema 路径必须位于 project root 内；classification 只有
`public|internal|confidential|restricted`；deadline 为 1～3600 秒。YAML 和其中所有对象都是 closed schema，拒绝 unknown/duplicate
field、merge/anchor/alias/tag、绝对路径、`..`、符号链接逃逸、Secret/token/数据库URL、任意endpoint和shell command。条件字段错误
会在网络请求、Artifact上传或lock写入前失败。完整可执行结构以
[`agent-compiler/v1`](../../contracts/product-experience/agent-compiler/v1/corpus.json) corpus 为准。

成功发布后，0600 `insight.lock`只保存manifest digest、name到Agent ID的映射、active revision/deployment摘要和binding alias。
它不保存credential或正文；每次使用仍向`/v1`重验。lock丢失时用`insight agent adopt`恢复寻址，不会重建或覆盖服务端事实。

## 本地 profile

```bash
insight doctor --json
insight dev --path ./my-agent
insight dev --path ./my-agent --features model,context
insight status --path ./my-agent
insight logs --path ./my-agent --role orchestration
insight stop --path ./my-agent
insight start --path ./my-agent
insight reset --path ./my-agent
insight reset --path ./my-agent --confirm my-agent
```

默认 profile 名为 `starter`。closed feature 是 `model`、`remote-capability`、`context`、`mcp`、`sandbox` 和其
canonical union `all`。增加 feature 只追加 identity/config/role；隐式移除被拒绝。`--offline` 只使用已验证 cache，
缺失时给出精确 pull 指令；`--from-source` 与 `--offline` 冲突，且不存在验证失败后的源码 fallback。

`status` 明确输出 `single-node-development`、`production=false` 和 L4～L6 `not_run`。

## 安装与更新

```bash
insight version [--json]
insight update check
insight update apply --version <exact-stable-version>
```

update 验证组织 Ed25519 trust root、canonical ReleaseBundle、目标平台、CLI size/digest、profile/schema digest 与 exact
image manifest，再以同目录原子 rename 替换 binary。它不自动重启本地环境。
