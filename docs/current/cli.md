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

- `agent validate|publish|list|get|adopt|run|logs|result`；作者包恢复、依赖查询和评测入口见[共享作者入口](agent-authoring.md)；
- `run` 默认等待终态并直接输出 Inline result，`--detach` 显式返回；
- `agent logs --follow` 使用 durable SSE cursor 重连，Ctrl-C 后可恢复；
- `text|json` 输出使用同一 closed DTO；常规输出、诊断及 Agent 发布命令记录不包含 token、Secret、对象 URL 或正文。

专用 Artifact 上传 journal 为中断恢复在私有 0600 文件中保存短期上传 URL 和 completion proof，目录权限为 0700。
这些上传能力受原上传期限和当前授权约束，不进入日志、报告或 Git；本地记录不授予续期或重开已完成上传的权限。
Agent 发布、独立上传和评测报告在同一项目内复用受锁保护的上传记录。每次新的上传意图先持久化独立命令身份，
中断重试沿用该身份和原期限；已完成记录只有通过当前权限下的 Ready Artifact 内容核对后才能复用。

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

CLI 与 Console 使用同一纯 Rust compiler；Console 运行该核心的 WASM 构建。普通声明可直接描述确定性处理或 Model chat；复杂 Agent 使用 `full_plan` 引用完整 Plan 源，或使用 `framework_graph` 引用显式静态框架导出。执行节点、schema 文档、exact dependency slot 和预算由唯一 [Plan owner](../../crates/definitions/platform-plan/src/lib.rs) 验证。

完整作者语法与边界限额由 [compiler](../../crates/authoring/platform-agent-compiler/src/lib.rs)、[source bundle](../../crates/authoring/platform-agent-compiler/src/boundary.rs) 和 [conformance corpus](../../contracts/product-experience/agent-compiler/v2/corpus.json) 定义。文件路径受 project root 约束；解析拒绝重复或未知字段、YAML 可执行扩展和逃逸路径。

成功发布后，0600 `insight.lock` 与 publication cache 以完整 source bundle digest 绑定作者输入，分别保留 manifest digest。
新的发布意图有独立、预先持久化的命令身份；中断重试复用该身份和原请求条件，不能因响应丢失换键。
完成记录只证明过去的命令。重复发布是否无需修改，由当前已激活版本的源包和完整执行闭包决定；将源码从 B 改为 C 再改回 B 是一次新的发布。
本地记录保存寻址与恢复信息，使用时仍重验服务端 authority。`agent adopt` 恢复寻址；`agent restore-source` 读取 exact 已发布版本，恢复可重新编译的作者源与冻结的编译 profile，详情见[共享作者入口](agent-authoring.md)。

普通信号使用 `insight agent signal <run-id> <signal-key> --path ./project [--file signal-request.json]`。文件是公开 Run signal 的 typed payload DTO；省略文件发送无 payload 信号。同一 tenant、Run、信号和请求内容复用相同幂等键，每次重试仍经过当前服务端授权，成功响应不缓存为新的授权。

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
canonical union `all`。同一 release/source 内增加 feature 保留现有本地身份，并重建所选角色的完整配置与实际 binary manifest；
隐式移除被拒绝。配置更新要求先停止已有角色，在本地 lifecycle lock 下验证整批旧摘要与私有 staging 的新摘要，
以原子写入 runtime profile 作为提交点。中断后 `dev/start/status/stop` 会先恢复该有界 journal；未知文件漂移或仍有活跃角色则拒绝。
其他 profile 读取遇未恢复 journal 也会停止，不消费半更新配置。源码构建指纹覆盖迁移后的服务、CLI、编译器/合同及工具源，
排除构建缓存；实际 executable digest 另行校验，不能由源码指纹代替。切换 exact
release/source 时必须先以 persisted feature 集合运行一次 `dev`，不能在同一次操作中同时切 identity 和增加 feature。`start` 从已验证的
runtime profile 恢复 exact feature/release/source closure，并在安全 running 点修复 project summary；它不会从可能滞后的 summary 反向切换。
`--offline` 只使用已验证 cache，缺失时给出精确 pull 指令；`--from-source` 与 `--offline` 冲突，且不存在验证失败后的源码 fallback。

Artifact、Policy 与配额的初始化输入由 CLI 和建库工具共用的[部署合同](../../crates/deployment/platform-deployment-contracts/src/development.rs)校验。
同一项目重建运行配置时保留其原始文件与持久身份，恢复 journal 也不能替换该输入。文件缺失、摘要漂移或当前策略与物理绑定不一致时拒绝，
需要初始化新的本地身份；不会重分配身份来绕过数据库的重放检查。

`status` 明确输出 `single-node-development`、`production=false` 和 L4～L6 `not_run`。

## 安装与更新

```bash
insight version [--json]
insight update check
insight update apply --version <exact-stable-version>
```

update 验证组织 Ed25519 trust root、canonical ReleaseBundle、目标平台、CLI size/digest、profile/schema digest 与 exact
image manifest，再以同目录原子 rename 替换 binary。它不自动变更 project-local runtime identity；先 `insight stop`，再以
原 feature 集合运行一次 `insight dev` 完成 release transition，之后才可在另一次 `dev` 增加 feature。

Task 的高级 `list` 命令每次只读取一个有界页面，支持 `--purpose respondable|viewable`、状态、种类、Run、page-size 与 cursor；空页面仍可能有下一页。`get --purpose viewable` 只请求安全元数据，表单和修改继续各自检查当前权限。CLI 依据拥有域区分 Approval 的拒绝与交互拒绝，重试保留原 Receipt 和 ETag。

`agent run` 从目标 Resource 的 active ID 读取 exact Deployment 与冻结 Published Plan，再取得输入 schema、classification 和 deadline；它不从 draft 取默认值。命令携带已读取的 exact 部署条件，active 变化会明确冲突且不隐式换 Receipt 重发。当前授权仍先于重放；只要原 Receipt 记录仍保留在数据库中，重放原 Run 不重新检查 fresh deadline 或 active 条件。高级 Run 文件请求可不带此条件，表示调用方明确选择提交时的 active 部署。
