# `insight` CLI

模型来源的批量导入、多个厂商的变量映射、密钥文件、连接检测与默认选择见[模型配置](model-configuration.md)。

`insight` 是公开 `/v1` 客户端，不拥有业务 authority。整个平台的默认启动方式见[部署与运维](operations.md)；
CLI 可以连接部署好的服务完成模型管理和 Agent 编写、发布与运行。

`run watch` 先读取 Run，再排空该次事件页；已观察终态时仍继续读取满页后的事件，保留原游标。
普通和持久游标路径均通过[真实 HTTP 回归](../specs/unified-installation/model-public-event-review.md#recorded-local-evidence)。
O 的真实 CLI 旅程也已取得 Run 完成之后的最后一条 Node 事件，见
[部署证据](../specs/unified-installation/deployment-review.md#o-current-delivery-evidence)。
历史 watch 未取得预期事件不能作为重新派发模型请求的依据。

`insight connect` 为已有 workspace 保存私有连接引用；它不创建 Tenant、复制 token 或启动服务。
安装工具签发短期 session 后，使用实际输出的 origin、Tenant 和 token 文件建立连接。普通 `init/dev/start`
等入口只说明统一安装方式；AWS 物理资格使用文末明确命名空间。

## Agent 北极星旅程

```bash
mkdir -m 700 ./my-agent
insight connect --path ./my-agent --endpoint "$CONSOLE_ORIGIN" \
  --tenant "$TENANT_ID" --token-file "$SESSION_FILE" --ca-file "$PUBLIC_CA_FILE"
insight agent validate --path ./my-agent --file ./my-agent/agent.yaml
insight agent publish --path ./my-agent --file ./my-agent/agent.yaml
insight agent run my-agent --path ./my-agent --input '{"message":"hello"}'
```

同源 Console origin 已转发管理与运行请求；直接连接两个 Gateway 时，`--endpoint` 指向管理端，
另显式提供 `--runtime-endpoint`。默认本地安装将 `PUBLIC_CA_FILE` 指向安装工具
[交付的公共 CA](installation.md#obtain-the-public-ca)，同一信任输入也用于 Artifact 上传。
全部端点使用公开受信任证书时可省略 `--ca-file`；该参数只接受公共证书。
连接文件由 [CLI connection owner](../../apps/insight-cli/src/connection.rs) 定义并校验；`.insight` 为
0700，文件为 0600。每个命令重新读取 token 文件，续发不复制 token 到连接文件。连接保存只确认
本地引用；JWT Tenant/expiry 检查不验证签名、不产生授权，当前权限与认证仍由 Gateway 判断。
同 workspace 不可切换两个 origin 或 Tenant；换目标使用新 workspace，保留原 Receipt/CAS journal。
可重新执行同一 `connect` 更新 token/CA 文件引用。已有旧安装状态或未绑定的发布、恢复记录不能被
首次连接接管。离线 `agent validate` 仍只需要显式精确 compiler 输入。

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
    ref: project/default
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

## 显式 AWS 物理资格

仓库的 AWS 物理资格流程使用独立的 `qualification-aws` 命名空间；它不是默认安装入口。
此环境保留现有受限本地 profile、已签 runtime cache 和精确进程清理，用于原资格测试。
成功准备环境后，资格 producer 从实际 profile 和身份导出普通公共连接；公共 Agent/Run 命令没有
读取旧 LocalProjectState 的回退路径。

```bash
insight doctor --json
insight qualification-aws init --path ./aws-fixture --name aws-fixture
insight qualification-aws dev --path ./aws-fixture --from-source
insight qualification-aws status --path ./aws-fixture
insight qualification-aws stop --path ./aws-fixture
insight qualification-aws start --path ./aws-fixture
insight qualification-aws reset --path ./aws-fixture --confirm aws-fixture
```

显式资格的预构建 runtime 仅支持 Linux x86_64/ARM64；macOS 资格使用 `--from-source` 和 Rust。
profile 更改、restart 身份验证、原 cache、lease/generation、停止与清理的边界保持不变；不支持或漂移
仍拒绝，不能切换到另一身份绕过失败。`doctor.ready` 只证明报告所列依赖，不构成资格或生产证明。
正常 Native、Compose 和 Helm 使用[统一安装入口](operations.md)，不通过此资格 namespace 启动。

## 安装与更新

```bash
insight version [--json]
insight update check
insight update apply --version <exact-stable-version>
```

update 验证组织 Ed25519 trust root、canonical ReleaseBundle、目标平台、CLI size/digest、profile/schema digest 与 exact
image manifest，再以同目录原子 rename 替换 binary。它不自动变更服务安装或已有运行环境；服务版本更新由部署工具管理。
显式 AWS 资格的 release transition 仍先停止 profile，再以原 feature 集合执行 `qualification-aws dev`；不能借 CLI 更新改写原恢复身份。

Task 的高级 `list` 命令每次只读取一个有界页面，支持 `--purpose respondable|viewable`、状态、种类、Run、page-size 与 cursor；空页面仍可能有下一页。`get --purpose viewable` 只请求安全元数据，表单和修改继续各自检查当前权限。CLI 依据拥有域区分 Approval 的拒绝与交互拒绝，重试保留原 Receipt 和 ETag。

`agent run` 从目标 Resource 的 active ID 读取 exact Deployment 与冻结 Published Plan，再取得输入 schema、classification 和 deadline；它不从 draft 取默认值。命令携带已读取的 exact 部署条件，active 变化会明确冲突且不隐式换 Receipt 重发。当前授权仍先于重放；只要原 Receipt 记录仍保留在数据库中，重放原 Run 不重新检查 fresh deadline 或 active 条件。高级 Run 文件请求可不带此条件，表示调用方明确选择提交时的 active 部署。
