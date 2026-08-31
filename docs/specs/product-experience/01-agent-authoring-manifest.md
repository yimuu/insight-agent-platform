# Spec 01：简化 Agent authoring manifest

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-210 |
| 日期 | 2026-08-31 |
| 输入 | `agent.yaml` |
| 输出 | 现有 `/v1` Resource、Artifact、Version、Deployment 与 activation 请求 |

## 1. 目标

普通作者只描述 Agent 的名字、行为、输入输出和所需绑定，不填写 revision number、content digest、Artifact ID、
ResourceVersion ID、Deployment ID、Receipt、ETag 或完整 Deployment closure。客户端将 manifest 确定性编译为
当前 Platform v1 合同并执行现有生命周期。

manifest 是本地 authoring source；PostgreSQL 中的 Resource/ResourceVersion/Deployment 仍是唯一 authority。

## 2. 首版范围

首版只支持两种可解释模板：

- `deterministic`：`start -> return`，用于无模型密钥的 quickstart、schema 和运行链验证；
- `model_chat`：`start -> model_loop -> return`，使用一个已安装的 exact Model Deployment。

Capability、Context、MCP、Skill、Subagent、HumanTask、Timer、Signal 和 Sandbox Plan 继续使用高级
`insight apply`。后续只有在用户旅程证明需要后，才能逐项加入 manifest；不得在首版预埋自由 node JSON。

## 3. YAML 合同

示例：

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

顶层和所有对象均为 closed schema，unknown field 必须失败。解析器使用 YAML 1.2 JSON-compatible subset，并拒绝：

- duplicate key、merge key、anchor、alias、自定义 tag；
- 非 UTF-8、非规范换行、隐式 timestamp、NaN/Infinity；
- 绝对路径、`..`、符号链接逃逸和 project root 外文件；
- Secret value、token、数据库 URL、任意 endpoint URL 和 shell command。

字段合同：

| 字段 | 约束 |
|---|---|
| `apiVersion` | exact `insight.platform/v1` |
| `kind` | exact `Agent` |
| `metadata.name` | project-local稳定 key，`[a-z][a-z0-9-]{0,62}` |
| `metadata.displayName` | 1～255字符；缺省为 name 的展示形式 |
| `spec.execution.kind` | `deterministic` 或 `model_chat` |
| `spec.instructions` | model_chat必填；UTF-8、1～16384 bytes、拒绝NUL；不是 platform/system role |
| `spec.model.ref` | model_chat必填；project alias 或 advanced exact Deployment ID |
| `spec.input/output.schema` | project-relative JSON Schema 文件 |
| `spec.input.classification` | `public/internal/confidential/restricted` |
| `spec.limits.deadlineSeconds` | 1～3600；省略时从 project profile 显式物化 |
| `spec.publish.environment` | closed environment name；省略时从 project config 显式物化 |

`deterministic` 不允许 `instructions` 或 `model`，并要求input/output schema的canonical digest完全相同，因为首版模板没有可改变
value/schema的Compute或Capability节点；Return直接消费exact RunInput port。`model_chat` 不允许缺失 model binding。条件字段组合错误必须在
任何 HTTP、Artifact upload 或本地 lock 写入前失败。

## 4. 确定性编译

实现应提供一个无网络、无数据库、无执行能力的 shared authoring compiler。输入为 manifest bytes、引用文件 bytes、
project profile和已解析 exact binding；输出为：

1. canonical manifest digest；
2. closed `AgentResourceIntent`，包含Resource业务字段、authoring/plan artifact purpose与digest，但不含Artifact ID；
3. canonical Typed Plan v5 bytes与digest；
4. input/output/error schema digest；
5. Deployment binding intent；
6. 有序 lifecycle request plan，但不包含尚未由服务端生成的ID、ETag或Receipt。

完整`ResourceDocument::Agent`不是纯编译输出：它强制包含authoring与Typed Plan的exact Artifact ID，而这些ID只能由
`prepare-upload`成功响应生成。publish executor必须先上传并验证compiler冻结的两组bytes/digest，再把服务端返回的exact
`artifact_id + content_digest`填入`AgentResourceIntent::materialize`；该纯materializer重验purpose、digest、classification、Ready状态摘要
与compiler intent后才构造现有closed `AgentResourceSpec`。调用方、lock或journal不得预留、猜测或替换Artifact ID。

ordered lifecycle plan使用closed logical output reference（例如`authoring_artifact`、`typed_plan_artifact`）表达步骤依赖；这些reference
只存在于客户端编排模型，不进入public request、PostgreSQL、Event或digest。Receipt request digest在每个HTTP步骤物化后绑定实际closed body，
不能把placeholder发给Gateway。

相同输入必须字节级产生相同输出；文件遍历顺序、YAML map 顺序、操作系统和 CPU 架构不得影响 digest。
CLI 与 Console 实现若使用不同语言，必须共享同一 conformance fixture corpus并逐字节比较编译产物。

`model_chat.spec.instructions`确定性映射到`AgentResourceIntent.author_instructions`，materialize后成为immutable
`AgentResourceSpec.author_instructions`。运行时只从Run冻结的exact Agent Revision读取它，并在Plan node instruction之后、required
Skill之前生成`AgentInstruction` assembly block；该block固定为`user` role且`trusted_instruction=false`。`deterministic`的该字段固定为
`null`。compiler、publisher或runtime不得把作者正文拼入platform safety、Agent contract或Plan node instruction。

`deterministic`的Typed Plan为`start -> return`，Return的`RunInput.schema_digest`同时等于input/output schema digest；compiler不得
伪造output digest、插入隐式coercion或生成一个只能在terminal materialization失败的Plan。

## 5. 发布执行

`publish` 按现有 authority 顺序执行：

```text
parse/validate locally
-> resolve exact bindings
-> compile plan
-> upload and verify authoring/plan artifacts
-> materialize exact Agent ResourceDocument from returned Artifact authority
-> create or update Agent Draft
-> validate Draft and wait Operation
-> publish immutable revisions
-> create Deployment from server-owned exact IDs
-> activate Deployment
-> verify resulting active binding
```

客户端不得跳过 validation、伪造 server-owned ID、直接写数据库或把半完成状态报告为成功。进程在任一步崩溃后，
以 manifest digest、project lock和服务端 Receipt/ETag恢复现有步骤，不重新发布不同内容。

## 6. Project lock

成功发布后写入 project-local `insight.lock`：

- manifest digest；
- `metadata.name -> resource_id` 映射；
- exact active Version/Deployment摘要；
- binding alias解析结果；
- environment与最后成功时间。

lock 只用于寻址、恢复和展示，不是 authority。每次使用必须向 `/v1` 重验 current Resource/Deployment。lock不得保存
token、Secret value、signed URL、Artifact body或数据库连接。丢失 lock 不影响服务端资源；`agent adopt` 可通过显式 Agent ID
重新建立映射。

## 7. 错误

默认错误面只使用面向作者的稳定 code：

- `agent_manifest_invalid`
- `agent_reference_missing`
- `agent_binding_not_ready`
- `agent_compile_failed`
- `agent_publish_conflict`
- `agent_publish_recovery_required`

错误保留原始 public Problem code、request ID、trace ID和retryability到诊断结构，但默认终端不打印 authority payload。

## 8. 验收

- positive fixtures覆盖两个 execution kind及 YAML/JSON 等价 canonicalization；
- deterministic input/output schema不同的fixture在任何副作用前失败；
- negative fixtures覆盖所有 YAML危险特性、路径逃逸、unknown field、超限和条件字段；
- CLI/Console compiler corpus产生相同 digest、Typed Plan和请求序列；
- 第一次发布、无变化重放、内容更新、并发发布和每个 crash window 都不产生重复 effect；
- manifest中不存在 ResourceVersion、Deployment、Job、Receipt或ETag字段；
- 发布后的 Run仍冻结与高级 lifecycle完全相同的 exact closure。
