# Spec 02：Agent CLI 产品旅程

| 属性 | 值 |
|---|---|
| 状态 | Accepted / CR-207 |
| 日期 | 2026-08-31 |
| 命令入口 | `insight agent ...` |
| authority | public `/v1` only |

## 1. 目标

CLI 默认使用 Agent 领域词汇完成验证、发布、运行和结果读取。现有 `apply`、`run`、`operation` 命令保留为
高级/自动化入口，但不再是 Quickstart 的必需知识。

## 2. 命令面

首版固定为：

```text
insight agent validate --file <agent.yaml>
insight agent publish --file <agent.yaml> [--wait] [--output text|json]
insight agent list [--output text|json]
insight agent get <name-or-agent-id> [--output text|json]
insight agent adopt <name> --agent-id <agt_...>
insight agent run <name-or-agent-id> (--input <json>|--file <input.json>)
                  [--detach] [--timeout-seconds <n>] [--output text|json]
insight agent logs <name-or-run-id> [--follow]
insight agent result <run-id> [--output text|json]
```

不增加 `agent create` 和 `agent deploy` 两个独立默认步骤；`publish` 是以同一 manifest digest恢复的显式生命周期
编排。用户仍可通过 `insight apply --advanced`操作底层资源。

## 3. 命令行为

### 3.1 validate

- 不发网络请求、不创建 Artifact、不写 lock；
- 校验 YAML、引用文件、schema和确定性编译；
- project alias无法离线解析时报告 `unresolved`，只有 `--online` 才访问 `/v1` 重验；
- 成功输出 manifest digest、execution kind和required feature集合。

### 3.2 publish

- 调用 Spec 01 的编译和发布算法；
- 默认等待 validation Operation 和 activation完成；
- `--wait=false` 只允许在返回一个可恢复 publication handle时使用，不能把 accepted写成 published；
- 无变化重放返回 `unchanged`，不得创建新 revision；
- 成功默认只显示 Agent名、状态、environment和可直接复制的 run命令。

### 3.3 list/get

- `list` 默认列出当前 project lock管理的 Agent，并在线标记 `ready/drifted/unreachable`；
- Console所需的远端全集分页查询由 Spec 03 的现有 Resource authority投影提供；
- `get` 接受 project name或exact Agent ID；名称只在 project内唯一；
- `adopt` 要求读取 exact Agent、kind和tenant，不能按 display name猜测或接管其他tenant资源。

### 3.4 run

- name首先解析为 Agent ID，并在线重验 active Deployment；
- 根据 manifest input schema本地校验输入，再调用现有 `POST /v1/runs`；
- deadline由命令参数或已物化 manifest limit确定，不能依赖客户端当前时间之外的隐藏默认；
- 默认订阅 durable SSE直到terminal并输出结果；终端中断后打印一个可复制的resume命令；
- `--detach` 返回高层 Run summary，不显示 Job或Receipt；
- terminal failure返回非零，结构化输出保留稳定 failure code和retryability。

### 3.5 logs/result

- `logs`读取Run public events，不读取进程日志、数据库或内部trace payload；
- name只解析最近由当前project创建的Run；有歧义时要求明确Run ID；
- cursor在本地受限journal中恢复，用户不手写；
- `result`只读取terminal safe projection，并验证Run ID、schema/content digest和Artifact引用完整性。

## 4. 输出分层

默认文本输出示例：

```text
Published support-agent to development
Status: ready
Run: insight agent run support-agent --input '{"message":"hello"}'
```

`--output json`使用独立、closed、版本化的产品 DTO，只包含：

- `schema_version`
- `agent_name`、`agent_id`
- `state`、`environment`
- `run_id`、`run_state`、`result`（适用时）
- `problem` 的安全摘要（失败时）

`--verbose`增加时间、manifest digest、trace/request ID；只有 `--debug-authority` 增加 Version、Deployment、Operation、
Receipt、ETag和cursor。任何模式都不输出token、Secret、signed URL或内部credential。

## 5. 幂等、并发与恢复

- 所有Receipt由 `project identity + manifest/run digest + action`确定生成并保存在0600 journal；
- ETag由CLI读取、携带和刷新；不会要求用户复制；
- 网络超时后先读取Receipt/Resource/Run确认effect，再决定重试；
- 同一project两个publish进程竞争时最多一个推进current head，另一个返回可理解的conflict和重试建议；
- journal损坏、权限过宽或manifest digest不匹配时fail closed，不猜测上一步成功状态。

## 6. 兼容与高级入口

- 不删除现有`run/task/artifact/operation/apply`机器命令；它们从Quickstart移到“Platform automation”章节；
- 新命令仍调用相同public DTO，不增加CLI-only server header或路由；
- JSON产品DTO不是OpenAPI对象的替代authority；脚本需要完整authority时显式使用高级命令；
- clean-cut后不保留旧help文本的双重默认路径。

## 7. 验收

- fresh预构建CLI以四条北极星命令完成自定义Agent首次Run；
- 默认旅程没有用户提供的digest、revision、Artifact/Version/Deployment/Job/Receipt/ETag/cursor；
- publish每个HTTP边界前后kill/restart均可由同一命令恢复且无重复effect；
- run覆盖terminal success/failure、Ctrl-C后resume、SSE重连、Gateway重启和Artifact result；
- text/json/verbose/debug-authority的字段泄漏矩阵通过；
- CLI help首屏只展示`init/dev/agent`，高级命令有明确入口但不干扰首次使用。
