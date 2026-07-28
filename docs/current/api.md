# HTTP 与 SSE API

状态：Current

基础路径：`/v1`

HTTP `/v1` 是服务 API 版本，与 Agent DSL 的 `insight.agent/v1` 是两个独立版本空间。除健康检查外，
路由按平台认证配置执行授权。

## 健康与发现

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 综合健康状态 |
| `GET` | `/health/live` | 进程存活 |
| `GET` | `/health/ready` | repository 与 runtime 就绪 |
| `GET` | `/v1/agents` | Agent discovery 与输入 schema |
| `GET` | `/v1/agents/{agent_id}` | 单个 Agent discovery |

## Graph 发布

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/graph-agents/{agent_id}/revisions` | 校验并发布不可变定义和部署 revision |
| `GET` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}` | 读取 Graph 作者文档 |
| `POST` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/semantic-edits` | 通过 base hash/head 双 CAS 提交语义编辑 |
| `GET/PUT` | `/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/view` | 读取或 CAS 更新 ViewDocument |

GraphAuthorDocument 在发布时重新验证并编译为 Canonical Plan。ViewDocument 只保存布局，不影响已经
发布的 revision 或 pinned Run。

## Run

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/agents/{agent_id}/deployments/{deployment_revision_id}/runs` | 从指定 Deployment Revision 创建 pinned Run |
| `POST` | `/v1/agents/{agent_id}/runs` | 创建 Detached Run |
| `POST` | `/v1/agents/{agent_id}/runs/stream` | 创建 Attached SSE Run |
| `GET` | `/v1/runs/{run_id}` | 查询 Run 状态与已持久化终态；恢复/重放能力由 DTO capability 区分 |
| `DELETE` | `/v1/runs/{run_id}` | 请求取消 |
| `GET` | `/v1/runs/{run_id}/execution-graph` | 读取固定 revision 的执行图 |
| `GET` | `/v1/runs/{run_id}/trace` | 读取 trace overlay |
| `GET` | `/v1/runs/{run_id}/artifacts/{artifact_id}` | 读取经过归属、引用、大小和 retention 校验的 Artifact |
| `POST` | `/v1/runs/{run_id}/pause` | 暂停新的调度 admission |
| `POST` | `/v1/runs/{run_id}/resume` | 恢复调度 |
| `POST` | `/v1/runs/{run_id}/signals/{signal_name}` | 幂等提交 typed signal |
| `POST` | `/v1/runs/{run_id}/redrive` | 使用原 revision 重驱，可复用兼容闭合前缀 |
| `POST` | `/v1/runs/{run_id}/fork` | 从 checkpoint 分叉 Run |
| `POST` | `/v1/runs/{run_id}/migrate` | 两阶段迁移到已部署 revision |
| `POST` | `/v1/runs/{run_id}/continue-as-new` | 闭合当前 generation 并开启下一代 |

创建和控制类请求应携带稳定的 `X-Request-ID`。恢复类接口还要求
`expected_projection_version`；checkpoint hash、effect proof 和 revision/schema 兼容证据由服务端
从 durable authority 推导，客户端不能注入。

Detached 创建接口以 durable admission commit 为成功边界，成功响应统一为 `202 Accepted`。
响应中的 Run 可以仍是 `created`，也可以已经被后台 coordinator 推进为 `running` 或终态。
admission 后的调度、worker 或 terminal commit 故障不会反向改写本次 HTTP 响应；客户端必须使用
`GET /v1/runs/{run_id}` 查询最终结果。对于具备 admission 幂等契约的 Terminal-only 和
Conversation 写接口，客户端遇到传输失败或 5xx 时必须使用相同 `X-Request-ID` 重试，因为响应
生成失败前 admission 可能已经提交；服务端 replay 不得重复创建 Run 或 Conversation user
message。

`GET/DELETE /v1/runs/{run_id}` 以及 pause、resume、signal 和 recovery capability 检查对普通
Run 接受可选 `X-Tenant-ID`，未提供时使用兼容默认值 `default`。Conversation turn 创建的 Run
绑定完整 Conversation principal；客户端断线后必须继续携带同一组可信 `X-Tenant-ID` 与
`X-User-ID`。缺少 user、跨 tenant 或跨 user 查询统一返回 404。

Conversation privacy delete 删除 messages、summaries 和关联 scoped objects。Full runtime 的
workflow payload 仍属于独立 Run audit/retention authority，不作为 Conversation object 重复删除；
所有公共 payload 读取在 Conversation tombstone 后 fail closed：Run GET 只返回已清除
input/output/error 的终态，artifact、trace 与 recovery 派生面返回 not found，execution graph 只返
回不含 Run payload 的冻结 Plan。

Run admission 容量与业务状态冲突是两个不同合同：

| 条件 | HTTP | code | Header |
|---|---:|---|---|
| active Run slot 已满 | `429` | `RUN_CAPACITY_EXCEEDED` | `Retry-After: 1` |
| 请求或当前 Run 状态冲突 | `409` | `RUN_CONFLICT` | 无 |

客户端收到 429 后应使用 exponential backoff 与 jitter；不能无间隔重试。容量拒绝没有创建 Run，
也不应被统计为成功 create latency。

所有 Run DTO 显式返回 Deployment Revision 冻结的持久化语义：

```json
{
  "persistence_mode": "terminal_only",
  "recovery_capability": "none",
  "event_replay": false,
  "volatile_waits_enabled": false,
  "wait_recovery": "none"
}
```

独立的 `full` Run 对应 `recovery_capability: "full"` 和 `event_replay: true`。
Conversation-bound `full` Run 对应 `recovery_capability: "restart_only"` 和
`event_replay: true`：平台可在进程重启后恢复其原执行，但不开放会派生或改变 Conversation
lineage 的 pause、resume、signal、redrive、fork、migrate 或 continue-as-new。
`terminal_only` Run 对应 `recovery_capability: "none"` 和 `event_replay: false`，不保存恢复
checkpoint。对后两类 Run 调用上述不支持的接口都会返回 `422`，且不带 `Retry-After`；public error
code 相同，安全 message 按能力原因区分：

| Run 能力 | code | message |
|---|---|---|
| `restart_only` | `RUN_CAPABILITY_UNAVAILABLE` | `Conversation-bound full Runs do not support recovery lineage` |
| `none` | `RUN_CAPABILITY_UNAVAILABLE` | `this run does not persist recovery checkpoints` |

Agent discovery 额外返回 immutable Deployment Revision 的
`volatile_waits_enabled` 与 `wait_recovery`。若显式启用 process-local wait，
`volatile_waits_enabled` 为 `true`，但 `wait_recovery` 仍为 `none`，表示 Pod/进程退出会丢失等待。

## Attached SSE

`/runs/stream` 使用 `response-stream/v1` 用户响应协议：

1. 发送 `response.created` 和 `response.in_progress`；
2. 发送作者通过 LLM `publish: true` 授权的实时内容；
3. 发送唯一的 durable terminal snapshot；
4. terminal 后立即 EOF。

该协议 live-only：不发送 SSE `id`，不接受 `Last-Event-ID`，也不提供历史 replay。临时 delta 有界且
best-effort；发生丢失时客户端通过 `workflow.stream.gap` 和最终快照校准。Run 非终态时 Attached
连接断开会提交取消意图；需要脱离连接继续执行时，应使用 Detached Run。

`stream` 只控制 Provider 请求模式，`publish` 只控制 provisional 内容可见性。无论组合如何，最终
快照都包含强类型 workflow result、持久化 response identity 和 OpenAI 命名的 token usage。
模型工具的实时状态使用 `workflow.tool.started`、`workflow.tool.completed` 和
`workflow.tool.failed`；客户端应以 `call_id` 关联同一次调用。工具名、参数和结果分别受冻结的
Action `public_policy` 与 LLM `publish` 双重授权；内置 `tool_assistant` 示例仅公开工具名和状态，
因此 `started.arguments` 缺省且 `completed.content` 为空。只有显式授权
`arguments: all` 的工具才会另外发送标准 `response.output_item.*` function-call 与
`response.function_call_arguments.*` 事件。
LLM 基础设施失败会使用脱敏且可操作的稳定分类，不会把 Provider 响应正文、请求正文或凭据放入
终态：

| code | message |
|---|---|
| `LLM_PROVIDER_AUTHENTICATION_FAILED` | `model provider authentication failed` |
| `LLM_PROVIDER_PERMISSION_DENIED` | `model provider denied access` |
| `LLM_PROVIDER_CONNECTION_FAILED` | `failed to connect to model provider` |
| `LLM_PROVIDER_REQUEST_TIMEOUT` | `model provider request timed out` |
| `LLM_PROVIDER_REQUEST_REJECTED` | `model provider rejected the request` |
| `LLM_PROVIDER_RATE_LIMITED` | `model provider rate limit exceeded` |
| `LLM_PROVIDER_UNAVAILABLE` | `model provider is unavailable` |
| `LLM_PROVIDER_STREAM_FAILED` | `model provider stream failed` |
| `LLM_PROVIDER_RESPONSE_INVALID` | `model provider returned an invalid response` |
| `LLM_PROVIDER_RESPONSE_TOO_LARGE` | `model provider response exceeded the size limit` |
| `LLM_PROVIDER_FAILED` | `model provider request failed` |

内部日志另外记录固定字段 `provider_origin`、`model`、`request_mode`、`failure_code`，并按失败阶段
记录 `transport_kind` 或 `http_status`；这些字段仍不包含 Provider body 或凭据。
Conversation Attached turn 的 terminal frame 还必须等待 `Run result + assistant message` 同一事务
提交；token/delta 和 provider chunk 不写入 Conversation。Privacy delete 开始时服务端会取消该
Conversation 的活动 SSE dispatcher；之后不再入队 live 内容，尚未被传输层消费的有界 delta 队列
会被丢弃。已线性化为先于 delete 的 socket write 无法撤回，但 `DELETE` 成功后不会继续暴露缓冲或
新产生的私密 frame。

## Conversation

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/conversations` | 创建绑定 tenant/user/agent 的 Conversation |
| `GET` | `/v1/conversations/{conversation_id}` | 读取 metadata |
| `GET` | `/v1/conversations/{conversation_id}/messages` | cursor 分页读取消息 |
| `POST` | `/v1/conversations/{conversation_id}/messages` | 追加 user message 并创建 Detached Run |
| `POST` | `/v1/conversations/{conversation_id}/messages/stream` | 追加 user message 并创建 Attached SSE Run |
| `POST` | `/v1/conversations/{conversation_id}/archive` | 幂等归档 |
| `DELETE` | `/v1/conversations/{conversation_id}` | 执行 privacy delete |

当前 HTTP principal carrier 是必填的 `X-Tenant-ID` 与 `X-User-ID`。它们必须由可信认证层注入或
覆盖，不能直接信任公网客户端提供的值；后续 IdP principal resolver 可以替换 carrier，但不得改变
repository 的 tenant/user ownership 校验。所有 Conversation 写请求还要求非空 `X-Request-ID`。

创建 body 为 `{"agent_id":"..."}`，追加消息 body 为 `{"content":...}`，archive body 为 `{}`；
这些 body 都 strict reject 未知字段。消息列表使用
`?cursor=<opaque>&limit=<configured-max>`；默认部署为 `limit=50`、最大 200，实际边界取运行时
Conversation 配置。cursor 是无 padding 的 canonical base64url，
内部只编码 `(message_order,message_id)`，客户端不得构造 offset。页面按
`message_order DESC` 返回，并以 `next_cursor` 继续读取更旧消息。

Conversation 只返回不可变 user/assistant message；最终 assistant message 绑定唯一 `run_id`。
archive 不删除内容，`DELETE` 才进入 retention/privacy 删除语义。跨 tenant/user 读取统一表现为
`404 CONVERSATION_NOT_FOUND`，不会泄漏资源是否存在。

## 人工任务

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/v1/human-tasks?limit=100` | 列出当前身份可处理的 open/claimed 工作项 |
| `POST` | `/v1/human-tasks/{work_item_id}/claim` | 使用 `{}` 和 `X-Request-ID` 幂等抢占 |
| `POST` | `/v1/human-tasks/{work_item_id}/complete` | 使用 `{claim_fence, value}` 幂等完成 |

人工任务使用独立的 request-scoped principal。claim 返回单调递增的 `claim_fence`，complete 必须回传
该 fence；租约过期后的旧处理人不能提交。

SSE 事件类型、终态快照和错误映射由公开 DTO 与 API 合同测试共同约束。
