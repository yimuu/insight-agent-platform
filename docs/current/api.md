# HTTP 与 SSE API

状态：Current

基础路径：`/v1`

HTTP `/v1` 是服务 API 版本，与 Agent DSL 的 `insight.agent/v3` 是两个独立版本空间。除健康检查外，
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
| `GET` | `/v1/runs/{run_id}` | 查询 durable Run projection |
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

## 人工任务

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/v1/human-tasks?limit=100` | 列出当前身份可处理的 open/claimed 工作项 |
| `POST` | `/v1/human-tasks/{work_item_id}/claim` | 使用 `{}` 和 `X-Request-ID` 幂等抢占 |
| `POST` | `/v1/human-tasks/{work_item_id}/complete` | 使用 `{claim_fence, value}` 幂等完成 |

人工任务使用独立的 request-scoped principal。claim 返回单调递增的 `claim_fence`，complete 必须回传
该 fence；租约过期后的旧处理人不能提交。

完整协议合同见 [Response 实时流与 LLM 发布控制规范](specifications/2026-07-19-response-streaming-and-llm-publication-design.md)。
