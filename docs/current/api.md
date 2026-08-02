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
| `GET` | `/v1/mcp/profiles` | MCP profile 与 capability matrix |

Agent discovery 在 Deployment Revision 含 MCP binding 时返回闭合的 MCP capability summary，包括
所选协议版本、transport、principal scope、primitive 和支持的 `run-stream` 协议；不会暴露远程
tool、URI 或 Prompt 名称。

## MCP Catalog、Context 与 Interaction

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/v1/mcp/servers` | 当前 principal 可见的冻结 Server catalog |
| `GET` | `/v1/mcp/servers/{server_id}/tools` | 分页读取显式导入 Tool |
| `GET` | `/v1/mcp/servers/{server_id}/resources` | 分页读取显式导入 Resource |
| `POST` | `/v1/mcp/servers/{server_id}/resources/read` | 按 allowlist 读取 Resource 快照 |
| `GET` | `/v1/mcp/servers/{server_id}/prompts` | 分页读取显式导入 Prompt |
| `POST` | `/v1/mcp/servers/{server_id}/prompts/{prompt_name}/preview` | 获取 user-invocation Prompt 快照 |
| `POST` | `/v1/mcp/servers/{server_id}/completion` | 请求受限 Completion |
| `POST` | `/v1/mcp/servers/{server_id}/agents/{agent_id}/runs` | 固定选定 Resource/Prompt 后创建 Run |
| `GET` | `/v1/mcp/interactions` | 分页读取当前 principal 的安全 interaction 摘要 |
| `GET` | `/v1/mcp/interactions/{interaction_id}` | 读取单个安全 interaction 摘要 |
| `POST` | `/v1/mcp/interactions/{interaction_id}/respond` | 提交 form/approval 响应 |
| `POST` | `/v1/mcp/interactions/{interaction_id}/open` | 确认打开 URL interaction |
| `POST` | `/v1/mcp/interactions/{interaction_id}/decline` | 拒绝 interaction |
| `POST` | `/v1/mcp/interactions/{interaction_id}/cancel` | 取消 interaction |
| `POST` | `/v1/mcp/servers/{server_id}/authorize` | 创建 OAuth Authorization Code + PKCE 事务 |
| `GET` | `/v1/mcp/connections` | 读取当前 principal 的连接摘要 |
| `DELETE` | `/v1/mcp/connections/{server_id}` | 本地优先撤销连接 |

这些接口要求可信认证层提供 `X-Tenant-ID` / `X-User-ID`，返回
`Cache-Control: private, no-store`，且不返回 Resource/Prompt/interaction 私密正文、OAuth token、
PKCE verifier 或 requestState。Context admission 最多选择 16 个属于路径 Server 的导入项，先固定
快照并执行 Agent input schema 校验，再进入普通 Run admission。

## MCP Operator 管理 API

`/v1/admin/mcp/**` 是 installation-scoped 控制面，只接受独立 Operator Bearer token。它与普通
`ApiAuth`、tenant/user principal 和 MCP OAuth token 完全隔离。读取返回 `private, no-store`；mutation
要求 `X-Request-ID`，Draft/Server CAS mutation 还要求对应 `If-Match: "draft-N"` 或
`"server-N"`。

| 资源组 | 接口 |
|---|---|
| Server/Draft | `POST/GET /v1/admin/mcp/servers`；`GET/DELETE /servers/{id}`；`GET/PUT /servers/{id}/draft` |
| Manifest/Discovery | `/servers/{id}/manifests/**`；`/servers/{id}/discoveries/**` 及候选 `tools/resources/prompts` |
| 显式导入 | `POST /tool-import-previews`；三个 `PUT /draft/imports/{tools|resources|prompts}` |
| Validation/Revision | `POST/GET /validations/**`；`POST/GET /revisions/**` |
| 生命周期 | `PUT/DELETE /active-revision`；`POST /retirement` |

Discovery 是 durable 异步 operation，HTTP 202 后以 GET 状态为权威。publish 不隐式激活；active
Revision 才投影到用户 Catalog 和 Agent 发布 resolver。完整 endpoint、错误码、DTO 和并发语义见
[MCP 使用、运行与安全合同](mcp.md)及
[`schemas/mcp-management-v1.json`](../../schemas/mcp-management-v1.json)。机器可读的 28-operation
HTTP 合同见
[`schemas/mcp-management-v1.openapi.json`](../../schemas/mcp-management-v1.openapi.json)。

最小创建与 discovery 请求如下；token 只从本地环境读取，不能写进请求文件：

```bash
curl -sS -X POST 'https://platform.example/v1/admin/mcp/servers' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-create-engineering-001' \
  --data '{"server_id":"engineering","display_name":"Engineering MCP","draft":{"transport":{"type":"streamable_http","endpoint":"https://mcp.example/mcp"},"discovery":{"type":"live_service_account"},"authorization":{"type":"none"}}}'

curl -sS -X POST 'https://platform.example/v1/admin/mcp/servers/engineering/discoveries' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-discover-engineering-001' \
  -H 'If-Match: "draft-1"' \
  --data '{}'
```

第二个请求返回 `202`；客户端轮询响应中的 discovery ID。候选 `tools/list` 结果不会自动进入 Draft，
必须先 preview，再把逐项 `candidate_schema_hash` 和 Operator policy 完整 PUT 回 imports。

后续生命周期的最小请求形状如下。示例中的 ID、hash 和 ETag 必须取自上一步响应，不能自行猜测；
Resource/Prompt 即使为空也分别 PUT，以形成明确的逐项审阅结果：

```bash
curl -sS -X POST 'https://platform.example/v1/admin/mcp/servers/engineering/tool-import-previews' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"discovery_id":"mcpdisc_...","selection":{"mode":"all"},"alias_prefix":"engineering_"}'

curl -sS -X PUT 'https://platform.example/v1/admin/mcp/servers/engineering/draft/imports/tools' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-import-tools-001' \
  -H 'If-Match: "draft-1"' \
  --data '{"discovery_id":"mcpdisc_...","items":[{"remote":"search","candidate_schema_hash":"sha256:...","as":"engineering_search","description":{"mode":"disabled"},"effect":"mutating","idempotency":"unknown","cancellation":"not_supported","required_capabilities":[],"approval":"always","input_required":"denied","tasks":"denied","terminal_only_compatible":false,"public":{"call":false}}]}'

curl -sS -X PUT 'https://platform.example/v1/admin/mcp/servers/engineering/draft/imports/resources' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-import-resources-001' \
  -H 'If-Match: "draft-2"' \
  --data '{"discovery_id":"mcpdisc_...","items":[]}'

curl -sS -X PUT 'https://platform.example/v1/admin/mcp/servers/engineering/draft/imports/prompts' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-import-prompts-001' \
  -H 'If-Match: "draft-3"' \
  --data '{"discovery_id":"mcpdisc_...","items":[]}'

curl -sS -X POST 'https://platform.example/v1/admin/mcp/servers/engineering/validations' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-validate-001' \
  -H 'If-Match: "draft-4"' \
  --data '{"discovery_id":"mcpdisc_..."}'

curl -sS -X POST 'https://platform.example/v1/admin/mcp/servers/engineering/revisions' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-publish-001' \
  -H 'If-Match: "draft-4"' \
  --data '{"draft_version":4,"discovery_id":"mcpdisc_...","validation_id":"mcpval_..."}'

curl -sS -X PUT 'https://platform.example/v1/admin/mcp/servers/engineering/active-revision' \
  -H "Authorization: Bearer ${INSIGHT_MCP_OPERATOR_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: mcp-activate-001' \
  -H 'If-Match: "server-1"' \
  --data '{"revision_id":"mcprev_..."}'
```

## Agent 与 Provider Operator 管理 API

`/v1/admin/agents/**` 和 `/v1/admin/providers/**` 与 MCP 管理面共用 `management.version: 1` 的
Operator Bearer principal，但 capability 互不隐含。Agent 的执行链严格拆成
`Draft → Validation → Definition Revision → Deployment Resolution → Deployment Revision → Activate`；
Provider 拆成 `Draft → Discovery/Test/Validation → Provider Revision → Activate`。publish 和 deploy
都不会改变 public route，只有带 entity ETag 的 activate 才会改变 durable current pointer。

Agent 管理共 30 个操作，覆盖 YAML/Graph Draft、Graph semantic edits、独立 View CAS、revision、
resolution、deployment、rollback/deactivate、archive/restore 和 admin-only Debug Session/SSE。Provider
管理共 27 个操作，覆盖模板、显式模型 discovery/import、connection test、revision、active pointer、
suspension/resume 与 retirement。完整列表和请求合同见 [Agent 与 Provider 管理面](management.md)、
[`agent-management-v1.openapi.json`](../../schemas/agent-management-v1.openapi.json) 与
[`provider-management-v1.openapi.json`](../../schemas/provider-management-v1.openapi.json)。

Agent publish body 必须同时提交 `draft_version` 与 `validation_id`，Deployment create 必须同时提交
`definition_revision_id` 与 `resolution_id`；服务端会把它们与 ETag/durable evidence 逐项核对。Live Debug
首次返回 `428` 风险预览，只有带回同一 `risk_preview_hash` 的显式确认才返回 `202` 并创建 Session。

旧 `/v1/graph-agents/**` 已删除。Graph 和 YAML 都先编辑 Agent Draft；ViewDocument 不改变 Draft
version、`author_hash` 或执行语义。旧 public historical Deployment admission 也已删除。

## Run

| 方法 | 路径 | 用途 |
|---|---|---|
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

普通新 Run 只能通过当前 active Agent route admission。历史 Deployment 仅用于服务端恢复链和
admin Debug Session；公共客户端不能自行选择 inactive history，从而无法绕过 deactivate、rollback
或 archive。

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

`/runs/stream` 统一使用 `run-stream/v1` 用户响应协议：

1. 发送 `run.lifecycle.created` 和 `run.lifecycle.running`；
2. 发送作者通过 LLM `publish: true` 授权的实时内容；
3. 发送唯一的 durable terminal snapshot；
4. terminal 后立即 EOF。

Attached 创建成功时，HTTP 响应携带 `X-Run-ID` 和回显的 `X-Request-ID`，不再返回
`X-Response-ID`。`run_id` 是 admission、查询、取消、Artifact、trace、Conversation ownership
与 SSE 校准共享的唯一公开执行身份。

该协议 live-only：不发送 SSE `id`，不接受 `Last-Event-ID`，也不提供历史 replay。临时 delta 有界且
best-effort；发生丢失时客户端通过 `run.stream.gap` 和最终快照校准。Run 非终态时 Attached
连接断开会提交取消意图；需要脱离连接继续执行时，应使用 Detached Run。
`run.lifecycle.running` 只由执行 authority 的真实 started 信号产生；幂等请求若已存在 terminal
snapshot，可以直接发送 `created → terminal`。致命传输/投影故障使用 `run.stream.error`，其
`code` 固定以 `RUN_STREAM_` 开头；该事件结束当前流但不是 Run 终态，客户端应使用 `X-Run-ID`
查询权威状态。

`stream` 只控制 Provider 请求模式，`publish` 只控制 provisional 内容可见性。无论组合如何，最终
快照都是单一、按终态闭合的 `run` 对象，包含强类型 `result`、公开输出、工具结果、检索结果以及
OpenAI 命名的 token usage。Run terminal path 在同一 durable transaction 内将尚未闭合的
interaction 以 first-winner 转为 `run_terminal`，冻结全部安全 `interactions[]` 摘要，并把该
数组写入 canonical `run_payload`；`snapshot_hash` 覆盖包含该数组的完整 payload。一个
Run 最多冻结 1024 个 interaction 摘要，超过上限必须 fail closed，不得静默截断。
完整闭合定义见
[`schemas/run-stream-v1.json`](../../schemas/run-stream-v1.json)。

`run-stream/v1` 直接包含 body-free 的 `run.interaction.required` /
`run.interaction.closed`，并在 terminal snapshot 中加入安全 `interactions[]`。该公开
协议是 27 个事件的单一闭合合同，客户端不需要发送版本协商 Header，平台也不公开
其他 run-stream 协议身份。live `required` / `closed` 帧只是及时通知，可在断线或
best-effort 投影中丢失；恢复、重连和终态校准必须以 durable terminal snapshot 为权威。
完整定义见
[`schemas/run-stream-v1.json`](../../schemas/run-stream-v1.json)，全事件样本见
[`schemas/run-stream-v1.samples.json`](../../schemas/run-stream-v1.samples.json)。MCP Catalog、OAuth、
Interaction 与 `/mcp` wire 详见 [MCP 使用、运行与安全合同](mcp.md)。
模型工具意图和真实执行是两个公开面：

- `run.output.item.*` 与 `run.output.function_call.arguments.*` 表示模型正在形成或已经形成调用
  意图，不证明 Action 已执行；
- `run.tool.started/progress/completed/failed` 表示 runtime 中的真实 Action 生命周期；
- 两组事件都使用模型生成并经 runtime 验证的 `call_id` 关联，不能按工具名、数组位置或事件相邻性
  关联。

工具名、参数、进度和结果分别受冻结的 Action `public_policy` 与 LLM `publish` 双重授权。
`progress` 还必须通过独立的闭合 JSON Schema，只允许 `output_text` 或 `output_json` content。
`run.tool.completed` 与 `run.tool.failed` 的 `duration_ms` 从 logical tool call 第一次进入
Action execution boundary 开始计算，并覆盖 retry/backoff。可重试 Attempt 的失败不会产生
`run.tool.failed`。

成功且允许公开调用元数据的工具总会产生 `completed`。`content: []` 表示“执行成功，但结果正文
未获授权公开”，不表示 Action 返回了 `null` 或空值。最终 `run.tool_results` 同样保留这类
status-only 成功项，用于校准 live 事件丢失后的 UI。

只有 `arguments: all` 才会发送标准 function-call item/argument 事件。进度是 live-only、
best-effort 观测：它不进入 durable execution history、Conversation message、GET Run 或 terminal
snapshot；队列满、订阅者断开、超频或 late publisher 会丢弃进度，但不能改变 Action 结果。

### 工具调用 SSE 示例

默认配置中的 `progress_tool_assistant` 使用一个显式公开安全参数、进度和结果的
`progress_counter` 示例 Action：

```bash
curl -N \
  -X POST 'http://127.0.0.1:3000/v1/agents/progress_tool_assistant/runs/stream' \
  -H 'accept: text/event-stream' \
  -H 'content-type: application/json' \
  -H 'x-request-id: progress-example-1' \
  -d '{"request":"演示工具执行进度"}'
```

以下省略与重点无关的 content-part 帧和部分字段，但每个 `data` 都是实际 wire shape 的完整 JSON
对象；服务端不会保证示例中的具体 ID 或序号：

```text
event: run.output.item.added
data: {"type":"run.output.item.added","sequence_number":2,"output_index":0,"item":{"type":"function_call","id":"item_fc_1","status":"in_progress","call_id":"call_1","name":"progress_counter","arguments":""}}

event: run.output.function_call.arguments.delta
data: {"type":"run.output.function_call.arguments.delta","sequence_number":3,"item_id":"item_fc_1","output_index":0,"delta":"{\"total\":10}"}

event: run.output.function_call.arguments.done
data: {"type":"run.output.function_call.arguments.done","sequence_number":4,"item_id":"item_fc_1","output_index":0,"name":"progress_counter","arguments":"{\"total\":10}"}

event: run.tool.started
data: {"type":"run.tool.started","sequence_number":6,"call_id":"call_1","tool_name":"progress_counter","arguments":{"total":10}}

event: run.tool.progress
data: {"type":"run.tool.progress","sequence_number":7,"call_id":"call_1","tool_name":"progress_counter","content":[{"type":"output_json","json":{"completed":5,"total":10}}]}

event: run.tool.progress
data: {"type":"run.tool.progress","sequence_number":8,"call_id":"call_1","tool_name":"progress_counter","content":[{"type":"output_json","json":{"completed":10,"total":10}}]}

event: run.tool.completed
data: {"type":"run.tool.completed","sequence_number":9,"call_id":"call_1","tool_name":"progress_counter","duration_ms":12,"content":[{"type":"output_json","json":{"completed":10,"total":10}}]}

event: run.output.text.delta
data: {"type":"run.output.text.delta","sequence_number":12,"item_id":"item_msg_1","output_index":1,"content_index":0,"delta":"工具进度已完成：10/10。"}

event: run.lifecycle.completed
data: {"type":"run.lifecycle.completed","sequence_number":16,"run":{"id":"run_1","object":"run","status":"completed","output":[{"type":"function_call","id":"item_fc_1","status":"completed","call_id":"call_1","name":"progress_counter","arguments":"{\"total\":10}"},{"type":"message","id":"item_msg_1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"工具进度已完成：10/10。","annotations":[]}]}],"result":"工具进度已完成：10/10。","tool_results":[{"call_id":"call_1","tool_name":"progress_counter","content":[{"type":"output_json","json":{"completed":10,"total":10}}]}],"retrievals":[],"interactions":[],"usage":null,"usage_status":"partial"}}
```

客户端 reducer 应按以下规则处理：

1. 以 `call_id` 为 key；`progress` 先于 `started` 时惰性创建 running 卡片；
2. 同一 `call_id` 的重复 `started` 表示 retry，更新原卡片而不是新增卡片；
3. 不同 `call_id` 可以交错，只按 SSE `sequence_number` 消费；
4. 收到 `completed/failed` 后忽略该调用的 late provisional 事件；
5. `run.lifecycle.completed` 用 `run.tool_results` 把仍为 running 的成功项校准为 completed；
6. Run failed/cancelled/timed_out/interrupted 时只关闭残留 running 卡片，不伪造具体工具错误。

内置 `tool_assistant` 中 `current_time`、`text_metrics` 和 `integer_calculator` 公开闭合的安全结果；
`text_replace` 只公开调用成功状态，结果正文继续保持私有。

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
记录 `transport_kind` 或 `http_status`；本地结构化响应失败另以
`llm.response_validation_failed` 的 `validation_stage=json_parse|schema_validation` 区分。上述字段
仍不包含 Provider body、模型响应正文或凭据。
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
