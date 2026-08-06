# Agent 调用、Conversation 与 S3 文件合同规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Qualified |
| 日期 | 2026-08-05 |
| 变更类型 | Breaking HTTP Contract / DSL Input Surface / Conversation / File Service / Object Storage |
| 影响范围 | Public Agent discovery、Run admission、Conversation、DSL `inputs`、LLM 多模态消息、Artifact/File 存储、配置、数据库、SDK 与测试 |

> 本文的代码、schema、配置、Helm 与当前文档已经交付，并于 2026-08-06 通过
> [真实 RustFS/S3 资格验收](../qualifications/2026-08-06-agent-invocation-rustfs-s3-qualification.md)。发生冲突时，
> 当前 schema、实现、conformance tests 和 [`docs/current`](../../current/README.md) 优先。

## 1. 决策摘要

平台采用一个稳定的调用信封，并把对话语义、业务输入和文件资源明确分开：

```json
{
  "query": "比较这两张图片",
  "messages": [],
  "files": [
    {"file_id": "file_01K..."},
    {"file_id": "file_01M..."}
  ],
  "inputs": {
    "response_style": "concise"
  }
}
```

本规范采用以下决定：

1. `query` 表示本次用户文本，不再要求调用方构造 `message.content[0].text`；
2. `messages` 只表示本次请求之前的历史消息，因此使用复数；
3. `files` 只提交平台签发的 `file_id`，允许一次提交多个文件；
4. `inputs` 保存 Agent 自己的业务字段，不改名为含义模糊的 `payload`；
5. 请求正文是 `AgentInvocation`，不是直接的 Agent 运行时输入；平台验证、解析文件并规范化后形成
   `AgentInput`；
6. Agent 作者不配置 JSONPath、JSON Pointer 或变量 source mapping。DSL 中同名输入自动接收
   `query`、`messages`、`files` 和 `inputs` 内的业务字段；
7. 无会话调用继续使用 `/v1/agents/{agent_id}/runs`；Conversation 是可选能力，不是 Run 的必需
   容器；
8. Conversation turn 继续由 `/v1/conversations/{conversation_id}/messages` 创建，不新增
   `/turns` 资源；`turn` 只是“一条 user message + 一个 Run + 可选 assistant message”的内部聚合概念；
9. 无会话 Run 可以由调用方提供 `messages`；Conversation turn 的历史由平台托管，调用方不得覆盖；
10. 用户确认、表单提交、审批和其他协议级事件继续使用各自的 typed endpoint，不伪装成 message；
11. `Idempotency-Key` 与 `X-Request-ID` 分工：前者负责可选的业务幂等，后者只负责链路追踪；
12. 文件内容永远通过 S3 协议访问，首个部署实现使用 RustFS；应用使用 `aws-sdk-s3`，不依赖
    RustFS 专用 SDK，也不增加可配置的本地文件存储后端；
13. 平台外部只暴露 `file_id`；bucket、object key、ETag、预签名 URL 和存储凭据都属于内部实现；
14. 本地文件表示“调用方本机上的待上传文件”，不是一种服务端存储 backend。它通过上传 API 或
    预签名 URL 进入 RustFS 后才能用于 Run。

## 2. 目标与非目标

### 2.1 目标

1. 为聊天型和非聊天型 Agent 提供同一个可发现、可严格校验的调用信封；
2. 让简单文本调用保持简单，同时支持历史消息、多个文件和任意结构化业务输入；
3. 让 Agent DSL 直接使用 `$query`、`$messages`、`$files` 和业务字段，不引入变量映射层；
4. 明确无会话 Run 与平台托管 Conversation 的历史权威边界；
5. 建立完整的文件创建、直传、完成确认、归属校验、Run 绑定、引用保留和删除合同；
6. 使用一个 S3 client 与统一 object-key namespace 承载上传文件、Run Artifact 和超限
   Conversation 内容；
7. 保持 Attached/Detached Run、durable terminal、SSE live-only 和 Conversation 原子事务语义；
8. 允许未来替换任意 S3-compatible 服务时只改变 endpoint、credential 和部署配置，不改变业务 API；
9. 建立 S3 兼容性、故障、隐私、容量和生命周期验收门槛。

### 2.2 非目标

- 不提供服务端任意本地路径读取，例如 `{"path":"/tmp/a.png"}`；
- 不提供 `local_filesystem`、`shared_filesystem` 与 S3 的长期双 backend 产品合同；
- 不向客户端暴露 RustFS admin API、access key、secret key、bucket 或 object key；
- 不允许客户端在 Conversation turn 中替换、删除、重排或注入历史消息；
- 不把 system、developer、tool 历史开放给普通调用方；
- 不把确认、审批、表单、signal 或 tool result 编码为特殊 `message.kind`；
- 不引入 `/turns` 路由、`message.kind`、`content.kind`、`data_schema` 或变量 source mapping；
- 不承诺兼容 AWS S3 的全部 API，只对平台实际使用的闭合操作集负责；
- 第一阶段不实现音频、视频或任意文档直接送入模型；文件服务可以保存它们，但 Provider admission
  必须按已冻结 capability 决定能否直接消费；
- 不允许客户端用 URL 替代已登记的 `file_id` 绕过归属、大小、媒体类型和生命周期校验。

## 3. 术语与权威边界

### 3.1 `AgentInvocation`

公共 HTTP 请求正文。它拥有稳定的平台字段 `query`、`messages`、`files` 和 `inputs`，不是 Agent
工作流直接执行的最终 JSON 值。

### 3.2 `AgentInput`

平台完成请求校验、默认值物化、文件解析和 Conversation 历史选择后生成的内部输入。它与不可变
Deployment Revision 的编译输入 schema 精确匹配，是 Run input hash 和执行的权威值。

### 3.3 `Message`

一条不可变的 `user` 或 `assistant` 消息。Message 表示对话内容，不表示审批、表单、signal 或其他
协议事件。消息 content 是有序 part 列表，可以包含文本和平台文件引用。

### 3.4 `FileRef` 与 `File`

- `FileRef` 是公共请求中的最小引用：`{"file_id":"..."}`；
- `File` 是解析归属和状态后的平台内建值，包含安全元数据，但不含 bucket、object key、凭据或可长期
  复用的下载 URL；
- object locator 只存在于 File repository 与 S3 adapter 内部。

### 3.5 Conversation 历史权威

Conversation repository 是 Conversation 历史的唯一权威。Agent 配置决定如何使用传入的
`$messages`，但不能自行修改 repository 历史；客户端通过消息接口追加本次 user message，而不是提交
完整 Conversation 快照。

## 4. 公共调用信封

### 4.1 严格 wire shape

`AgentInvocation` 是封闭对象：

```text
AgentInvocation {
  query?: string,
  messages?: Message[],
  files?: FileRef[],
  inputs?: object
}
```

通用规则：

- 未提供 `messages`、`files` 或 `inputs` 时分别物化为 `[]`、`[]` 和 `{}`；
- `query` 是否必填由 Agent 的 DSL 输入声明决定；提供时必须是有效 UTF-8 JSON string；
- `messages` 保持调用方顺序，平台不自动重排、合并或补写 role；
- `files` 保持调用方顺序，重复 `file_id` 拒绝，不静默去重；
- `inputs` 必须是对象；未知业务字段由 Agent schema 拒绝；
- 顶层未知字段一律拒绝；
- `message`、`content`、`input`、`payload` 和 `response_mode` 都不是此信封的 alias；
- Attached 或 Detached 由路由决定，不在 body 中再传 `response_mode`。

### 4.2 纯业务 Agent

不需要消息或 Conversation 的 Agent 只发送业务输入：

```http
POST /v1/agents/text_metrics/runs
Content-Type: application/json

{
  "inputs": {
    "text": "hello world"
  }
}
```

这类 Agent 不声明 `query` 或 `messages`，因此提交这些字段会在 admission 前失败。

### 4.3 无会话文本调用

```http
POST /v1/agents/assistant/runs/stream
Content-Type: application/json

{
  "query": "什么是对象存储？"
}
```

### 4.4 调用方控制历史的无会话 Run

```json
{
  "query": "再简短一点",
  "messages": [
    {
      "role": "user",
      "content": [{"text": "介绍一下对象存储"}]
    },
    {
      "role": "assistant",
      "content": [{"text": "对象存储通过 key 管理非结构化数据。"}]
    }
  ]
}
```

`messages` 只允许 `user` 和 `assistant`。system instruction 始终由冻结的 Agent Definition 提供。

### 4.5 多文件调用

```json
{
  "query": "比较这两张图片中的差异",
  "files": [
    {"file_id": "file_01K..."},
    {"file_id": "file_01M..."}
  ],
  "inputs": {
    "response_style": "concise"
  }
}
```

请求不提交 object URL。平台在 admission 时解析两个不可变文件版本并绑定到 Run。

## 5. DSL 输入映射与发现合同

### 5.1 按名称自动映射

Agent 继续只声明 `inputs`，不新增 `entrypoints.variables`：

```yaml
inputs:
  query: string

  messages:
    type: Message[]
    default: []

  files:
    type: File[]
    default: []
    max_items: 10

  response_style:
    type: string
    enum: [concise, detailed]
    default: concise
```

工作流直接引用：

```yaml
messages:
  - role: system
    content:
      - text: system
  - $messages
  - role: user
    content:
      - text: $query
      - attachments: $files
```

映射规则：

| DSL 输入名 | 公共来源 | 规范化后的值 |
|---|---|---|
| `query` | 请求顶层 `query` | string |
| `messages` | 请求顶层 `messages` 或 Conversation repository | `Message[]` |
| `files` | 请求顶层 `files` | 已解析、已绑定的 `File[]` |
| 其他输入名 | 请求 `inputs` 中的同名字段 | Agent schema 验证后的业务值 |

### 5.2 保留输入名

`query`、`messages` 和 `files` 是保留输入名：

- `query` 只能声明为 `string`，可以使用现有 string constraints、`optional` 或 `default`；
- `messages` 只能声明为 `Message[]`；
- `files` 只能声明为 `File[]`；
- 声明了某个保留输入，公共请求才允许对应顶层字段；
- 未声明的保留输入不会进入工作流作用域；
- 其他业务输入不能通过 `inputs.query`、`inputs.messages` 或 `inputs.files` 绕过保留语义。

编译器仍生成一个规范化 `AgentInput` schema。HTTP 层另由同一声明派生 exact
`invocation_schema`，不得手写第二份 schema。

### 5.3 Conversation eligibility

Agent 只有同时满足以下条件才可以创建 Conversation：

1. 声明必需或非 null 的 `query: string`；
2. 声明 `messages: Message[]`，通常默认 `[]`；
3. 当前 Deployment Revision 支持已有 Conversation persistence policy；
4. 若声明 `files`，其约束与文件服务、Provider capability 和 Conversation retention 相容。

不满足条件时，`POST /v1/conversations` 返回 `422 AGENT_CONVERSATION_UNSUPPORTED`。这让普通 task
Agent 无须声明“关闭会话”的额外配置。

### 5.4 Agent discovery

Agent list/detail 在目标合同中返回以下结构；示例为便于阅读省略了 `Message`、`File` 和
`BusinessInputs` 的完整 `$defs`：

```json
{
  "id": "assistant",
  "version": "sha256:...",
  "input_schema": {
    "type": "object",
    "required": ["query", "messages", "files", "response_style"],
    "additionalProperties": false,
    "properties": {
      "query": {"type": "string"},
      "messages": {"type": "array"},
      "files": {"type": "array", "maxItems": 10},
      "response_style": {
        "type": "string",
        "enum": ["concise", "detailed"]
      }
    }
  },
  "invocation_schema": {
    "type": "object",
    "required": ["query"],
    "additionalProperties": false,
    "properties": {
      "query": {"type": "string"},
      "messages": {"type": "array"},
      "files": {"type": "array", "maxItems": 10},
      "inputs": {"$ref": "#/$defs/BusinessInputs"}
    }
  },
  "conversation_message_schema": {
    "type": "object",
    "required": ["query"],
    "additionalProperties": false,
    "properties": {
      "query": {"type": "string"},
      "files": {"type": "array", "maxItems": 10},
      "inputs": {"$ref": "#/$defs/BusinessInputs"}
    }
  },
  "capabilities": {
    "conversations": true,
    "client_supplied_history": true,
    "files": true
  }
}
```

- `input_schema` 延续现有含义，精确描述规范化后的完整 `AgentInput`；
- `invocation_schema` 精确描述 Run HTTP body；
- `conversation_message_schema` 只在 Agent 支持 Conversation 时返回，精确描述 Conversation turn body；
- `invocation_schema.$defs.BusinessInputs` 是从 `input_schema` 中排除三个保留输入后派生的业务对象；
- 三份 schema 都从同一编译类型合同派生；作者声明进入 Agent version hash，派生结果不得另行配置；
- schema annotations 仍是公开信息，不能包含 secret。

## 6. Run 与 Conversation 路由

### 6.1 无会话 Run

保留现有路由：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/agents/{agent_id}/runs` | 创建 Detached Run |
| `POST` | `/v1/agents/{agent_id}/runs/stream` | 创建 Attached SSE Run |
| `GET` | `/v1/runs/{run_id}` | 查询 durable 状态与终态 |
| `DELETE` | `/v1/runs/{run_id}` | 请求取消 |

两个 POST 的 body 都从当前“直接 AgentInput”clean-cut 为 `AgentInvocation`。Detached/Attached 的持久化、
Run ID、SSE `run-stream/v1`、terminal calibration 和断线语义保持不变。

### 6.2 Conversation

保留现有路由：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/conversations` | 创建绑定 tenant/user/agent 的 Conversation |
| `GET` | `/v1/conversations/{conversation_id}` | 读取 metadata |
| `GET` | `/v1/conversations/{conversation_id}/messages` | cursor 分页读取不可变消息 |
| `POST` | `/v1/conversations/{conversation_id}/messages` | 追加 user message 并创建 Detached Run |
| `POST` | `/v1/conversations/{conversation_id}/messages/stream` | 追加 user message 并创建 Attached SSE Run |
| `POST` | `/v1/conversations/{conversation_id}/archive` | 幂等归档 |
| `DELETE` | `/v1/conversations/{conversation_id}` | privacy delete |

不增加 `/turns`，原因是客户端执行的资源动作是“发送消息”；Run 已有独立资源和 `run_id`，再暴露 Turn
会产生第三个重叠身份。响应可以继续使用 `ConversationTurnDto` 作为聚合 DTO，但 `turn_id` 不成为公共
持久化身份。

### 6.3 创建 Conversation

```http
POST /v1/conversations
Content-Type: application/json

{
  "agent_id": "assistant"
}
```

Conversation 只冻结 Agent/Deployment、principal 和历史权威，不保存一份隐式可覆盖的业务
`context`。每个 turn 所需的业务字段明确放在当次 `inputs` 中；需要长期业务状态的产品应使用独立
domain resource，再把其 ID 作为普通 Agent input 传入。未来若增加 Conversation metadata，必须形成
独立规范，不能与消息历史或 AgentInput 暗中合并。

### 6.4 创建 Conversation message

```http
POST /v1/conversations/conv_01.../messages/stream
Idempotency-Key: message-002
X-Request-ID: req-7e23
Content-Type: application/json

{
  "query": "再解释得简单一点",
  "files": [
    {"file_id": "file_01K..."}
  ],
  "inputs": {
    "response_style": "concise"
  }
}
```

Conversation turn body 禁止 `messages`。admission 顺序为：

1. 解析 principal、Conversation 和冻结的 Deployment Revision；
2. 验证该 Agent 的 Conversation invocation schema；
3. 解析并 pin `files`；
4. 从 Conversation repository 选择 summary + 最近消息；
5. 生成 canonical user Message；
6. 规范化 `AgentInput = query + selected messages + resolved files + business inputs`；
7. 对完整规范化值计算 input hash；
8. 在既有原子事务中提交 user message、file bindings 与 Run admission；
9. terminal 时在既有原子事务中提交 Run result 与最终 assistant message。

业务 `inputs` 属于该 Run 的执行输入，不自动写入 Conversation message，也不会在后续 turn 自动继承。

### 6.5 Canonical Conversation message

用户请求在 repository 中保存为：

```json
{
  "message_id": "msg_01...",
  "role": "user",
  "content": [
    {"text": "再解释得简单一点"},
    {"file": {"file_id": "file_01K..."}}
  ],
  "run_id": "run_01..."
}
```

规则：

- 不保存 `kind: question`，因为 role + content 已足以表达普通对话；
- 文本 part 在文件 part 之前，多个文件保持请求顺序；
- assistant message 仍只保存最终公开内容，不保存逐 token delta；
- file part 只保存 `file_id` 和必要的冻结安全展示元数据，不保存预签名 URL；
- `inputs` 不伪装成 content part；
- 用户确认、表单提交等仍通过对应 interaction/signal endpoint 写入其自己的 durable authority。

## 7. 历史消息控制

| 调用方式 | `messages` 来源 | 调用方能否提交 |
|---|---|---|
| 无会话 `/runs` | 请求 body | 可以，前提是 Agent 声明 `messages` |
| Conversation `/messages` | Conversation repository | 不可以 |
| Agent system instruction | 冻结 Agent Definition | 永远不可以 |
| Tool/interaction continuation | Runtime durable state | 永远不可以 |

无会话历史的限制：

- 只允许 `user`、`assistant`；
- content 只允许平台公开的安全 part；
- file part 必须重新执行 file ownership、ready、retention 和 size 校验；
- 执行 message count、总字节和 token budget；超过限制明确失败，不静默截断；
- 字符串中的模板标记按普通文本处理，不进行二次模板渲染；
- 客户端承担历史完整性，平台不保证它与任何先前 Run 一致。

Conversation 历史继续使用服务端 summary + contiguous recent suffix 策略。客户端若需要完全控制上下文，
应选择无会话 Run，而不是试图覆盖 Conversation repository。

## 8. Idempotency 与请求追踪

### 8.1 Header 分工

| Header | 是否必填 | 作用 |
|---|---|---|
| `Idempotency-Key` | 否，强烈建议用于所有创建/追加操作 | 对网络重试去重 |
| `X-Request-ID` | 否 | trace、日志关联和响应回显 |
| `X-Tenant-ID` / `X-User-ID` | 沿用当前可信 principal carrier 合同 | ownership 与隔离 |

不提供 `X-Request-ID` 时平台生成一个并在响应回显。它不参与资源唯一性，也不能替代
`Idempotency-Key`。

不提供 `Idempotency-Key` 时服务端仍可以正常创建 Run、Conversation、message 或 file；但调用方在
未知结果的网络失败后重试，可能创建第二个资源。这是明确的客户端可靠性选择，不是服务端实现失败。

### 8.2 幂等 scope

幂等记录按以下 tuple 隔离：

```text
(tenant_id, user_id, HTTP method, canonical route identity, idempotency_key)
```

- 同 key、同 canonical request hash 返回第一次的资源与 `replayed: true`；
- 同 key、不同 request hash 返回 `409 IDEMPOTENCY_KEY_REUSED`；
- hash 包含 `query`、有序 history、file IDs、business inputs canonical JSON 和冻结的 Agent route；
- 不包含 `X-Request-ID`、预签名 URL、临时 S3 token 或服务端生成时间；
- 幂等记录 retention 不得短于对应资源的 admission retry window；
- Attached replay 仍是 live-only：不补发已经丢失的 provisional delta，只用当前 live tail 和 durable
  terminal snapshot 校准。

File create 的幂等 replay 返回同一个 `file_id`；若文件仍是 `pending_upload` 且旧预签名 URL 已过期，
响应可以为同一 object key 签发新的短期上传 URL。临时 capability 的变化不等于创建第二个 File。

迁移时停止把 `X-Request-ID` 同时用作幂等键。旧客户端必须显式迁移到 `Idempotency-Key`；不提供
header alias 或双重优先级。

## 9. File Service

### 9.1 File metadata

平台数据库保存：

```text
files
  file_id
  tenant_id
  user_id
  filename
  media_type
  expected_size_bytes
  actual_size_bytes
  checksum_sha256?
  object_key
  object_etag
  object_version_id?
  status
  created_at
  ready_at?
  deleted_at?
```

公共 `File` DTO 只返回：

```json
{
  "file_id": "file_01K...",
  "filename": "image-a.png",
  "media_type": "image/png",
  "size_bytes": 182736,
  "status": "ready",
  "created_at": "2026-08-05T12:00:00Z"
}
```

状态机：

```text
pending_upload -> ready
pending_upload -> expired
pending_upload -> failed
ready -> deleting -> deleted
```

`ready` 后文件内容不可覆盖；新内容必须创建新 `file_id` 和新 object key。

### 9.2 创建上传

```http
POST /v1/files
Idempotency-Key: upload-image-a
Content-Type: application/json

{
  "filename": "image-a.png",
  "media_type": "image/png",
  "size_bytes": 182736,
  "sha256": "optional-lowercase-hex"
}
```

返回 `201 Created`：

```json
{
  "code": "OK",
  "message": "ok",
  "data": {
    "file": {
      "file_id": "file_01K...",
      "filename": "image-a.png",
      "media_type": "image/png",
      "size_bytes": 182736,
      "status": "pending_upload"
    },
    "upload": {
      "method": "PUT",
      "url": "https://rustfs.example/...signed...",
      "headers": {
        "Content-Type": "image/png"
      },
      "expires_at": "2026-08-05T12:15:00Z"
    },
    "replayed": false
  }
}
```

预签名 URL 是短期 capability：

- 只允许 PUT 到本次生成的唯一 object key；
- 固定 content length、content type 和支持时的 checksum 条件；
- 不能列 bucket、读取其他对象或覆盖 ready object；
- 不写入日志、trace、Conversation message 或 durable Run payload；
- API 响应使用 `Cache-Control: private, no-store`。

### 9.3 完成上传

客户端 PUT 成功后调用：

```http
POST /v1/files/file_01K.../complete
Content-Type: application/json

{}
```

平台使用 S3 `HeadObject` 验证 object 存在、size、ETag、必要 metadata 和可用 checksum。全部通过才把
状态从 `pending_upload` CAS 到 `ready`。完成接口幂等：已 ready 且 object identity 相同则返回同一
File；不一致返回失败并禁止用于 Run。

目标公开路由：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/files` | 创建 pending file 和预签名上传 |
| `POST` | `/v1/files/{file_id}/complete` | HEAD 校验并转为 ready |
| `GET` | `/v1/files/{file_id}` | 读取安全 metadata |
| `POST` | `/v1/files/{file_id}/download-url` | 生成短期只读预签名 URL |
| `DELETE` | `/v1/files/{file_id}` | 请求删除 |

`download-url` 只接受 `{}`，每次生成新的短期 capability，要求相同 principal ownership，并返回
`Cache-Control: private, no-store`。它不改变 File 状态，也不成为 Run/Conversation 的持久化输入。

第一阶段不提供服务端 multipart proxy。浏览器、CLI 和桌面客户端都读取本地文件并 PUT 到预签名 URL；
因此“支持本地文件”不需要服务端本地存储 backend。超过单 PUT 阈值后可以增加 S3 Multipart
upload-session API，但必须形成闭合的 part、complete、abort 和 orphan cleanup 合同。

### 9.4 Run admission 与不可变绑定

提交 `file_id` 时平台必须在创建 Run 前验证：

1. tenant/user ownership 匹配；
2. status 为 `ready`；
3. object identity 与完成时冻结的 ETag/version 一致；
4. media type、单文件大小、文件数和总大小满足 Agent 与平台限制；
5. Agent 声明 `files: File[]`；
6. 使用位置需要的 Provider/Action capability 已在 Deployment Revision 冻结。

admission transaction 创建 `run_file_bindings`。Conversation user message 额外创建
`conversation_file_bindings`。绑定只保存 file identity 和冻结 display metadata；运行时通过 repository
解析 object locator。

文件 DELETE 先 tombstone 用户可见资源。仍被未到期 Run、Conversation 或 Artifact 引用时，不立即
物理删除对象；引用释放后由 GC 使用条件删除。正在执行的 Run 不因并发 DELETE 失去输入。

## 10. File 到 LLM 多模态输入

### 10.1 平台内建 `File`

Agent 不自定义 File schema。安全运行时形状为：

```json
{
  "file_id": "file_01K...",
  "filename": "image-a.png",
  "media_type": "image/png",
  "size_bytes": 182736
}
```

object key、bucket、ETag 和 URL 不进入模板作用域，防止 prompt、日志或 Agent output 泄漏存储结构。

### 10.2 作者语法

LLM user message content 增加一个列表展开 part：

```yaml
messages:
  - role: user
    content:
      - text: $query
      - attachments: $files
```

规则：

- `attachments` 只能出现在 `user` message；
- 值必须静态为 `File[]`；
- 按数组顺序展开为零个或多个 Provider-neutral input part；
- 空数组展开为零个 part；若最终 content 为空，编译或运行失败；
- Agent 不能把任意 string、URL 或业务 object 当作 attachment；
- `Message` 的用户历史 file part 在规范化后也转换成同一 File 绑定，不绕过 admission 校验。

### 10.3 Provider delivery

第一阶段直接进入 LLM 的 attachment 支持图片媒体类型。Provider adapter 根据冻结配置使用以下一种
明确模式，不能在运行时静默猜测：

```text
inline_data
  平台通过 S3 GetObject 读取受限 bytes，编码为 Provider 支持的 inline image

presigned_url
  平台生成短期只读 URL；只允许配置在 Provider 网络能够访问对象 endpoint 时
```

Provider Revision 必须冻结 delivery mode、支持 media types、单文件 byte limit、总 byte limit 和
最大图片数。`inline_data` 不暴露 RustFS 网络；`presigned_url` 不暴露 S3 credential，但 URL 本身是
短期 secret，禁止日志记录。非图片文件只有在对应 adapter 明确实现并冻结 capability 后才能直接送入
模型，否则返回 `FILE_PROVIDER_UNSUPPORTED`；它们仍可由 Action 或 Retrieval 通过授权的文件读取能力
消费。

平台不把 `file_id` 原样发送给模型并期待模型自行访问 RustFS。`file_id` 只在平台内解析。

## 11. S3 与 RustFS 存储合同

### 11.1 依赖和命名

Rust 实现统一使用：

```text
aws-sdk-s3 -> S3 protocol -> RustFS
```

核心具体实现命名为 `S3Storage` 或 `S3ArtifactStore`，不命名为 `RustFsStorage`。不增加新的通用
`ObjectStore` backend selector；现有 engine/domain port 可以保留以维持 crate 边界，但 composition
只装配 S3 实现。测试 fake 不是公开 backend。

### 11.2 配置

目标配置：

```yaml
object_storage:
  # 只有 Provider 网络能访问 public_endpoint 时才显式选择 presigned_url。
  llm_attachment_delivery: inline_data
  s3:
    endpoint: http://rustfs:9000
    public_endpoint: https://files.example.com
    region: us-east-1
    bucket: insight-agent-platform
    force_path_style: true
    access_key_env: S3_ACCESS_KEY
    secret_key_env: S3_SECRET_KEY
    connect_timeout: 5s
    request_timeout: 30s
    presign_upload_ttl: 15m
    presign_download_ttl: 5m
    allow_insecure_internal_endpoint: true

  limits:
    max_file_bytes: 26214400
    max_files_per_invocation: 10
    max_total_file_bytes_per_invocation: 52428800
    pending_upload_ttl: 1h

  gc:
    interval: 1m
    deletion_claim_seconds: 60
```

要求：

- strict reject 未知字段；
- credential 只通过 secret environment/reference 注入；
- `endpoint` 是平台服务端访问 RustFS 的内部地址；`public_endpoint` 是客户端执行预签名 PUT/GET 时
  访问的地址，必须路由到同一 S3 服务并参与签名；不配置时默认与 `endpoint` 相同；
- 开启浏览器直传时，RustFS 或其前置代理必须只对受信任 Origin 开放所需 PUT/GET/HEAD 和签名 headers
  的 CORS，不使用 `*` 暴露 credentialed request；
- production endpoint 必须使用 TLS，除非 endpoint 是同一受信任网络内的明确例外并通过 deployment
  policy；
- bucket 必须在启动前创建，应用默认不持有创建/删除 bucket 权限；
- readiness 至少执行凭据加载和受限 `HeadBucket`/等价探测；
- 不把 endpoint 中的 userinfo、签名 query 或 credential 写入错误和日志；
- path-style 明确配置，不能依赖 SDK/endpoint 猜测。

### 11.3 Object key namespace

object key 由服务端生成，不包含原始 filename：

```text
files/{tenant_hash}/{file_id}/content
run-artifacts/{tenant_hash}/{artifact_id}/content
conversation-content/{tenant_hash}/{content_id}/content
```

key 使用不可逆 tenant namespace 或安全内部 ID，避免泄漏 user input 和路径穿越。客户端 filename 只作为
metadata 保存和展示。

### 11.4 闭合 S3 操作集

平台只承诺并验收：

- `PutObject`（通过预签名 URL）；
- `HeadObject`；
- `GetObject` 与 bounded/range read；
- `DeleteObject` 与条件/identity 校验；
- presigned PUT/GET；
- 后续大文件阶段的 Create/UploadPart/Complete/Abort Multipart Upload；
- 必要的 metadata、content type、content length、checksum 和 ETag 行为。

不根据产品宣传推断兼容性；每次 RustFS 升级都运行真实 S3 contract suite。

### 11.5 现有 Artifact 迁移

目标状态下，上传文件、Run Artifact 和超限 Conversation 内容共用同一个 S3 client 与不同 key
namespace。现有 `shared_filesystem` authority 不能原地改写为 S3：

1. 新增不可变 `s3` artifact-store authority identity；
2. 新写入切换到 S3 前先完成双 Runtime 可见性和恢复资格测试；
3. 旧 shared-filesystem Artifact 在 retention 期内继续只读；
4. 如需迁移，使用校验 hash 的离线 copy + verify，不在读取路径做隐式搬迁；
5. retention 结束后 clean-cut 删除 filesystem backend 配置与写路径；
6. 快速测试可以使用临时 RustFS 实例或受控 fake，但产品配置不重新暴露 local backend。

## 12. 安全、隐私与生命周期

- File metadata、object 和绑定都按 tenant/user fail closed；跨 principal 统一返回 404；
- File ID 必须高熵且不可枚举，但高熵 ID 不替代 repository ownership 检查；
- 文件内容默认视为不可信数据，不能进入 system instruction；
- filename、media type、图片 metadata 和模型识别文本都可能恶意，日志不得记录正文；
- 上传完成前执行 size/checksum 校验；可选 malware/content scan 必须在 `pending_upload -> ready` 之间
  成为明确状态，而不是异步放行后再撤销；
- 模型使用的图片 bytes 必须设置独立解码尺寸、像素数和解压后内存限制，不能只检查对象字节数；
- Conversation privacy delete 删除消息和 file bindings，并释放引用；独立 File 是否同步 tombstone 由所有权
  关系决定，不能删除仍被其他合法资源引用的对象；
- 预签名 URL 永远不持久化；Run durability 保存 file identity，不保存临时 URL；
- S3 retry 只用于可证明幂等的读取、HEAD、条件写和条件删除；不安全重试必须依赖 object identity 与
  idempotency record；
- GC 使用 durable claim/fence，S3 删除成功但数据库 ack 失败时允许幂等重试；
- metrics 不使用 tenant、user、file、run 或 object key 作为 label。

## 13. 错误合同

| HTTP | code | 条件 |
|---:|---|---|
| `400` | `INVOCATION_INVALID` | 信封形状、未知字段或基础类型错误 |
| `422` | `AGENT_INPUT_INVALID` | `inputs` 或保留输入不满足 Agent schema |
| `422` | `AGENT_CONVERSATION_UNSUPPORTED` | Agent 不满足 Conversation eligibility |
| `422` | `CLIENT_HISTORY_UNSUPPORTED` | Agent 未声明 messages 却提交历史 |
| `400` | `CONVERSATION_HISTORY_MANAGED` | Conversation turn 提交 messages |
| `404` | `FILE_NOT_FOUND` | file 不存在或 principal 不匹配 |
| `409` | `FILE_NOT_READY` | file 尚未完成、已失败或正在删除 |
| `422` | `FILE_LIMIT_EXCEEDED` | 数量、单文件或总大小超限 |
| `422` | `FILE_MEDIA_TYPE_UNSUPPORTED` | Agent/平台不接受该媒体类型 |
| `422` | `FILE_PROVIDER_UNSUPPORTED` | 使用位置的 Provider 无对应输入能力 |
| `409` | `FILE_CONTENT_MISMATCH` | complete 时对象 identity/size/checksum 不匹配 |
| `409` | `IDEMPOTENCY_KEY_REUSED` | 相同 key 对应不同 canonical request |
| `503` | `OBJECT_STORAGE_UNAVAILABLE` | S3 暂时不可用且 operation 未提交 |

错误 message 只包含安全定位信息，不返回 object key、bucket、预签名 URL、credential、文件正文、消息
正文或完整 AgentInput。`OBJECT_STORAGE_UNAVAILABLE` 是否可重试由 safe error metadata 和
`Retry-After` 明确表达。

## 14. 兼容性与迁移

本规范是有意的 clean-cut 变更：

| 当前合同 | 目标合同 |
|---|---|
| Run body 直接等于 AgentInput | Run body 是 `AgentInvocation` |
| Conversation message body `{"content": ...}` | `{"query", "files", "inputs"}` |
| 内部注入 `message`、`conversation_context` | 自动注入 `query`、`messages`、`files` 与业务 inputs |
| `X-Request-ID` 同时承担必填幂等 | `Idempotency-Key` 可选幂等，`X-Request-ID` 可选追踪 |
| 图片由调用方传 `image_url` | 调用方传 `file_id`，平台解析与投递 |
| 生产 Artifact 使用 shared filesystem | 目标使用 S3/RustFS |

迁移规则：

1. 不在同一路由同时猜测旧 body 和新 body；
2. 不接受 `content`、`message`、`input`、`payload` alias；
3. 所有受影响的 Agent Definition 必须发布新 revision，把 `message`/`conversation_context` 或
   `image_url` 改为 `query`/`messages`/`files`；
4. 现有 Conversation 绑定旧 Deployment Revision，不可被新 wire 静默继续；rollout 前应关闭旧
   Conversation 或提供显式版本化迁移工具；
5. discovery、OpenAPI/JSON Schema、SDK fixtures、CLI 和示例必须在同一 release 切换；
6. 当前文档只有在实现与 conformance tests 完成后才更新，不能提前宣称目标合同已交付。

## 15. 实施阶段

### Phase 1：调用合同与编译器

- 增加 `AgentInvocation` strict DTO；
- 增加平台内建 `File`；
- 实现保留输入名和 exact `invocation_schema` 派生；
- 实现 invocation 到 canonical AgentInput 的纯规范化；
- 更新 discovery 与 schema fixtures；
- 保持旧 Run/Conversation 行为不变，先用未公开 feature gate 完成编译器和 DTO 测试。

完成标准：每个 Agent 的 HTTP schema 和运行时 input schema 都由同一声明派生，无法漂移。

### Phase 2：S3/RustFS 与 File Service

- 使用 `aws-sdk-s3` 实现具体 `S3Storage`；
- 增加 File metadata schema、状态机、预签名上传、complete HEAD 校验和 GC；
- 增加 file principal、limit、immutability 和 deletion binding；
- 建立真实 RustFS contract suite；
- 增加部署配置、secret、readiness、Helm 和运维文档。

完成标准：本地浏览器/CLI 文件可以直传 RustFS，只有 ready file 能进入 Run，服务端没有任意本地路径
读取面。

### Phase 3：Run 与 Conversation clean-cut

- 两个 Run POST 切换到 `AgentInvocation`；
- 两个 Conversation message POST 切换到 `query/files/inputs`；
- Conversation repository 保存 canonical Message 和 file bindings；
- 历史由 repository 注入 `$messages`；
- 分离 `Idempotency-Key` 与 `X-Request-ID`；
- 更新所有仓库 Agent、SDK interop 和 API 文档。

完成标准：相同 idempotency key 不产生重复 Run/user message/file binding，故障点不产生孤立资源。

### Phase 4：LLM attachment

- 增加 `attachments: $files` 编译、类型和 provider-neutral IR；
- 实现图片 inline/presigned delivery；
- 冻结 Provider attachment capability；
- 增加多图片、大小、媒体类型、URL secret 和历史重放测试。

完成标准：模型收到有序图片内容，而不是不可解析的 `file_id`；任何日志和 durable payload 都不出现
预签名 URL。

### Phase 5：Artifact S3 authority 与 filesystem 退役

- 为 Worker Artifact 和 Conversation large content 增加 S3 authority；
- 完成多 Runtime、恢复、GC 和 retention 资格测试；
- 切换新写入，保留旧 backend 只读到 retention 结束；
- 删除产品 `local_filesystem/shared_filesystem` 配置和写路径。

完成标准：所有产品对象内容只通过 S3 协议访问；磁盘只用于数据库、进程临时缓冲和 RustFS 自身挂载，
不作为平台文件 backend。

## 16. 测试与验收

### 16.1 Contract

- strict 顶层字段、缺省物化、业务 inputs 和保留输入名；
- task-only、query-only、history、files 和组合请求；
- discovery `input_schema`/`invocation_schema` 与实际 admission 使用同一文档；
- Agent 未声明字段时拒绝对应调用面；
- Conversation eligibility 静态验证；
- `message/content/input/payload` alias 全部拒绝。

### 16.2 Conversation

- 无会话 Run 接受安全 client history；
- Conversation turn 拒绝 client history；
- user message + Run admission + file bindings 原子提交；
- terminal result + assistant message 原子提交；
- summary + recent suffix 顺序稳定；
- archive/delete、跨 principal 404 和 file reference release；
- 相同幂等键 replay 不追加第二条 user message。

### 16.3 S3/File

- presigned PUT、HEAD、GET、DELETE、content type、content length、checksum 和 ETag；
- pending/ready/failed/expired/deleting/deleted 全状态；
- 过期 URL、错误 size、错误 checksum、缺失 object 和并发 complete；
- ready object 不可覆盖；
- S3 timeout、连接失败、重试、GC crash/restart 和删除 ack 丢失；
- RustFS 实例重启后 metadata/object identity 保持；
- 平台升级 RustFS 版本时完整 contract suite 必须通过。

### 16.4 多模态

- 零、一和多张图片保持顺序；
- inline/presigned 模式生成等价 Provider-neutral 请求；
- private endpoint 不误用 presigned 模式；
- 非图片、欺骗 media type、超大对象、像素炸弹和解码失败；
- Conversation 历史 file part 重用同一 immutable binding；
- provider 无 capability 时在外部调用前失败。

### 16.5 安全与非干扰

- 跨 tenant/user 的 file ID 统一 404；
- 日志、trace、metrics、错误、Run summary 和 message 不含 object key、URL、credential 或文件正文；
- 用户历史不能注入 system/developer/tool role；
- filename 不参与 object path；
- privacy delete 后所有公共文件读取 fail closed；
- malformed file/message 不触发 Provider 或 Action。

### 16.6 完成定义

只有同时满足以下条件才能把本文标记为 Implemented：

1. 四字段调用信封、保留输入映射和 discovery schema 已交付；
2. Run 与 Conversation clean-cut 完成，仓库中不存在旧 body alias；
3. File Service、RustFS/S3 contract、引用与 GC 已通过故障测试；
4. 多图片通过真实 Provider adapter 或闭合 wire fixture 验证；
5. `Idempotency-Key` 与 `X-Request-ID` 已完全分工；
6. 当前文档、schema、SDK fixture、Helm、示例和 migration note 同步；
7. 若声明“所有对象只走 S3”，Artifact filesystem backend 已按 Phase 5 完成退役，而不是只迁移用户
   上传文件。

## 17. 被拒绝的替代方案

### 17.1 所有内容包在 `message.content`

这会迫使非聊天 Agent 伪造 message，并让简单文本访问变成 `message.content[0].text`。本规范用
`query` 保持常见调用简单，把历史和文件分别建模。

### 17.2 使用 `message.kind`

普通用户消息的语义由 role、文本和附件表达。确认、表单和审批是独立协议状态机，不应塞进 Message
union；否则历史、幂等、权限和执行 side effect 会混在一起。

### 17.3 把整个 body 命名为 `payload`

`payload` 不能表达字段权威、验证来源或生命周期，只会把所有 Agent 差异推给配置 mapping。固定信封
加 `inputs` 能稳定 SDK，同时保留任意业务扩展。

### 17.4 新增 `/turns`

现有 message 写入已经创建 Run，且 Run 有独立公共身份。再增加 Turn 资源没有新的生命周期权威，反而
让查询、取消、幂等和错误归属出现歧义。

### 17.5 Conversation 客户端提交完整历史

客户端快照可能丢失并发 message、绕过 summary policy、伪造 assistant 输出或与 durable Run 结果
冲突。需要完全控制历史的调用方使用无会话 Run。

### 17.6 `X-Request-ID` 同时作为幂等键

trace ID 常由 gateway 每次重试重新生成，而业务幂等键需要跨重试稳定。两者合一会让观测和资源唯一性
互相约束，因此目标合同明确分离。

### 17.7 增加本地文件 backend

它会产生不同的 URL、共享可见性、故障恢复、权限和 GC 语义。开发环境运行单节点 RustFS 即可复用
生产 S3 路径；用户本地文件通过上传进入 RustFS。

### 17.8 使用 RustFS 专用业务接口

RustFS 已实现 S3-compatible 协议，官方推荐标准 S3 SDK。绑定产品 SDK 或产品命名会给未来 endpoint
替换制造无收益耦合；平台只依赖已验收的 S3 操作集。

### 17.9 把业务字段平铺到请求根对象

例如直接提交 `{"query":"...","response_style":"concise"}` 会让任意业务字段与未来的平台协议字段
争夺同一命名空间。`inputs` 为 Agent 自有字段提供封闭命名空间；`query/messages/files` 则保留稳定的
跨 Agent SDK 语义。这里使用复数 `inputs`，因为它表示一组命名输入变量，不是单个 message 或一个
不透明 payload。
