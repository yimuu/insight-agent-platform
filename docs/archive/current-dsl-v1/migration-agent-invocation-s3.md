# AgentInvocation 与 S3 clean-cut 迁移

状态：Current

本次变更不在同一路由兼容旧 body。调用方、Agent Definition 和部署配置必须在同一 release 切换。

## HTTP 调用方

| 旧写法 | 当前写法 |
|---|---|
| Run body 直接等于 AgentInput | `{query?, messages?, files?, inputs?}` |
| Conversation `{"content":...}` | `{"query":"...","files":[],"inputs":{}}` |
| `X-Request-ID` 负责重试去重 | `Idempotency-Key` 去重，`X-Request-ID` 只追踪 |
| URL 或 `image_url` | 先创建/上传/complete File，再提交 `file_id` |

`message`、`content`、`input` 和 `payload` 没有 alias。Conversation turn 不接受 `messages`；需要自己
控制历史时改用无会话 `/runs`。

## Agent Definition

把聊天输入声明为：

```yaml
inputs:
  query: string
  messages: {type: Message[], default: []}
  files: {type: File[], default: [], max_items: 10}
  response_style: {type: string, default: concise}
```

工作流使用 `$query`、`$messages`、`$files` 和 `$response_style`。图片位置改为
`attachments: $files`。业务字段仍是 DSL 顶层 input，但 HTTP 调用时放进 `inputs` 对象；不增加变量
mapping 配置。

旧 Conversation 冻结旧 Deployment Revision，不能静默改绑。发布并 activate 新 revision 后，应关闭
旧 Conversation，或者由产品提供显式、可审计的版本迁移流程。

## 部署与数据

产品 composition 只安装 S3 adapter。配置必须提供 `object_storage.s3` 和 Artifact `namespace`，凭据
只通过配置引用的环境变量注入。RustFS bucket 在启动前创建，readiness 会执行 `HeadBucket`。
图片默认使用 `object_storage.llm_attachment_delivery: inline_data`；只有模型 Provider 能访问配置的
`public_endpoint` 时才显式切换为 `presigned_url`。

旧 shared-filesystem 内容若仍在 retention 内，只能由旧 runtime 只读保留或通过离线
copy → hash verify → authority cutover 迁移；当前读取路径不会隐式搬迁。新数据库目标使用当前 durable
schema contract，pre-1.0 部署不对旧目标执行在线 migration。

本次 clean-cut 已完成 discovery/schema fixture、Run/Conversation 幂等 replay、File 上传/complete、
真实 RustFS contract、图片 Provider wire fixture、privacy delete/GC 和恢复验证；正式证据见
[AgentInvocation、Conversation 与 RustFS/S3 资格验收](../archive/qualifications/2026-08-06-agent-invocation-rustfs-s3-qualification.md)。
后续 RustFS、endpoint/proxy 或 S3 client 版本变化仍须重新执行资格测试。
