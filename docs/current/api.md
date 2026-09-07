# Public `/v1` HTTP API

[`contracts/platform-v1/openapi.yaml`](../../contracts/platform-v1/openapi.yaml) 是公开 HTTP authority。管理面覆盖
Resource/Draft/Version/Deployment；运行面覆盖 Run、Task、Artifact、Operation、Event stream 与 MCP callback。

请求使用 OIDC 或受支持的 workload credential。mutation 必须携带 `Idempotency-Key`；对既有 aggregate 的修改同时
携带 `If-Match`。错误使用 closed Problem body并保留 request/trace ID、稳定 code、retryability 与安全 detail。
Run events 是 durable bounded SSE page，客户端使用 opaque `Last-Event-ID` 恢复，不自行解释 cursor。

可执行 curl lifecycle 见 [HTTP authoring](http-authoring.md)。CLI 和 Console 都只使用同一公开合同，不访问数据库或
内部 RPC。

Artifact 上传完成的重试绑定原 Artifact、并发条件与完成证明。服务端先检查当前权限，再核对仍保留的完成及扫描收据；
命中后返回原 Artifact 与 Job 的当前安全投影，不重复对象检查、上传或扫描。对象去重和原上传期限经过不抹去已提交的成功。
prepare 只有在原上传对象、授权和扫描义务仍有效且未过原期限时才能刷新 URL；终态返回状态冲突，不重新开放上传。

作者依赖 discovery 与 bindings resolve 是服务端登记的只读查询；resolve 虽使用 POST，也不产生命令 Receipt。Active 选择只在解析时转为 exact binding，后续发布和运行不读取 active fallback。字段、输入限额和路由分类见 OpenAPI 及 [Registry authoring owner](../../crates/definitions/platform-registry/src/authoring.rs)。

Task inbox 按已扫描位置分页，允许空 `items` 仍带下一页。默认 `respondable` 查询使用当前响应权限与资格；显式 `viewable` 按种类检查当前读取权限，游标绑定该目的。View/Form schema 2 返回服务端允许动作；form 另需当前内容权限与响应资格，并绑定实际 generation、version、ETag、提示与冻结 schema。可查看元数据不意味着可读取表单或修改 Task，动作提示也不替代提交时的当前授权和 first-winner 检查。Run value metadata/list 与 child link 查询仅返回安全元信息；result、value content、Task form 和 Artifact bytes 均经过当前内容授权。无正文权限时返回 403，不将现有 result DTO 静默改成 metadata。

列表快照使用数据库时间，游标有效期使用 Gateway 时间；两者不要求时钟完全相等。数据库校验快照与行的位置，
续页保留原快照、过滤条件和到期时间，并重新检查当前权限。翻页不会刷新游标有效期；Task View/Form 的 HTTP ETag 与正文版本一致。

SSE 的成功响应给出 public sequence 高水位、retention floor 与历史截断标记。签名 cursor 即使未过 TTL，若已低于 floor 仍返回明确 history gap；消费端不得自动丢弃 cursor 重试 Initial。持续跟随、表单和完整作者输入的产品行为见[Console](console.md)与[共享作者入口](agent-authoring.md)。
