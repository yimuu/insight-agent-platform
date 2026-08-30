# Public `/v1` HTTP API

[`contracts/platform-v1/openapi.yaml`](../../contracts/platform-v1/openapi.yaml) 是公开 HTTP authority。管理面覆盖
Resource/Draft/Version/Deployment；运行面覆盖 Run、Task、Artifact、Operation、Event stream 与 MCP callback。

请求使用 OIDC 或受支持的 workload credential。mutation 必须携带 `Idempotency-Key`；对既有 aggregate 的修改同时
携带 `If-Match`。错误使用 closed Problem body并保留 request/trace ID、稳定 code、retryability 与安全 detail。
Run events 是 durable bounded SSE page，客户端使用 opaque `Last-Event-ID` 恢复，不自行解释 cursor。

可执行 curl lifecycle 见 [HTTP authoring](http-authoring.md)。CLI 和 Console 都只使用同一公开合同，不访问数据库或
内部 RPC。
