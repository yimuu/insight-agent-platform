# MCP、remote Capability 与 Sandbox

首发 MCP 支持 remote Streamable HTTP，保留 Tool、Resource、Prompt、Task、authorization 与 subscription 语义。
managed stdio 与 persistent sandbox session 不在当前范围。Remote MCP/HTTP 调用必须通过 exact Egress catalog、TLS、
Secret binding、timeout 与 byte limit；未安装 endpoint 在 I/O 前 fail closed。

[`examples/productization/langgraph-reference`](../../examples/productization/langgraph-reference/) 是固定
`@langchain/langgraph` 1.4.13 的独立 typed HTTP reference。它不读取 Platform DB，也不被链接进 Gateway、Scheduler 或
Worker。Python SDK 与 Agno adapter 已取消。

Sandbox 首发后端是 restricted WASI 与每 Job gVisor container。运行时依赖在 publication 时冻结；执行时禁止 package
manager、mutable image tag、host/runc execution 和字符串拼接 shell。
