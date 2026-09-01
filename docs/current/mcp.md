# MCP、remote Capability 与 Sandbox

首发 MCP 支持 remote Streamable HTTP，保留 Tool、Resource、Prompt、Task、authorization 与 subscription 语义。
managed stdio 与 persistent sandbox session 不在当前范围。Remote MCP/HTTP 调用必须通过 exact Egress catalog、TLS、
Secret binding、timeout 与 byte limit；未安装 endpoint 在 I/O 前 fail closed。

[`examples/productization/langgraph-reference`](../../examples/productization/langgraph-reference/) 是固定
`@langchain/langgraph` 1.4.13 的独立 typed HTTP reference。它不读取 Platform DB，也不被链接进 Gateway、Scheduler 或
Worker。Python SDK 与 Agno adapter 已取消。

Sandbox首发后端只有OpenSandbox Kubernetes provider。运行时与Profile digest在publication/admission时冻结；每个shared Job使用
独立BatchSandbox和immutable fixed runner，输入/输出是bounded canonical frame，结果只能从fixed只读路径取得并校验schema、digest和size。
运行时禁止package manager、mutable image tag、host execution、runtime socket和字符串拼接shell。

单个Sandbox只执行一个Job。这样Job lease、tenant identity、quota、deadline、runner boot identity、网络策略、result frame和cleanup
都有唯一生命周期；跨Job复用会把残留状态与副作用边界混在一起，因此不属于当前合同。workload内部访问API、数据库或消息系统产生的
副作用幂等由Package与目标服务负责，Platform不尝试推断或重放这些外部业务操作。
