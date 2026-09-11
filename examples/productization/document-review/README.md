# 公开文档检索与回答审核

这个示例包含文档检索服务、完整 Agent 源码、普通 Registry 资源构造器和公开端点协议验收入口。
它检索随目录冻结的两份项目公开文档，返回匹配原文、来源 URI、版本和行范围；
没有匹配时返回空结果。固定来源与字节摘要见 [corpus manifest](corpus/manifest.json)，不是当前主干文档的实时镜像。

[server.py](server.py) 只使用 Python 标准库，采用现有 Remote Context HTTP wire。查询是 `{"question":"…"}`；
调用方仍需提供生产 encoder 使用的完整、有界请求及 canonical 摘要。普通 ContextQuery 的空过滤、空投影和单页查询受支持，
其他形状明确拒绝。大小、并发和期限由服务实现的常量定义。

```bash
python3 examples/productization/document-review/server.py \
  --host 127.0.0.1 --port 8443 \
  --certificate /absolute/path/server-chain.pem \
  --private-key /absolute/path/server.key
```

服务启动时验证冻结 manifest 和原始 UTF-8 字节，请求不会下载文档或打开调用方指定的路径。
检索以英文词项和中文双字词项匹配原文段落，用确定性整数分数排序；这是小型关键词检索示例，不承诺语义召回率。
每连接只处理一个请求并关闭；已读缓冲中的多余正文后字节拒绝，尚未到达的下一请求不再处理。
TLS 握手前申请并发名额，满额直接关闭新连接；已接受请求的绝对期限覆盖握手、读取和发送。没有应用等待队列。
服务不写逐请求日志，避免日志管道阻塞耗尽连接名额；启动失败只返回安全诊断。当前 Egress 的 HTTP 错误映射不承诺自动重试。

## 接入完整 Agent

[agent/](agent/) 已包含可由共享编译器读取的 FullPlan、输入与输出 schema。流程是
Start → ContextQuery → Compute → ModelLoop → HumanTask → Compute → Return。
第一个 Compute 将原问题与实际检索观察一起交给模型；第二个保留模型草稿与人的明确决定，形成最终结果。
模型工具预算为零。没有匹配时回答不得编造引用；人的 `approve` 或 `reject` 是实际 Task 输入，示例不会替人提交。
默认只检索一段并最多输出一条引用，给完整观察元数据、问题、指令和输出 schema 留出模型输入空间；
实际 prompt 仍受所选模型与策略预算约束，超过预算会拒绝，不隐藏截断检索结果。

按 [PUBLICATION.md](PUBLICATION.md) 完成物理目的地声明、真实协议证据上传、Policy/Context 资源发布、
公开依赖解析、Agent 发布和人工处理。构造器复用现有 owning types，输出文件不包含新造的业务 ID。
发布仍要求所在环境真实、可授权的 exact Context 和 Model deployments，以及服务端返回的 feature evidence。

生产 Egress 只允许受信任的公开 HTTPS 目的地址。上述本地监听用于服务开发和 TLS 测试；实际接入需要可达的公开 HTTPS
部署及匹配其证书的信任配置。不要为了运行示例开放 loopback、私有地址或 DNS 绕过。
模型凭证经已安装的 Secret provider、引用加密 adapter 与 SecretBinding 配置，不放进 Agent 文件或公开请求。

Context 仍拥有观察结果和引用映射。这里的 URI、原文件 SHA256、行范围都是 provider 来源元数据，平台引用保持
RemoteOpaque / ObservationOnly。原始文件摘要与 JSON 正文的 canonical digest 各有用途，不能互换。
模型应只基于实际 Context 输出作答；在发起人工确认前独立核对回答引用与原始行范围。模型回答不是人工批准。

## 验证范围

```bash
python3 -m unittest tools/tests/test_document_review.py
cargo test -p insight-platform-contract-tooling --bin document_review_source
cargo run -p insight-platform-contract-tooling --bin document_review_source -- --check
cargo test -p insight-platform-egress --lib remote_context::
```

Python 测试用 OpenSSL 3 创建临时 TLS CA，使用实际 corpus 覆盖原文、中文、空结果、严格 JSON、文件篡改、证书/SAN、连接容量和期限。
Rust 测试用生产 request encoder 生成输入，调用实际 Python `--query-stdin`，再由生产 normalizer 接收返回值并独立核对原文。
`--query-stdin` 只检查本地协议互通，不证明生产 Egress 网络调用或完整 Context Run。

最后一条默认不执行显式 ignored 的公开端点验收。其实际调用方法、首次派发前的持久记录和失败保留规则见发布说明。
当前源代码/编译器、本地 corpus 和 TLS 回归不能代替本示例尚待实际端点的完整 Context Run、模型回答与人工确认。
