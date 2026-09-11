# 原始 `/v1` HTTP authoring

Platform 的 HTTP authoring authority 是 Management Gateway 的公开 `/v1`，与 `insight apply` 使用同一组
Resource、Version、Deployment、Operation、Receipt 与 ETag 合同。统一安装的普通入口是
[`insight connect`](cli.md)，分步公开发布示例见[文档检索发布流程](../../examples/productization/document-review/PUBLICATION.md)。
仓库另保留显式 AWS qualification 的 checked curl fixture：

```console
examples/productization/http-resource-lifecycle.sh \
  --project /path/to/local-project \
  --file /path/to/policy.apply.json \
  --timeout-seconds 120
```

前置条件是已经为独立 AWS fixture 执行 `insight qualification-aws init` 并通过
`insight qualification-aws dev` 启动（源码方式显式加 `--from-source`），并安装 `curl`、`jq`。
这个脚本仍读取该 qualification profile，不是统一安装的 session 客户端。输入文件与
`insight advanced apply --file` 相同：`schema_version = 1`、`kind = insight.platform.apply/v1`，其中 `create`、
`publish` 和 `deployment` 分别是公开请求正文。fixture 不调用 `insight apply`、数据库或 internal RPC，也不生成
Secret value、Plan 或 mutable binding。

fixture 会显式执行并检查：

1. `POST /v1/{resource_noun}`，保存 Resource ID、Location 和 ETag；
2. 使用同一 create `Idempotency-Key` 和相同 body 重放，要求返回同一 effect；
3. 使用同一 Receipt 和不同 body 探测 closed 409 `idempotency_conflict`；
4. 携带 create ETag 调用 `draft:validate`，轮询公开 Operation 到 terminal；
5. 读取 exact validated Draft，再携带新 ETag 调用 `draft:publish`；
6. 从 publish authority response 构造 exact self Version ref，创建 Deployment；
7. 重新读取 Resource ETag并激活 exact Deployment。

所有 mutation 都设置独立 W3C `traceparent`、确定性 `Idempotency-Key` 以及需要的 exact `If-Match`。每个响应必须
具有 `application/json`、closed private no-store cache policy、合法 `trace-id`，mutation 的 response trace 必须与
request trace 相同，success body/header ETag 也必须一致。curl 禁止 proxy 和 redirect，并设置连接及总 timeout。

成功时 stdout 只包含一个 `insight.platform.http-resource-lifecycle-report/v1` JSON 文档，列出 Resource、Operation、
Deployment、最终 ETag、Receipt replay/Problem 结论和每一步 trace ID。local OIDC token 只从 project-local `0600`
identity 文件读取，不写入 stdout、stderr 或报告。失败时 fixture 输出 bounded status/Problem 摘要，不输出 token。

该 fixture 支持 `agents`、`skills`、`capabilities`、`contexts`、`models`、`mcp-servers`、`policies` 和
`sandboxes` 的 self Version 映射。其他 implementation、policy、provider、runtime、Secret 与 evidence binding 仍必须
由输入 manifest 显式提供 exact authority ref。

契约来源仍是 [`contracts/platform-v1/openapi.yaml`](../../contracts/platform-v1/openapi.yaml)；fixture 的 mock
authority contract test 位于
[`tools/tests/test_productization_http_lifecycle.py`](../../tools/tests/test_productization_http_lifecycle.py)，真实
fresh PostgreSQL/Gateway/Registry Validation Worker 调用则属于 `deterministic-first-run` P2 journey。

Resource 创建可以提供稳定别名，作用域为当前租户和 Resource kind，独立于可编辑的 display_name；
创建后的 Draft 更新必须保留原别名。模型默认值由 Tenant 持有：`GET /v1/model-default` 返回当前值和
Tenant ETag，`PUT /v1/model-default` 在 ModelWrite、If-Match 和 Receipt 下显式设置 exact ModelDeployment
或清空。读取设置本身不保证当前仍可执行；authoring 的 `alias` 和仅适用于 Model 的 `default_model`
选择器会在同一数据库快照校验环境、当前部署及其 Provider/Secret 闭包，再返回 exact binding。
没有默认值会明确报未配置，不扫描替代模型；调用者仍须给出真实的 Selection Policy binding。
默认变化只影响后续解析，不改写已经冻结的 Agent 或 Run；正常启动也不覆盖这个用户设置。

`POST /v1/model-credentials` 将一项显式输入的 API key 导入已经安装的 SecretProvider，返回 exact
SecretBinding。请求使用客户端预先保存的 operation identity；结果未知时保留这个身份和原输入重试，
不把 key 写入恢复文件或 Resource。Gateway 只通过专属 mTLS 身份调用 Egress，Security 在外部写入前
以及最终注册事务中检查当前权限。外部版本和数据库提交并非一个事务；未知结果有独立错误码，
已准备但未注册的外部材料不会被自动删除。响应禁止缓存，日志、Receipt 和 Event 不包含 key 或其公开摘要。
这条导入链的测试证明凭据隔离与恢复，不代表任何模型已通过能力或业务验收。
