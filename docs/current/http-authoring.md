# 原始 `/v1` HTTP authoring

Platform 的 HTTP authoring authority 是 Management Gateway 的公开 `/v1`，与 `insight apply` 使用同一组
Resource、Version、Deployment、Operation、Receipt 与 ETag 合同。仓库提供 checked curl fixture：

```console
examples/productization/http-resource-lifecycle.sh \
  --project /path/to/local-project \
  --file /path/to/policy.apply.json \
  --timeout-seconds 120
```

前置条件是已经执行 `insight init` 和 `insight dev --profile base`，并安装 `curl`、`jq`。输入文件与
`insight apply --file` 相同：`schema_version = 1`、`kind = insight.platform.apply/v1`，其中 `create`、
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
[`scripts/tests/test_productization_http_lifecycle.py`](../../scripts/tests/test_productization_http_lifecycle.py)，真实
fresh PostgreSQL/Gateway/Registry Validation Worker 调用则属于 `deterministic-first-run` P2 journey。
