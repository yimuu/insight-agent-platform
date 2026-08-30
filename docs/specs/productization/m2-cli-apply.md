# M2：`insight apply` 公共生命周期

| 属性 | 值 |
|---|---|
| 状态 | Implemented / M2 In Progress |
| 日期 | 2026-08-29 |
| owner | `crates/insight-cli/src/apply.rs` |
| public authority | Management Gateway `/v1` |

## 1. 已实现的命令

```text
insight apply --file <manifest.json> [--path <project>] [--timeout-seconds <1..3600>]
insight operation wait <job_id> [--path <project>] [--timeout-seconds <1..3600>]
```

两条命令只读取 `insight init/dev` 生成的本地 project、短期 OIDC token 和 loopback Gateway endpoint。它们不依赖
PostgreSQL、内部 RPC、privileged header 或 CLI 专用业务 authority。HTTP client 禁止 redirect 和 proxy，限制单次
request/response 大小及 timeout，不在错误或报告中输出 token。

## 2. Apply manifest

manifest 固定为 `schema_version = 1`、`kind = insight.platform.apply/v1`，顶层只接受：

- `resource_noun`：`agents`、`skills`、`capabilities`、`contexts`、`models`、`mcp-servers`、`policies` 或
  `sandboxes`；
- `create`：公开 `CreateResourceRequestV1` 的 `display_name + ResourceDocument`；
- `publish`：公开 `single` 或 `agent` publish request；
- `deployment`：`environment` 与该 Resource kind 的 typed Deployment bindings。

manifest 使用 deny-unknown-fields、strict JSON depth/item/property/string limits 和 1 MiB 总上限。typed
`ResourceDocument`、digest、ArtifactRef、SecretBindingRef、Policy/Deployment/Version ref 继续由 Platform nominal type
验证；manifest 不增加 Plan、Node、tool、URL、shell、Secret value 或调度语义。

Deployment bindings 只省略本次生命周期尚未生成的 self Version ref。CLI 在 publish 成功后按下表从 authority
响应生成 exact `ExactVersionRef`：

Agent Context slot还必须省略本次Deployment create才生成的`context_binding_id`、`owner_agent_deployment_id`、Context
snapshot digest和外层slot digest。manifest只提交exact Context binding intent；CLI校验响应中每个server-generated `xcb`
均归属返回的`adep`，并用相同canonical规则重建完整closure与digest，不能自行预留或接受漂移。

| Resource | authority 生成并填入的 self ref | Deployment 主 Version |
|---|---|---|
| Agent | Interface Revision、Plan Revision | Plan Revision |
| Skill | Skill Revision | Skill Revision |
| Capability | Capability Interface Revision | Capability Interface Revision |
| Context | Context Source Interface Revision | Context Source Interface Revision |
| Model | Model Profile Revision | Model Profile Revision |
| MCP | MCP Server Revision | MCP Server Revision |
| Policy | Policy Revision | Policy Revision |
| Sandbox | Sandbox Profile Revision | Sandbox Profile Revision |

其他 Implementation、Policy、Provider、runtime、Secret 与 evidence binding 必须已经是 manifest 中显式给出的 exact
authority ref；CLI 不从 active head、名称或 mutable default 推断它们。

## 3. 执行顺序与 fence

一次初始执行严格按以下顺序调用公开接口：

1. `POST /v1/{resource_noun}`；
2. `POST .../{resource_id}/draft:validate`，携带 create Resource ETag；
3. 轮询 `GET /v1/operations/{job_id}` 到 terminal；
4. `GET /v1/{resource_noun}/{resource_id}`，确认 exact Draft 已具有 ValidationSummary；
5. `POST .../draft:publish`，携带 validated Resource ETag；
6. `POST .../deployments`，携带 publish Resource ETag 和 authority 生成的 self Version ref；
7. `POST .../deployments/{deployment_id}:activate`，携带 publish Resource ETag。

每个 mutation 使用基于 canonical manifest digest 和固定 step name 的确定性 Idempotency-Key，并生成独立 W3C
traceparent。CLI 要求响应 trace-id 与请求一致，要求 body/header ETag 一致，并检查 Location、tenant、Resource kind、
Version matrix、content digest、Deployment closure digest 和最终 gate。最终 stdout 是 machine-readable
`insight.platform.apply-report/v1`，包含 Resource、validation Operation、published Version、Deployment、最终 ETag 和
每一步 trace ID。

CLI 在 `.insight/apply/<canonical-manifest-digest>.json` 持久化 bounded、closed、原子替换的
`insight.platform.apply-journal/v1`。每个 mutation 在发出请求前先写入确定性的 Receipt 与原始 If-Match，收到并校验
authority 响应后再写入 Resource、Operation、Version、Deployment、ETag 与 trace ID。journal 不保存 access token、
Secret、Resource document、Deployment closure 或 Artifact body。相同 manifest 再次执行时复用原始 intent；若响应在
服务端提交后丢失，则由服务端 Receipt authority 返回同一 effect，CLI 校验后从最近完成步骤继续。已完成 journal 可在
Gateway 不可用时直接重建同一 machine-readable report。

## 4. 当前证据与剩余门禁

当前 loopback HTTP fixture 已覆盖七步 Policy lifecycle，并断言 Authorization、Receipt、If-Match、trace、Location、
Operation terminal、publish digest 以及 Deployment self Version resolution。fixture 还会在首个 mutation 已被接收后主动
丢弃响应，确认重试复用同一 Receipt 并完成剩余 lifecycle；关闭 HTTP server 后再次 apply 会从完整 journal 返回相同
report 且不访问网络。单独 fixture 覆盖成功 Operation envelope 和 429 closed Problem 的 retry/trace 字段保留。

命令级失败 fixture 现已覆盖 create 的 409 `idempotency_conflict` 与 429 `rate_limited`、create 成功后
validate 携带 exact If-Match/Receipt 时的 412 `precondition_failed`，并保留 closed Problem 的 status、code、
retryability 与 retry-after。Validation Operation 的 `failed` authority 投影会保留 bounded safe code/message；
`cancelled`、`timed_out` 与 `reconciliation_required` 同样 fail closed。独立 1 秒 timeout fixture 持续返回 queued
Operation，证明 CLI 返回 exact timeout，且超时后没有继续 publish、deploy 或 activate mutation。

[`http-resource-lifecycle.sh`](../../../examples/productization/http-resource-lifecycle.sh) 现以 `curl+jq` 执行同一七步
公开 lifecycle，并额外证明 exact create Receipt replay 与 changed-body 409 `idempotency_conflict`。fixture 对每一步
固定 Authorization、Accept、Content-Type、traceparent、Receipt 和 If-Match 规则，拒绝 proxy/redirect，校验
Location、cache、trace、body/header ETag、Operation terminal 与 exact Deployment closure。普通 CI 的 mock authority
测试已覆盖 queued -> running -> succeeded 时 Operation ETag 演进、Policy self Version 注入和 token 不泄露；真实 fresh
P2 journey 已在 exact revision `939cd9e9d766ce17b242627daba7697fa3687799` 通过，并形成 `http_fixture=passed`
的 checked incomplete 资格报告；报告只因同场景 Console 入口尚未运行而保持 incomplete。

八类 closure matrix 现逐一构造 Agent Interface/Plan、Skill、Capability Interface、Context Source Interface、Model
Profile、MCP Server、Policy 与 Sandbox Profile publish summary，确认 CLI 只把 authority 返回的 exact self Version ID
和 semantic digest 注入对应 typed Deployment 字段。额外交叉类型 probe 用 Skill Revision 解析 Policy closure 时必须
fail closed，防止仅凭 published list 顺序或 mutable head 选取 Version。

Apply/Operation 命令面既定正常、失败、恢复、curl 与八类 self closure contract matrix 已闭合。以下是更高层 M2/M4
仍未完成的 fresh scenario 门禁：

- fresh PostgreSQL + 真实 Gateway/Registry Validation Worker 的 Policy/Agent publication、Run create/watch/result 与
  Orchestration Worker restart 及新增 curl lifecycle 已形成 P2 journey；其余 Resource kinds 仍未完成。

因此本文件是 Apply 子命令完成声明，不是整个 M2/M4 或 spec00～18 完成声明，也不改变 Platform v2 production
L4～L6 状态。
