# M2：`insight apply` 公共生命周期

| 属性 | 值 |
|---|---|
| 状态 | In Progress / initial lifecycle implemented |
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

## 4. 当前证据与剩余门禁

当前 loopback HTTP fixture 已覆盖七步 Policy lifecycle，并断言 Authorization、Receipt、If-Match、trace、Location、
Operation terminal、publish digest 以及 Deployment self Version resolution；单独 fixture 覆盖成功 Operation envelope 和
429 closed Problem 的 retry/trace 字段保留。

以下仍是 M2 未完成项：

- 请求发出但响应丢失时的 checked-in apply journal 与 effect-aware resume；当前确定性 Receipt 已存在，但尚不能把
  每一步原始 If-Match/authority result 跨 CLI 进程恢复；
- Agent/其他七种 closure 的 checked fixture，以及 409/412/429、Operation failure/timeout 的完整 contract matrix；
- Artifact upload/read、Run create/watch/control/result、Task resolve 及原始 curl lifecycle fixture；
- fresh PostgreSQL + 真实 Gateway/Registry Validation Worker 的 P1 journey，以及 worker restart 后的恢复观察。

因此本文件只描述已实现的 initial lifecycle，不是 M2 完成声明，也不改变 Platform v2 production L4～L6 状态。
