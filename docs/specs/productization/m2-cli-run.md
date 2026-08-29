# M2 CLI Run lifecycle evidence

| 属性 | 值 |
|---|---|
| 状态 | Implemented subset / M2 In Progress |
| public authority | Runtime Gateway `/v1/runs*` |
| 当前命令 | `run create|get|pause|resume|cancel|result` |

## 1. 命令面

```text
insight run create --file <request.json> [--path <project>]
insight run get <run_id> [--path <project>]
insight run pause|resume|cancel <run_id> [--path <project>]
insight run result <run_id> [--path <project>]
```

`create` 文件就是 authoritative closed `CreateRunRequestV1 { agent_id, input, deadline }`，不是新的 Agent DSL。
CLI 不接受 Deployment、Plan entry、Node、Job、binding closure 或任意内部 evaluator 字段。`agent_id` 必须是 Agent
Resource ID；Artifact input 必须通过 nominal `ArtifactRef` 校验，且 classification 必须与 input 一致。

Run 命令只连接 local profile 的 Runtime Gateway 端口。Management Gateway 继续只承载 Resource lifecycle；CLI 不会因
本地使用方便而绕过 Gateway 读取 PostgreSQL 或调用内部 RPC。

## 2. authority 校验

- create Receipt 由 canonical request digest 确定，重放相同文件会使用同一 Receipt；
- create 要求 `201`、exact `/v1/runs/{run_id}` Location、body/header ETag 一致；
- get/control 校验 Run、Agent Deployment、RunValue ID kind、version、时间顺序和 exact Run ETag；
- pause/resume/cancel 先读取 current Run ETag，再以 If-Match 和按 action/Run/ETag 确定的 Receipt 提交；
- result 按合同不要求 ETag，但仍要求 closed JSON、trace、private no-store envelope；Inline 结果重新计算 canonical
  content digest，Artifact 结果重验 digest 与 classification。

## 3. 当前证据与剩余门禁

loopback HTTP fixture 已覆盖 create -> read current -> CAS pause -> result，断言 Authorization、Receipt、If-Match、
trace、Location、ETag，以及无 ETag 的 result envelope。closed request 和 result digest 另有负向单元测试。

本批尚不关闭 M2：

- `run watch` 的 SSE cursor、Last-Event-ID、断线 reconnect 与 durable replay 尚未实现；
- control mutation 的 request-before-send journal 与 response-loss 精确恢复尚未实现；
- 真实 Gateway + fresh PostgreSQL + Orchestration Worker 的 first Run 尚未形成 P1 journey；
- terminal failure、409/412/429、result-not-ready 和跨 worker restart 的 CLI fixture 尚未完成。

因此该命令面是可测试的初始 public client，不是首次 Run 或 durable observation 的完成声明，也不改变 spec00～18
Accepted/In Progress 状态。
