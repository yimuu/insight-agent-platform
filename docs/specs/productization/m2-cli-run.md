# M2 CLI Run lifecycle evidence

| 属性 | 值 |
|---|---|
| 状态 | Implemented subset / M2 In Progress |
| public authority | Runtime Gateway `/v1/runs*` |
| 当前命令 | `run create|get|pause|resume|cancel|result|watch` |

## 1. 命令面

```text
insight run create --file <request.json> [--path <project>]
insight run get <run_id> [--path <project>]
insight run pause|resume|cancel <run_id> [--path <project>]
insight run result <run_id> [--path <project>]
insight run watch <run_id> [--timeout-seconds <1..3600>] [--path <project>]
```

`create` 文件就是 authoritative closed `CreateRunRequestV1 { agent_id, input, deadline }`，不是新的 Agent DSL。
CLI 不接受 Deployment、Plan entry、Node、Job、binding closure 或任意内部 evaluator 字段。`agent_id` 必须是 Agent
Resource ID；Artifact input 必须通过 nominal `ArtifactRef` 校验，且 classification 必须与 input 一致。

Run 命令只连接 local profile 的 Runtime Gateway 端口。Management Gateway 继续只承载 Resource lifecycle；CLI 不会因
本地使用方便而绕过 Gateway 读取 PostgreSQL 或调用内部 RPC。

`watch` 读取 `/v1/runs/{run_id}/events` 的已提交 durable SSE page，并在每次连接关闭后携带最后一条 opaque
`Last-Event-ID` 重连。每个 frame 都重验 closed `PublicRunEvent`、Run ID、durability、cursor、event type 和严格递增
sequence，随后立即输出一条 tagged JSON Line 并 flush。Run authority 达到 terminal 后，CLI 先排空非满事件页，再输出
一条 terminal Run record；timeout 不改变 Run 状态。

## 2. authority 校验

- create Receipt 由 canonical request digest 确定，重放相同文件会使用同一 Receipt；
- create 要求 `201`、exact `/v1/runs/{run_id}` Location、body/header ETag 一致；
- get/control 校验 Run、Agent Deployment、RunValue ID kind、version、时间顺序和 exact Run ETag；
- pause/resume/cancel 先读取 current Run ETag，再以 If-Match 和按 action/Run/ETag 确定的 Receipt 提交；
- result 按合同不要求 ETag，但仍要求 closed JSON、trace、private no-store envelope；Inline 结果重新计算 canonical
  content digest，Artifact 结果重验 digest 与 classification。

control mutation 在 `.insight/run-control/<run_id>-<action>.json` 使用 bounded、closed、原子替换的本地 journal。CLI
在发送前写入 exact Receipt/If-Match，响应校验后只保存 public Run projection。未决 journal 下次执行时不先读取可能已
变化的 ETag，而是重放原 intent；已完成 journal 则读取 current Run，并以 pause/cancel generation 判断该 effect 是否仍
为 current。journal 不保存 token、请求 Authorization 或内部 control payload。

## 3. 当前证据与剩余门禁

loopback HTTP fixture 已覆盖 create -> read current -> CAS pause -> result，断言 Authorization、Receipt、If-Match、
trace、Location、ETag，以及无 ETag 的 result envelope。另一 fixture 使用两次独立 SSE 连接，断言第二次请求携带第一
页 opaque cursor、sequence 单调、event/Run identity 一致，并证明两个 event record 与 terminal record 各自 flush。
closed request 和 result digest 另有负向单元测试。

control crash-window fixture 在服务端收到 pause 后丢弃响应，确认第二次 CLI 调用跳过 GET 并复用 exact Receipt/If-Match；
authority 响应持久化后，第三次调用只读取 current Run，不创建第二个 mutation。

本批尚不关闭 M2：

- cursor expired/invalid、429/503、truncated/oversized/duplicate SSE frame 与 timeout 的完整负向矩阵尚未实现；
- 真实 Gateway + fresh PostgreSQL + Orchestration Worker 的 first Run 和跨 worker restart 尚未形成 P1 journey；
- terminal failure、409/412/429 与 result-not-ready 的 CLI fixture 尚未完成。

因此该命令面是可测试的初始 public client，不是首次 Run 或 durable observation 的完成声明，也不改变 spec00～18
Accepted/In Progress 状态。
