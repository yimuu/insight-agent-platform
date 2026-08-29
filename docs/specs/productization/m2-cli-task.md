# M2 CLI Task lifecycle evidence

| 属性 | 值 |
|---|---|
| 状态 | Implemented subset / M2 In Progress |
| public authority | Runtime Gateway `/v1/tasks*` |
| 当前命令 | `task get|submit-input|approve|reject|cancel` |

## 1. 命令面

```text
insight task get <task_id> [--path <project>]
insight task submit-input <task_id> --file <input.json> [--path <project>]
insight task approve|reject|cancel <task_id> [--path <project>]
```

CLI 只接受 Interaction 或 Approval Task ID。`submit-input` 文件是 closed
`SubmitTaskInputV1 { classification, schema_digest, value }`；Inline/Artifact `ValueRef` 使用 bounded strict JSON 与
nominal validation，Artifact classification 必须与 input 一致。其他 action 必须为空 body。

所有读取和 mutation 只连接 local profile 的 Runtime Gateway，不访问 PostgreSQL 或 internal RPC。CLI 重验 Task kind 与
ID kind、owner-safe link、generation/version、safe prompt key、body/header exact ETag 和 private no-store public envelope。

## 2. mutation 与恢复

每个 mutation 先 GET current Task，再用 current ETag 作为 If-Match，并按 Task/action/ETag/input canonical digest 生成
deterministic Receipt。发送前将 exact intent 写入 `.insight/task-control/<task_id>-<action>.json`；journal 是 bounded、
closed、0600 文件并通过原子替换持久化，不保存 token、Authorization 或内部 Task payload。

若 authority 已接受 mutation 但响应丢失，下一次调用跳过 GET，按 journal 原样重放 Receipt、If-Match 与 closed input。
authority 响应必须是 action 对应的 exact terminal state，验证后才写入 public Task result；完成后的同一命令只读取
current Task，不创建第二个 mutation。不同 input 不能复用旧 journal。

## 3. 当前证据与剩余门禁

单元测试覆盖 closed input、unknown field、Task/owner ID kind 与 ETag。loopback response-loss fixture 在服务端接收
`submit-input` 后丢弃响应，证明第二次 CLI 调用复用 exact Receipt/If-Match，第三次调用只 GET current Task，且 journal
不包含 Bearer token。

命令级负向 fixture 现已覆盖 409 `invalid_state_transition`、412 `precondition_failed`、429 `rate_limited`、503
`temporarily_unavailable` 和错误 assignee 的 403 `permission_denied`，并保留 status、problem code、retryability 与
retry-after。expired Interaction 的 submit-input，以及 Approved/Rejected/Cancelled authority state 上的
approve/reject/cancel 冲突均返回 closed 409；成功矩阵则逐一要求 approve、reject、cancel 返回各自 exact terminal state，
同时断言空 body、current If-Match 与 deterministic Receipt。

fresh P2 journey 已通过真实 Gateway、PostgreSQL 与 Orchestration Worker 发布 HumanTask Agent 并创建 Run；测试只从
公开 Run SSE 的 `interaction.required` 投影取得 Task ID，经 `task get` 验证 pending InteractionForm 与 exact owner/schema，
再用 `task submit-input` 提交 typed Inline response。原 SSE 随后出现 `interaction.resolved`，Run durable resume 并以该 response
成功返回。测试不查询数据库或 internal RPC。

Task 命令面的既定权限、过期、CAS/backpressure 与 action 冲突矩阵已闭合。M2 仍受 Artifact 与完整原始 HTTP
lifecycle 等跨命令门禁约束，因此不得把 M2 或 spec 00–18 标记为 Verified。
