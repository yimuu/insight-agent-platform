# 架构概览

状态：Current

适用版本：`insight.agent/v1`

Insight Agent Platform 将作者编写的结构化 YAML 或画布 Graph 编译为同一个不可变、类型化的
Canonical Plan。Deployment Revision 另外冻结 `full` 或 `terminal_only` persistence policy；Run
不能覆盖该故障语义。

```text
Agent YAML / Graph Author Document
              ↓ parse / type-check / link
        Canonical Typed Plan
              ↓ immutable Deployment policy
        ┌──── full ────→ durable scheduler/checkpoint/lease
Run ────┤
        └ terminal_only → process-local scheduler → terminal result
              ↓
PostgreSQL（生产）/ SQLite（单进程开发） + Artifact store
```

Agent 和 Provider 的 managed 对象以数据库为唯一 live authority。工作区 `agents/*` 与内置 Provider
Catalog 只作为 fixture、template 或显式导入输入，进程启动不会扫描它们来覆盖 current route。Agent
Draft 可变；Definition、Resolution、Deployment 与 Run pin 不可变。Provider active pointer 只影响新
resolution，Provider suspension、MCP disable 和 Agent archive 另作为 admission/leaf-start 安全门。

## 核心对象

- **Definition Revision**：发布后不可变的作者定义版本；
- **Deployment Revision**：将定义绑定到资源和运行环境后的不可变版本；
- **Run**：一次执行及其全局状态、输入、输出和终止权威；
- **Scope**：结构化控制流产生的持久化执行范围；
- **Activation**：Plan 节点的一次逻辑激活，具有稳定身份；
- **Attempt**：Worker 对 Activation 的一次带 lease 执行尝试；
- **Artifact**：超过内联阈值的大值或二进制内容，按内容寻址保存；Conversation/terminal scoped
  envelope 可在对象层按 tenant 派生 key 做带版本 AEAD 加密。
- **Agent Entity/Draft**：稳定 public ID 与唯一可变 authoring package；
- **Deployment Resolution**：一次有期限的精确 Provider/MCP/Action/Retrieval/Subflow head 提案；
- **Provider Revision**：冻结 adapter、endpoint identity、credential reference 和显式模型事实；
- **Debug Session**：不写 public head 的 admin-only 临时 exact Deployment 与 `debugrun_*` Run。

Run 始终固定到不可变 revision。ViewDocument 和 trace overlay 用于布局与观察，不参与执行真相。
Deployment binding hash 包含 persistence policy，因此相同 Plan 的 `full` 与 `terminal_only`
Deployment Revision 必须具有不同 identity。旧 revision 未携带该字段时按历史合同解释为 `full`。

## 核心不变量

- `full` Run 的 Scope、Activation、Attempt、控制 token、timer、signal 和 task outbox 具有稳定的
  持久化身份；其 scheduler 不依赖进程内调用栈或内存事件历史恢复；
- `terminal_only` 只持久化 revision-bound admission、terminal result 及可选 Conversation 最终消息；
  执行期 Scope、Activation、Attempt 和 wait state 不是恢复权威；
- `full` Worker lease 使用 epoch/fence；terminal-only owner fence 只阻止过期 owner 的迟到
  terminal commit，两者都不允许旧执行者覆盖新的权威结果；
- `full` 的 join、signal/timeout 等竞态和两种路径的 terminal commit 都由数据库事务决定唯一赢家；
- 外部副作用默认只承诺 at-least-once，不能伪装成 exactly-once；
- 小值内联，大值写入 Artifact store，并在结果事务中提交引用；
- 生产 runtime 必须共享 PostgreSQL 16 和同一个物理 Artifact store；
- SQLite 只承诺确定性的单进程开发语义，不承诺 HA、多 runtime lease 或生产恢复。

## 代码边界

| Package | 所有权边界 |
|---|---|
| `insight-engine` | 无 I/O 的执行合同内核：Plan、纯 scheduler、状态机和公开 DTO |
| `insight-dsl` | DSL v1 与 Graph authoring 的解析、校验、类型检查和 lowering |
| `insight-durable` | 后端中立的持久化 ports、commands、claims、receipts 和 projection models |
| `insight-resources` | Model/Action/Retrieval SPI、Provider/model registry 与具体 adapter |
| `insight-mcp` | MCP wire、codec、transport、OAuth、Tasks 与 Server dispatcher |
| `insight-storage` | SQLite/PostgreSQL、Graph SQL、Artifact store 和 PostgreSQL live broker adapter |
| `insight-runtime` | catalog/deployment、leaf adapter、WorkCoordinator、RunService 和 live Run stream |
| `insight-api` | Axum HTTP、认证、请求/错误映射和 SSE transport |
| `insight-agent-platform` | 根 facade、平台配置、进程 bootstrap 和 binary composition |

依赖只从高层 consumer 指向低层 owner。`storage` 与 `runtime` 不直接依赖彼此，而是通过
`engine`/`durable` 所有的 ports 在根 composition 中组合；workspace member 直接导入 owner crate，
不通过根 facade 形成反向依赖。

平台根层从 durable active Provider Revision 重建 `insight-resources` 模型 registry，并把 Agent 的
结构化 `{provider, id}` selector 解析为精确实现。只读 Provider Catalog 是 template/import 输入，
不是 live route。Provider Revision 冻结 endpoint identity、adapter/worker、受信任 adapter manifest
digest 和非秘密 credential reference；模型 ID 保持 Provider 原始身份。activation readiness 与每次
投影都复验这组 exact evidence 和 secret allowlist。`insight-runtime` 在 Agent Deployment resolution 时把
解析证据写入 Deployment Revision，scheduler 不在执行时重新路由，也不会跨区域或跨 Provider 自动
故障转移。

PostgreSQL Provider registry 投影使用“durable snapshot + opaque wake hint + generation poll”：schema
trigger 在 management outbox commit 后发不含对象 identity 的通知，runtime 每次都重读 active 与被历史
Deployment/Run 引用的 archive。通知只缩短延迟；丢失、重复、乱序和 reconnect 均不改变 durable head、
suspension fence 或 exact revision 的权威。

MCP 使用独立 `insight-mcp` 协议边界。Host 在 publication 时冻结远程 discovery/list evidence，并把
Tool、Resource 和 Prompt 分别适配到 Action、Retrieval 与 untrusted Prompt snapshot；运行时只执行
精确 binding。Server `/mcp` 只投影显式 export。Interaction、OAuth credential、remote/server Task
由 `insight-durable` port 和 SQLite/PostgreSQL adapter 提供 first-winner authority，正文与 opaque
handle 不进入公共事件。详细合同见 [MCP 使用、运行与安全合同](mcp.md)。

## 两条执行路径

`full` 路径保留现有 durable contract：

admission 创建持久化 Run；进程级 `WorkCoordinator` 合并数据库通知、本地提交提示、deadline 和
5 秒安全轮询，再按空闲 execution permit 有界领取工作。worker 不再各自轮询所有 durable queue。
scheduler recovery drive 和叶节点 worker 共用同一个全局 permit 池；recovery 可以跨 Run 有界
并发，但同一 Run 仍由数据库 lease/fence 串行化。scheduler 根据已提交事实生成工作；Worker 获取
Activation lease、执行叶节点并用 fence 提交结果。进程退出、通知丢失或 lease 过期后，runtime
从数据库重新领取未闭合工作，不重放已经提交的 Activation。

`max_concurrent_runs` 限制 active/waiting Run admission，`max_concurrent_operations` 限制同时执行
的 operation，两者不是同一个容量维度。等待 timer、signal、human 或外部 continuation 的 Run 不占
execution permit。runtime 自己提交的 PostgreSQL 事务先直接唤醒进程内 coordinator；跨进程
`LISTEN/NOTIFY` 由进程级 publisher 在 250ms 窗口内合并为一个不含业务数据的全 work-class
提示，不进入权威事务的提交临界区。非 runtime repository writer 仍可由 schema trigger 提交提示。
durable row、claim、lease 与 fence 始终是唯一权威，任一提示丢失都由 5 秒 safety scan 恢复。

核心调度路径不依赖 Redis 或外部消息队列。Redis 只适合后续缓存可回源、允许短暂过期的 immutable
metadata；外部消息队列只适合 durable public outbox 之后的跨服务 fan-out。两者都不能成为 Run、
lease、timer、signal、admission 或 outbox 的权威。

暂停只阻止新的调度 admission；取消、超时和信号仍通过持久化控制意图收敛。redrive、fork、
migrate 与 continue-as-new 都基于固定 revision、checkpoint 和服务端推导的兼容证据工作。

`terminal_only` 使用独立的进程内 scheduler/active registry，不调用 full repository 的 execution
event、projection/scheduler checkpoint、claim/lease、public ledger 或 replay 写路径。持久化权威
只有 admission 和 terminal result；Conversation turn 另外原子写入 user/final assistant message。
owner instance lease 只用于判断无 result admission 已中断，并阻止过期 owner 的迟到 terminal
commit。进程失败后不 recovery、不接管、不自动 retry，也不承诺外部副作用 exactly-once。

terminal-only publication 会静态拒绝 durable wait、人工作业、长 timer、durable child subflow 和要求
durable effect fence 的 provider。对 terminal-only Run 调用 pause/resume、signal、redrive、fork、
migrate 或 continue-as-new 属于 capability error，不会退回 full 引擎。

## Conversation

Conversation 是 tenant/user 与 Agent 的多轮消息容器，不是 workflow checkpoint 或长期 Agent
memory。它只保存不可变 user/assistant 最终消息和低频不可变 summary；SSE token/delta、provider
chunk、Run 中间状态与 tool scratchpad 不进入 Conversation 表。一个 turn 的 user message 与
admission 原子提交，terminal result 与 assistant message 原子提交。Prompt context 由“最新有效
summary + 其后的最近消息”构成，数量和 token budget 有界；summary object 缺失或损坏时会丢弃
该优化并从整个 Conversation 重新读取精确的最近消息后缀。

`message_order` 的权威范围是单个 Conversation。PostgreSQL repository 在 transaction-scoped
per-Conversation advisory lock 下，通过已有 cursor index 求 `MAX(message_order)+1`；SQLite 的单
writer transaction 使用相同规则。这样既保证已提交消息在 Conversation 内唯一、严格递增和 cursor
稳定，也让顺序对应同一 Conversation 的 repository 串行提交次序，而不需要更新 Conversation head
row。schema 的 global cached sequence 只作为 direct-SQL default 保留：连接池 backend 各自预留的
cache 区间可能让后提交消息取得更小的旧缓存值，因此它不作为 runtime 的 Conversation 排序权威。

超过 inline 阈值的 user/final assistant/summary 内容写入 tenant/source scoped object，message
只保存 ref 与原始内容 hash。object 先进入 replay-safe staging，随后与权威 message 或 summary
在同一事务消费；失败提交不会留下半个 terminal result。启用 terminal-only engine 时由它唯一执行
privacy deletion 与 orphan staging 回收；仅启用 full Conversation 时由 RunService maintenance
pump 执行同一闭环以及有界 Conversation/历史 terminal Run retention，二者不会同时 claim。

## 公共 Run stream 与内部事实

Detached Run 通过查询接口读取其 persistence mode 对应的状态或已提交 terminal result。Attached
Run 使用闭合的 `run-stream/v1` live-only SSE 投影实时内容，不提供 `Last-Event-ID` replay；临时
delta 是有界、best-effort 数据，最终已持久化的状态特化 `run` snapshot 才是交付权威。Full
runtime 将 canonical payload 存入 `run_stream_snapshots.run_payload`；Terminal-only 从相同类型
构建完全一致的终态 wire，但不为 delta 或工具进度增加持久化写入。full runtime 发布 Public Event
后直接执行本地 durable-by-ID 投递，不在每个 outbox
publication 权威事务中发送 PostgreSQL 通知；远端订阅者以 100ms 有界 durable-order poll 保证进展，
非 runtime publisher 仍可发 commit-scoped hint。内部 execution ledger 不直接暴露为公共事件历史。

## 相关文档

- [DSL v1 指南](dsl.md)
- [HTTP 与 SSE API](api.md)
- [部署与运维](operations.md)
- [开发指南](development.md)
- [文档权威关系](../README.md#权威关系)
