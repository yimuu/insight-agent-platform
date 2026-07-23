# 架构概览

状态：Current

适用版本：`insight.agent/v3`

Insight Agent Platform 将作者编写的结构化 YAML 或画布 Graph 编译为同一个不可变、类型化的
Canonical Plan。调度器只根据 Plan、持久化事实和数据库时间作出决定，Worker 通过带 fence 的
lease 执行并提交结果。

```text
Agent YAML / Graph Author Document
              ↓ parse / type-check / link
        Canonical Typed Plan
              ↓ durable admission
Run → Scope → Activation → Attempt
              ↓ lease / heartbeat / fenced commit
PostgreSQL（生产）/ SQLite（单进程开发）
              ↓
content-addressed Artifact store
```

## 核心对象

- **Definition Revision**：发布后不可变的作者定义版本；
- **Deployment Revision**：将定义绑定到资源和运行环境后的不可变版本；
- **Run**：一次执行及其全局状态、输入、输出和终止权威；
- **Scope**：结构化控制流产生的持久化执行范围；
- **Activation**：Plan 节点的一次逻辑激活，具有稳定身份；
- **Attempt**：Worker 对 Activation 的一次带 lease 执行尝试；
- **Artifact**：超过内联阈值的大值或二进制内容，按内容寻址保存。

Run 始终固定到不可变 revision。ViewDocument 和 trace overlay 用于布局与观察，不参与执行真相。

## 核心不变量

- Run、Scope、Activation、Attempt、控制 token、timer、signal 和 task outbox 都有稳定身份并持久化；
- scheduler 不依赖进程内调用栈或内存事件历史恢复；
- Worker lease 使用 epoch/fence，过期 Worker 不能覆盖新的权威结果；
- 终态、join、signal/timeout 和 cancel/success 竞态由数据库事务决定唯一赢家；
- 外部副作用默认只承诺 at-least-once，不能伪装成 exactly-once；
- 小值内联，大值写入 Artifact store，并在结果事务中提交引用；
- 生产 runtime 必须共享 PostgreSQL 16 和同一个物理 Artifact store；
- SQLite 只承诺确定性的单进程开发语义，不承诺 HA、多 runtime lease 或生产恢复。

## 代码边界

| Package | 所有权边界 |
|---|---|
| `insight-engine` | 无 I/O 的执行合同内核：Plan、纯 scheduler、状态机和公开 DTO |
| `insight-dsl` | DSL v3 与 Graph authoring 的解析、校验、类型检查和 lowering |
| `insight-durable` | 后端中立的持久化 ports、commands、claims、receipts 和 projection models |
| `insight-resources` | Model/Action/Retrieval SPI、registry 与具体 provider |
| `insight-storage` | SQLite/PostgreSQL、Graph SQL、Artifact store 和 PostgreSQL live broker adapter |
| `insight-runtime` | catalog/deployment、leaf adapter、scheduler/worker pump、RunService 和 live response |
| `insight-api` | Axum HTTP、认证、请求/错误映射和 SSE transport |
| `insight-agent-platform` | 根兼容 facade、平台配置、进程 bootstrap 和 binary composition |

依赖只从高层 consumer 指向低层 owner。`storage` 与 `runtime` 不直接依赖彼此，而是通过
`engine`/`durable` 所有的 ports 在根 composition 中组合；workspace member 直接导入 owner crate，
不通过根 facade 形成反向依赖。

## 执行与恢复

admission 创建持久化 Run；scheduler 领取调度权威并根据已提交事实生成工作；Worker 获取
Activation lease、执行叶节点并用 fence 提交结果。进程退出或 lease 过期后，新 runtime 从数据库中
重新领取未闭合工作，不重放已经提交的 Activation。

暂停只阻止新的调度 admission；取消、超时和信号仍通过持久化控制意图收敛。redrive、fork、
migrate 与 continue-as-new 都基于固定 revision、checkpoint 和服务端推导的兼容证据工作。

## 公共响应与内部事实

Detached Run 通过查询接口读取 durable projection。Attached Run 使用 live-only SSE 投影实时内容，
不提供 `Last-Event-ID` replay；临时 delta 是有界、best-effort 数据，最终 durable terminal snapshot
才是交付权威。内部 execution ledger 不直接暴露为公共事件历史。

## 规范入口

- [DSL v3 持久化图执行架构规范](specifications/2026-07-18-dsl-v3-durable-graph-execution-design.md)
- [Response 实时流与 LLM 发布控制规范](specifications/2026-07-19-response-streaming-and-llm-publication-design.md)
- [Rust Workspace 与 Crate 边界拆分规范](specifications/2026-07-21-rust-workspace-crate-boundaries-design.md)
- [文档权威关系](../README.md#权威关系)
