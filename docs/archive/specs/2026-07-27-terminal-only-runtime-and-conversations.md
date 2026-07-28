# Terminal-only Runtime 存储与 Conversation 规范

日期：2026-07-27

状态：Implemented / capacity-qualified

验收日期：2026-07-28

目标版本：pre-1.0 storage semantics cutover

影响范围：`insight-runtime`、`insight-storage`、`insight-api`、平台配置、PostgreSQL/SQLite
schema、Helm、benchmark、HTTP/SSE 合同

> 归档说明：Phase 0、Gate A～D、最终静态验证链与完成定义 1～12 已全部关闭。本文件保存设计和
> 验收边界，当前可执行合同以 [`docs/current`](../../current/README.md) 为准。

关联证据：

- [Durable Runtime 50 活跃 Run 并发优化规范](2026-07-26-durable-runtime-50-active-runs-optimization.md)
- [Durable Runtime 50 并发优化与容量资格报告](../../../bench/reports/2026-07-26-durable-runtime-50-active-runs-optimized.md)
- [Terminal-only Runtime 与 Conversation 资格报告](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)
- [Terminal-only 验收与 WAL 资格](../qualifications/2026-07-28-terminal-only-qualification.md)
- [Terminal-only 默认模式 rollout 决策](../reviews/2026-07-28-terminal-only-default-rollout-decision.md)

## 1. 决策摘要

平台新增独立的 `terminal_only` 执行路径。该路径在进程内执行完整 Run，只持久化 Run admission
和 terminal result，不持久化中间 execution event、projection checkpoint、scheduler
checkpoint、claim/lease、public-event ledger 或 replay authority。

`terminal_only` 明确放弃以下能力：

- runtime/Pod/数据库重启后的 Run 恢复；
- 中间事件与 projection 历史重放；
- fork-from-checkpoint、redrive prefix reuse、migration 和 continue-as-new；
- 跨重启 timer、signal、human wait；
- 对外部副作用的 durable fence 与恢复期去重。

平台同时新增 Conversation。Conversation 是用户与 Agent 的多轮消息容器，不是执行 checkpoint。
Conversation 只持久化用户消息、最终 assistant 消息和低频摘要；SSE token/delta、Run 中间状态和
provider chunk 不写入 Conversation 表。

目标写入模型：

```text
一次无 Conversation 的 terminal-only Run
  = 1 条 run_admissions INSERT
  + 1 条 run_results INSERT

一次成功的 Conversation turn
  = 1 条 conversation_messages(user) INSERT
  + 1 条 run_admissions INSERT
  + 1 条 run_results INSERT
  + 1 条 conversation_messages(assistant) INSERT
```

当前完整 durable 引擎继续保留为 `full` 模式。第一阶段由 Deployment Revision 显式选择
`terminal_only`。兼容性和 WAL Gate 是资格证据，不自动授权平台默认值切换；2026-07-28 的独立
rollout 决议明确保留 `full`，未来任何默认值修改都必须形成新的 rollout 决策。两种模式不共享
同一组执行期 repository 写路径，禁止在现有 `DurableRepository` 的每个方法中散布
`if terminal_only`。

长期 Agent memory、自动事实提取、embedding 和向量检索不属于本规范。

## 2. 背景与问题

最终 v3 Gate D 在 10 arrival/s、2 小时窗口内完成 71,801 个 accepted Run，同时产生：

- 约 66.16GiB WAL，即约 0.94MiB/accepted Run；
- 约 9.40MiB/s WAL；
- 129 次 requested checkpoint，平均约 56 秒一次；
- 790 次超过 100ms 的普通 transaction/tuple lock wait；
- 最终约 33.4GiB 数据库，其中 `projection_checkpoints` 约 13GiB、
  `execution_events` 约 9.4GiB、`scheduler_checkpoints` 约 3.2GiB。

当前写放大不仅来自 PostgreSQL 配置，也来自执行合同本身：

1. 每次状态转移写 `execution_events`；
2. `execution_events.projection_ledger_batch` 保存 changed subject 的 canonical projection；
3. 同一 canonical projection 又拆成行写入 `projection_checkpoints`；
4. scheduler intent 再写入 `scheduler_checkpoints.fact_payload`；
5. public projection decision、receipt、outbox、delivery head 再次物化相同因果关系；
6. 多个 UNIQUE/FK/index 为每条逻辑事实增加额外 WAL；
7. 高频 checkpoint 让 full-page image 进一步放大物理 WAL。

增大 `max_wal_size` 可以减少 requested checkpoint 和 full-page-image 放大，但不能删除逻辑层每
Run 的大量写入。若业务允许进程失败后中断 Run，继续微调 event/checkpoint 编码不能获得数量级
收益，应从热路径删除完整 event-sourcing/recovery ledger。

## 3. 目标

### 3.1 存储目标

- 小型本地 action 的 `terminal_only` Run 平均 WAL 不超过 32KiB/accepted Run；
- 同一 10 arrival/s、2 小时 profile 的应用 WAL 不超过 2.2GiB；
- 相对 66.16GiB 基线降低至少 96%；
- 除大 payload/artifact 外，数据库物理增长不超过 16KiB/terminal Run；
- `terminal_only` Run 不向现有 execution/checkpoint/public ledger 表写入任何行；
- 正常成功 Run 的核心权威写入为两次 INSERT，不通过逐 Run heartbeat 更新状态；
- Conversation turn 不持久化 SSE chunk、token delta 或 provider event；
- retention 使用分区 drop 或有界批处理，不能周期性扫描全部历史。

WAL gate 必须在 warm-up 后根据 `pg_stat_wal.wal_bytes` 和
`pg_stat_statements.wal_bytes` 的差值计算。大输入、输出和 Artifact 字节单独报告，不得混入
小型 Run 的结构性写入指标。

### 3.2 产品目标

- 保留 `X-Request-ID` admission 幂等；
- Run 成功后仍可通过 `GET /v1/runs/{run_id}` 获取最终结果；
- Conversation 能可靠保存用户消息和最终 assistant 消息；
- Conversation context 使用“最新摘要 + 最近消息”，不会随历史长度无界增长；
- Attached SSE 保持 live-only，最终 frame 与已提交 terminal result 一致；
- full 模式现有恢复能力和 conformance tests 不回退；
- API 明确暴露 persistence/recovery capability，不能把 best-effort Run 描述为 durable recovery。

### 3.3 性能目标

- 50 个 terminal-only Run 同时到达时，不因数据库 checkpoint/event 写入形成队列；
- 10 arrival/s、2 小时 accepted closure 为 100%（不包含故障注入窗口）；
- lifecycle p95 ≤ 1s、p99 ≤ 3s，或不差于当前 v3 Gate D；
- terminal-only 模式不启动 WorkCoordinator recovery discovery；
- Conversation 最近 50 条消息查询 p95 ≤ 20ms（本地 PostgreSQL 基准）；
- Conversation turn 的 DB 写入次数不随生成 token 数量增长。

## 4. 非目标

- 不在本期实现长期 Agent memory；
- 不在本期实现 vector store、Redis conversation cache 或外部 MQ；
- 不保证 terminal-only Run 的 crash recovery；
- 不保证跨 runtime 实例迁移或 ownership 接管；
- 不保证外部副作用 exactly-once；
- 不保存可用于 forensic replay 的完整 execution event；
- 不支持从 terminal-only Run fork、redrive、migrate 或 continue-as-new；
- 不把 Conversation 当作 workflow state、scheduler checkpoint 或 tool scratchpad；
- 不在第一阶段删除 full 模式的现有 schema；
- 不在没有 benchmark 证据前将默认模式静默改为 terminal-only。

## 5. 术语与能力矩阵

### 5.1 术语

- **Conversation**：tenant/user 与一个 Agent 的多轮消息容器；
- **Message**：Conversation 中不可变的 user/assistant 消息；
- **Turn**：一个 user message、一个 Run 和可选的最终 assistant message；
- **Terminal-only Run**：执行中间状态只存在于进程内，数据库只保存 admission/result 的 Run；
- **Full Run**：使用现有 durable event/checkpoint/lease/fence 语义的 Run；
- **Owner lease**：用于判断 terminal-only Run 所属进程是否仍存活的实例级提示，不是 Run 恢复租约。

### 5.2 能力矩阵

| 能力 | `terminal_only` | `full` |
|---|---|---|
| admission 幂等 | 支持 | 支持 |
| 查询最终结果 | 支持 | 支持 |
| live SSE | 支持 | 支持 |
| 中间 event replay | 不支持 | 内部支持 |
| Pod 重启后恢复 | 不支持 | 支持 |
| durable timer/signal/human wait | 不支持 | 支持 |
| fork/redrive/migrate/continue-as-new | 不支持 | 支持 |
| durable task lease/fence | 不支持 | 支持 |
| terminal-only Conversation | 支持 | 支持 |
| 多 runtime ownership handoff | 不支持 | 支持 |

terminal-only v1 只允许一个 runtime replica。多副本 API 可以读取 Conversation 和 terminal result，
但创建、取消和查询 active Run 必须路由到唯一 runtime owner。解除单 runtime 限制需要单独规范，
不能通过重新引入 per-Run 数据库 heartbeat 达成。

## 6. 架构

```text
                       ┌───────────────────────────┐
POST conversation turn│ ConversationRepository    │
        ──────────────▶│ user message + admission  │
                       └─────────────┬─────────────┘
                                     │ commit
                                     ▼
                       ┌───────────────────────────┐
                       │ TerminalOnlyRunEngine     │
                       │ in-memory graph state     │
                       │ in-memory timer/retry     │
                       │ live SSE                  │
                       └─────────────┬─────────────┘
                                     │ terminal
                                     ▼
                       ┌───────────────────────────┐
                       │ one PostgreSQL transaction│
                       │ run result                │
                       │ assistant message         │
                       └───────────────────────────┘

process crash
    └── owner lease expires
          └── admission without result is reported as interrupted
                └── no replay, no ownership takeover
```

### 6.1 Package 边界

| Package | 新职责 |
|---|---|
| `insight-engine` | 暴露可在内存中推进的纯 scheduler state，不依赖 repository callback |
| `insight-runtime` | `TerminalOnlyRunEngine`、active Run registry、实例级 owner lease |
| `insight-storage` | `TerminalRunStore`、`ConversationStore` 的 PostgreSQL/SQLite adapter |
| `insight-api` | persistence capability、Conversation routes、错误映射和 SSE terminal barrier |
| 根 binary | 按 Deployment Revision 的 immutable persistence policy 选择 engine |

`TerminalRunStore` 与现有 `DurableRepository` 是两个独立 port。terminal-only engine 不允许调用：

- execution event append；
- projection checkpoint finalize/rebuild；
- scheduler recovery/claim；
- public-event decision/outbox/receipt；
- task/model durable claim；
- wait late-audit。

### 6.2 权威关系

terminal-only 模式下：

1. `run_admissions` 是“请求已接受且绑定到某 owner epoch”的权威；
2. 进程内 state 是 active execution 的临时权威；
3. `run_results` 是终态结果的唯一持久化权威；
4. Conversation user message 与 admission 在同一事务提交；
5. terminal result 与 assistant message 在同一事务提交；
6. owner lease 只用于推导 unresolved admission 已 interrupted；
7. 日志、trace、metrics 和 SSE 都不是恢复权威。

owner lease 同时承担单调终态保护：

- runtime 必须在 lease 续约失败、且剩余时间不足一个 heartbeat interval 时停止接收新 Run，并取消
  本地 active Run；
- terminal transaction 必须检查当前 `(instance_id,owner_epoch)` 仍匹配且 lease 未过期；
- lease 过期后旧 owner 的 terminal commit 必须失败，避免 API 已返回 interrupted 后又反转为
  succeeded；
- runtime 重启必须增加 owner epoch，不能沿用旧 epoch；
- PostgreSQL 重启清空 UNLOGGED owner 表后，runtime 必须丢弃旧内存 Run、注册新 epoch 后才能重新
  admission。

## 7. PostgreSQL Schema

以下为逻辑 schema。命名、ID CHECK 和 tenant FK 应沿用仓库现有约束风格。

### 7.1 Runtime instance lease

```sql
CREATE UNLOGGED TABLE terminal_runtime_instances (
    instance_id       text PRIMARY KEY,
    owner_epoch       bigint NOT NULL,
    endpoint          text NOT NULL,
    lease_expires_at  timestamptz NOT NULL,
    started_at        timestamptz NOT NULL,
    CHECK (owner_epoch >= 1)
);
```

选择 `UNLOGGED` 是刻意行为：

- 实例心跳不进入 WAL；
- PostgreSQL 重启后表被清空，所有 unresolved admission 自然解释为 interrupted；
- 该表不能被当成用户数据或恢复 authority；
- 每个实例最多每 10 秒更新一行，不按 Run 更新。

### 7.2 Admission

```sql
CREATE TABLE terminal_run_admissions (
    run_id                    text PRIMARY KEY,
    tenant_id                 text NOT NULL,
    request_id                text NOT NULL,
    agent_id                  text NOT NULL,
    definition_revision_id    text NOT NULL,
    deployment_revision_id    text NOT NULL,
    conversation_id           text,
    user_message_id           text,
    input_ref                 text,
    input_hash                text NOT NULL,
    selected_context_hash     text,
    owner_instance_id         text NOT NULL,
    owner_epoch               bigint NOT NULL,
    accepted_at               timestamptz NOT NULL,
    UNIQUE (tenant_id, request_id),
    CHECK (owner_epoch >= 1)
);
```

规则：

- INSERT 后不更新；
- 不保存 lifecycle、scheduler lease、projection version 或 next event seq；
- `conversation_id` 与 `user_message_id` 同为空或同为非空；
- 重试相同 `(tenant_id, request_id)` 返回原 admission；
- owner 已失效且没有 result 时，相同 request 仍返回原 interrupted Run，不自动重新执行；
- 客户端要重新执行必须使用新的 request ID，避免平台隐式重复外部副作用。

### 7.3 Terminal result

```sql
CREATE TABLE terminal_run_results (
    run_id              text PRIMARY KEY
                        REFERENCES terminal_run_admissions(run_id),
    terminal_state      text NOT NULL,
    response_id         text NOT NULL UNIQUE,
    output_ref          text,
    output_hash         text,
    error_code          text,
    usage_json          jsonb,
    started_at          timestamptz NOT NULL,
    terminal_at         timestamptz NOT NULL,
    CHECK (terminal_state IN ('succeeded','failed','cancelled','timed_out')),
    CHECK (terminal_at >= started_at)
);
```

规则：

- 每个 Run 最多 INSERT 一次；
- 不保存中间 lifecycle；
- 不为 `terminal_state` 建普通 B-tree 索引，避免每 Run 额外索引写；
- 大 output/usage 写 Artifact/object store，表中只保留 ref/hash；
- graceful shutdown 可以选择写 `cancelled`，非优雅 crash 不逐 Run 补写 `interrupted`；
- `interrupted` 是 admission + owner lease + missing result 推导出的视图状态。

### 7.4 Conversation

```sql
CREATE TABLE conversations (
    conversation_id  text PRIMARY KEY,
    tenant_id        text NOT NULL,
    user_id          text NOT NULL,
    agent_id         text NOT NULL,
    created_at       timestamptz NOT NULL,
    archived_at      timestamptz
);

CREATE SEQUENCE conversation_message_order_seq CACHE 1000;

CREATE TABLE conversation_messages (
    message_id         text PRIMARY KEY,
    conversation_id    text NOT NULL REFERENCES conversations(conversation_id),
    message_order      bigint NOT NULL
                       DEFAULT nextval('conversation_message_order_seq'),
    role               text NOT NULL,
    run_id             text,
    content_inline     jsonb,
    content_ref        text,
    content_hash       text NOT NULL,
    created_at         timestamptz NOT NULL,
    CHECK (role IN ('user','assistant')),
    CHECK ((content_inline IS NULL) <> (content_ref IS NULL)),
    UNIQUE (conversation_id, message_order)
);

CREATE INDEX idx_conversation_messages_page
    ON conversation_messages(conversation_id, message_order DESC);

CREATE UNIQUE INDEX uq_conversation_assistant_run
    ON conversation_messages(conversation_id, run_id)
    WHERE role='assistant';
```

设计选择：

- 不在 `conversations` 上维护 `next_seq` 或每消息更新 `updated_at`，避免 conversation head 热行；
- 外部不变量是每个 Conversation 内消息按 repository 串行提交顺序获得唯一且严格递增的
  `message_order`；cursor 只依赖该顺序和 `message_id`，并允许 ordinal 空洞；
- repository 的 PostgreSQL 写路径按 `conversation_id` 获取 transaction-scoped advisory lock，
  再通过现有 page index 求 `MAX(message_order)+1`；这会串行化同一 Conversation 的消息提交，
  但不会更新 Conversation head row。SQLite 依靠单 writer transaction 使用相同分配规则；
- schema 中的全局 cached sequence 仅保留为 direct-SQL/default 兼容兜底，不是 repository
  排序权威。连接池中的 PostgreSQL backend 会各自预留 sequence cache 区间，较晚的消息可能从较早
  backend 的低值缓存中取号，从而排在已经提交的高值消息之前，因此运行时不能用它表达稳定的
  Conversation 顺序；
- user/assistant message 不可修改；编辑产生新消息或显式 replacement 语义，留待后续规范；
- 小于配置阈值的内容内联一次，大内容只保存 object ref/hash；
- SSE chunk 和 reasoning delta 不进入该表；
- assistant message 必须绑定唯一 `run_id`；
- Conversation 归档不删除消息；删除由显式 privacy/retention workflow 执行。

### 7.5 Conversation summary

```sql
CREATE TABLE conversation_summaries (
    conversation_id       text NOT NULL REFERENCES conversations(conversation_id),
    through_message_order bigint NOT NULL,
    summary_ref           text NOT NULL,
    summary_hash          text NOT NULL,
    model_revision        text NOT NULL,
    created_at            timestamptz NOT NULL,
    PRIMARY KEY (conversation_id, through_message_order)
);
```

摘要规则：

- 只在消息数或 context token 达到阈值后异步生成；
- 每次生成一个不可变摘要，不更新旧大 JSON；
- 失败时回退到最近消息，不阻塞新的 Conversation turn；
- prompt context 为“最新有效摘要 + 摘要之后的最近消息”；
- 每个 Conversation 同时最多一个 summary job；
- 摘要不是 Agent memory，不能跨 Conversation 自动复用。

### 7.6 SQLite

SQLite 提供相同逻辑 schema 和单进程语义：

- `terminal_runtime_instances` 使用普通表，但数据库打开时清空；
- 不承诺多进程 owner lease；
- Conversation ordering、幂等和 terminal transaction 与 PostgreSQL parity；
- schema contract ID 必须与 PostgreSQL 同步变更。

## 8. Transaction 边界

### 8.1 创建独立 Run

1. 在内存中预留 active Run slot；
2. 校验 Deployment Revision 支持 terminal-only；
3. 一个事务 INSERT `terminal_run_admissions`；
4. commit 后把 Run 放入本地 ready queue；
5. commit 失败释放 slot；
6. commit 成功但执行前 crash，Run 最终推导为 interrupted。

### 8.2 创建 Conversation turn

一个事务完成：

1. 校验 Conversation tenant/user/agent ownership；
2. INSERT user `conversation_messages`；
3. INSERT `terminal_run_admissions`，引用该 message；
4. commit。

相同 `X-Request-ID` replay 必须返回同一个 message/run，不得追加第二条 user message。

### 8.3 完成 Conversation turn

一个事务完成：

1. INSERT `terminal_run_results`；
2. INSERT assistant `conversation_messages`；
3. commit；
4. commit 后发送 terminal SSE frame。

事务在 INSERT 前必须以 admission 中的 owner identity 检查 active owner lease。检查失败意味着
Run 已 interrupted，禁止迟到 terminal commit。如果终态事务暂时失败且 owner lease 仍有效，
runtime 在内存中有界重试。若进程在 commit 前崩溃，Run 变为 interrupted，不会出现只有
assistant message 或只有 terminal result 的半提交状态。

非 Conversation Run 只 INSERT terminal result。

## 9. API 合同

### 9.1 Persistence policy

Deployment Revision 增加不可变字段：

```yaml
execution:
  persistence_mode: terminal_only # terminal_only | full
```

Run 创建请求不能覆盖 Deployment Revision 的 persistence mode，避免同一个已部署 revision 在不同
请求下拥有不同故障语义。

Run DTO 增加：

```json
{
  "persistence_mode": "terminal_only",
  "recovery_capability": "none",
  "event_replay": false
}
```

对 terminal-only Run 调用不支持的接口时返回：

```json
{
  "error": {
    "code": "RUN_CAPABILITY_UNAVAILABLE",
    "message": "this run does not persist recovery checkpoints"
  }
}
```

HTTP 使用 `422`；不得复用容量 `429` 或状态冲突 `409`。

不支持的接口包括：

- `/pause`
- `/resume`
- `/signals/{signal_name}`
- `/redrive`
- `/fork`
- `/migrate`
- `/continue-as-new`

`DELETE /v1/runs/{run_id}` 在 owner 存活时是 best-effort 进程内取消；owner 已失效时 GET 已返回
interrupted，DELETE 幂等返回当前状态。

### 9.2 Conversation routes

新增：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/conversations` | 创建绑定 tenant/user/agent 的 Conversation |
| `GET` | `/v1/conversations/{conversation_id}` | 读取 Conversation metadata |
| `GET` | `/v1/conversations/{conversation_id}/messages` | cursor 分页读取消息 |
| `POST` | `/v1/conversations/{conversation_id}/messages` | 追加 user message 并创建 Detached Run |
| `POST` | `/v1/conversations/{conversation_id}/messages/stream` | 追加 user message 并创建 Attached SSE Run |
| `POST` | `/v1/conversations/{conversation_id}/archive` | 幂等归档 |
| `DELETE` | `/v1/conversations/{conversation_id}` | 执行受 retention/privacy 控制的删除 |

所有写请求要求 `X-Request-ID`。消息分页 cursor 编码 `(message_order,message_id)`，不得使用 offset。

### 9.3 SSE

- provisional delta 继续 live-only；
- 不写入 `conversation_messages`；
- terminal result + assistant message commit 后才能发送 terminal frame；
- 客户端断线后通过 Conversation messages 或 Run GET 获取最终结果；
- terminal-only 不提供 `Last-Event-ID`；
- Attached 连接断开后的取消仍是 best-effort，不形成 durable cancel intent。

## 10. Workflow 兼容性

Deployment validation 必须静态判断 terminal-only 是否兼容。

第一阶段允许：

- 同步 action；
- LLM/retrieval/tool 调用；
- 进程内短 retry；
- fork/join 等纯内存控制流；
- Attached/Detached 短 Run。

第一阶段默认拒绝：

- human task；
- external signal wait；
- 长时间 timer；
- continue-as-new；
- recovery fork/migration；
- 要求 durable effect fence 的 provider；
- 声明执行时间可能超过 owner lease/平台 graceful shutdown budget 的 workflow。

可选配置 `allow_volatile_waits` 默认必须为 `false`。即使未来开启，也必须在 API/Agent discovery 中
标记等待会在进程退出时丢失。

## 11. 故障语义

| 故障点 | 结果 |
|---|---|
| admission commit 前 | 没有 Run；slot 释放 |
| admission commit 后、开始执行前 | owner lease 失效后为 interrupted |
| 执行中 runtime crash | interrupted；不恢复 |
| 外部副作用成功、terminal commit 前 crash | 副作用可能已发生；Run interrupted |
| terminal transaction commit 后 crash | terminal result 与 assistant message均可查询 |
| terminal transaction 暂时失败 | owner lease 有效时内存有界重试；超过 budget 后 Run 仍可能 interrupted |
| owner lease 过期后旧进程恢复 | terminal commit 被拒绝，不允许 interrupted 反转为 terminal |
| SSE 发送失败 | 不影响已提交 terminal result；客户端 GET 校准 |
| PostgreSQL restart | UNLOGGED owner 表清空；所有无 result admission 为 interrupted |
| Conversation summary 失败 | 使用最近消息构造 context，不影响 turn |

平台不得自动重试 interrupted terminal-only Run。客户端使用新 request ID 重试时，外部副作用去重由
业务/provider idempotency key 负责。

## 12. 配置

建议配置：

```yaml
runtime:
  default_persistence_mode: full
  terminal_only:
    enabled: true
    owner_lease_seconds: 30
    owner_heartbeat_seconds: 10
    terminal_commit_retry_seconds: 10
    run_retention_days: 30
    allow_volatile_waits: false
    max_concurrent_runs: 50

conversations:
  enabled: true
  inline_content_max_bytes: 8192
  message_page_size_default: 50
  message_page_size_max: 200
  summary_trigger_messages: 30
  summary_trigger_tokens: 24000
  recent_context_messages: 20
  retention_days: 90
```

要求：

- strict unknown-field rejection；
- heartbeat 小于 lease 的三分之一或等于三分之一；
- inline/content/page/token/retention 均有上下界；
- Deployment Revision 未声明时，第一阶段使用平台默认 `full`；
- Gate A～D 通过只形成修改 quickstart/default 的前置证据，不自动授权改为 `terminal_only`；
- 默认值是否变化必须由独立 rollout 决策明确批准；本轮 Accepted decision 保持 `full`；
- Helm values、环境变量和配置文件使用同一字段名与默认值。

## 13. 数据生命周期

### 13.1 Run

- admissions/results 使用相同 retention bucket；
- 到期结果优先使用时间分区 drop；
- 第一版若未分区，删除必须按主键/时间索引有界批处理；
- retention 不扫描 execution/checkpoint 表；
- 大 input/output 通过 object store lifecycle 删除；
- unresolved admission 在 owner 失效后进入 interrupted retention，而不是补写 result。

### 13.2 Conversation

- Conversation retention 独立于 Run retention；
- 删除 Conversation 必须删除 messages、summaries 和关联 object；
- terminal result 可以保留，但删除后不得再暴露已删除 message 内容；
- privacy delete 的完成 receipt 和失败重试可以低频持久化；
- 不允许以“未来 Agent memory”为由无限保留 Conversation；
- object store content 应支持 tenant-scoped encryption 和删除。

### 13.3 索引原则

- 热表只保留 API/幂等所需索引；
- 不为低选择性的 lifecycle/role 单列建索引；
- Conversation 使用 cursor index，不使用 offset pagination；
- 大历史时间查询优先 BRIN 或分区，不增加每行多个宽 B-tree；
- 每新增索引必须提交 `pg_stat_statements.wal_bytes` 和查询收益证据。

## 14. 可观测性

新增固定低基数指标：

- `terminal_run_active`
- `terminal_run_admissions_total`
- `terminal_run_results_total{terminal_state}`
- `terminal_run_interrupted_total{reason}`
- `terminal_run_terminal_commit_retries_total`
- `terminal_runtime_owner_lease_expired_total`
- `conversation_messages_total{role}`
- `conversation_summary_jobs_total{result}`
- `conversation_context_messages`
- `conversation_context_tokens`

禁止使用 run、conversation、message、tenant、user ID 作为 metrics label。

Benchmark 保存：

- `pg_stat_wal` before/after；
- `pg_stat_statements` 按 WAL 排名前 30 SQL；
- table/index size before/after；
- checkpoints requested/timed；
- fsync/write latency；
- 每 accepted Run 的 WAL、关系增长和事务数；
- Conversation 每 turn 写入语句数；
- process RSS、active Run、terminal commit retry；
- failure test 中 interrupted/terminal 的精确数量。

## 15. 实施阶段

### Phase 0：基线拆分

- 在干净 PostgreSQL 上重新运行现有 10 rps profile；
- 启用 `pg_stat_statements.track_wal` 证据；
- 按 SQL/table/index 拆分 0.94MiB/Run；
- 将 payload/artifact 字节与结构性 WAL 分开；
- 固定 `action_demo` 和 Conversation message fixture。

完成标准：能解释至少 95% WAL 来源。

### Phase 1：轻量 schema 与 ports

- 增加 terminal runtime instance/admission/result schema；
- 增加 Conversation/message/summary schema；
- PostgreSQL/SQLite parity；
- 新增 `TerminalRunStore` 与 `ConversationStore`；
- repository contract、约束、索引和 schema layout tests；
- schema contract ID 更新。

完成标准：事务边界、幂等、消息顺序和 tenant isolation tests 全绿。

### Phase 2：TerminalOnlyRunEngine

- 新建独立 engine，不调用 `DurableRepository`；
- 进程内 active registry、permit 和 cancellation；
- owner heartbeat/lease；
- terminal commit retry；
- crash 后 interrupted 推导；
- compatibility validator；
- 不支持 capability 的 typed error。

完成标准：terminal-only Run 对所有现有 event/checkpoint ledger 表的写入 delta 为 0。

### Phase 3：Conversation API

- 创建、读取、消息分页、Detached/Attached turn、archive/delete；
- user message + admission 原子提交；
- result + assistant message 原子提交；
- idempotent replay；
- live SSE 与 GET/messages 校准；
- context assembler 和异步 summary。

完成标准：任意 fault point 不产生孤立 assistant message 或重复 user message。

### Phase 4：迁移与 rollout

- Deployment Revision 增加 immutable persistence policy；
- existing deployment 默认 `full`；
- 新建兼容 agent opt-in terminal-only；
- 不迁移正在运行的 full Run；
- full/terminal-only 指标和 API 明确区分；
- Gate 完成不自动修改平台默认；任何切换都需要新的独立 rollout 决策；
- 第一版不删除旧表和 full engine。

完成标准：关闭 terminal-only feature flag 后，现有行为完全恢复。

### Phase 5：数据生命周期

- Run 和 Conversation retention；
- object content lifecycle；
- 分区或有界删除；
- privacy delete；
- aged Conversation 查询；
- vacuum/WAL 证据。

完成标准：历史增长不会使 active/admission/message-page 查询退化为全表扫描。

## 16. 测试与验收

### 16.1 单元/配置

- persistence mode parse/serialize/strict rejection；
- Deployment compatibility；
- owner lease 状态推导；
- context selection 与 token budget；
- summary threshold；
- cursor encode/decode；
- inline/ref exactly-one；
- unsupported capability mapping；
- metrics label 低基数。

### 16.2 Repository

- admission request idempotency；
- user message + admission 原子性；
- result + assistant message 原子性；
- duplicate terminal commit；
- duplicate assistant message；
- Conversation ordering 与 cursor pagination；
- tenant/user/agent isolation；
- owner table reset 后 interrupted；
- retention 与 privacy delete；
- PostgreSQL/SQLite schema parity。

### 16.3 Real-process failure

- admission commit 后、执行前 kill；
- LLM/action 执行中 kill；
- external effect 后、terminal commit 前 kill；
- terminal commit 后、SSE 前 kill；
- PostgreSQL restart；
- summary worker crash；
- graceful shutdown；
- 相同 request ID replay；
- 新 request ID 显式 retry。

每个场景必须断言是否允许 external effect 重复，不能只检查 HTTP 状态。

### 16.4 Gate A：写路径

对一个成功小型 action Run 断言：

- `terminal_run_admissions` +1；
- `terminal_run_results` +1；
- 所有现有 execution/checkpoint/scheduler/public ledger 表 +0；
- 无 Conversation 时核心事务为 2 次；
- Conversation turn 时 message 为 2 条；
- SSE frame 数量不影响数据库行数。

### 16.5 Gate B：2 小时 WAL

同现有 v3 Gate D：

- 10 arrival/s；
- 2 小时；
- 单 runtime；
- 最大 50 active；
- 相同 `action_demo`；
- warm-up 后重新采样 WAL；
- 不注入 runtime restart，因为 terminal-only 的预期结果就是中断。

通过门槛：

| 指标 | 门槛 |
|---|---:|
| accepted closure | 100% |
| scheduled success | ≥99.9% |
| completed throughput | ≥9 run/s |
| lifecycle p95/p99 | ≤1s / ≤3s |
| WAL/accepted Run | ≤32KiB |
| 2h WAL | ≤2.2GiB |
| 结构性 DB 增长/Run | ≤16KiB |
| requested checkpoint | 0 |
| deadlock / temp spill / OOM | 0 |
| existing ledger row delta | 0 |

若 payload 大小使 32KiB 门槛不适用，必须同时报告：

```text
structural WAL
payload/object WAL
total WAL
```

不得通过关闭 `fsync`、`full_page_writes`、`synchronous_commit` 或使用 UNLOGGED
admission/result 表来通过 Gate。

### 16.6 Gate C：故障语义

- 在 50 active Run 中 kill runtime；
- kill 前已提交 result 的 Run 仍可查询 terminal；
- kill 时无 result 的 admission 全部变为 interrupted；
- 不出现自动 recovery；
- 不出现第二个 terminal result；
- 同 request ID 不重新执行；
- 新 request ID 可以显式重新执行；
- fault 后 Conversation 不产生孤立 assistant message。

该 Gate 的目标不是“零中断”，而是证明中断语义准确、可预测且没有被伪装成恢复成功。

### 16.7 Gate D：Conversation

- 100 个 Conversation，每个 100 个 turn；
- user/assistant 顺序正确；
- idempotent retry 不重复消息；
- cursor 分页无遗漏/重复；
- context 只包含最新摘要和配置数量的最近消息；
- token/chunk 数量从 1 倍增加到 10 倍时，DB message 行数不变；
- aged 1,000,000 messages 下最近 50 条查询 p95 ≤20ms；
- summary failure 不阻塞 turn；
- privacy delete 后内容不可读取。

## 17. 风险与缓解

| 风险 | 后果 | 缓解 |
|---|---|---|
| 用户误以为 terminal-only 可恢复 | 重启后 Run 丢失 | DTO、Agent discovery、部署配置显式 capability |
| 外部副作用后 crash | retry 可能重复 | provider idempotency key；默认不自动 retry |
| 长等待占内存 | active slot 长期占用 | terminal-only validator 默认拒绝 durable wait |
| 单 runtime 限制 | 无水平接管 | 第一版明确限制；需要时单独设计 owner routing |
| terminal commit 失败后 crash | 结果丢失 | 有界内存重试；仍明确可能 interrupted |
| Conversation 无界增长 | 存储和 context 成本上升 | summary、cursor、retention、object lifecycle |
| summary 错误 | 后续上下文偏差 | 保留 source range/hash；失败回退最近消息 |
| 两套 engine 复杂度 | 维护成本增加 | 独立 ports/paths；禁止 repository 内到处分支 |
| 为过 Gate 关闭 PG durability | 数据不真实 | 验收禁止关闭 fsync/full_page_writes/synchronous_commit |
| 未来 memory 混入 message | 隐私和写放大 | Agent memory 明确为非目标，需独立规范 |

## 18. 被拒绝的替代方案

### 18.1 只提高 `max_wal_size`

可以减少 requested checkpoint 和 full-page image，但不能删除每 Run 的逻辑 event/checkpoint/index
写入，预计不能获得 96% 降幅。

### 18.2 只把 projection checkpoint 改成增量

当前 checkpoint 已按 changed subject 生成，但同一 canonical projection 同时存在于
`projection_ledger_batch` 和 `projection_checkpoints`。去重会有收益，仍保留 execution、
scheduler、public-event 和多索引写入，无法达到目标。

### 18.3 使用 UNLOGGED event/checkpoint

UNLOGGED 只减少 WAL，仍产生 heap/index 写入和 vacuum 成本；重启后 ledger 不完整又无法安全恢复，
处于“既不轻量也不 durable”的中间状态。

### 18.4 把 event/checkpoint 移到 Redis/MQ

只是把磁盘写入和一致性问题搬到另一个系统，仍需处理双写、重放、保留和故障恢复，不符合
terminal-only 的简化目标。

### 18.5 每个 token 保存为 Conversation message

会让生成长度直接转化为行数、索引和 WAL。Conversation 只保存最终 assistant message，实时内容走
live SSE。

## 19. 完成定义

只有同时满足以下条件，才能把本规范改为 Implemented：

1. TerminalRunStore、ConversationStore、PostgreSQL/SQLite schema parity 完成；
2. TerminalOnlyRunEngine 与现有 DurableRunEngine 完全分离；
3. persistence capability、unsupported API 和故障语义写入公开合同；
4. user message + admission、result + assistant message 原子性 tests 全绿；
5. full 模式全部 conformance tests 无回退；
6. Gate A～D 全部通过；
7. 2 小时 WAL ≤2.2GiB 且 ≤32KiB/accepted Run；
8. 没有通过关闭 PostgreSQL durability 参数伪造低 WAL；
9. `docs/current`、配置、Helm、API baseline 和运维文档同步；
10. 明确记录 terminal-only 不支持恢复、事件重放和 durable wait；
11. Agent memory 仍未被隐式加入 Conversation/Run 热路径；
12. 默认值是否从 `full` 切到 `terminal_only` 由独立 rollout 决策记录确认。

验收结论（2026-07-28）：上述 1～12 项全部通过。Phase 0、Gate A～D 的正式 aggregate 与
不可变镜像身份记录在
[资格报告](../../../bench/reports/2026-07-27-terminal-only-runtime-and-conversations-qualified.md)；
当前 API、架构、运维和复现合同已同步到
[`docs/current`](../../current/README.md)。独立
[rollout 决策](../reviews/2026-07-28-terminal-only-default-rollout-decision.md)
接受“默认保持 `full`、兼容 immutable revision 显式 opt-in、不迁移现有 revision/Run”；
Gate 通过不改变该决定。

本规范完成后的可靠表述应是：

> 平台支持低写入的 terminal-only Run 和持久化 Conversation。terminal-only Run 只持久化
> admission、最终结果、用户消息和最终 assistant 消息；进程失败会中断未完成 Run，平台不提供
> checkpoint 恢复或中间事件重放。需要 durable wait、recovery、fork/redrive/migration 的部署
> 必须继续使用 full 模式。
