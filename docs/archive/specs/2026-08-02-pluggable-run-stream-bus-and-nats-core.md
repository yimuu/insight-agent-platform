# Run Stream 可插拔实时消息总线与 NATS Core 优化规范

| 属性 | 值 |
|---|---|
| 状态 | Implemented / Verified |
| 日期 | 2026-08-02 |
| 资格报告 | `bench/reports/2026-08-02-run-stream-nats-core-qualified.md` |
| 平台协议 | `run-stream/v1`（公共 wire 不变） |
| 变更类型 | Breaking Configuration / Runtime Transport / Database Pressure / Deployment Topology / Security / Qualification |
| 影响范围 | `insight-engine`、`insight-runtime`、`insight-storage`、根 composition、平台配置、Helm、测试与文档 |

> 本文目标已完成实现与资格验收；正式结果见
> [Run Stream 可插拔总线与 NATS Core 资格报告](../../../bench/reports/2026-08-02-run-stream-nats-core-qualified.md)。
> 发生冲突时，当前 schema、实现、conformance tests 和 [`docs/current`](../../current/README.md) 优先。

## 1. 决策摘要

平台把 Run 的权威状态、实时 observation 传输和 SSE 连接缓冲明确拆成三层：

```text
PostgreSQL / SQLite durable store
  └─ Run lifecycle、checkpoint、effect、terminal snapshot、snapshot hash

Live Run Stream Bus
  └─ output delta、tool activity、retrieval observation、gap、seal

SSE connection-local dispatcher
  └─ connection sequence、keepalive、write timeout、terminal calibration、EOF
```

本规范采用以下决定：

1. **PostgreSQL 继续是生产 Run 的唯一 durable authority。** NATS 或内存总线均不保存 Run
   lifecycle、checkpoint、lease、fence、effect、terminal snapshot 或审计记录；
2. **实时消息总线只承载 live-only observation。** 总线故障可以造成 delta 缺口，但不能改变 Run
   的 durable 结果；
3. **交付两个一等后端：`in_memory` 与 `nats_core`。** 当前受支持的单 Runtime 拓扑默认使用
   `in_memory`；需要跨 Runtime 投递时必须使用 `nats_core`；
4. **删除 `postgres_notify` Run Stream backend。** 不保留配置 alias、双发或 shadow path；
   PostgreSQL `LISTEN/NOTIFY` 仍可继续用于 scheduler/public-event 的可丢失唤醒 hint，但不再传输
   Run Stream 正文；
5. **`nats_core` 使用 Core NATS，不使用 JetStream。** 它的 at-most-once、无存储、无 replay
   语义与现有 Attached SSE live-only 合同一致；
6. **每个 Runtime 只创建一个共享 NATS client connection。** 每个本地 Attached Run 创建一个
   普通 subject subscription 和一个本地有界队列；SSE 不直接连接 NATS；
7. **Run Stream subscription 禁止使用 NATS queue group。** 同一 Run 的消息必须送到每个持有该
   Run SSE 的 Runtime；queue group 只会随机选择一个成员，语义不匹配；
8. **NATS publication 不逐帧 flush、request/reply 或等待 server ack。** durable Worker 只做本地
   非阻塞有界入队，网络发送由独立 pump 完成；
9. **订阅建立必须先于 Attached Run admission。** NATS `SUB + flush/PONG` 完成后才允许 durable
   创建/接纳 Run，避免首批 live frame 早于远端 interest；
10. **terminal snapshot 不进入 NATS。** durable terminal transaction、terminal barrier、gap
    calibration 和最终 `GET /v1/runs/{run_id}` 权威保持不变；
11. **公共 `run-stream/v1` 不升级版本。** 本规范只替换内部 transport，不增加 replay、resume、
    `Last-Event-ID` 或新的公共事件；
12. **NATS 生产连接必须认证、加密并受 subject ACL 限制。** 凭据只以 secret reference 注入，
    不进入 ConfigMap、日志、trace、metric label 或错误正文；
13. **本优化必须以数据库连接数和 SQL 量验收。** `in_memory`/`nats_core` 下，每条 SSE 不得创建
    PostgreSQL listener，每个 live frame 不得执行 `pg_notify` SQL。

## 2. 当前基线与问题定义

### 2.1 当前实现

当前配置是：

```yaml
runtime:
  run_stream:
    broker: postgres_notify
```

快速启动使用 `in_process`，生产配置和 Helm 则使用 `postgres_notify`。PostgreSQL adapter 的当前
数据路径是：

```text
Worker
  -> per-Run outbound queue
  -> SELECT pg_notify(channel, encoded_frame)
  -> PostgreSQL
  -> per-Run PgListener
  -> per-Run inbound queue
  -> SSE dispatcher
```

当前实现有两个与流量线性增长的数据库成本：

1. `subscribe(run_id)` 为每个 Attached Run 调用 `PgListener::connect_with(pool)` 并持有该 listener；
2. outbound pump 为每个 publication、gap 或 seal 执行一次 `SELECT pg_notify($1, $2)`。

因此，若同时有 `S` 条 Attached SSE、期间产生 `F` 个 live frame，Run Stream transport 会额外产生：

```text
O(S) 个 PostgreSQL listener connection
O(F) 次 PostgreSQL publish query
```

这些连接和查询不提供 durable recovery，却与真正的 claim、checkpoint、commit、terminal projection
竞争数据库连接池、CPU 和调度时间。

### 2.2 SSE 本身为什么不应占数据库连接

SSE 需要长期持有的是 HTTP socket、一个 async task 和有界内存队列，不是数据库事务。正常 Operation
执行仍采用短事务：

```text
claim transaction -> release connection
external LLM/MCP/Action call
commit transaction -> release connection
```

实时 delta 也不需要持久化。最终一致性由 durable terminal snapshot 保证；客户端收到 terminal 后
用 `run.output`、`run.tool_results`、`run.retrievals`、`run.usage` 和 `run.status` 校准 provisional UI。

### 2.3 优化后预期变化

| 成本 | `postgres_notify` 当前实现 | `in_memory` 目标 | `nats_core` 目标 |
|---|---:|---:|---:|
| Run Stream PostgreSQL listener | 每 SSE 1 个 | 0 | 0 |
| live frame PostgreSQL SQL | 每帧 1 次 | 0 | 0 |
| Runtime 到消息系统的长连接 | PostgreSQL，随 SSE 增长 | 0 | 每 Runtime 1 个 NATS connection |
| 本地 Run queue | 每 Run 有界 | 每 Run 有界 | 每 Run 有界 |
| durable replay | 无 | 无 | 无 |
| terminal authority | PostgreSQL/SQLite | PostgreSQL/SQLite | PostgreSQL/SQLite |

该变化释放数据库容量，但不改变以下瓶颈：

- Provider、MCP、Action 或 Retrieval 的外部延迟；
- `max_concurrent_operations` 与 per-Run operation permit；
- durable commit、terminal projection 和 `GET Run` 查询；
- HTTP connection、Runtime CPU、内存和网络出口；
- NATS 服务本身的容量与可用性。

因此，本规范不直接宣称新的 Run 并发数字；容量结论必须通过第 16 节资格测试产生。

## 3. 规范效力与既有合同

本规范原地扩展以下已交付合同，不替换它们：

- [Run Stream v1 统一事件模型](2026-07-29-run-stream-v1-unified-event-model.md)；
- [Response Streaming 与 LLM Publication](2026-07-19-response-streaming-and-llm-publication-design.md)；
- [Durable Runtime 50 活跃 Run 并发优化](2026-07-26-durable-runtime-50-active-runs-optimization.md)。

以下既有规则保持有效：

1. Attached SSE 是 live-only，不接受或解释 `Last-Event-ID`，不提供历史 replay 或 resume；
2. `sequence_number` 是 connection-local，由 SSE dispatcher 分配，不是 NATS sequence；
3. output item 使用 item/attempt identity 与 item-local sequence 进行保序、去重和 gap 检测；
4. tool progress、retrieval observation 和 output delta 都可以丢失；
5. observation 丢失不得失败 durable Worker；
6. output 缺口必须通过 `run.stream.gap` 或 terminal unknown-tail calibration 显式收敛；
7. terminal snapshot 是单一 durable public Run payload，发送后立即 EOF；
8. terminal-only 不因实时流增加逐 token、逐 progress 或逐 tool event 数据库写入；
9. `run.stream.error` 结束流，但不把 Run 标记为 failed；客户端必须查询权威 Run；
10. reasoning、secret、Provider 原始 body、未授权工具结果不得进入 live bus。

## 4. 目标与非目标

### 4.1 目标

1. 从 Run Stream live path 删除 per-SSE PostgreSQL connection；
2. 从 Run Stream live path 删除 per-frame PostgreSQL query；
3. 让当前单 Runtime 部署使用零外部依赖的 `in_memory` backend；
4. 为未来 `Runtime A 执行 Worker、Runtime B 持有 SSE` 提供 `nats_core` fan-out transport；
5. 保持 Worker publication 非阻塞、有界、可丢失；
6. 保持 per-Run 隔离、控制消息优先级、gap、seal 和 terminal barrier；
7. 为 NATS 断连、重连、slow consumer、错误 subject、错误 wire 和过大 payload 定义闭合行为；
8. 建立严格配置、secret、TLS、ACL、readiness、metric、测试和 rollout 合同；
9. 保持公共 API、schema、event set 和客户端状态机不变；
10. 为未来增加其他 live-only backend 保留封闭扩展点。

### 4.2 非目标

- 不启用或声明生产多 Runtime scheduler；
- 不改变当前 durable claim、lease、fence、retry、checkpoint 或 terminal transaction；
- 不用 NATS 承载 task queue、scheduler work、MCP task、Provider operation 或 public-event outbox；
- 不实现 JetStream、Kafka、RabbitMQ、Redis Pub/Sub 或 Redis Streams；
- 不实现消息持久化、ack、重试投递、consumer offset、replay 或 exactly-once；
- 不把 SSE 改成 WebSocket；
- 不允许同一 Run 建立多个公共 Attached subscriber；
- 不把 terminal snapshot 复制到 NATS；
- 不由平台 Helm chart 管理生产 NATS cluster 生命周期；
- 不因本规范调大 operation permits、数据库 pool 或 Runtime replica 数；
- 不保留 `postgres_notify` Run Stream 兼容模式。

`redis_pubsub` 可以在未来形成独立规范，但必须复用本文的 backend-neutral port、failure semantics 和
qualification suite，不能通过扩展字符串枚举绕过严格配置。

## 5. 术语与权威边界

### 5.1 `LiveRunStreamBroker`

现有 Rust port 名称可以保留为 `LiveRunStreamBroker`，本文中的“Run Stream Bus”指该 port 及其具体
adapter。它不是 durable message broker，也不是执行 journal。

### 5.2 Publication

Worker 产生的一个安全、已授权、live-only observation。Publication 包含 source identity、
item-local sequence 和 closed payload，不包含 SSE connection sequence。

### 5.3 Seal

某个公开 output item/attempt 的 live producer 水位线。Seal 让 terminal barrier 判断已完整接收，或
把缺失尾部收敛为 gap。Seal 不是 durable terminal。

### 5.4 Gap

某个 provisional output item 的已知缺口或未知尾部。Gap 不表示 durable event 缺失，也不表示 NATS
有 replay offset。

### 5.5 Topology

- `single_runtime`：同一受支持 deployment 中，Attached admission、Worker publication 和 SSE
  dispatcher 保证在同一个 Runtime process；
- `distributed`：publisher 和 subscriber 可能位于不同 Runtime process，必须使用 shared backend。

本文交付 NATS transport 能力，但不自动把当前 Helm Deployment 升级成多个可并行执行的完整 Runtime。

## 6. 目标架构

### 6.1 单 Runtime

```text
Worker ──non-blocking publish──> InMemory RunQueue ──recv──> SSE dispatcher
                                            │
Durable terminal store ─────────────────────┴──────> terminal calibration + EOF
```

`in_memory` 不序列化、不访问网络、不访问数据库。它继续使用 engine-owned `RunQueue` 的 body/control
容量、byte limit、gap、seal、dedupe 和 close 语义。

### 6.2 跨 Runtime

```text
Runtime A                                      Runtime B
Worker                                         Attached SSE
  │                                                ▲
  ▼                                                │
per-Run outbound queue                        per-Run inbound queue
  │                                                ▲
  └──── shared NATS client ── Core NATS ── shared NATS client
             1 connection                         1 connection

PostgreSQL durable Run/terminal authority is independent of the live path
```

NATS adapter 只负责 publication/gap/seal。Runtime B 继续从 durable terminal path 获得最终 snapshot；
它不能等待一条“NATS terminal message”决定 Run 已结束。

### 6.3 三层背压

目标实现必须保持三个独立的有界层：

1. **Producer layer**：per-Run body/control queue，保护 durable Worker；
2. **Transport layer**：Runtime-wide pending message/byte limit，保护 NATS client connection；
3. **Connection layer**：per-Run inbound body/control queue 和 SSE write timeout，隔离慢客户端。

HTTP 客户端变慢时，NATS subscription task 必须继续快速 drain NATS message，并把 overload 转换为本地
gap/drop；不能让一个 SSE 慢客户端把整个共享 NATS connection 变成 slow consumer。

## 7. 配置合同

### 7.1 Clean-cut 结构

旧结构：

```yaml
runtime:
  run_stream:
    broker: postgres_notify
```

目标结构使用严格 tagged object：

```yaml
runtime:
  run_stream:
    topology: single_runtime
    broker:
      type: in_memory
    body_queue_capacity: 256
    control_queue_capacity: 32
    max_frame_bytes: 4096
    max_item_bytes: 4194304
    max_run_bytes: 16777216
    terminal_barrier_timeout: 2s
    outbound_write_timeout: 10s
```

以下旧值必须作为未知/非法配置拒绝：

- `broker: in_process`；
- `broker: postgres_notify`；
- `broker: nats_core` 字符串简写；
- 未知 backend type；
- backend object 中的未知字段。

没有旧客户端或配置需要兼容，因此不提供 serde alias、环境变量 fallback 或 warning-only 迁移。

### 7.2 `nats_core` 示例

```yaml
runtime:
  run_stream:
    topology: distributed
    broker:
      type: nats_core
      servers:
        - nats://nats-0.nats.svc:4222
        - nats://nats-1.nats.svc:4222
        - nats://nats-2.nats.svc:4222
      namespace: prod_cn1
      credentials_env: INSIGHT_RUN_STREAM_NATS_CREDENTIALS
      tls:
        required: true
        root_certificates:
          - /var/run/secrets/insight-nats/ca.pem
        client_certificate: null
        client_private_key: null
      connect_timeout: 5s
      subscription_ready_timeout: 2s
      reconnect_min_delay: 100ms
      reconnect_max_delay: 5s
      max_pending_messages: 4096
      max_pending_bytes: 16777216
      drain_timeout: 5s
    body_queue_capacity: 256
    control_queue_capacity: 32
    max_frame_bytes: 65536
    max_item_bytes: 4194304
    max_run_bytes: 16777216
    terminal_barrier_timeout: 2s
    outbound_write_timeout: 10s
```

`credentials_env` 的值是环境变量名称；环境变量内容是 NATS user credentials 文本。配置文件只保存
reference，不保存 credential value。实现不得在 `Debug`、错误或启动日志中打印解析后的 credentials。

### 7.3 Backend 与 topology 矩阵

| topology | `in_memory` | `nats_core` |
|---|---|---|
| `single_runtime` | 允许，当前默认 | 允许，用于提前验证或统一基础设施 |
| `distributed` | 启动 fail closed | 允许，且必须通过 shared readiness |

额外约束：

- SQLite 只允许 `single_runtime`；
- 当前 Helm 仍只资格化 `replicaCount: 1` 和 `Recreate`；本文不删除其他单 Runtime gate；
- Helm 若未来支持 distributed role/replica，必须在 render 阶段拒绝 `in_memory`；
- 直接运行 binary 的 operator 必须保证声明的 topology 与真实编排一致；错误声明
  `single_runtime` 属于部署配置错误，不能被解释为允许静默丢失跨进程 frame；
- Agent publication、deployment 和 Run admission gate 必须读取 topology + backend capability，不能继续
  使用“production 一律要求 shared broker”的旧判断；
- `nats_core` 报告 `Shared` capability；`in_memory` 报告 `SingleProcess` capability。

### 7.4 NATS 字段校验

`nats_core` 必须满足：

1. `servers` 为 1～16 个去重地址；禁止 URL userinfo、query、fragment 和 inline password/token；
2. `namespace` 匹配 `^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$`，长度为 1～64；
3. 所有 duration 大于零；
4. `reconnect_min_delay <= reconnect_max_delay <= 30s`；
5. `max_pending_messages` 为 1～1,000,000；
6. `max_pending_bytes` 不小于 `max_frame_bytes`，且不大于 1 GiB；
7. `max_frame_bytes` 为 256～65,536，并且不超过 NATS server INFO 的 `max_payload`；
8. production 要求 `credentials_env` 非空且 `tls.required: true`；
9. `client_certificate` 和 `client_private_key` 必须同时为空或同时存在；
10. certificate/key 字段只接受文件路径，private key value 不得写入 YAML；
11. production 不接受跳过 hostname/certificate 校验的选项；
12. subject validation 不得关闭；
13. NATS `no_echo` 不得启用，因为同一 combined Runtime 需要收到自己发布到已订阅 Run subject 的消息；
14. reconnect 尝试在进程生命周期内不设次数上限，但每次退避必须有 jitter 且受上述上限约束；
15. initial connect 仍受 `connect_timeout` 限制，不能因无限 reconnect 让启动永久挂起。

### 7.5 默认值与 Helm

目标默认：

- `config/platform.yaml`：`topology: single_runtime` + `type: in_memory`；
- `config/platform.quickstart.yaml`：同上；
- 当前 Helm：`replicaCount: 1`、`topology: single_runtime`、`type: in_memory`；
- qualification values：增加独立 `nats_core` profile；
- Helm 不自动部署生产 NATS cluster；operator 提供 server endpoints、credentials Secret 和 TLS files；
- Helm 必须把 credentials 从既有 Kubernetes Secret 注入环境变量，不能渲染到 ConfigMap；
- 切换到 `nats_core` 时，template 必须校验 Secret name/key、namespace、server list 与 TLS mount。

## 8. Backend-neutral Rust 边界

### 8.1 Port

`LiveRunStreamBroker` 的核心合同保持：

```rust
#[async_trait]
pub trait LiveRunStreamBroker: Send + Sync {
    fn deployment_capability(&self) -> LiveRunStreamBrokerCapability;
    async fn check_readiness(&self, timeout: Duration) -> Result<(), LiveRunStreamBrokerError>;
    async fn shutdown(&self, grace: Duration) -> Result<(), LiveRunStreamBrokerError>;
    async fn subscribe(&self, run_id: RunId)
        -> Result<Box<dyn LiveRunStreamSubscriber>, LiveRunStreamBrokerError>;
    fn publish(&self, publication: LiveRunStreamPublication) -> LiveRunStreamPublishOutcome;
    fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome;
    fn close_run(&self, run_id: &RunId) -> LiveRunStreamCloseOutcome;
}
```

约束：

- `publish`、`seal` 和 `close_run` 保持同步、非阻塞；
- 它们只能操作内存队列/计数器，不能执行网络或数据库 I/O；
- `subscribe` 可以等待 NATS subscription readiness，但必须有界；
- adapter error 使用 backend-neutral 公共错误分类，内部 source chain 不进入 HTTP body；
- Worker 不得因 transport publish outcome 把 durable Operation 标记失败；
- `RunQueue` 的 ordering/gap/seal authority 继续位于 engine，不在 NATS adapter 复制第二套规则。

可以在代码中把 concrete 类型命名为 `InMemoryLiveRunStreamBroker` 与
`NatsCoreLiveRunStreamBroker`。本规范不要求为了“Bus”措辞对所有既有 trait/call site 做无收益重命名。

### 8.2 Composition 解耦

当前 `initialize_repository_and_live_run_stream(history, run_stream)` 把 repository 与 broker 初始化绑定。
目标 composition 必须拆为：

```text
initialize_repository(history)
initialize_live_run_stream_bus(run_stream, deployment_contract)
```

因此：

- `nats_core` adapter 不接收 `PgPool`；
- `in_memory` adapter 不接收 repository；
- `insight-storage` 删除 `PostgresLiveRunStreamBroker` 与 PostgreSQL-specific live wire；
- backend-neutral codec 和 NATS adapter 位于 `insight-runtime` 或单独的 runtime adapter module；
- `insight-engine` 只拥有 closed types、queue semantics 和 port；
- 根 crate 解析 secret/TLS path 后构造 adapter；
- `async-nats` 以 workspace dependency 固定，由 lockfile 固定实际版本和 feature set。

### 8.3 `postgres_notify` 删除边界

必须删除：

- `LiveRunStreamBrokerProvider::PostgresNotify`；
- `PostgresLiveRunStreamBroker`、options、channel helper 和 wire codec；
- `src/runtime::postgres_run_stream_broker` re-export；
- Run Stream readiness 的临时 PostgreSQL listener probe；
- Run Stream frame 的 `SELECT pg_notify`；
- 与上述路径绑定的 tests、配置和文档。

不得误删：

- WorkCoordinator 的 PostgreSQL notification hint；
- public durable event 的 commit-after notification/poll；
- Provider/MCP 管理投影的必要 listener；
- PostgreSQL durable repository、terminal store 或 ordinary SQL；
- scheduler safety poll。

## 9. 内部消息协议

### 9.1 `live-run-stream-bus/v1`

NATS 使用一个 backend-neutral、closed、strict JSON envelope。内部协议名为
`live-run-stream-bus/v1`，与公共 `run-stream/v1` 分离。

闭合 kind：

```text
publication
gap
seal
```

示意：

```json
{
  "schema_version": 1,
  "kind": "publication",
  "source": {
    "source_kind": "output_item",
    "run_id": "run_...",
    "activation_id": "activation_...",
    "attempt_no": 1,
    "model_call_no": 1,
    "item_id": "item_...",
    "output_index": 0
  },
  "local_sequence": 3,
  "payload": {
    "type": "output_text_delta",
    "delta": "safe public text"
  }
}
```

Wire 要求：

1. UTF-8 JSON，`deny_unknown_fields`；
2. `schema_version` 精确等于 1，未知版本 fail closed；
3. encode 前和 decode 后都执行现有 identity/payload invariant validation；
4. encoded bytes 不超过 `max_frame_bytes`；
5. decoder 以订阅时的 expected `RunId` 验证 body 中的 `run_id`；
6. mismatch、invalid enum、invalid sequence、invalid gap union、oversize 和 trailing data 全部丢弃并计数；
7. wire 类型不得派生会打印 payload 正文的 unrestricted `Debug`；
8. codec fixture 必须覆盖 publication、known gap、unknown-tail gap、completed/incomplete seal；
9. 不在 envelope 中加入 durable event ID、SSE `sequence_number`、NATS offset 或 terminal snapshot；
10. 不依赖 JSON object field order作为语义。

现有 PostgreSQL wire 可作为字段迁移输入，但目标 codec 必须改名并脱离 PostgreSQL 常量、8 KiB 限制和
channel 规则；不能把 `PostgresLiveRunStreamWireV3` 直接当作永久 NATS contract 名称。

### 9.2 Subject

每个 Run 使用：

```text
insight.<namespace>.run_stream.v1.<run_key>
```

其中：

```text
run_key = lowercase_hex(sha256(canonical_run_id_bytes))
```

约束：

- subject 不包含原始 Run ID、tenant ID、Agent ID、Provider ID、MCP server ID 或用户正文；
- SHA-256 使用完整 64 个十六进制字符，不截断；
- publisher 只能发布 fully-qualified subject，不发布 wildcard；
- subscriber 只订阅一个 expected Run subject；
- ordinary Runtime credential 的 publish/subscribe ACL 只允许
  `insight.<namespace>.run_stream.v1.*`；
- 如果 NATS cluster 为多个 installation 共用，应使用独立 NATS Account；namespace 不是 Account 隔离的
  替代品；
- 日志只记录 subject hash 的短诊断指纹，不能记录 body 或完整 subject + identity 组合。

### 9.3 不使用 queue group

普通 NATS subject subscription 是 1:N fan-out；同一 subject 的每个普通 subscriber 都收到一份消息。
queue group 则在组内随机选择一个 subscriber。Run Stream 需要前者。

实现和配置层均不得暴露 `queue_group` 字段。测试必须证明：

- Runtime A 和 B 同时普通订阅同一 Run 时，两者都收到 publication；
- 代码库不存在对 Run Stream subject 的 `queue_subscribe`；
- NATS ACL 不授予或鼓励 queue permission 作为扩容方案。

## 10. NATS connection、订阅与发布

### 10.1 一个 Runtime 一个 connection

每个 `NatsCoreLiveRunStreamBroker` 生命周期内只构造一个 active NATS `Client`。所有 clone 必须复用
该 client 的同一底层 connection。

允许的额外连接只有 qualification/monitoring 工具，不能由每条 SSE 创建。Runtime 指标必须能证明：

```text
nats data connections per Runtime = 1
```

重连是同一 logical client lifecycle，不得每次重连遗留旧 pump、subscription 或 socket。

### 10.2 Attached subscription barrier

Attached Run 的严格顺序：

1. 生成/确定 `RunId`；
2. 计算 subject；
3. 在共享 client 上创建普通 subscription；
4. 执行 bounded flush，等待 server PONG，证明 SUB 已进入 server interest graph；
5. 将 subscription 注册到本地 per-Run inbound queue；
6. 才允许 durable Run admission/creation 对 Worker 可见；
7. SSE 发送 `run.lifecycle.created`，随后按既有状态机投递 running/live/terminal。

若第 3～5 步失败或超时：

- 返回 `503 RUN_STREAM_BUS_UNAVAILABLE`；
- 不创建 durable Run；
- 不消耗 active Run slot；
- 不返回一个随后必然缺失首批 frame 的 200 SSE。

幂等 admission 必须保持既有语义；订阅失败不能留下一个只有 idempotency key、没有可返回 Run 的半成品。

### 10.3 Dynamic interest

每个本地 Attached Run 使用一个独立 subject subscription；不使用 installation-wide `>` wildcard。
这样只有实际持有该 SSE 的 Runtime 接收该 Run 的 frame，不让每个 Runtime 处理所有 Run 的正文。

subscription 在以下任一条件成立后 unsubscribe 并释放：

- terminal frame 已写出并 EOF；
- `run.stream.error` 已写出并 EOF；
- SSE disconnect/cancel path 完成；
- Attached admission 回滚；
- Runtime shutdown；
- `close_run` 确认该本地 registration 可关闭。

同一 Runtime 对同一 Run 的第二个 subscriber 继续返回
`LIVE_RUN_STREAM_SUBSCRIBER_EXISTS` 类错误，不在本规范扩大公共 fan-out。

### 10.4 Publication pump

`publish()` 与 `seal()` 只执行：

```text
validate -> reserve bounded memory -> enqueue -> return outcome
```

独立 pump 执行 encode 和 NATS async publish。pump 必须满足：

- per-Run body/control 隔离；
- control queue 为 gap/seal 保留容量；
- 一个高流量 Run 不能无限占用 shared transport pending budget；
- ready Run 使用 bounded burst + round-robin 或等价公平调度；
- NATS client pending messages/bytes 同时受全局上限约束；
- 网络 await 不发生在 durable Worker task 的 publication 调用栈；
- publish error 记录安全 error code，并按 identity/sequence 形成之后可检测的缺口；
- 不因一次 publish error自动重试同一 frame，避免 ambiguous send 产生 application duplicate；
- 不逐帧调用 `flush()`；
- 不使用 request/reply 或 JetStream publish ack；
- shutdown 可以在 `drain_timeout` 内 best-effort drain，超时后丢弃 live body 并继续进程关闭。

### 10.5 Ordering 与 dedupe

实现不依赖跨 publisher 的全局 NATS 顺序。唯一可依赖的业务顺序仍是：

```text
(source identity, local_sequence)
```

subscriber 必须：

- 对同一 source 的重复 sequence 幂等忽略；
- 对向前跳跃的 output item sequence 产生 known gap；
- 对失去 seal 的尾部由 durable terminal manifest 产生 unknown-tail gap；
- 对不同 source 的消息按实际到达顺序交给 dispatcher；
- 由 dispatcher 分配严格单调的 SSE connection sequence；
- 不把 NATS reconnect、server ID 或 subscription ID 暴露给客户端。

## 11. 丢失、背压与 terminal barrier

### 11.1 Core NATS 语义

Core NATS 是 ephemeral at-most-once：只向发布时在线且已有 interest 的 subscriber 发送，不保存离线
期间消息，也不提供 acknowledgement 或 replay。本规范主动采用该语义，不把它包装成更强保证。

可能丢失 frame 的位置包括：

- Worker 本地 body queue 满；
- Runtime-wide pending message/byte budget 满；
- NATS client 断连或 reconnect buffer 丢弃；
- subscriber 未完成 subscription barrier；
- client/server 检测 slow consumer 后 drop/disconnect；
- inbound per-Run body queue 满；
- SSE connection write timeout 或断开。

### 11.2 Drop policy

闭合处理：

| 类型 | overload 行为 | 对 durable Run 的影响 |
|---|---|---|
| output publication | 丢弃并形成 known/unknown-tail gap evidence | 无 |
| tool started/progress/completed live observation | 可丢弃并计数；terminal tool result 校准 | 无 |
| retrieval live observation | 可丢弃并计数；terminal retrieval 校准 | 无 |
| gap | 使用 control reserve；仍失败则 terminal barrier 重建 | 无 |
| seal | 使用 control reserve；仍失败则 terminal manifest 产生 unknown tail | 无 |
| durable terminal snapshot | 不走该总线，不允许按本表丢弃 | authoritative |

`run.stream.gap` 仍只表达 provisional output item，不扩展为 tool/retrieval/NATS transport 通用错误。

### 11.3 Terminal path

严格顺序保持：

1. durable terminal transaction 原子提交 Run terminal 和 canonical snapshot；
2. Attached dispatcher 从 durable terminal path 获得 terminal signal/snapshot；
3. dispatcher 在 `terminal_barrier_timeout` 内等待已知 seal watermark；
4. 对缺失 range 先发送 known gap；对 incomplete/unsealed tail 发送 unknown-tail gap；
5. 发送 durable terminal Run frame；
6. 立即 EOF。

NATS 不可用、subject 无 interest 或 publish 失败都不能阻止第 1 步。terminal barrier 不能因为 NATS
断线无限等待，也不能仅以“NATS queue 为空”判断完整。

### 11.4 SSE 不逐事件持久化

本规范不增加 live-event table、outbox、JetStream stream 或 Redis history。数据库仍只保存既有业务
authority 和 terminal snapshot。特别是：

- delta 不保存；
- tool progress 不保存为 Run Stream replay；
- gap 不保存；
- NATS message 不镜像到 PostgreSQL；
- SSE keepalive 不保存；
- connection sequence 不保存。

## 12. 故障、重连与 readiness

### 12.1 Startup

`nats_core` 启动必须在 `connect_timeout` 内完成：

1. DNS/TCP；
2. TLS handshake 与 server identity verification；
3. credentials authentication；
4. server INFO 校验，包括 `max_payload`；
5. PING/PONG flush；
6. event callback 和 bounded capacity 安装。

任何一步失败时 Runtime 启动 fail closed。不能先报告 ready，再后台无限等待第一次连接。

### 12.2 运行中断连

连接建立后发生断连：

- `run_stream_bus_ready` 立即变为 false；
- overall readiness 对包含 Attached endpoint 的 Runtime 变为 false；
- 新 Attached admission 返回 503，且不创建 Run；
- 已开始的 durable Run、Operation 和 terminal commit 继续；
- 已存在 SSE 继续 keepalive、等待 reconnect 或 durable terminal，不因一次 bus disconnect 直接把 Run
  标记 failed；
- publisher 只在本地有界预算内暂存，超出后按第 11 节 drop；
- client 按 bounded jitter backoff 无限重连；
- reconnect 后恢复所有仍活动的 per-Run subscriptions，并 flush 后恢复 bus readiness；
- 离线期间不 replay，后续 sequence/seal/terminal barrier 显式暴露缺口。

若 NATS 永久不可恢复，已有 Run 仍必须能完成 durable terminal，客户端可通过 SSE terminal 或
`GET /v1/runs/{run_id}` 获得权威结果。若 HTTP connection 自身先失败，则按既有 Attached cancel intent
合同处理。

### 12.3 Slow consumer

NATS client event callback 必须处理并统计 slow consumer。subscriber task 不得把 HTTP backpressure
反向传播到共享 NATS connection：

- NATS message 先快速 validate/decode；
- 尝试非阻塞写入 per-Run RunQueue；
- queue 满时执行 drop/gap policy；
- 不等待 SSE socket 可写；
- server disconnect 后走统一 reconnect；
- 不通过无限增大 subscriber buffer 隐藏问题。

### 12.4 Readiness 与 liveness 分离

- liveness 只证明进程事件循环仍工作；NATS 暂时断连不应触发 crash loop；
- readiness 证明当前可以安全接纳新的 Attached Run；
- `in_memory` readiness 由本地 broker accepting 状态决定；
- `nats_core` readiness 需要 authenticated connection、成功 flush 和 subscription subsystem 可用；
- detached API 是否继续接流量由部署路由决定，但服务内部不得让 bus readiness 失败改写 durable
  execution semantics。

## 13. 安全、隐私与多租户

### 13.1 Payload 边界

只有已经通过现有 public projection/safety contract 的 `LiveRunStreamPayload` 可以 encode。禁止进入 NATS：

- Provider 原始 request/response body；
- reasoning、chain-of-thought 或未公开 refusal；
- secret value、OAuth token、MCP credential、NATS credential；
- 未经过 public projection 的 MCP/tool raw result；
- internal prompt/message materialization；
- database URL、artifact encryption key 或 operator token。

### 13.2 NATS 权限

生产 NATS credential 至少满足：

```text
publish allow:   insight.<namespace>.run_stream.v1.*
subscribe allow: insight.<namespace>.run_stream.v1.*
deny:            other installation/application subjects
```

若未来拆分 role：

- Worker-only credential 只授予 publish；
- API/SSE-only credential 只授予 subscribe；
- combined Runtime 才同时授予两者。

不得授予普通 Runtime 全局 `>` 权限。NATS monitoring/admin credential 与 Runtime data credential 分离。

### 13.3 TLS 与 secret

- production 强制 TLS server verification；
- private CA 通过只读 Secret volume 提供；
- 可选 mTLS cert/key 通过只读 Secret volume 提供；
- NATS user credentials 通过 named environment Secret 提供；
- server URL 禁止 inline user/password/token；
- Secret rotation 通过 homogeneous rollout 或 client reconnect 流程完成；
- 错误只返回稳定 code，例如 `RUN_STREAM_BUS_UNAVAILABLE`、`RUN_STREAM_BUS_AUTH_FAILED`、
  `RUN_STREAM_BUS_CONFIG_INVALID`；
- 日志/trace/metric 不记录 credential、正文、完整 Run identity 或 subject。

### 13.4 Namespace 隔离

`namespace` 必须在一个 NATS Account 内唯一。部署 smoke test 要求 publisher/subscriber 使用同一
namespace 和 codec version。若共享 NATS cluster 承载多个 installation，生产建议每 installation
使用独立 Account；仅依赖字符串 prefix 不能防止被错误 ACL 授权的其他客户端订阅。

## 14. 可观测性

必须增加低基数指标；所有 label 使用闭合 enum，禁止 Run ID、subject、tenant、Agent、Provider 或
error message：

```text
run_stream_bus_ready{backend}
run_stream_bus_connections{backend,state}
run_stream_bus_reconnect_total{backend,outcome}
run_stream_bus_active_subscriptions{backend}
run_stream_bus_tasks{backend,state}
run_stream_bus_publish_total{backend,event_class,outcome}
run_stream_bus_publish_latency_seconds{backend}
run_stream_bus_pending_messages{backend,queue_class}
run_stream_bus_pending_bytes{backend,queue_class}
run_stream_bus_dropped_total{backend,event_class,reason}
run_stream_bus_gap_total{backend,gap_kind,reason}
run_stream_bus_decode_error_total{backend,reason}
run_stream_bus_slow_consumer_total{backend,scope}
run_stream_bus_subscription_ready_seconds{backend}
run_stream_bus_terminal_barrier_seconds{backend,outcome}
```

`event_class` 只允许 `output`、`tool`、`retrieval`、`gap`、`seal`；`outcome/reason` 使用 closed enum。

结构化日志只记录：

```text
backend
runtime_instance_fingerprint
event_class
safe error code
count / bytes / duration
connection state transition
```

不得记录：

- NATS message payload；
- credential contents；
- 完整 subject；
- output delta；
- tool content；
- raw decode error body；
- Run ID 与 subject hash 的可关联组合。

运维 dashboard 至少展示：

- 当前 readiness/connection state；
- reconnect 与 slow consumer rate；
- active subscription 和 pending budget；
- drop/gap rate；
- terminal barrier timeout rate；
- PostgreSQL pool acquire latency 与 connection usage；
- NATS server connection、in/out message、slow consumer 指标。

## 15. PostgreSQL 压力与并发边界

### 15.1 必须消除的成本

当 backend 为 `in_memory` 或 `nats_core` 时，下列断言必须成立：

```text
LiveRunStreamBroker::subscribe: 0 PgPool acquire
LiveRunStreamBroker::publish:   0 SQL
LiveRunStreamBroker::seal:      0 SQL
每条 SSE 生命周期:             0 Run-Stream-specific PgListener
每个 live frame:               0 Run-Stream-specific pg_notify
```

这不表示整个 SSE 生命周期完全不读数据库。Run admission、lifecycle signal、terminal snapshot 获取、
取消意图和 `GET Run` 仍使用既有 durable path，但每次操作必须是短事务/短查询，不能为等待实时 frame
长期持有 pool connection。

### 15.2 理论收益

假设数据库 pool 为 10，当前同时存在 10 条各持有 listener 的 SSE，普通 claim/commit 可能等待空闲
connection。切换后 Run Stream listener 数为零，pool 可以用于短事务。若每秒产生 1,000 个 delta，
当前 broker 可能额外产生约 1,000 次 `pg_notify` query/s；切换后该类 SQL 为零。

这是一项资源模型推导，不是容量承诺。实际收益受 durable SQL、Provider latency、Runtime CPU、NATS
RTT、frame size 和客户端读取速度影响。

### 15.3 不应采取的替代优化

- 不能仅把 PostgreSQL pool 从 10 调到 100；这会把连接压力推给 PostgreSQL；
- 不能让每条 SSE 使用独立的非 pool PostgreSQL connection；总连接仍按 SSE 增长；
- 不能批量拼接多个 delta 后继续使用 `pg_notify` 作为长期 bus；仍有 payload ceiling 和 DB contention；
- 不能通过保存每条 delta 来换取 replay；这会引入写放大和新的 retention authority；
- 不能把 operation permit 调大当作 live transport 修复。

## 16. 测试与资格验收

### 16.1 Config tests

必须覆盖：

- 新 tagged backend object 正常解析；
- `in_process`、`postgres_notify` 和字符串 `nats_core` 被拒绝；
- strict unknown fields；
- topology/backend 矩阵；
- SQLite/distributed 拒绝；
- server URL userinfo/query/fragment 拒绝；
- production missing credentials/TLS 拒绝；
- namespace、duration、pending limit、frame limit 边界；
- Helm render 对 `replicaCount`、topology、Secret、TLS 的 fail-closed 校验；
- ConfigMap 不包含 secret value。

### 16.2 Backend contract suite

同一套 contract tests 必须对 `in_memory` 与 `nats_core` 运行：

- one subscriber per Run；
- publish before/after subscribe；
- strict per-item ordering；
- duplicate sequence dedupe；
- known gap；
- unknown-tail gap；
- completed/incomplete seal；
- body queue overflow；
- control reserve；
- oversize publication；
- run observation drop；
- close/drop subscriber；
- late publisher；
- shutdown drain timeout；
- terminal barrier 后 terminal + EOF；
- Worker durable result 不受 publish failure 影响。

### 16.3 Real NATS integration

测试使用固定版本的真实 `nats-server`，不能用只实现 happy path 的 mock 替代。至少覆盖：

1. Runtime A broker publish，Runtime B broker subscribe；
2. A/B 同时普通订阅同一 subject，两者均收到，证明未使用 queue group；
3. 单 Runtime 50 个 active subscription 仍只有一个 NATS client connection；
4. subscription flush barrier 前不 admission；
5. NATS server restart，client 重连并恢复 active subscriptions；
6. 断线期间 message 不 replay，后续 gap/terminal calibration 正确；
7. client-side pending overflow；
8. server/client slow consumer 事件；
9. wrong namespace、wrong run body、wrong schema version、oversize、malformed JSON 被拒绝；
10. subject ACL 拒绝跨 namespace publish/subscribe；
11. TLS server verification、bad CA、bad credential；
12. credentials、payload 和 raw subject 不出现在 test-captured logs。

Rust adapter 是平台内部 transport，不增加 Go/TypeScript Run Stream client；本规范的 interoperability
边界是 Rust adapter 与真实 NATS server protocol。

### 16.4 Database regression

PostgreSQL qualification profile：

- pool `max_connections = 10`；
- 50 个 Attached active Run；
- 每个 Run 产生可控 output/tool/retrieval observation；
- 分别运行 `in_memory` 和 `nats_core`；
- 收集 `pg_stat_activity`、`pg_stat_statements`、pool acquire latency 和 NATS metrics。

通过标准：

1. Run Stream-specific listener connection 为 0；
2. Run Stream-specific `pg_notify` SQL invocation 为 0；
3. 50 条 SSE 不把 database pool active connection 基线提高 50；
4. DB pool acquire p95 不高于 durable runtime 既有 100ms gate；
5. 无 deadlock、pool timeout、OOM 或 workload-induced restart；
6. terminal success 100%，live drop 必须由 metric/gap/terminal calibration 解释；
7. NATS data connection 每 Runtime 为 1；
8. 所有 terminal snapshot hash 与 `GET Run` 结果一致。

### 16.5 负载与稳定性

至少运行：

- 20 轮、每轮 50 个 Attached 短 Run burst；
- 30 分钟 50 active Run 混合 output/tool/retrieval 流；
- 2 小时 NATS qualification soak；
- soak 中至少一次 NATS rolling restart、一次 Runtime subscriber restart、一次 credential/ACL negative
  test 和一次 slow-client injection。

通过标准：

- durable terminal success 不低于既有 workload baseline；
- 无 Run 因 live bus publish failure 被标记 failed；
- reconnect 后 subscription、task、memory、connection 数不持续增长；
- 最后 30 分钟 RSS、pending bytes、active subscriptions 和 task count 无泄漏趋势；
- 可写 SSE 的最后一帧始终为 Run terminal 或 `run.stream.error`；
- terminal 后立即 EOF；
- drop/gap 数与注入故障一致，无静默 terminal 截断；
- PostgreSQL Run Stream query/connection 成本保持为零。

## 17. 实施阶段

### Phase 0：固定合同与基线

1. 保存当前 PostgreSQL listener/query/connection 基线；
2. 固定 backend contract fixtures；
3. 固定公共 `run-stream/v1` schema/baseline，证明本轮不改 wire；
4. 增加本文配置和 metric 名称的 negative tests。

### Phase 1：配置与 composition

1. 引入 strict `RunStreamBrokerYaml` tagged union；
2. 增加 `topology` 与 `NatsCoreRunStreamConfig`；
3. `in_process` clean-cut 为 `in_memory`；
4. 拆分 repository 与 bus 初始化；
5. 把 production admission gate 改为 topology-aware capability；
6. 更新 platform config、quickstart、Helm 和 config tests。

### Phase 2：Backend-neutral wire 与 queue

1. 把 PostgreSQL-specific wire 收敛为 `live-run-stream-bus/v1`；
2. 保持 closed serde、identity validation、byte bound；
3. 建立共享 backend contract suite；
4. 保持 `RunQueue` 为 ordering/gap/seal 单一实现；
5. 补齐全局 pending message/byte reservation 与公平调度。

### Phase 3：NATS Core adapter

1. 引入最小 feature set 的 `async-nats`；
2. 实现单 shared connection、event callback、readiness、reconnect 和 shutdown；
3. 实现 subject、dynamic subscription、flush-before-admission；
4. 实现非阻塞 publication pump、control reserve 和 drop accounting；
5. 实现 TLS、credentials、ACL-oriented config；
6. 通过真实 NATS integration suite。

### Phase 4：删除 PostgreSQL Run Stream transport

1. 删除 `PostgresLiveRunStreamBroker` 和 re-export；
2. 删除 per-Run PgListener 与 per-frame `pg_notify`；
3. 删除旧 provider enum/config/docs/tests；
4. 证明 scheduler/public-event 等其他 PostgreSQL notification path 未受影响；
5. 运行 workspace hygiene search，确保没有遗留 Run Stream `postgres_notify`。

### Phase 5：运维、文档与资格

1. 增加 metrics/dashboard/runbook；
2. 增加 NATS Secret/TLS/ACL 部署示例；
3. 增加 NATS outage、slow consumer、namespace mismatch 排障；
4. 运行数据库 regression、50 Run profile 和 2 小时 soak；
5. 保存 commit SHA、config、NATS/PostgreSQL 版本、raw metrics 和 qualification report；
6. 同步 `docs/current` 后归档本文。

## 18. Rollout、混合版本与 rollback

### 18.1 Clean cutover

没有旧客户端需要兼容，因此一个 release cohort 内必须同时切换：

- binary；
- platform YAML；
- Helm values/template；
- readiness/smoke tests；
- NATS Secret/TLS/ACL（若选择 `nats_core`）；
- runbook 与 dashboard。

当前单 Runtime Helm 默认切到 `in_memory`，不要求先部署 NATS。需要验证未来 shared topology 的环境
显式选择 `nats_core`。

### 18.2 混合版本

旧 Runtime 发布 PostgreSQL NOTIFY，新 Runtime 订阅 NATS 时互不可见，因此同一 Attached Run cohort
不得混跑两个 transport。当前 `replicaCount: 1` + `Recreate` 天然避免该问题。

未来若采用 blue/green：

1. 暂停新的 Attached admission；
2. drain 或允许现有 SSE 通过 terminal/GET 收敛；
3. 整体切换 Runtime cohort 与配置；
4. readiness 通过后恢复 Attached admission。

不实现 PostgreSQL + NATS 双发，因为双发会增加 duplicate/gap 状态、继续给数据库施压，并把临时迁移
代码变成长期第二 authority。

### 18.3 Rollback

本规范无 durable schema migration，rollback 不需要转换 Run snapshot。若回滚旧 binary：

- 必须同时恢复旧配置；
- in-flight live delta 可以丢失，durable terminal 和 `GET Run` 仍有效；
- 不允许旧新 Runtime 同时服务 Attached admission；
- NATS infrastructure 可以保留但不会成为 durable orphan 数据源，因为 Core NATS 不存储消息。

## 19. 风险与控制

| 风险 | 控制 |
|---|---|
| 把 NATS 当 durable queue | Core-only、terminal 不入总线、文档/类型/测试固定 authority |
| `in_memory` 被误用于多 Runtime | topology/backend strict matrix、Helm fail-closed、admission capability gate |
| 使用 queue group 导致只有一个 SSE Runtime 收到 | 不暴露配置、普通 subscription、双 subscriber integration test |
| combined Runtime 启用 `no_echo` 后收不到自身 frame | 配置不可用、adapter 固定 echo、self-publication test |
| NATS 断连导致 live frame 丢失 | at-most-once 明示、bounded drop、sequence/gap/seal/terminal calibration |
| slow SSE 拖垮共享 NATS connection | 快速 drain 到本地有界队列、非阻塞 drop、slow consumer metric |
| 一个 Run 填满全局 NATS buffer | per-Run queue、global bytes/messages、control reserve、公平调度 |
| subject 泄漏业务 identity | full SHA-256 run key、不含 tenant/Agent/正文、TLS、Account/ACL |
| namespace 配错导致跨 Runtime 不通 | strict config、同 cohort values、startup/smoke cross-runtime probe |
| 凭据进入 ConfigMap/日志 | `credentials_env`、Secret mount、redacted types、log-capture negative test |
| NATS server payload 小于平台 frame | startup INFO `max_payload` 校验、oversize fail closed |
| 删除 PG broker 误删 scheduler notification | 模块边界、targeted tests、workspace search、durable scheduler soak |
| DB 压力下降被误解为 permits 可无限增大 | permit/config 不变、单独容量资格、明确非目标 |
| mixed transport cohort 静默丢帧 | Recreate/homogeneous rollout、暂停 Attached admission、禁止双发 |
| NATS dependency 增加运维成本 | 当前单 Runtime 默认 in-memory；shared topology 才引入 external NATS |

## 20. 被拒绝的方案

### 20.1 继续用 PostgreSQL NOTIFY，仅增大 pool

拒绝。它不能消除 per-SSE listener 和 per-frame SQL，只把压力从 pool 转移到 PostgreSQL 总连接、CPU 和
事务调度。

### 20.2 每个 SSE 一个独立 NATS connection

拒绝。NATS 支持一个 connection 上的多 subscription；per-SSE connection 会复制当前 PostgreSQL
线性连接问题，并增加 TLS/auth/heartbeat/reconnect 成本。

### 20.3 NATS queue group

拒绝。queue group 是竞争消费，组内每条消息只交给一个成员；Run Stream 需要所有持有该 Run SSE 的
Runtime 收到消息。

### 20.4 JetStream

拒绝。现有协议不支持 replay/offset/ack，terminal 已有 durable authority。JetStream 会引入 stream
retention、consumer lifecycle、ack/redelivery、duplicate 和清理合同，而没有当前需求。

### 20.5 Redis Pub/Sub 同时交付

拒绝本轮同时实现。它与 Core NATS 都是 at-most-once，但需要另一套连接、ACL、cluster/slow-consumer
qualification。backend port 为未来保留扩展点即可。

### 20.6 NATS 保存 terminal snapshot

拒绝。Core NATS 不保存消息；即使改用 JetStream，复制 terminal 也会制造 PostgreSQL 与消息系统的双
authority 和双写失败窗口。

### 20.7 installation-wide wildcard subscription

拒绝作为普通 Runtime path。它会让每个 Runtime 收到整个 installation 的所有 live 正文，放大网络、
decode 和内存成本。动态 per-Run interest 更符合 NATS subject routing。

### 20.8 PostgreSQL 与 NATS 双发迁移

拒绝。没有兼容客户端要求，当前部署是单 Runtime Recreate；双发只会保留数据库压力并扩大 correctness
surface。

## 21. 完成定义

只有同时满足以下条件，本文才能标记 Implemented / Verified 并移入 `docs/archive/specs`：

1. 新 strict config、topology/backend matrix、Helm/Secret/TLS 校验完成；
2. `in_memory` 成为当前单 Runtime 默认；
3. `nats_core` 使用一个 shared connection，并通过真实 NATS server integration；
4. subscription 在 Attached durable admission 前完成 flush barrier；
5. NATS publication 不发生在 durable Worker 网络 await 调用栈；
6. per-Run queue、global message/byte bound、control reserve 和公平调度完成；
7. reconnect、slow consumer、drop、gap、seal、terminal barrier 行为由故障测试证明；
8. queue group 与 `no_echo` 不可配置且有 negative/behavior test；
9. subject 不包含 raw identity，production TLS/credentials/ACL 通过测试；
10. `PostgresLiveRunStreamBroker`、`postgres_notify` 配置和 Run Stream `pg_notify` SQL 完全删除；
11. scheduler/public-event 等其他 durable notification/recovery path 无回归；
12. 50 Attached Run 下 Run Stream-specific PostgreSQL listener 和 publish query 均为零；
13. NATS data connection 数保持每 Runtime 一个，disconnect/reconnect 后不泄漏；
14. durable terminal success、snapshot hash、terminal calibration 和 EOF conformance 全部通过；
15. 公共 `run-stream/v1` schema/baseline 无变化；
16. 2 小时 qualification soak 通过并保存可复验证据；
17. `docs/current`、configuration、deployment、operations 和 troubleshooting 文档同步；
18. 活动规范 README 更新，本文归档。

## 22. 外部语义参考

本文只依赖 NATS 官方文档中与内部 transport 决策直接相关的语义：

- [Core NATS 是无存储、无 ack、at-most-once 的 ephemeral Pub/Sub](https://docs.nats.io/learn/core-nats/)；
- [NATS subject 与 wildcard/interest routing](https://docs.nats.io/concepts/subjects)；
- [Queue group 在组内随机选择一个 subscriber](https://docs.nats.io/nats-concepts/core-nats/queue)；
- [Slow consumer 可能在 client 丢消息或被 server 断开](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)；
- [NATS 支持 per-user subject publish/subscribe 权限](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization)；
- [NATS TLS 加密与 server/client identity](https://docs.nats.io/using-nats/developer/connecting/tls)；
- [NATS clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering)。

这些参考解释 NATS transport 的能力，不提升平台公共 Run Stream 的 delivery guarantee。
