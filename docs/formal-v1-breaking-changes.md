# 正式 V1 破坏性变更

仓库原有的 `/v1`、Agent YAML、事件和历史结构都是未发布的原型，不是稳定合同。本次重写不提供兼容层，而是把第一个稳定合同直接命名为 HTTP/DSL/Event `V1`。这样可以避免为了兼容一个从未正式发布的原型而永久保留 `v2` 和双运行时分支。

迁移原则：重新编译 Agent、切换客户端合同、使用全新的历史数据库。旧客户端和旧 YAML 会明确失败，不会被静默解释成新语义。

## 变更总表

fork/join 引入的 DSL、调度和事件接口变化是有意的；本次 live-only SSE 基线还会删除公开事件恢复路由。删除原因不是事件不再持久化，而是公开补发会让重连潮直接竞争数据库连接与 journal 写入，并且既有 Run 的纯实时订阅无法补齐创建到订阅之间的事件缺口。

| 原型 | 正式 V1 | 为什么改 | 最小迁移示例 |
|---|---|---|---|
| `steps: [...]` 隐式顺序，混合 `goto/end` | `entry` + `nodes` DAG | 顺序、跳转和依赖都是图语义；启动期可统一检查缺边、环、不可达节点和前驱引用 | `steps: [{id: answer,...}]` → `entry: answer; nodes: {answer: {...}}` |
| `prompt` / `text` step | `core.template` | 模板渲染与模型调用、内容发布是不同职责 | `type: text; prompt: "{{ input.x }}"` → `type: core.template; config.value: "{{ input.x }}"` |
| 通用 `llm` step 和 Agent 顶层 model | 命名模型资源 + `core.chat` | chat、embedding、speech、rerank 需要不同合同；不应塞进一个可选字段集合 | `type: llm` → `type: core.chat; config.model: general_chat` |
| `tool` 与 `code` 两套调用边界 | `core.action` + `ActionRegistry` | 两者本质上都是严格 JSON 能力调用；统一 Schema、取消和错误语义 | `type: code; handler: example.text_metrics` → `type: core.action; config.action: example.text_metrics` |
| 运行时编译 condition/CEL | `core.condition` 在启动时预编译 | 平台自有工作流的表达式错误不应延迟到用户请求 | `cases[].goto` → `config.cases[].next`，上下文从 `steps.x` 改为 `nodes.x` |
| 最后一个 step 隐式成为结果 | 必需的 `core.end` success/failure 终端 | 主流程和 Fork 分支都必须显式返回成功或 authored workflow failure | 添加 `result: {type: core.end, config: {outcome: success, content: {template: ...}, format: markdown}}`；分支使用自己的 End，不再指向 Join |
| 节点 `stream: true/false` | 公共 envelope 的 `emit: content/none` | 供应商是否使用流传输与内容是否公开给客户端是两件事 | `stream: true` → `emit: content` |
| Agent `public/default_public/exposure` | 平台 `auth.mode` + `agents.enabled` | 工作流元数据不能决定部署安全；鉴权和启用策略必须分离 | 删除 `public`，在平台配置中显式写 `auth.mode` 和 `agents.enabled` |
| SSE 连接拥有所有 Run | attached 与 detached 两种创建接口 | 交互式断开取消和后台执行是两种明确意图 | `/runs/stream` 创建 attached；`/runs` 创建 detached 并返回 202 |
| 传输消费时才写历史 | 独立 `EventHub` + bounded journal + live-only Attached SSE | 持久化不能依赖 SSE 消费者；公开补发会让重连潮直接竞争数据库连接和 journal 写入 | Attached 使用 `/runs/stream`；Detached 使用 `/runs` 后轮询 Run 资源 |
| 数字 API/event code | 稳定字符串码 | 字符串自描述，避免维护未文档化的数字区间 | `"code":0` → `"code":"OK"`；错误如 `RUN_NOT_FOUND` |
| 原型 SQLite/PostgreSQL 表 | 全新 formal V1 `runs/run_events/node_outputs` | 新生命周期、attachment、序号和终态事务约束无法安全套用旧表 | 删除本地旧 SQLite 文件或重建开发 PostgreSQL volume 后启动 |

## DSL 迁移

原型：

```yaml
id: demo
model: {provider: provider_a, type: llm, model: model_a}
input:
  schema: {type: object}
steps:
  - id: answer
    type: llm
    prompt: "{{ input.question }}"
    stream: true
```

正式 V1：

```yaml
version: 1
id: demo
name: Demo
input:
  schema: {type: object}
entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    emit: content
    config:
      model: general_chat
      messages:
        - role: user
          content: "{{ input.question }}"
  result:
    type: core.end
    config:
      outcome: success
      content: {template: "{{ nodes.answer.output.text }}"}
      format: markdown
```

Prompt 文件继续相对 Agent 目录声明，但消息通过 `{template_ref: name}` 引用。所有跨节点模板和 CEL 引用使用 `nodes.<node_id>.output`；编译器只允许引用所有到达路径上都已完成的前驱。

`core.end` 是严格联合类型并禁止 `next`：success 至少提供 `content` 或 `data`，failure 只提供静态 `WORKFLOW_...` code 与静态单行 message。主流程 End 决定 Run 成功或 workflow failure；分支 End 只结算所属分支。failure End 本身仍成功执行，所以事件顺序是 `node.completed(core.end)` 后跟 `branch.failed(kind=workflow)` 或 `run.failed(kind=workflow)`，而不是 `node.failed`。

Fork 分支不再直接进入 Join。每个静态成功路径必须到达分支局部 End；所有分支结算后 `all_settled` Join 才执行。Join 输出的每个失败包含 `error.kind: workflow|node|timeout`，`summary.failures` 分别计数三种来源。即使所有分支失败，Join 也会执行；Join 后的 Condition 显式选择 degraded success 或主流程 failure End。

## HTTP 与事件迁移

正式端点只有：

```text
GET    /health
GET    /health/live
GET    /health/ready
GET    /v1/agents
GET    /v1/agents/{agent_id}
POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs
GET    /v1/runs/{run_id}
DELETE /v1/runs/{run_id}
```

`GET /v1/agents` 与 `GET /v1/agents/{agent_id}` 使用相同的公开 Agent 元数据结构：`id`、`name`、`description`、`version`、`input_schema`。`input_schema` 是编译期通过 Draft 7 策略校验、运行期用于校验两个 Run POST 完整 JSON body 的同一份结构化文档；API 不公开 prompt、节点图、模型或 Action 配置。Schema 变化会进入现有 Agent `version`，不另增 `schema_hash`。

`/health/live` 是不查询 history 或 journal 的公开 liveness；handler 可响应时返回 `200/OK`。`/health/ready` 是公开 readiness，要求 Run admission 开放、journal 健康且有界 history probe 成功；并发请求通过 single-flight 合并，成功与失败最多缓存 250ms。`/health` 保留为 `/health/ready` 的直接兼容别名，两者在所有状态下返回完全相同的状态码和 JSON；失败统一为经清理的 `503/RUNTIME_UNHEALTHY`。三个探针都返回 `Cache-Control: no-store`。关停先关闭 admission 并保持 HTTP 提供探针与查询，运行时 drain 完成或失败后才关闭 HTTP；关停期间的新 Run 返回 `503/RUN_SERVICE_UNAVAILABLE`。新增可选 runtime 字段 `readiness_probe_timeout`、`shutdown_grace_period` 和 `shutdown_hard_deadline`，默认分别为 `2s`、`30s` 和 `35s`，且 hard deadline 必须严格大于 grace period。

原型的 Run 列表/过滤端点和 `X-Caller-Service/X-Tenant-Id/X-User-Id` 元数据不属于正式 V1。原因是执行合同不应提前绑定多租户管理面；后续查询/管理 API 可以在独立授权与分页合同下增加。`X-Request-Id` 保留用于关联请求。

attached POST 在构造 SSE 前完成 JSON 与 Schema 校验，先订阅实时广播再启动 Run，并返回 X-Run-Id；终态事件发送后连接立即结束，非终态连接断开会立即取消 Run。正式 V1 不提供 GET /runs/{run_id}/events、after_seq 或 Last-Event-ID 恢复：公开恢复会让并发重连直接查询事件库，而 live-only 的既有 Run 订阅又无法保证创建到订阅之间无缺口。detached POST 返回 202，客户端通过 GET Run 轮询最终状态；DELETE 是唯一显式取消接口且幂等。

事件的 schema_version 仍为 1，seq 和 SSE id 仍表示单 Run 顺序并用于审计关联，但不再表示可恢复游标。事件继续先持久化再广播；数据库事件历史保留给内部终态恢复和审计。本次 pre-adoption 终端重写会直接改写 Formal V1 initial migration，不提供旧本地数据库的就地升级。

## 配置与安全迁移

- 平台 YAML 和模型 YAML 都必须写 `version: 1`，每层拒绝未知字段。
- `PLATFORM_CONFIG` 一旦设置，目标文件必须存在；相对路径以平台文件目录为基准。
- `auth.mode` 必须明确为 `disabled` 或 `bearer_env`。
- `agents.enabled` 默认空，不再从 Agent YAML 推断公开范围。
- YAML 只保存环境变量名；Bearer token、模型 API key 和 PostgreSQL URL 从环境变量读取，`Debug` 输出脱敏。
- 模型别名取代 provider/type/model 三元组，Agent 不再关心供应商组织方式。
- A8 后 `open_ai_chat.base_url` 默认只接受 HTTPS。既有 HTTP 模型服务必须改为 HTTPS，或显式声明 `transport.plaintext_http: loopback` / `trusted_private`。`trusted_private` 是部署方对私有网络明文链路的风险接受，不是运行时自动内网判定。
- A8 后 Agent 节点 `timeout` 只接受正整数紧跟 `ms`、`s` 或 `m`。把 `1 sec`、`90 seconds`、`1h`、`1s 500ms` 等写法改为 `1s`、`90s`、`60m` 或等价毫秒值。

## Dependency governance: PostgreSQL TLS transport

Remote PostgreSQL history URLs now require `sslmode=verify-full`. This intentionally breaks remote URLs that relied on SQLx's default `prefer` behavior because that mode can fall back to plaintext. Local development may keep exact loopback or Unix socket URLs.

Migration example:

```text
postgres://user:password@database/private
postgres://user:password@database/private?sslmode=verify-full
```

## Dependency governance: Axum 0.8 route syntax

- Phase 3 upgrades the server framework from Axum 0.7 to Axum 0.8.
- Public Formal V1 paths are unchanged, but internal route definitions now use Axum 0.8 `{param}` captures instead of Axum 0.7 `:param` captures.
- This is intentional: relying on old route syntax after the major upgrade would hide framework-compatibility behavior inside the route table and make future route reviews less clear.
- API response envelopes, SSE event payloads, auth behavior, cancellation behavior, and unsupported replay/recovery routes are unchanged.

## Dependency governance: Reqwest 0.13 TLS contract

- Phase 3 upgrades the HTTP client from Reqwest 0.12 to Reqwest 0.13.
- The direct feature selection changes from `rustls-tls` to `rustls` because Reqwest 0.13 changed its TLS feature contract.
- Reqwest default features remain disabled. The platform does not opt into implicit `default-tls`, system proxy behavior, HTTP/2, compression, cookies, SOCKS, HTTP/3, or alternate DNS behavior in this phase.
- Project-built Reqwest clients explicitly call `tls_backend_rustls()` so model and action traffic do not depend on a backend-abstract default TLS alias.
- HTTPS verification uses Reqwest 0.13's rustls/platform verifier path and the runtime environment's trusted roots. Private HTTPS model services should install private CA roots at the host/container layer until a future explicit per-model CA configuration exists.
- Existing `transport.plaintext_http` semantics are unchanged: default HTTPS-only, `loopback` for exact local hosts, and `trusted_private` when the deployment owner accepts a private-network plaintext model hop.

医学示例也不是兼容合同。正式示例使用单个 `image_url` 展示一个有 `vision` 能力的多模态消息；原型的 `images` 数组调用方需要选择一个图片 URL，或通过自定义节点/后续多图内容类型扩展。这个限制只属于当前示例输入，不是核心运行时的医学规则。

## 历史重置

不执行原型表或旧 Formal V1 终端表的就地升级，也不读取原始输入。`core.end` 重写增加显式 `error_kind` 和完整终态字段约束，因此已有本地 Formal V1 数据库必须整体重建，不能只清空 runs 表。

停止所有使用目标 history store 的进程后执行以下一种操作。

SQLite：

```bash
SQLITE_DB=/absolute/path/to/formal_v1.sqlite3
rm -f "$SQLITE_DB" "$SQLITE_DB-wal" "$SQLITE_DB-shm"
```

仓库默认开发配置可直接删除 `data/formal_v1.sqlite3`，quickstart 可删除 `data/quickstart.sqlite3`；两者也应同时删除对应的 `-wal`、`-shm` 文件。

PostgreSQL 本地开发 volume：

```bash
docker compose -f docker-compose.postgres.yml down -v
docker compose -f docker-compose.postgres.yml up -d
```

正式 migration 只位于 `migrations/formal_v1/{sqlite,postgres}`。重启新二进制后由 SQLx 在空数据库上重新应用 initial migration。应用不会自动删除或改写不兼容历史。生产环境如需保留原型审计数据，应先导出到独立只读存储；不要把旧表复制进正式 V1 数据库。

### A0 Action validation error containment

旧版本可能把 Action JSON Schema 校验失败的原始 input/output instance 写入 Run 和事件错误消息。A0 从校验源头改为固定消息：

- `ACTION_INPUT_INVALID` / `action input validation failed`
- `ACTION_OUTPUT_INVALID` / `action output validation failed`

错误码和 HTTP/SSE/Event/Run 数据结构不变；动态 validator 文本不再是兼容合同。由于旧错误文本不可靠区分诊断内容和敏感值，本次升级不迁移或扫描历史 Run，部署前必须显式重置历史。

停止所有使用目标 history store 的进程后执行以下一种操作。

SQLite：

```bash
SQLITE_DB=/absolute/path/to/run_history.sqlite3
rm -f "$SQLITE_DB" "$SQLITE_DB-wal" "$SQLITE_DB-shm"
```

PostgreSQL：

```bash
psql "$RUN_HISTORY_DATABASE_URL" <<'SQL'
BEGIN;
TRUNCATE TABLE node_outputs, run_events, runs;
COMMIT;
SQL
```

不要删除 SQLx migration metadata。应用不会自动删除历史，也不提供 reset API/CLI。删除后的历史不能通过回滚旧二进制恢复；不要把 A0 前的 Run/Event 数据重新导入运行库。

部署顺序：停止服务、重置历史、部署新二进制、启动并完成 migration check、运行 `cargo test --test action_error_containment -- --nocapture --test-threads=1`、检查 `/health/ready`，然后恢复流量。

### A5 Semantic compile-time validation

A5 收紧 Agent DSL 的编译期语义校验：

- fully static `core.action.config.input` 会在 Agent 编译期按注册 Action input schema 校验，失败码为 `ACTION_INPUT_INVALID`，消息固定为 `action input validation failed`；
- 节点 ID 和 fork branch ID 必须匹配 `[A-Za-z_][A-Za-z0-9_]*`；
- 跨节点引用只能使用 `nodes.<node_id>.output`；
- CEL 中的 `nodes["id"].output`、`nodes[id].output`、`nodes.<id>["output"]`、直接访问 `nodes` map 等形式会失败；
- Handlebars/CEL 字符串、注释、raw 文本中的 `nodes.<id>.output` 不再被误识别为图依赖。

这样做的原因是编译期图校验必须知道每个跨节点依赖的确定 node ID。computed/indexed access 依赖运行时值，无法可靠参与 predecessor、parallel branch 和 post-join 校验。静态非法 Action input 已经在部署前可知，延迟到用户请求时失败不符合 fail-before-serving 合同。

迁移方式：

```yaml
# 不再支持
route:
  type: core.condition
  config:
    cases:
      - when: 'nodes["classify"].output.kind == "medical"'
        next: medical
    default: general

# 使用 canonical dotted reference
route:
  type: core.condition
  config:
    cases:
      - when: 'nodes.classify.output.kind == "medical"'
        next: medical
    default: general
```

如果旧 Agent 使用 `some-node` 这类 ID，需要改为 `some_node` 并同步更新所有 `next`、fork branch、join output 和模板/CEL 引用。A5 不需要数据库 migration，也不需要重置 Run 历史；它只影响 Agent 启动编译。

## JSON Schema validator boundary

`CompiledAgent.input_schema` and resource internals now use the project-owned `JsonSchemaValidator` adapter instead of exposing the upstream `jsonschema::JSONSchema` type.

Reason: the platform needs a stable validation contract while upgrading `jsonschema` from 0.18 to 0.47. The adapter fixes the platform default at Draft 7, disables upstream HTTP/file/CLI default features, rejects non-Draft-7 `$schema` values, rejects external `$ref`, and keeps runtime validation errors redacted.

Runtime API behavior is unchanged: invalid run input, Action input/output, and OpenAI model parameters keep the existing public error codes and fixed messages.
